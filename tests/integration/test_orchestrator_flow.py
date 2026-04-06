from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from pydantic import BaseModel

from saga_orchestrator.admin import SagaAdmin
from saga_orchestrator.core import SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.exceptions import (
    ActiveSagaAlreadyExistsError,
    SagaStateError,
)
from saga_orchestrator.domain.models import BaseStep, ExponentialRetry
from saga_orchestrator.domain.models.enums import SagaStatus
from tests.integration.models import IntegrationSagaState


class StartInput(BaseModel):
    value: int


class StartOutput(BaseModel):
    value: int


class NextInput(BaseModel):
    value: int


class NextOutput(BaseModel):
    value: int


class LongOutput(BaseModel):
    endpoint: str


class AddOneStep(BaseStep[StartInput, StartOutput]):
    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)


class FlakyStep(BaseStep[NextInput, NextOutput]):
    def __init__(self) -> None:
        self.calls = 0
        self.compensated = False

    async def execute(self, inp: NextInput) -> NextOutput:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary")
        return NextOutput(value=inp.value + 10)

    async def compensate(self, inp: NextInput, out: NextOutput) -> None:
        self.compensated = True


class FailsOnceStep(BaseStep[StartInput, StartOutput]):
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, inp: StartInput) -> StartOutput:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary")
        return StartOutput(value=inp.value + 1)


@pytest.mark.asyncio
async def test_start_completes_saga(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("simple", builder.build())

    saga_id = await orchestrator.start(
        saga_name="simple",
        initial_data={"value": 1},
        aggregation_id="agg-simple",
    )

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.COMPLETED
    assert len(state.step_history) == 1


@pytest.mark.asyncio
async def test_retry_flow_with_run_due(session_maker):
    second = FailsOnceStep()
    builder = SagaBuilder()
    ref = builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=second,
        depends_on=ref,
        input_map=lambda out: StartInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=2, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("retry", builder.build())

    saga_id = await orchestrator.start(
        saga_name="retry",
        initial_data={"value": 1},
        aggregation_id="agg-retry",
    )

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.SUSPENDED

    resumed = await orchestrator.run_due()
    assert resumed == 1

    final = await admin.get_saga(saga_id)
    assert final.status == SagaStatus.COMPLETED


@pytest.mark.asyncio
async def test_start_rejects_duplicate_active_aggregation_id(session_maker):
    class AlwaysFailStep(BaseStep[StartInput, StartOutput]):
        async def execute(self, inp: StartInput) -> StartOutput:
            raise RuntimeError("temporary")

    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1,
            base_delay=timedelta(hours=1),
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("dedupe", builder.build())

    first_saga_id = await orchestrator.start(
        saga_name="dedupe",
        initial_data={"value": 1},
        aggregation_id="agg-dup",
    )
    state = await admin.get_saga(first_saga_id)
    assert state.status == SagaStatus.SUSPENDED

    with pytest.raises(ActiveSagaAlreadyExistsError):
        await orchestrator.start(
            saga_name="dedupe",
            initial_data={"value": 2},
            aggregation_id="agg-dup",
        )


@pytest.mark.asyncio
async def test_retry_and_run_due(session_maker):
    flaky = FlakyStep()
    builder = SagaBuilder()
    first_ref = builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=flaky,
        depends_on=first_ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=2, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    orchestrator.register("demo", builder.build())
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)

    saga_id = await orchestrator.start(
        saga_name="demo",
        initial_data={"value": 5},
        aggregation_id="agg-1",
        trace_id="trace-1",
    )

    state_after_first_attempt = await admin.get_saga(saga_id)
    assert state_after_first_attempt.status == SagaStatus.SUSPENDED
    assert state_after_first_attempt.retry_counter == 1

    resumed = await orchestrator.run_due(limit=10)
    assert resumed == 1

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPLETED
    assert flaky.calls == 2


@pytest.mark.asyncio
async def test_run_due_recovers_expired_running_step(session_maker):
    class RecoverableStep(BaseStep[StartInput, StartOutput]):
        def __init__(self) -> None:
            self.calls = 0
            self.started = asyncio.Event()

        async def execute(self, inp: StartInput) -> StartOutput:
            self.calls += 1
            self.started.set()
            if self.calls == 1:
                await asyncio.Future()
            return StartOutput(value=inp.value + 10)

    recoverable = RecoverableStep()
    builder = SagaBuilder()
    builder.add_step(
        step=recoverable,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
        execution_lease=timedelta(seconds=0),
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("recover", builder.build())

    start_task = asyncio.create_task(
        orchestrator.start(
            saga_name="recover",
            initial_data={"value": 1},
            aggregation_id="agg-recover",
        )
    )
    await recoverable.started.wait()
    await asyncio.sleep(0)
    start_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start_task

    async with session_maker() as session:
        saga = await orchestrator.repository.get_active_by_aggregation_id_for_update(
            session,
            "agg-recover",
        )
        assert saga is not None
        recovered_saga_id = saga.id

    state_before = await admin.get_saga(recovered_saga_id)
    assert state_before.status == SagaStatus.RUNNING

    resumed = await orchestrator.run_due(limit=10)
    assert resumed == 1

    state_after = await admin.get_saga(recovered_saga_id)
    assert state_after.status == SagaStatus.COMPLETED
    assert recoverable.calls == 2


@pytest.mark.asyncio
async def test_run_due_recovers_expired_compensation(session_maker):
    class ReservingStep(BaseStep[StartInput, StartOutput]):
        def __init__(self) -> None:
            self.compensation_calls = 0
            self.compensation_started = asyncio.Event()

        async def execute(self, inp: StartInput) -> StartOutput:
            return StartOutput(value=inp.value + 1)

        async def compensate(self, inp: StartInput, out: StartOutput) -> None:
            self.compensation_calls += 1
            self.compensation_started.set()
            if self.compensation_calls == 1:
                await asyncio.Future()

    class FailingStep(BaseStep[NextInput, NextOutput]):
        async def execute(self, inp: NextInput) -> NextOutput:
            raise RuntimeError("boom")

    reserving = ReservingStep()
    builder = SagaBuilder()
    ref = builder.add_step(
        step=reserving,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=FailingStep(),
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=1)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
        execution_lease=timedelta(seconds=0),
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("compensate-recover", builder.build())

    start_task = asyncio.create_task(
        orchestrator.start(
            saga_name="compensate-recover",
            initial_data={"value": 1},
            aggregation_id="agg-compensate",
        )
    )
    await reserving.compensation_started.wait()
    await asyncio.sleep(0)
    start_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start_task

    async with session_maker() as session:
        saga = await orchestrator.repository.get_active_by_aggregation_id_for_update(
            session,
            "agg-compensate",
        )
        assert saga is not None
        recovered_saga_id = saga.id

    state_before = await admin.get_saga(recovered_saga_id)
    assert state_before.status == SagaStatus.COMPENSATING

    resumed = await orchestrator.run_due(limit=10)
    assert resumed == 1

    state_after = await admin.get_saga(recovered_saga_id)
    assert state_after.status == SagaStatus.FAILED
    assert reserving.compensation_calls == 2


@pytest.mark.asyncio
async def test_notify_rejects_stale_token(session_maker):
    class AlwaysFailStep(BaseStep[StartInput, StartOutput]):
        async def execute(self, inp: StartInput) -> StartOutput:
            raise RuntimeError("always fail")

    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1, base_delay=timedelta(seconds=3600)
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    orchestrator.register("notify", builder.build())
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)

    saga_id = await orchestrator.start(
        saga_name="notify",
        initial_data={"value": 1},
        aggregation_id="agg-2",
    )
    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.SUSPENDED
    token_before = state.step_execution_token

    accepted = await orchestrator.notify(saga_id=saga_id, token=uuid.uuid4())
    assert not accepted
    state_after = await admin.get_saga(saga_id)
    assert state_after.step_execution_token == token_before
    assert state_after.status == state.status


@pytest.mark.asyncio
async def test_admin_retry_rejects_failed_saga_after_compensation(session_maker):
    class ReservingStep(BaseStep[StartInput, StartOutput]):
        async def execute(self, inp: StartInput) -> StartOutput:
            return StartOutput(value=inp.value + 1)

        async def compensate(self, inp: StartInput, out: StartOutput) -> None:
            return None

    class FailingStep(BaseStep[NextInput, NextOutput]):
        async def execute(self, inp: NextInput) -> NextOutput:
            raise RuntimeError("boom")

    builder = SagaBuilder()
    ref = builder.add_step(
        step=ReservingStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=FailingStep(),
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=1)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("retry-after-compensation", builder.build())

    saga_id = await orchestrator.start(
        saga_name="retry-after-compensation",
        initial_data={"value": 1},
        aggregation_id="agg-admin-retry",
    )
    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.FAILED
    assert any(entry["phase"] == "compensate" for entry in state.step_history)

    with pytest.raises(SagaStateError):
        await admin.retry_step(saga_id)


@pytest.mark.asyncio
async def test_admin_skip_requires_suspended_state(session_maker):
    class LongRunningStep(BaseStep[StartInput, StartOutput]):
        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def execute(self, inp: StartInput) -> StartOutput:
            self.started.set()
            await asyncio.Future()

    long_running = LongRunningStep()
    builder = SagaBuilder()
    builder.add_step(
        step=long_running,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("skip-running", builder.build())

    start_task = asyncio.create_task(
        orchestrator.start(
            saga_name="skip-running",
            initial_data={"value": 1},
            aggregation_id="agg-skip-running",
        )
    )
    await long_running.started.wait()
    await asyncio.sleep(0)
    start_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start_task

    async with session_maker() as session:
        saga = await orchestrator.repository.get_active_by_aggregation_id_for_update(
            session,
            "agg-skip-running",
        )
        assert saga is not None
        saga_id = saga.id

    with pytest.raises(SagaStateError):
        await admin.skip_step(
            saga_id,
            mock_output=StartOutput(value=999),
        )


@pytest.mark.asyncio
async def test_admin_compensate_rolls_back_suspended_saga(session_maker):
    class ReservingStep(BaseStep[StartInput, StartOutput]):
        def __init__(self) -> None:
            self.compensated = False

        async def execute(self, inp: StartInput) -> StartOutput:
            return StartOutput(value=inp.value + 1)

        async def compensate(self, inp: StartInput, out: StartOutput) -> None:
            self.compensated = True

    class WaitingStep(BaseStep[NextInput, NextOutput]):
        async def execute(self, inp: NextInput) -> NextOutput:
            raise RuntimeError("pending approval")

    reserving = ReservingStep()
    builder = SagaBuilder()
    ref = builder.add_step(
        step=reserving,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=WaitingStep(),
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=1, base_delay=timedelta(hours=1)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("admin-compensate", builder.build())

    saga_id = await orchestrator.start(
        saga_name="admin-compensate",
        initial_data={"value": 1},
        aggregation_id="agg-admin-compensate",
    )

    state_before = await admin.get_saga(saga_id)
    assert state_before.status == SagaStatus.SUSPENDED

    await admin.compensate_step(saga_id)

    state_after = await admin.get_saga(saga_id)
    assert state_after.status == SagaStatus.FAILED
    assert reserving.compensated is True
    assert any(entry["phase"] == "compensate" for entry in state_after.step_history)


@pytest.mark.asyncio
async def test_admin_compensate_rejects_saga_without_completed_steps(session_maker):
    class WaitingStep(BaseStep[StartInput, StartOutput]):
        async def execute(self, inp: StartInput) -> StartOutput:
            raise RuntimeError("pending approval")

    builder = SagaBuilder()
    builder.add_step(
        step=WaitingStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(max_attempts=1, base_delay=timedelta(hours=1)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("admin-compensate-empty", builder.build())

    saga_id = await orchestrator.start(
        saga_name="admin-compensate-empty",
        initial_data={"value": 1},
        aggregation_id="agg-admin-compensate-empty",
    )

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.SUSPENDED

    with pytest.raises(SagaStateError):
        await admin.compensate_step(saga_id)
