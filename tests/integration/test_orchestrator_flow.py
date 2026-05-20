from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from pydantic import BaseModel
from sqlalchemy import select

from saga_orchestrator import InputContext
from saga_orchestrator.admin import SagaAdmin
from saga_orchestrator.core import SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.exceptions import (
    ActiveSagaAlreadyExistsError,
    SagaStateError,
)
from saga_orchestrator.domain.models import BaseStep, ExponentialRetry, StepAwaitEvent
from saga_orchestrator.domain.models.context import SagaContext
from saga_orchestrator.domain.models.enums import SagaStatus, SagaStepPhase
from saga_orchestrator.domain.models.enums.saga_step_status import SagaStepStatus
from saga_orchestrator.domain.models.notify import (
    AwaitingEvent,
    NotifyEvent,
    NotifyResult,
)
from saga_orchestrator.outbox import OutboxDispatcher, OutboxEvent, OutboxStatus
from tests.integration.models import (
    IntegrationInboxMessage,
    IntegrationOutboxMessage,
    IntegrationSagaState,
)


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


class HttpInput(BaseModel):
    order_id: str


class HttpOutput(BaseModel):
    order_id: str
    gateway_url: str


class ReserveQueueInput(BaseModel):
    order_id: str
    gateway_url: str
    correlation_id: str
    event_type: str | None = None
    event_payload: dict | None = None


class ReserveQueueOutput(BaseModel):
    reservation_id: str


class ActivateQueueInput(BaseModel):
    reservation_id: str
    correlation_id: str
    event_type: str | None = None
    event_payload: dict | None = None


class ActivateQueueOutput(BaseModel):
    deployment_id: str


class InboxWaitInput(BaseModel):
    value: int
    event_type: str | None = None


class InboxWaitOutput(BaseModel):
    value: int


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


class HttpStep(BaseStep[HttpInput, HttpOutput]):
    async def execute(self, inp: HttpInput) -> HttpOutput:
        return HttpOutput(
            order_id=inp.order_id,
            gateway_url=f"https://gw/{inp.order_id}",
        )


class ReserveQueueStep(BaseStep[ReserveQueueInput, ReserveQueueOutput]):
    async def execute(
        self,
        inp: ReserveQueueInput,
    ) -> ReserveQueueOutput | StepAwaitEvent:
        if inp.event_type is None:
            return StepAwaitEvent(
                event_types=("reserve.success", "reserve.failed"),
                correlation_id=inp.correlation_id,
                outbox_events=(
                    OutboxEvent(
                        topic="reserve.command",
                        key=inp.order_id,
                        headers={"correlation_id": inp.correlation_id},
                        payload={
                            "order_id": inp.order_id,
                            "gateway_url": inp.gateway_url,
                        },
                    ),
                ),
            )
        if inp.event_type == "reserve.failed":
            raise RuntimeError("reserve failed")
        if inp.event_type != "reserve.success":
            raise RuntimeError("unexpected reserve event")
        payload = inp.event_payload or {}
        return ReserveQueueOutput(reservation_id=payload["reservation_id"])


class ActivateQueueStep(BaseStep[ActivateQueueInput, ActivateQueueOutput]):
    async def execute(
        self,
        inp: ActivateQueueInput,
    ) -> ActivateQueueOutput | StepAwaitEvent:
        if inp.event_type is None:
            return StepAwaitEvent(
                event_types=("activate.success", "activate.failed"),
                correlation_id=inp.correlation_id,
                outbox_events=(
                    OutboxEvent(
                        topic="activate.command",
                        key=inp.reservation_id,
                        headers={"correlation_id": inp.correlation_id},
                        payload={"reservation_id": inp.reservation_id},
                    ),
                ),
            )
        if inp.event_type == "activate.failed":
            raise RuntimeError("activate failed")
        if inp.event_type != "activate.success":
            raise RuntimeError("unexpected activate event")
        payload = inp.event_payload or {}
        return ActivateQueueOutput(deployment_id=payload["deployment_id"])


class InboxWaitStep(BaseStep[InboxWaitInput, InboxWaitOutput]):
    async def execute(self, inp: InboxWaitInput) -> InboxWaitOutput | StepAwaitEvent:
        if inp.event_type is None:
            return StepAwaitEvent(
                event_types=("inbox.success", "inbox.failed"),
                correlation_id=f"inbox-{inp.value}",
            )
        if inp.event_type == "inbox.failed":
            raise RuntimeError("inbox failed")
        return InboxWaitOutput(value=inp.value + 1)


class FailingStep(BaseStep[NextInput, NextOutput]):
    """Простой шаг, который всегда падает, чтобы запустить компенсацию."""

    async def execute(self, inp: NextInput) -> NextOutput:
        raise RuntimeError("boom")


class CompensateWaitsStep(BaseStep[StartInput, StartOutput]):
    """Шаг, чья компенсация может ожидать события."""

    def __init__(self) -> None:
        self.compensation_calls = 0
        self.compensation_completed = False

    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self, inp: StartInput, out: StartOutput
    ) -> StepAwaitEvent | None:
        self.compensation_calls += 1
        if self.compensation_calls == 1:
            return StepAwaitEvent(
                event_types=("compensation.continue",),
                correlation_id=f"compensate-{out.value}",
            )
        self.compensation_completed = True
        return None


class CompensateWaitsWithTimeoutStep(BaseStep[StartInput, StartOutput]):
    """Шаг, чья компенсация ожидает события с таймаутом."""

    def __init__(self) -> None:
        self.compensation_calls = 0
        self.compensation_completed = False

    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self, inp: StartInput, out: StartOutput
    ) -> StepAwaitEvent | None:
        self.compensation_calls += 1
        if self.compensation_calls == 1:
            return StepAwaitEvent(
                event_types=("compensation.continue",),
                correlation_id=f"compensate-{out.value}",
                until=timedelta(milliseconds=1),
            )
        self.compensation_completed = True
        return None


class FlakyCompensateStep(BaseStep[StartInput, StartOutput]):
    """Шаг, чья компенсация падает при первом вызове."""

    def __init__(self) -> None:
        self.compensation_calls = 0
        self.compensation_completed = False

    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(self, inp: StartInput, out: StartOutput) -> None:
        self.compensation_calls += 1
        if self.compensation_calls == 1:
            raise RuntimeError("compensation failed once")
        self.compensation_completed = True


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
    assert state_after.status == SagaStatus.COMPENSATED
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
async def test_await_event_configures_wait_contract(session_maker):
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
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("notify-await", builder.build())

    saga_id = await orchestrator.start(
        saga_name="notify-await",
        initial_data={"value": 1},
        aggregation_id="agg-notify-await",
    )
    state_before = await admin.get_saga(saga_id)
    assert state_before.status == SagaStatus.SUSPENDED
    assert state_before.deadline_at is not None

    new_token = await orchestrator.await_event(
        saga_id=saga_id,
        event=AwaitingEvent(
            event_type="model.approved",
            correlation_id="corr-await",
            until="2999-01-01T00:00:00+00:00",
        ),
    )

    state_after = await admin.get_saga(saga_id)
    assert state_after.status == SagaStatus.SUSPENDED
    assert state_after.step_execution_token == new_token
    assert state_after.step_execution_token != state_before.step_execution_token
    assert state_after.deadline_at is None
    assert state_after.context["awaiting_event_type"] == "model.approved"
    assert state_after.context["awaiting_correlation_id"] == "corr-await"
    assert state_after.context["awaiting_until"] == "2999-01-01T00:00:00+00:00"


@pytest.mark.asyncio
async def test_notify_detailed_rejects_duplicate_event(session_maker):
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
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("notify-duplicate", builder.build())

    saga_id = await orchestrator.start(
        saga_name="notify-duplicate",
        initial_data={"value": 1},
        aggregation_id="agg-notify-dup",
    )
    state = await admin.get_saga(saga_id)

    event = NotifyEvent(
        event_id="evt-dup-1",
        event_type="model.approved",
        payload={"approved": True},
    )
    first = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=state.step_execution_token,  # type: ignore[arg-type]
        event=event,
    )
    assert first == NotifyResult.ACCEPTED

    resumed_state = await admin.get_saga(saga_id)
    duplicate = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=resumed_state.step_execution_token,  # type: ignore[arg-type]
        event=event,
    )
    assert duplicate in {NotifyResult.NOT_SUSPENDED, NotifyResult.DUPLICATE}


@pytest.mark.asyncio
async def test_notify_detailed_enforces_expected_event_fields(session_maker):
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
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("notify-expect", builder.build())

    saga_id = await orchestrator.start(
        saga_name="notify-expect",
        initial_data={"value": 1},
        aggregation_id="agg-notify-expect",
    )

    await orchestrator.await_event(
        saga_id=saga_id,
        event=AwaitingEvent(
            event_type="model.approved",
            correlation_id="corr-123",
            until="2999-01-01T00:00:00+00:00",
        ),
    )

    state = await admin.get_saga(saga_id)
    mismatch_type = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=state.step_execution_token,  # type: ignore[arg-type]
        event=NotifyEvent(
            event_id="evt-type",
            event_type="model.rejected",
            correlation_id="corr-123",
            payload={"ok": False},
        ),
    )
    assert mismatch_type == NotifyResult.EVENT_TYPE_MISMATCH

    state = await admin.get_saga(saga_id)
    mismatch_corr = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=state.step_execution_token,  # type: ignore[arg-type]
        event=NotifyEvent(
            event_id="evt-corr",
            event_type="model.approved",
            correlation_id="corr-999",
            payload={"ok": True},
        ),
    )
    assert mismatch_corr == NotifyResult.CORRELATION_MISMATCH


@pytest.mark.asyncio
async def test_notify_detailed_rejects_expired_waiting_window(session_maker):
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
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("notify-expired", builder.build())

    saga_id = await orchestrator.start(
        saga_name="notify-expired",
        initial_data={"value": 1},
        aggregation_id="agg-notify-expired",
    )

    await orchestrator.await_event(
        saga_id=saga_id,
        event=AwaitingEvent(until="2001-01-01T00:00:00+00:00"),
    )

    state = await admin.get_saga(saga_id)
    result = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=state.step_execution_token,  # type: ignore[arg-type]
        event=NotifyEvent(event_id="evt-expired", payload={"x": 1}),
    )
    assert result == NotifyResult.EXPIRED


@pytest.mark.asyncio
async def test_inbox_ingest_and_run_due_applies_event(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=InboxWaitStep(),
        input_map=lambda ctx: InboxWaitInput(
            value=ctx.initial_data["value"],
            event_type=(ctx.context.get("latest_event_meta") or {}).get("event_type"),
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        inbox_model_class=IntegrationInboxMessage,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("inbox-flow", builder.build())

    saga_id = await orchestrator.start(
        saga_name="inbox-flow",
        initial_data={"value": 41},
        aggregation_id="agg-inbox-flow",
    )
    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.SUSPENDED

    first = await orchestrator.ingest_event(
        aggregation_id="agg-inbox-flow",
        event=NotifyEvent(
            event_id="evt-inbox-1",
            event_type="inbox.success",
            correlation_id="inbox-41",
            payload={"ok": True},
        ),
    )
    duplicate = await orchestrator.ingest_event(
        aggregation_id="agg-inbox-flow",
        event=NotifyEvent(
            event_id="evt-inbox-1",
            event_type="inbox.success",
            correlation_id="inbox-41",
            payload={"ok": True},
        ),
    )
    assert first is True
    assert duplicate is False

    processed = await orchestrator.run_inbox_due(limit=10)
    assert processed == 1

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPLETED
    assert final_state.current_step_index == 1


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
    assert state.status == SagaStatus.COMPENSATED
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
    assert state_after.status == SagaStatus.COMPENSATED
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


@pytest.mark.asyncio
async def test_outbox_message_created_on_successful_step(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        outbox_map=lambda inp, out: [
            OutboxEvent(
                topic="saga.step.completed",
                payload={"value": out.value},
                key="agg-outbox-create",
            )
        ],
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("outbox-create", builder.build())

    await orchestrator.start(
        saga_name="outbox-create",
        initial_data={"value": 10},
        aggregation_id="agg-outbox-create",
    )

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    assert rows[0].topic == "saga.step.completed"
    assert rows[0].payload == {"value": 11}
    assert rows[0].status == OutboxStatus.PENDING


@pytest.mark.asyncio
async def test_outbox_dispatcher_marks_message_sent(session_maker):
    class MemoryPublisher:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def publish(
            self,
            *,
            topic: str,
            payload: dict[str, object],
            key: str | None = None,
            headers: dict[str, object] | None = None,
        ) -> None:
            self.calls.append(
                {
                    "topic": topic,
                    "payload": payload,
                    "key": key,
                    "headers": headers or {},
                }
            )

    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        outbox_map=lambda inp, out: [
            OutboxEvent(
                topic="saga.step.completed",
                payload={"value": out.value},
                key="agg-outbox-dispatch",
            )
        ],
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("outbox-dispatch", builder.build())
    await orchestrator.start(
        saga_name="outbox-dispatch",
        initial_data={"value": 20},
        aggregation_id="agg-outbox-dispatch",
    )

    publisher = MemoryPublisher()
    dispatcher = OutboxDispatcher(
        session_maker=session_maker,
        model_class=IntegrationOutboxMessage,
        publisher=publisher,
    )
    processed = await dispatcher.run_once(limit=10)
    assert processed == 1
    assert len(publisher.calls) == 1

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    assert rows[0].status == OutboxStatus.SENT
    assert rows[0].sent_at is not None


@pytest.mark.asyncio
async def test_outbox_dispatcher_marks_message_failed_and_schedules_retry(
    session_maker,
):
    class FailingPublisher:
        async def publish(
            self,
            *,
            topic: str,
            payload: dict[str, object],
            key: str | None = None,
            headers: dict[str, object] | None = None,
        ) -> None:
            raise RuntimeError("broker unavailable")

    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        outbox_map=lambda inp, out: [
            OutboxEvent(
                topic="saga.step.completed",
                payload={"value": out.value},
                key="agg-outbox-fail",
            )
        ],
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("outbox-fail", builder.build())
    await orchestrator.start(
        saga_name="outbox-fail",
        initial_data={"value": 30},
        aggregation_id="agg-outbox-fail",
    )

    dispatcher = OutboxDispatcher(
        session_maker=session_maker,
        model_class=IntegrationOutboxMessage,
        publisher=FailingPublisher(),
        failure_backoff=timedelta(seconds=5),
    )
    processed = await dispatcher.run_once(limit=10)
    assert processed == 1

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    assert rows[0].status == OutboxStatus.FAILED
    assert rows[0].attempts == 1
    assert rows[0].last_error is not None


@pytest.mark.asyncio
async def test_three_step_http_and_queue_style_flow(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=HttpStep(),
        input_map=lambda ctx: HttpInput(order_id=ctx.initial_data["order_id"]),
    )
    builder.add_step(
        step=ReserveQueueStep(),
        input_map=lambda ctx: ReserveQueueInput(
            order_id=ctx.initial_data["order_id"],
            gateway_url=ctx.step_outputs["step_0"]["gateway_url"],
            correlation_id=f"reserve-{ctx.initial_data['order_id']}",
            event_type=(ctx.context.get("latest_event_meta") or {}).get("event_type"),
            event_payload=ctx.latest_event,
        ),
    )
    builder.add_step(
        step=ActivateQueueStep(),
        input_map=lambda ctx: ActivateQueueInput(
            reservation_id=ctx.step_outputs["step_1"]["reservation_id"],
            correlation_id=f"activate-{ctx.step_outputs['step_1']['reservation_id']}",
            event_type=(ctx.context.get("latest_event_meta") or {}).get("event_type"),
            event_payload=ctx.latest_event,
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("http-queue-3", builder.build())

    queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()

    class QueuePublisher:
        async def publish(
            self,
            *,
            topic: str,
            payload: dict[str, object],
            key: str | None = None,
            headers: dict[str, object] | None = None,
        ) -> None:
            await queue.put(
                {
                    "topic": topic,
                    "payload": payload,
                    "headers": headers or {},
                }
            )

    dispatcher = OutboxDispatcher(
        session_maker=session_maker,
        model_class=IntegrationOutboxMessage,
        publisher=QueuePublisher(),
    )

    saga_id = await orchestrator.start(
        saga_name="http-queue-3",
        initial_data={"order_id": "order-200"},
        aggregation_id="agg-http-queue-3",
    )

    state_after_start = await admin.get_saga(saga_id)
    assert state_after_start.status == SagaStatus.SUSPENDED
    assert state_after_start.current_step_index == 1
    assert state_after_start.context["awaiting_event_type"] == "reserve.success"

    processed = await dispatcher.run_once(limit=10)
    assert processed == 1
    first_command = await queue.get()
    queue.task_done()
    first_headers = first_command["headers"]
    assert isinstance(first_headers, dict)

    reserve_token = (await admin.get_saga(saga_id)).step_execution_token
    await orchestrator.notify(
        saga_id=saga_id,
        token=reserve_token,  # type: ignore[arg-type]
        event=NotifyEvent(
            event_id="evt-reserve-1",
            event_type="reserve.success",
            correlation_id=first_headers["correlation_id"],  # type: ignore[index]
            payload={"reservation_id": "res-200"},
        ),
    )

    state_after_reserve = await admin.get_saga(saga_id)
    assert state_after_reserve.status == SagaStatus.SUSPENDED
    assert state_after_reserve.current_step_index == 2
    assert state_after_reserve.context["awaiting_event_type"] == "activate.success"

    processed = await dispatcher.run_once(limit=10)
    assert processed == 1
    second_command = await queue.get()
    queue.task_done()
    second_headers = second_command["headers"]
    assert isinstance(second_headers, dict)

    activate_token = (await admin.get_saga(saga_id)).step_execution_token
    await orchestrator.notify(
        saga_id=saga_id,
        token=activate_token,  # type: ignore[arg-type]
        event=NotifyEvent(
            event_id="evt-activate-1",
            event_type="activate.success",
            correlation_id=second_headers["correlation_id"],  # type: ignore[index]
            payload={"deployment_id": "dep-200"},
        ),
    )

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPLETED
    assert final_state.current_step_index == 3


@pytest.mark.asyncio
async def test_compensation_can_wait_for_event_and_resume_on_notify(session_maker):
    """
    Проверяет, что шаг компенсации может запросить ожидание,
    переводя сагу в COMPENSATING_SUSPENDED, и затем возобновиться по notify.
    """
    compensating_step = CompensateWaitsStep()
    failing_step = FailingStep()

    builder = SagaBuilder()
    ref = builder.add_step(
        step=compensating_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=failing_step,
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(base_delay=timedelta(seconds=0), max_attempts=0),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("compensate-wait-notify", builder.build())

    saga_id = await orchestrator.start(
        saga_name="compensate-wait-notify",
        initial_data={"value": 100},
        aggregation_id="agg-compensate-wait",
    )

    state_after_fail = await admin.get_saga(saga_id)
    assert state_after_fail.status == SagaStatus.COMPENSATING_SUSPENDED
    assert state_after_fail.current_step_index == 1
    assert compensating_step.compensation_calls == 1
    assert not compensating_step.compensation_completed

    history = state_after_fail.step_history
    assert len(history) == 3
    assert history[2]["phase"] == SagaStepPhase.COMPENSATE
    assert history[2]["status"] == SagaStepStatus.WAITING

    token = state_after_fail.step_execution_token
    result = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=token,
        event=NotifyEvent(
            event_type="compensation.continue",
            correlation_id="compensate-101",
        ),
    )
    assert result == NotifyResult.ACCEPTED

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPENSATED
    assert compensating_step.compensation_calls == 2
    assert compensating_step.compensation_completed

    final_history = final_state.step_history
    assert len(final_history) == 4
    assert final_history[3]["phase"] == SagaStepPhase.COMPENSATE
    assert final_history[3]["status"] == SagaStepStatus.SUCCESS


@pytest.mark.asyncio
async def test_compensation_can_wait_and_resume_on_run_due(session_maker):
    compensating_step = CompensateWaitsWithTimeoutStep()
    failing_step = FailingStep()

    builder = SagaBuilder()
    ref = builder.add_step(
        step=compensating_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=failing_step,
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(base_delay=timedelta(seconds=0), max_attempts=0),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("compensate-wait-due", builder.build())

    saga_id = await orchestrator.start(
        saga_name="compensate-wait-due",
        initial_data={"value": 200},
        aggregation_id="agg-compensate-due",
    )

    state_before_due = await admin.get_saga(saga_id)
    assert state_before_due.status == SagaStatus.COMPENSATING_SUSPENDED
    assert state_before_due.deadline_at is not None
    assert compensating_step.compensation_calls == 1

    resumed = await orchestrator.run_due()
    assert resumed == 1

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPENSATED
    assert compensating_step.compensation_calls == 2
    assert compensating_step.compensation_completed


@pytest.mark.asyncio
async def test_compensation_error_retries_via_compensating_suspended(session_maker):
    """
    Проверяет, что при ошибке в шаге компенсации с retry_policy,
    сага переходит в COMPENSATING_SUSPENDED и может быть повторена через run_due.
    """
    compensating_step = FlakyCompensateStep()
    failing_step = FailingStep()

    builder = SagaBuilder()
    ref = builder.add_step(
        step=compensating_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(base_delay=timedelta(seconds=0), max_attempts=2),
    )
    builder.add_step(
        step=failing_step,
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(base_delay=timedelta(seconds=0), max_attempts=0),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("compensate-retry", builder.build())

    saga_id = await orchestrator.start(
        saga_name="compensate-retry",
        initial_data={"value": 300},
        aggregation_id="agg-compensate-retry",
    )

    state_after_fail = await admin.get_saga(saga_id)
    assert state_after_fail.status == SagaStatus.COMPENSATING_SUSPENDED
    assert compensating_step.compensation_calls == 1
    assert not compensating_step.compensation_completed
    assert state_after_fail.retry_counter == 1

    history = state_after_fail.step_history
    assert history[2]["phase"] == SagaStepPhase.COMPENSATE
    assert history[2]["status"] == SagaStepStatus.ERROR

    resumed = await orchestrator.run_due()
    assert resumed == 1

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPENSATED
    assert compensating_step.compensation_calls == 2
    assert compensating_step.compensation_completed


class CompensatingStep(BaseStep[StartInput, StartOutput]):
    """Простой шаг с рабочей компенсацией."""

    def __init__(self) -> None:
        self.compensated = False

    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(self, inp: StartInput, out: StartOutput) -> None:
        self.compensated = True


class IrreversibleStep(BaseStep[StartInput, StartOutput]):
    """Шаг, чья компенсация всегда падает."""

    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(self, inp: StartInput, out: StartOutput) -> None:
        raise RuntimeError("Cannot be compensated")


@pytest.mark.asyncio
async def test_saga_reaches_compensated_status_on_successful_rollback(session_maker):
    """
    Проверяет, что сага переходит в статус COMPENSATED, когда шаг падает
    и все предыдущие шаги успешно компенсируются.
    """
    compensating_step = CompensatingStep()
    failing_step = FailingStep()

    builder = SagaBuilder(compensate_on_failure=True)
    ref = builder.add_step(
        step=compensating_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=failing_step,
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("successful-compensation", builder.build())

    saga_id = await orchestrator.start(
        saga_name="successful-compensation",
        initial_data={"value": 1},
        aggregation_id="agg-successful-comp",
    )

    state = await admin.get_saga(saga_id)

    assert state.status == SagaStatus.COMPENSATED
    assert compensating_step.compensated is True

    compensate_history_entry = next(
        (
            entry
            for entry in state.step_history
            if entry["phase"] == SagaStepPhase.COMPENSATE
        ),
        None,
    )
    assert compensate_history_entry is not None
    assert compensate_history_entry["status"] == SagaStepStatus.SUCCESS
    assert state.last_error == "Compensation completed successfully"


@pytest.mark.asyncio
async def test_saga_reaches_failed_status_when_compensation_fails(session_maker):
    """
    Проверяет, что сага переходит в статус FAILED, когда сама компенсация падает
    и не может быть повторно выполнена.
    """
    irreversible_step = IrreversibleStep()
    failing_step = FailingStep()

    builder = SagaBuilder(compensate_on_failure=True)
    ref = builder.add_step(
        step=irreversible_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=0)),
    )
    builder.add_step(
        step=failing_step,
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState](engine=orchestrator.engine)
    orchestrator.register("failed-compensation", builder.build())

    saga_id = await orchestrator.start(
        saga_name="failed-compensation",
        initial_data={"value": 1},
        aggregation_id="agg-failed-comp",
    )

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.FAILED
    assert "Compensation failed for step" in (state.last_error or "")
    assert "Cannot be compensated" in (state.last_error or "")

    compensate_history_entry = next(
        (
            entry
            for entry in state.step_history
            if entry["phase"] == SagaStepPhase.COMPENSATE
        ),
        None,
    )
    assert compensate_history_entry is not None
    assert compensate_history_entry["status"] == SagaStepStatus.ERROR


class SagaStartedEvent(BaseModel):
    saga_id: uuid.UUID
    initial_value: int


class SagaTerminalEvent(BaseModel):
    saga_id: uuid.UUID
    final_status: str
    order_id: str
    last_error: str | None = None
    final_value: int | None = None


def create_start_event(ctx: InputContext) -> list[OutboxEvent]:
    payload = SagaStartedEvent(
        saga_id=ctx.saga_id,
        initial_value=ctx.initial_data["value"],
    )
    return [
        OutboxEvent(
            topic="saga.lifecycle.started",
            payload=payload.model_dump(mode="json"),
        )
    ]


def create_completed_event(ctx: SagaContext) -> list[OutboxEvent]:
    final_value = ctx["step_outputs"]["step_0"]["value"]
    payload = SagaTerminalEvent(
        saga_id=uuid.UUID(ctx["saga_id"]),
        final_status="COMPLETED",
        order_id=ctx["initial_data"]["order_id"],
        final_value=final_value,
    )
    return [
        OutboxEvent(
            topic="saga.lifecycle.terminal",
            payload=payload.model_dump(mode="json"),
        )
    ]


def create_compensated_event(ctx: SagaContext) -> list[OutboxEvent]:
    payload = SagaTerminalEvent(
        saga_id=uuid.UUID(ctx["saga_id"]),
        final_status="COMPENSATED",
        order_id=ctx["initial_data"]["order_id"],
    )
    return [
        OutboxEvent(
            topic="saga.lifecycle.terminal",
            payload=payload.model_dump(mode="json"),
        )
    ]


def create_failed_event(ctx: SagaContext, last_error: str | None) -> list[OutboxEvent]:
    payload = SagaTerminalEvent(
        saga_id=uuid.UUID(ctx["saga_id"]),
        final_status="FAILED",
        order_id=ctx["initial_data"]["order_id"],
        last_error=last_error,
    )
    return [
        OutboxEvent(
            topic="saga.lifecycle.terminal",
            payload=payload.model_dump(mode="json"),
        )
    ]


@pytest.mark.asyncio
async def test_on_start_hook_creates_outbox_event(session_maker):
    """
    Проверяет, что хук on_start создает событие в outbox в момент старта саги.
    """
    builder = SagaBuilder()
    builder.on_start(create_start_event)
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("hook-start", builder.build())

    await orchestrator.start(
        saga_name="hook-start",
        initial_data={"value": 100, "order_id": "ord-start"},
        aggregation_id="agg-hook-start",
    )

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    outbox_msg = rows[0]
    assert outbox_msg.topic == "saga.lifecycle.started"
    assert outbox_msg.payload["initial_value"] == 100
    assert outbox_msg.step_id == "__saga_start__"


@pytest.mark.asyncio
async def test_on_completed_hook_creates_outbox_event(session_maker):
    """
    Проверяет, что хук on_completed создает событие в outbox после успешного завершения саги.
    """
    builder = SagaBuilder()
    builder.on_completed(create_completed_event)
    builder.add_step(
        step_id="step_0",
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("hook-completed", builder.build())

    await orchestrator.start(
        saga_name="hook-completed",
        initial_data={"value": 200, "order_id": "ord-completed"},
        aggregation_id="agg-hook-completed",
    )

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    outbox_msg = rows[0]
    assert outbox_msg.topic == "saga.lifecycle.terminal"
    assert outbox_msg.payload["final_status"] == "COMPLETED"
    assert outbox_msg.payload["order_id"] == "ord-completed"
    assert outbox_msg.payload["final_value"] == 201  # 200 + 1
    assert outbox_msg.step_id == "__saga_completed__"


@pytest.mark.asyncio
async def test_on_compensated_hook_creates_outbox_event(session_maker):
    """
    Проверяет, что хук on_compensated создает событие в outbox после успешной компенсации.
    """
    builder = SagaBuilder()
    builder.on_compensated(create_compensated_event)
    ref = builder.add_step(
        step=CompensatingStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=FailingStep(),
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("hook-compensated", builder.build())

    await orchestrator.start(
        saga_name="hook-compensated",
        initial_data={"value": 300, "order_id": "ord-compensated"},
        aggregation_id="agg-hook-compensated",
    )

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    outbox_msg = rows[0]
    assert outbox_msg.topic == "saga.lifecycle.terminal"
    assert outbox_msg.payload["final_status"] == "COMPENSATED"
    assert outbox_msg.payload["order_id"] == "ord-compensated"
    assert outbox_msg.step_id == "__saga_compensated__"


@pytest.mark.asyncio
async def test_on_failed_hook_creates_outbox_event(session_maker):
    """
    Проверяет, что хук on_failed создает событие в outbox, когда сага падает.
    В данном случае - из-за провала компенсации.
    """
    builder = SagaBuilder()
    builder.on_failed(create_failed_event)
    ref = builder.add_step(
        step=IrreversibleStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=0)),
    )
    builder.add_step(
        step=FailingStep(),
        depends_on=ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=0, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    orchestrator.register("hook-failed", builder.build())

    await orchestrator.start(
        saga_name="hook-failed",
        initial_data={"value": 400, "order_id": "ord-failed"},
        aggregation_id="agg-hook-failed",
    )

    async with session_maker() as session:
        result = await session.execute(select(IntegrationOutboxMessage))
        rows = list(result.scalars().all())

    assert len(rows) == 1
    outbox_msg = rows[0]
    assert outbox_msg.topic == "saga.lifecycle.terminal"
    assert outbox_msg.payload["final_status"] == "FAILED"
    assert outbox_msg.payload["order_id"] == "ord-failed"
    assert "Cannot be compensated" in outbox_msg.payload["last_error"]
    assert outbox_msg.step_id == "__saga_failed__"


@pytest.mark.asyncio
async def test_hook_raises_error_if_outbox_is_not_configured(session_maker):
    """
    Проверяет, что движок падает с ошибкой, если хук определен,
    а outbox writer не сконфигурирован.
    """
    builder = SagaBuilder()
    builder.on_start(create_start_event)
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )
    orchestrator.register("hook-no-outbox", builder.build())

    with pytest.raises(SagaStateError) as exc_info:
        await orchestrator.start(
            saga_name="hook-no-outbox",
            initial_data={"value": 1, "order_id": "ord-no-outbox"},
            aggregation_id="agg-hook-no-outbox",
        )

    assert "on_start is configured, but outbox writer is not" in str(exc_info.value)
