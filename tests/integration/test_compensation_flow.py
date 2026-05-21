from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from saga_orchestrator import SagaAdmin, SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.models import ExponentialRetry
from saga_orchestrator.domain.models.enums import SagaStatus, SagaStepPhase
from saga_orchestrator.domain.models.enums.saga_step_status import SagaStepStatus
from saga_orchestrator.domain.models.notify import NotifyEvent, NotifyResult
from tests.integration.helpers import (
    CompensateWaitsStep,
    CompensateWaitsWithTimeoutStep,
    CompensatingStep,
    FailingStep,
    FlakyCompensateStep,
    IrreversibleStep,
    NextInput,
    ReservingStep,
    StartInput,
)
from tests.integration.models import IntegrationSagaHistory, IntegrationSagaState


@pytest.mark.asyncio
async def test_run_due_recovers_expired_compensation(session_maker):
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
        execution_lease=timedelta(seconds=0),
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
async def test_compensation_can_wait_for_event_and_resume_on_notify(session_maker):
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
    assert history[2].phase == SagaStepPhase.COMPENSATE
    assert history[2].status == SagaStepStatus.WAITING

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
    assert final_history[3].phase == SagaStepPhase.COMPENSATE
    assert final_history[3].status == SagaStepStatus.SUCCESS


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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
    assert history[2].phase == SagaStepPhase.COMPENSATE
    assert history[2].status == SagaStepStatus.ERROR

    resumed = await orchestrator.run_due()
    assert resumed == 1

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPENSATED
    assert compensating_step.compensation_calls == 2
    assert compensating_step.compensation_completed


@pytest.mark.asyncio
async def test_saga_reaches_compensated_status_on_successful_rollback(session_maker):
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
            if entry.phase == SagaStepPhase.COMPENSATE
        ),
        None,
    )
    assert compensate_history_entry is not None
    assert compensate_history_entry.status == SagaStepStatus.SUCCESS
    assert state.last_error == "Compensation completed successfully"


@pytest.mark.asyncio
async def test_saga_reaches_failed_status_when_compensation_fails(session_maker):
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
            if entry.phase == SagaStepPhase.COMPENSATE
        ),
        None,
    )
    assert compensate_history_entry is not None
    assert compensate_history_entry.status == SagaStepStatus.ERROR
