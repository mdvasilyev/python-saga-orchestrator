# tests/integration/test_admin_api.py

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from saga_orchestrator import SagaAdmin, SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.exceptions import SagaStateError
from saga_orchestrator.domain.models import ExponentialRetry
from saga_orchestrator.domain.models.enums import SagaStatus
from tests.integration.helpers import (
    AddOneStep,
    CompensatingStep,
    FailingStep,
    LongRunningStep,
    NextInput,
    StartInput,
    StartOutput,
    WaitingStep,
)
from tests.integration.models import IntegrationSagaState


@pytest.mark.asyncio
async def test_admin_retry_rejects_failed_saga_after_compensation(session_maker):
    builder = SagaBuilder()
    ref = builder.add_step(
        step=CompensatingStep(),
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
    compensating_step = CompensatingStep()
    builder = SagaBuilder()
    ref = builder.add_step(
        step=compensating_step,
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
    assert compensating_step.compensated is True
    assert any(entry["phase"] == "compensate" for entry in state_after.step_history)


@pytest.mark.asyncio
async def test_admin_compensate_rejects_saga_without_completed_steps(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=WaitingStep(),
        input_map=lambda ctx: NextInput(value=ctx.initial_data["value"]),
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
async def test_get_admin_snapshot_returns(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState](
        model_class=IntegrationSagaState,
        session_maker=session_maker,
    )

    saga_name = "test_admin_snapshot_saga"
    orchestrator.register(saga_name, builder.build())

    aggregation_id = "agg-admin-snap-1"

    saga_id = await orchestrator.start(
        saga_name=saga_name,
        initial_data={"value": 42},
        aggregation_id=aggregation_id,
    )

    admin_snapshot = await orchestrator.engine.get_admin_snapshot(saga_id)

    assert admin_snapshot is not None
    assert admin_snapshot.id == saga_id
    assert admin_snapshot.saga_name == saga_name
    assert admin_snapshot.aggregation_id == aggregation_id
    assert admin_snapshot.status == SagaStatus.COMPLETED

    assert admin_snapshot.context is not None
    assert admin_snapshot.context.saga_name == saga_name
    assert admin_snapshot.context.initial_data == {"value": 42}

    assert isinstance(admin_snapshot.step_history, list)
    assert len(admin_snapshot.step_history) == 1
    assert admin_snapshot.step_history[0]["step_name"] == "AddOneStep"
    assert admin_snapshot.step_history[0]["status"] == "success"
