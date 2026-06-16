from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from saga_orchestrator import SagaAdmin, SagaBuilder, SagaAdminSnapshot
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.models import ExponentialRetry
from saga_orchestrator.domain.models.enums import SagaStatus
from saga_orchestrator.domain.models.notify import (
    AwaitingEvent,
    NotifyEvent,
    NotifyResult,
)
from tests.integration.helpers import AlwaysFailStep, StartInput
from tests.integration.models import IntegrationSagaHistory, IntegrationSagaState


@pytest.mark.asyncio
async def test_notify_rejects_stale_token(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1, base_delay=timedelta(seconds=3600)
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    orchestrator.register("notify", builder.build())
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )

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
    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1, base_delay=timedelta(seconds=3600)
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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

    state_after: SagaAdminSnapshot = await admin.get_saga(saga_id)
    assert state_after.status == SagaStatus.SUSPENDED
    assert state_after.step_execution_token == new_token
    assert state_after.step_execution_token != state_before.step_execution_token
    assert state_after.deadline_at is None
    assert len(state_after.context.awaiting_event_types) == 1
    assert  "model.approved" in state_after.context.awaiting_event_types
    assert state_after.context.awaiting_correlation_id == "corr-await"
    assert state_after.context.awaiting_until == "2999-01-01T00:00:00+00:00"


@pytest.mark.asyncio
async def test_notify_detailed_rejects_duplicate_event(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1, base_delay=timedelta(seconds=3600)
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
        token=state.step_execution_token,
        event=event,
    )
    assert first == NotifyResult.ACCEPTED

    resumed_state = await admin.get_saga(saga_id)
    duplicate = await orchestrator.notify_detailed(
        saga_id=saga_id,
        token=resumed_state.step_execution_token,
        event=event,
    )
    assert duplicate in {NotifyResult.NOT_SUSPENDED, NotifyResult.DUPLICATE}


@pytest.mark.asyncio
async def test_notify_detailed_enforces_expected_event_fields(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1, base_delay=timedelta(seconds=3600)
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
        token=state.step_execution_token,
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
        token=state.step_execution_token,
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
    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1, base_delay=timedelta(seconds=3600)
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
        token=state.step_execution_token,
        event=NotifyEvent(event_id="evt-expired", payload={"x": 1}),
    )
    assert result == NotifyResult.EXPIRED
