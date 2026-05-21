from __future__ import annotations

import pytest

from saga_orchestrator import SagaAdmin, SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.models.enums import SagaStatus
from saga_orchestrator.domain.models.notify import NotifyEvent
from tests.integration.helpers import InboxWaitInput, InboxWaitStep
from tests.integration.models import (
    IntegrationInboxMessage,
    IntegrationSagaHistory,
    IntegrationSagaState,
)


@pytest.mark.asyncio
async def test_inbox_ingest_and_run_due_applies_event(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=InboxWaitStep(),
        input_map=lambda ctx: InboxWaitInput(
            value=ctx.initial_data["value"],
            event_type=ctx.context.latest_event_meta.get("event_type"),
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        inbox_model_class=IntegrationInboxMessage,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
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
