from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from saga_orchestrator import SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.exceptions import SagaStateError
from saga_orchestrator.domain.models import ExponentialRetry
from tests.integration.helpers import (
    AddOneStep,
    CompensatingStep,
    FailingStep,
    IrreversibleStep,
    NextInput,
    StartInput,
    create_compensated_event,
    create_completed_event,
    create_failed_event,
    create_start_event,
)
from tests.integration.models import IntegrationOutboxMessage, IntegrationSagaState


@pytest.mark.asyncio
async def test_on_start_hook_creates_outbox_event(session_maker):
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
