from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from saga_orchestrator import SagaBuilder
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.outbox import OutboxDispatcher, OutboxEvent, OutboxStatus
from tests.integration.helpers import AddOneStep, StartInput
from tests.integration.models import (
    IntegrationOutboxMessage,
    IntegrationSagaHistory,
    IntegrationSagaState,
)


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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
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

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
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
