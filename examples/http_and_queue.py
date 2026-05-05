from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import timedelta
from uuid import uuid4

from aio_pika import DeliveryMode, IncomingMessage, Message, connect_robust
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from saga_orchestrator import (
    BaseStep,
    NotifyEvent,
    OutboxDispatcher,
    OutboxEvent,
    SagaAdmin,
    SagaBuilder,
    SagaOrchestrator,
    SagaStateMixin,
    StepAwaitEvent,
)
from saga_orchestrator.inbox.models import InboxMessageMixin
from saga_orchestrator.outbox.models import OutboxMessageMixin

DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/saga_db",
)
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
DEMO_FAIL_MODE = os.getenv("DEMO_FAIL_MODE", "none")

COMMAND_TOPIC_RESERVE = "reserve.command"
COMMAND_TOPIC_ACTIVATE = "activate.command"
REPLY_TOPIC_RESERVE = "reserve.reply"
REPLY_TOPIC_ACTIVATE = "activate.reply"


class Base(DeclarativeBase):
    pass


class DemoSagaState(Base, SagaStateMixin):
    __tablename__ = "demo_http_queue_saga"


class DemoOutboxMessage(Base, OutboxMessageMixin):
    __tablename__ = "demo_http_queue_outbox"


class DemoInboxMessage(Base, InboxMessageMixin):
    __tablename__ = "demo_http_queue_inbox"


class PrepareInput(BaseModel):
    order_id: str


class PrepareOutput(BaseModel):
    order_id: str
    gateway_url: str


class ReserveInput(BaseModel):
    aggregation_id: str
    order_id: str
    gateway_url: str
    correlation_id: str
    response_event_type: str | None = None
    response_payload: dict | None = None


class ReserveOutput(BaseModel):
    reservation_id: str


class ActivateInput(BaseModel):
    aggregation_id: str
    reservation_id: str
    correlation_id: str
    response_event_type: str | None = None
    response_payload: dict | None = None


class ActivateOutput(BaseModel):
    deployment_id: str


class PrepareStep(BaseStep[PrepareInput, PrepareOutput]):
    async def execute(self, inp: PrepareInput) -> PrepareOutput:
        print(f"[http] request: prepare order={inp.order_id}")
        await asyncio.sleep(0.05)
        return PrepareOutput(
            order_id=inp.order_id,
            gateway_url=f"https://internal-gateway/{inp.order_id}",
        )


class ReserveQueueStep(BaseStep[ReserveInput, ReserveOutput]):
    async def execute(self, inp: ReserveInput) -> ReserveOutput | StepAwaitEvent:
        if inp.response_event_type is None:
            print(f"[queue] send reserve command for order={inp.order_id}")
            return StepAwaitEvent(
                event_types=("reserve.success", "reserve.failed"),
                correlation_id=inp.correlation_id,
                until=timedelta(seconds=30),
                outbox_events=(
                    OutboxEvent(
                        topic=COMMAND_TOPIC_RESERVE,
                        key=inp.order_id,
                        headers={"correlation_id": inp.correlation_id},
                        payload={
                            "aggregation_id": inp.aggregation_id,
                            "order_id": inp.order_id,
                            "gateway_url": inp.gateway_url,
                        },
                    ),
                ),
            )
        if inp.response_event_type == "reserve.failed":
            reason = (inp.response_payload or {}).get("reason", "unknown")
            raise RuntimeError(f"reserve failed: {reason}")
        if inp.response_event_type != "reserve.success":
            raise RuntimeError(f"unexpected reserve event: {inp.response_event_type}")
        payload = inp.response_payload or {}
        return ReserveOutput(reservation_id=payload["reservation_id"])


class ActivateQueueStep(BaseStep[ActivateInput, ActivateOutput]):
    async def execute(self, inp: ActivateInput) -> ActivateOutput | StepAwaitEvent:
        if inp.response_event_type is None:
            print(f"[queue] send activate command for reservation={inp.reservation_id}")
            return StepAwaitEvent(
                event_types=("activate.success", "activate.failed"),
                correlation_id=inp.correlation_id,
                until=timedelta(seconds=30),
                outbox_events=(
                    OutboxEvent(
                        topic=COMMAND_TOPIC_ACTIVATE,
                        key=inp.reservation_id,
                        headers={"correlation_id": inp.correlation_id},
                        payload={
                            "aggregation_id": inp.aggregation_id,
                            "reservation_id": inp.reservation_id,
                        },
                    ),
                ),
            )
        if inp.response_event_type == "activate.failed":
            reason = (inp.response_payload or {}).get("reason", "unknown")
            raise RuntimeError(f"activate failed: {reason}")
        if inp.response_event_type != "activate.success":
            raise RuntimeError(f"unexpected activate event: {inp.response_event_type}")
        payload = inp.response_payload or {}
        return ActivateOutput(deployment_id=payload["deployment_id"])


class RabbitMqPublisher:
    def __init__(self, channel) -> None:
        self._channel = channel

    async def publish(
        self,
        *,
        topic: str,
        payload: dict,
        key: str | None = None,
        headers: dict | None = None,
    ) -> None:
        normalized_headers = headers or {}
        await self._channel.default_exchange.publish(
            Message(
                body=json.dumps(payload).encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=str(uuid4()),
                correlation_id=normalized_headers.get("correlation_id"),
                type=topic,
                headers=normalized_headers,
            ),
            routing_key=topic,
        )


async def main() -> None:
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    orchestrator = SagaOrchestrator[DemoSagaState](
        model_class=DemoSagaState,
        inbox_model_class=DemoInboxMessage,
        outbox_model_class=DemoOutboxMessage,
        session_maker=session_maker,
    )
    admin = SagaAdmin[DemoSagaState](engine=orchestrator.engine)

    builder = SagaBuilder()
    builder.add_step(
        step=PrepareStep(),
        input_map=lambda ctx: PrepareInput(order_id=ctx.initial_data["order_id"]),
    )
    builder.add_step(
        step=ReserveQueueStep(),
        input_map=lambda ctx: ReserveInput(
            aggregation_id=ctx.initial_data["order_id"],
            order_id=ctx.initial_data["order_id"],
            gateway_url=ctx.step_outputs["step_0"]["gateway_url"],
            correlation_id=f"reserve-{ctx.initial_data['order_id']}",
            response_event_type=(ctx.context.get("latest_event_meta") or {}).get(
                "event_type"
            ),
            response_payload=ctx.latest_event,
        ),
    )
    builder.add_step(
        step=ActivateQueueStep(),
        input_map=lambda ctx: ActivateInput(
            aggregation_id=ctx.initial_data["order_id"],
            reservation_id=ctx.step_outputs["step_1"]["reservation_id"],
            correlation_id=f"activate-{ctx.step_outputs['step_1']['reservation_id']}",
            response_event_type=(ctx.context.get("latest_event_meta") or {}).get(
                "event_type"
            ),
            response_payload=ctx.latest_event,
        ),
    )
    orchestrator.register("http_queue_3_steps", builder.build())

    rabbit_connection = await connect_robust(RABBITMQ_URL)
    rabbit_channel = await rabbit_connection.channel()
    await rabbit_channel.set_qos(prefetch_count=50)
    await rabbit_channel.declare_queue(COMMAND_TOPIC_RESERVE, durable=True)
    await rabbit_channel.declare_queue(COMMAND_TOPIC_ACTIVATE, durable=True)
    await rabbit_channel.declare_queue(REPLY_TOPIC_RESERVE, durable=True)
    await rabbit_channel.declare_queue(REPLY_TOPIC_ACTIVATE, durable=True)

    dispatcher = OutboxDispatcher(
        session_maker=session_maker,
        model_class=DemoOutboxMessage,
        publisher=RabbitMqPublisher(rabbit_channel),
    )

    async def publish_reply(
        *,
        topic: str,
        reply: dict,
        correlation_id: str | None,
    ) -> None:
        await rabbit_channel.default_exchange.publish(
            Message(
                body=json.dumps(reply).encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=str(uuid4()),
                correlation_id=correlation_id,
                type=reply["event_type"],
                headers={"correlation_id": correlation_id} if correlation_id else {},
            ),
            routing_key=topic,
        )

    async def external_service_worker() -> None:
        async def handle_command(message: IncomingMessage) -> None:
            async with message.process(requeue=False):
                await asyncio.sleep(0.2)
                payload = json.loads(message.body.decode("utf-8"))
                headers = dict(message.headers or {})
                correlation_id = headers.get("correlation_id") or message.correlation_id
                topic = message.routing_key

                if topic == COMMAND_TOPIC_RESERVE:
                    should_fail = DEMO_FAIL_MODE == "reserve"
                    reply = {
                        "event_id": f"evt-reserve-{correlation_id}",
                        "event_type": (
                            "reserve.failed" if should_fail else "reserve.success"
                        ),
                        "correlation_id": correlation_id,
                        "aggregation_id": payload["aggregation_id"],
                        "payload": (
                            {"reason": "forced reserve failure"}
                            if should_fail
                            else {"reservation_id": f"res-{payload['order_id']}"}
                        ),
                    }
                    await publish_reply(
                        topic=REPLY_TOPIC_RESERVE,
                        reply=reply,
                        correlation_id=correlation_id,
                    )
                elif topic == COMMAND_TOPIC_ACTIVATE:
                    should_fail = DEMO_FAIL_MODE == "activate"
                    reply = {
                        "event_id": f"evt-activate-{correlation_id}",
                        "event_type": (
                            "activate.failed" if should_fail else "activate.success"
                        ),
                        "correlation_id": correlation_id,
                        "aggregation_id": payload["aggregation_id"],
                        "payload": (
                            {"reason": "forced activate failure"}
                            if should_fail
                            else {"deployment_id": f"dep-{payload['reservation_id']}"}
                        ),
                    }
                    await publish_reply(
                        topic=REPLY_TOPIC_ACTIVATE,
                        reply=reply,
                        correlation_id=correlation_id,
                    )

        reserve_command_queue = await rabbit_channel.declare_queue(
            COMMAND_TOPIC_RESERVE,
            durable=True,
        )
        activate_command_queue = await rabbit_channel.declare_queue(
            COMMAND_TOPIC_ACTIVATE,
            durable=True,
        )
        await reserve_command_queue.consume(handle_command)
        await activate_command_queue.consume(handle_command)
        await asyncio.Future()

    async def reply_listener() -> None:
        async def handle_reply(message: IncomingMessage) -> None:
            async with message.process(requeue=False):
                await asyncio.sleep(0.1)
                envelope = json.loads(message.body.decode("utf-8"))
                aggregation_id = envelope.get("aggregation_id")
                if not isinstance(aggregation_id, str):
                    return

                await orchestrator.ingest_event(
                    aggregation_id=aggregation_id,
                    event=NotifyEvent(
                        event_id=envelope.get("event_id"),
                        event_type=envelope.get("event_type"),
                        correlation_id=envelope.get("correlation_id"),
                        payload=envelope.get("payload"),
                    ),
                )

        reserve_reply_queue = await rabbit_channel.declare_queue(
            REPLY_TOPIC_RESERVE,
            durable=True,
        )
        activate_reply_queue = await rabbit_channel.declare_queue(
            REPLY_TOPIC_ACTIVATE,
            durable=True,
        )
        await reserve_reply_queue.consume(handle_reply)
        await activate_reply_queue.consume(handle_reply)
        await asyncio.Future()

    command_worker_task = asyncio.create_task(external_service_worker())
    reply_listener_task = asyncio.create_task(reply_listener())

    saga_id = await orchestrator.start(
        saga_name="http_queue_3_steps",
        initial_data={"order_id": "order-100"},
        aggregation_id="order-100",
    )
    print(f"started saga_id={saga_id}")

    async def dispatcher_poller() -> None:
        while True:
            await dispatcher.run_once(limit=10)
            await asyncio.sleep(0.05)

    async def inbox_poller() -> None:
        while True:
            await orchestrator.run_inbox_due(limit=10)
            await asyncio.sleep(0.05)

    poller_task = asyncio.create_task(dispatcher_poller())
    inbox_task = asyncio.create_task(inbox_poller())

    for _ in range(300):
        snapshot = await admin.get_saga(saga_id)
        if snapshot.status.value in {"COMPLETED", "FAILED"}:
            print(f"final status={snapshot.status.value}")
            print(f"step history entries={len(snapshot.step_history)}")
            break
        await asyncio.sleep(0.05)

    poller_task.cancel()
    inbox_task.cancel()
    command_worker_task.cancel()
    reply_listener_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await poller_task
    with contextlib.suppress(asyncio.CancelledError):
        await inbox_task
    with contextlib.suppress(asyncio.CancelledError):
        await command_worker_task
    with contextlib.suppress(asyncio.CancelledError):
        await reply_listener_task

    await rabbit_channel.close()
    await rabbit_connection.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
