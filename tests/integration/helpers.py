from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from pydantic import BaseModel

from saga_orchestrator import InputContext
from saga_orchestrator.domain.models import BaseStep, StepAwaitEvent
from saga_orchestrator.domain.models.context import SagaContext
from saga_orchestrator.outbox import OutboxEvent


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


# STEPS
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


class AlwaysFailStep(BaseStep[StartInput, StartOutput]):
    async def execute(self, inp: StartInput) -> StartOutput:
        raise RuntimeError("always fail")


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


class ReservingStep(BaseStep[StartInput, StartOutput]):
    def __init__(self) -> None:
        self.compensation_calls = 0
        self.compensation_started = asyncio.Event()
        self.compensated = False

    async def execute(self, inp: StartInput) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(self, inp: StartInput, out: StartOutput) -> None:
        self.compensation_calls += 1
        self.compensation_started.set()
        if self.compensation_calls == 1 and not self.compensated:
            # Emulate freeze on first real compensation call
            await asyncio.Future()
        self.compensated = True


class LongRunningStep(BaseStep[StartInput, StartOutput]):
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def execute(self, inp: StartInput) -> StartOutput:
        self.started.set()
        await asyncio.Future()


class WaitingStep(BaseStep[NextInput, NextOutput]):
    async def execute(self, inp: NextInput) -> NextOutput:
        raise RuntimeError("pending approval")


# --- Hook Helper Models & Functions ---


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
    final_value = ctx.step_outputs["step_0"]["value"]
    payload = SagaTerminalEvent(
        saga_id=ctx.saga_id,
        final_status="COMPLETED",
        order_id=ctx.initial_data["order_id"],
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
        saga_id=ctx.saga_id,
        final_status="COMPENSATED",
        order_id=ctx.initial_data["order_id"],
    )
    return [
        OutboxEvent(
            topic="saga.lifecycle.terminal",
            payload=payload.model_dump(mode="json"),
        )
    ]


def create_failed_event(ctx: SagaContext, last_error: str | None) -> list[OutboxEvent]:
    payload = SagaTerminalEvent(
        saga_id=ctx.saga_id,
        final_status="FAILED",
        order_id=ctx.initial_data["order_id"],
        last_error=last_error,
    )
    return [
        OutboxEvent(
            topic="saga.lifecycle.terminal",
            payload=payload.model_dump(mode="json"),
        )
    ]
