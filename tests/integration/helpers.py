from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

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


class ReserveQueueOutput(BaseModel):
    reservation_id: str


class ActivateQueueInput(BaseModel):
    reservation_id: str
    correlation_id: str


class ActivateQueueOutput(BaseModel):
    deployment_id: str


class InboxWaitInput(BaseModel):
    value: int


class InboxWaitOutput(BaseModel):
    value: int


class AddOneStep(BaseStep[StartInput, StartOutput]):
    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)


class FlakyStep(BaseStep[NextInput, NextOutput]):
    def __init__(self) -> None:
        self.calls = 0
        self.compensated = False

    async def execute(
        self,
        inp: NextInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> NextOutput:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary")
        return NextOutput(value=inp.value + 10)

    async def compensate(
        self,
        inp: NextInput,
        out: NextOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> None:
        self.compensated = True


class FailsOnceStep(BaseStep[StartInput, StartOutput]):
    def __init__(self) -> None:
        self.calls = 0

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("temporary")
        return StartOutput(value=inp.value + 1)


class AlwaysFailStep(BaseStep[StartInput, StartOutput]):
    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        raise RuntimeError("always fail")


class HttpStep(BaseStep[HttpInput, HttpOutput]):
    async def execute(
        self,
        inp: HttpInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> HttpOutput:
        return HttpOutput(
            order_id=inp.order_id,
            gateway_url=f"https://gw/{inp.order_id}",
        )


class ReserveQueueStep(BaseStep[ReserveQueueInput, ReserveQueueOutput]):
    async def execute(
        self,
        inp: ReserveQueueInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> ReserveQueueOutput | StepAwaitEvent:
        if event_type is None:
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
        if event_type == "reserve.failed":
            raise RuntimeError("reserve failed")
        if event_type != "reserve.success":
            raise RuntimeError("unexpected reserve event")

        payload = event_payload or {}
        return ReserveQueueOutput(reservation_id=payload["reservation_id"])


class ActivateQueueStep(BaseStep[ActivateQueueInput, ActivateQueueOutput]):
    async def execute(
        self,
        inp: ActivateQueueInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> ActivateQueueOutput | StepAwaitEvent:
        if event_type is None:
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
        if event_type == "activate.failed":
            raise RuntimeError("activate failed")
        if event_type != "activate.success":
            raise RuntimeError("unexpected activate event")

        payload = event_payload or {}
        return ActivateQueueOutput(deployment_id=payload["deployment_id"])


class InboxWaitStep(BaseStep[InboxWaitInput, InboxWaitOutput]):
    async def execute(
        self,
        inp: InboxWaitInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> InboxWaitOutput | StepAwaitEvent:
        if event_type is None:
            return StepAwaitEvent(
                event_types=("inbox.success", "inbox.failed"),
                correlation_id=f"inbox-{inp.value}",
            )
        if event_type == "inbox.failed":
            raise RuntimeError("inbox failed")
        return InboxWaitOutput(value=inp.value + 1)


class FailingStep(BaseStep[NextInput, NextOutput]):
    async def execute(
        self,
        inp: NextInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> NextOutput:
        raise RuntimeError("boom")


class CompensateWaitsStep(BaseStep[StartInput, StartOutput]):
    """Шаг, чья компенсация может ожидать события."""

    def __init__(self) -> None:
        self.compensation_calls = 0
        self.compensation_completed = False

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self,
        inp: StartInput,
        out: StartOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
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

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self,
        inp: StartInput,
        out: StartOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
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

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self,
        inp: StartInput,
        out: StartOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> None:
        self.compensation_calls += 1
        if self.compensation_calls == 1:
            raise RuntimeError("compensation failed once")
        self.compensation_completed = True


class CompensatingStep(BaseStep[StartInput, StartOutput]):
    """Простой шаг с рабочей компенсацией."""

    def __init__(self) -> None:
        self.compensated = False

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self,
        inp: StartInput,
        out: StartOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> None:
        self.compensated = True


class IrreversibleStep(BaseStep[StartInput, StartOutput]):
    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self,
        inp: StartInput,
        out: StartOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> None:
        raise RuntimeError("Cannot be compensated")


class RecoverableStep(BaseStep[StartInput, StartOutput]):
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
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

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        return StartOutput(value=inp.value + 1)

    async def compensate(
        self,
        inp: StartInput,
        out: StartOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> None:
        self.compensation_calls += 1
        self.compensation_started.set()
        if self.compensation_calls == 1 and not self.compensated:
            # Emulate freeze on first real compensation call
            await asyncio.Future()
        self.compensated = True


class LongRunningStep(BaseStep[StartInput, StartOutput]):
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StartOutput:
        self.started.set()
        await asyncio.Future()


class WaitingStep(BaseStep[NextInput, NextOutput]):
    async def execute(
        self,
        inp: NextInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> NextOutput:
        raise RuntimeError("pending approval")


class WaitingWithTimeoutStep(BaseStep[StartInput, StartOutput]):
    async def execute(
        self,
        inp: StartInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StepAwaitEvent | StartOutput:
        return StepAwaitEvent(
            event_types=("some.event",),
            until=timedelta(milliseconds=10),
        )


class RetryWaitInput(BaseModel):
    value: int


class RetryWaitStep(BaseStep[RetryWaitInput, StartOutput]):
    async def execute(
        self,
        inp: RetryWaitInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StepAwaitEvent | StartOutput:
        if event_type is None:
            return StepAwaitEvent(
                event_types=("some.event",),
                until=timedelta(milliseconds=10),
            )
        return StartOutput(value=inp.value + 1)


class LeakTrackInput(BaseModel):
    value: str


class LeakTrackOutput(BaseModel):
    value: str


class CompensateEventTrackerStep(BaseStep[LeakTrackInput, LeakTrackOutput]):
    """Шаг 1: Успешно выполняется, а при компенсации запоминает, какой ивент ему прислали."""

    def __init__(self) -> None:
        self.compensate_event_type: str | None = "NOT_CALLED"

    async def execute(
        self,
        inp: LeakTrackInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> LeakTrackOutput:
        return LeakTrackOutput(value=inp.value)

    async def compensate(
        self,
        inp: LeakTrackInput,
        out: LeakTrackOutput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> None:
        self.compensate_event_type = event_type


class AsyncFailInput(BaseModel):
    value: str


class AsyncFailOutput(BaseModel):
    value: str


class AsyncFailStep(BaseStep[AsyncFailInput, AsyncFailOutput]):
    """Шаг 2: Засыпает, а при получении события - кидает ошибку."""

    async def execute(
        self,
        inp: AsyncFailInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> AsyncFailOutput | StepAwaitEvent:
        if event_type is None:
            return StepAwaitEvent(
                event_types=("fail.event",),
                correlation_id=inp.value,
            )
        if event_type == "fail.event":
            raise RuntimeError("Step failed due to incoming event")

        return AsyncFailOutput(value=inp.value)


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
