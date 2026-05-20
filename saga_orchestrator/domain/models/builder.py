from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, TypeAlias

from ...outbox.event import OutboxEvent
from .context import SagaContext
from .step import InputContext, StepDefinition

OnStartMap: TypeAlias = Callable[[InputContext], list[OutboxEvent] | None]
OnFailedMap: TypeAlias = Callable[[SagaContext, str | None], list[OutboxEvent] | None]
OnTerminalStateMap: TypeAlias = Callable[[SagaContext], list[OutboxEvent] | None]


@dataclass(frozen=True)
class SagaDefinition:
    steps: tuple[StepDefinition[Any, Any], ...]
    compensate_on_failure: bool = True

    on_start_map: OnStartMap | None = None
    on_completed_map: OnTerminalStateMap | None = None
    on_failed_map: OnFailedMap | None = None
    on_compensated_map: OnTerminalStateMap | None = None
