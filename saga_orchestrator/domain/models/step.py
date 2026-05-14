from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Generic,
    TypeAlias,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import UUID

from pydantic import BaseModel

from ...outbox.event import OutboxEvent
from ..exceptions import TypeValidationError
from .retry import RetryPolicy

InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)
DepModelT = TypeVar("DepModelT", bound=BaseModel)


@dataclass(frozen=True)
class StepAwaitEvent:
    event_types: tuple[str, ...] | None = None
    correlation_id: str | None = None
    until: timedelta | None = None
    outbox_events: tuple[OutboxEvent, ...] | None = None

    def __post_init__(self) -> None:
        if self.event_types is not None and len(self.event_types) == 0:
            raise TypeValidationError("StepAwaitEvent.event_types cannot be empty")


@dataclass(frozen=True)
class StepRef(Generic[OutputModelT]):
    step_id: str
    output_model: type[OutputModelT]


@dataclass
class InputContext:
    saga_id: UUID
    initial_data: Any
    context: dict[str, Any]
    step_outputs: dict[str, Any]
    latest_event: Any | None = None
    events: list[Any] | None = None


RootInputMap: TypeAlias = Callable[[InputContext], InputModelT | dict[str, Any]]
StepInputMap: TypeAlias = RootInputMap | Callable[[Any], InputModelT | dict[str, Any]]
OutboxMap: TypeAlias = Callable[[InputModelT, OutputModelT], list[OutboxEvent] | None]


@dataclass
class StepDefinition(Generic[InputModelT, OutputModelT]):
    step_id: str
    step: BaseStep[InputModelT, OutputModelT]
    input_map: StepInputMap[InputModelT]
    timeout: timedelta | None
    retry_policy: RetryPolicy
    depends_on: StepRef[Any] | None = None
    outbox_map: OutboxMap[InputModelT, OutputModelT] | None = None

    @property
    def input_model(self) -> type[InputModelT]:
        return self.step.input_model

    @property
    def output_model(self) -> type[OutputModelT]:
        return self.step.output_model


class BaseStep(Generic[InputModelT, OutputModelT]):
    input_model: type[InputModelT]
    output_model: type[OutputModelT]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if cls is BaseStep:
            return

        hints = get_type_hints(cls.execute)
        if "inp" not in hints or "return" not in hints:
            raise TypeValidationError(
                f"Step '{cls.__name__}' must type annotate execute(inp) and return type"
            )
        input_model = hints["inp"]
        output_model = cls._resolve_output_model(hints["return"])
        if not (inspect.isclass(input_model) and issubclass(input_model, BaseModel)):
            raise TypeValidationError(
                f"Step '{cls.__name__}' input must inherit from pydantic BaseModel"
            )
        cls.input_model = input_model
        cls.output_model = output_model

    @staticmethod
    def _resolve_output_model(annotation: Any) -> type[BaseModel]:
        if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
            return annotation

        origin = get_origin(annotation)
        if origin is None:
            raise TypeValidationError("Step execute return type must include BaseModel")
        args = tuple(arg for arg in get_args(annotation) if arg is not type(None))
        model_candidates = [
            arg for arg in args if inspect.isclass(arg) and issubclass(arg, BaseModel)
        ]
        await_candidates = [arg for arg in args if arg is StepAwaitEvent]
        if len(model_candidates) == 1 and len(await_candidates) <= 1:
            return model_candidates[0]
        raise TypeValidationError(
            "Step execute return type must be BaseModel or BaseModel | StepAwaitEvent"
        )

    async def execute(self, inp: InputModelT) -> OutputModelT | StepAwaitEvent:
        raise NotImplementedError

    async def compensate(
        self, inp: InputModelT, out: OutputModelT
    ) -> StepAwaitEvent | None:
        raise NotImplementedError
