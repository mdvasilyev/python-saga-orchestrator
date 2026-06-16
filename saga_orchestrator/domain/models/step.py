from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Generic,
    Mapping,
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
from .context import SagaContext
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
    context: SagaContext
    step_outputs: dict[str, Any]
    latest_event: Any | None = None
    events: list[Any] | None = None

    @property
    def latest_event_type(self) -> str | None:
        """
        Возвращает тип ('event_type') последнего полученного события.
        """
        if not isinstance(self.context.latest_event_meta, Mapping):
            return None
        return self.context.latest_event_meta.get("event_type")

    @property
    def latest_event_payload(self) -> Any | None:
        """
        Возвращает "сырую" полезную нагрузку (payload) последнего полученного события.
        """
        return self.context.latest_event


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
        if cls is BaseStep or inspect.isabstract(cls):
            return
        generic_base = None
        for base in getattr(cls, "__orig_bases__", []):
            origin = get_origin(base)
            if origin and issubclass(origin, BaseStep):
                generic_base = base
                break

        if not generic_base:
            raise TypeValidationError(
                f"Could not find generic parameters for {cls.__name__}. "
                f"Ensure it inherits from a parameterized BaseStep, e.g., BaseStep[MyInput, MyOutput]"
            )

        concrete_args = get_args(generic_base)
        if not concrete_args or any(isinstance(arg, TypeVar) for arg in concrete_args):
            raise TypeValidationError(
                f"Step '{cls.__name__}' inherits from a generic Step "
                "but was not parameterized with concrete Input/Output models."
            )

        concrete_input_model, concrete_output_model = concrete_args
        try:
            hints = get_type_hints(
                cls.execute,
                globalns=inspect.getmodule(cls).__dict__,
                include_extras=True,
            )
        except (AttributeError, NameError) as e:
            raise TypeValidationError(
                f"Could not resolve type hints for '{cls.__name__}.execute'. "
                f"Ensure all types are correctly imported. Original error: {e}"
            )

        if "inp" not in hints or "return" not in hints:
            raise TypeValidationError(
                f"Step '{cls.__name__}' must type annotate execute(inp) and return type"
            )

        input_annotation = hints["inp"]
        if isinstance(input_annotation, TypeVar):
            input_model = concrete_input_model
        else:
            input_model = input_annotation

        return_annotation = hints["return"]
        output_model = cls._resolve_output_model(
            return_annotation, concrete_output_model
        )
        if not (inspect.isclass(input_model) and issubclass(input_model, BaseModel)):
            raise TypeValidationError(
                f"Step '{cls.__name__}' input must inherit from pydantic BaseModel"
            )
        cls.input_model = input_model
        cls.output_model = output_model

    @staticmethod
    def _resolve_output_model(
        annotation: Any, concrete_model: type[BaseModel] | None = None
    ) -> type[BaseModel]:
        def find_model_in_args(args_tuple: tuple) -> list[type[BaseModel]]:
            candidates = []
            for arg in args_tuple:
                if isinstance(arg, TypeVar) and concrete_model:
                    candidates.append(concrete_model)
                elif inspect.isclass(arg) and issubclass(arg, BaseModel):
                    candidates.append(arg)
            return list(dict.fromkeys(candidates))

        if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
            return annotation

        origin = get_origin(annotation)
        if origin is None:
            if isinstance(annotation, TypeVar) and concrete_model:
                return concrete_model
            raise TypeValidationError("Step execute return type must include BaseModel")

        args = get_args(annotation)
        model_candidates = find_model_in_args(args)
        await_candidates = [arg for arg in args if arg is StepAwaitEvent]

        if len(model_candidates) == 1 and len(await_candidates) <= 1:
            return model_candidates[0]

        raise TypeValidationError(
            f"Step execute return type must be BaseModel or BaseModel | StepAwaitEvent. "
            f"Found models: {[m.__name__ for m in model_candidates]}, "
            f"Found await events: {len(await_candidates)}."
        )

    async def execute(
        self,
        inp: InputModelT,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> OutputModelT | StepAwaitEvent:
        raise NotImplementedError

    async def compensate(
        self,
        inp: InputModelT,
        out: OutputModelT,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> StepAwaitEvent | None:
        raise NotImplementedError
