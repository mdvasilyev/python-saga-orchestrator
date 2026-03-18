from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeAlias, TypeVar, get_type_hints

from pydantic import BaseModel

from ..exceptions import TypeValidationError
from .retry import RetryPolicy

InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)
DepModelT = TypeVar("DepModelT", bound=BaseModel)


@dataclass(frozen=True)
class StepRef(Generic[OutputModelT]):
    step_id: str
    output_model: type[OutputModelT]


@dataclass
class InputContext:
    initial_data: Any
    context: dict[str, Any]
    step_outputs: dict[str, Any]


RootInputMap: TypeAlias = Callable[[InputContext], InputModelT | dict[str, Any]]
StepInputMap: TypeAlias = RootInputMap | Callable[[Any], InputModelT | dict[str, Any]]


@dataclass
class StepDefinition(Generic[InputModelT, OutputModelT]):
    step_id: str
    step: BaseStep[InputModelT, OutputModelT]
    input_map: StepInputMap[InputModelT]
    timeout: timedelta | None
    retry_policy: RetryPolicy
    depends_on: StepRef[Any] | None = None

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
        output_model = hints["return"]
        if not (inspect.isclass(input_model) and issubclass(input_model, BaseModel)):
            raise TypeValidationError(
                f"Step '{cls.__name__}' input must inherit from pydantic BaseModel"
            )
        if not (inspect.isclass(output_model) and issubclass(output_model, BaseModel)):
            raise TypeValidationError(
                f"Step '{cls.__name__}' output must inherit from pydantic BaseModel"
            )
        cls.input_model = input_model
        cls.output_model = output_model

    async def execute(self, inp: InputModelT) -> OutputModelT:
        raise NotImplementedError

    async def compensate(self, inp: InputModelT, out: OutputModelT) -> None:
        raise NotImplementedError
