from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from ...core.step import BaseStep
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


@dataclass
class StepDefinition(Generic[InputModelT, OutputModelT]):
    step_id: str
    step: BaseStep[InputModelT, OutputModelT]
    input_map: Callable[[Any], InputModelT | dict[str, Any]]
    timeout: timedelta | None
    retry_policy: RetryPolicy
    depends_on: StepRef[Any] | None = None

    @property
    def input_model(self) -> type[InputModelT]:
        return self.step.input_model

    @property
    def output_model(self) -> type[OutputModelT]:
        return self.step.output_model
