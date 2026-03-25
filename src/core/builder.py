from __future__ import annotations

import inspect
from datetime import timedelta
from typing import Any, Callable, get_type_hints

from pydantic import BaseModel

from ..domain.exceptions import SagaDefinitionError, TypeValidationError
from ..domain.models import (
    BaseStep,
    InputContext,
    NoRetry,
    RetryPolicy,
    SagaDefinition,
    StepDefinition,
    StepInputMap,
    StepRef,
)


class SagaBuilder:
    """Build a saga definition from ordered step definitions."""

    def __init__(self, *, compensate_on_failure: bool = True) -> None:
        """Initialize the builder configuration."""
        self._steps: list[StepDefinition[Any, Any]] = []
        self._compensate_on_failure = compensate_on_failure

    def add_step(
        self,
        *,
        step: BaseStep[Any, Any],
        input_map: StepInputMap[Any],
        timeout: timedelta | None = None,
        retry_policy: RetryPolicy | None = None,
        depends_on: StepRef[Any] | None = None,
        step_id: str | None = None,
    ) -> StepRef[Any]:
        """Add one step definition and return a reference to its output."""
        if not callable(input_map):
            raise SagaDefinitionError("input_map must be callable")
        self.validate_input_map_types(input_map, step.input_model, depends_on)

        normalized_step_id = step_id or f"step_{len(self._steps)}"
        if any(existing.step_id == normalized_step_id for existing in self._steps):
            raise SagaDefinitionError(f"Duplicate step id: {normalized_step_id}")

        definition = StepDefinition(
            step_id=normalized_step_id,
            step=step,
            input_map=input_map,
            timeout=timeout,
            retry_policy=retry_policy or NoRetry(),
            depends_on=depends_on,
        )
        self._steps.append(definition)
        return StepRef(step_id=normalized_step_id, output_model=step.output_model)

    def build(self) -> SagaDefinition:
        """Return the final saga definition."""
        if not self._steps:
            raise SagaDefinitionError("Saga must contain at least one step")
        return SagaDefinition(
            steps=tuple(self._steps),
            compensate_on_failure=self._compensate_on_failure,
        )

    @staticmethod
    def validate_input_map_types(
        input_map: Callable[[Any], Any],
        expected_input_model: type[BaseModel],
        depends_on: StepRef[Any] | None,
    ) -> None:
        """Validate the input and return type annotations of ``input_map``."""
        hints = get_type_hints(input_map)
        params = list(inspect.signature(input_map).parameters.values())

        if depends_on is not None and params:
            dep_param_name = params[0].name
            dep_type = hints.get(dep_param_name)
            if dep_type is not None and dep_type is not depends_on.output_model:
                raise TypeValidationError(
                    "input_map first arg is "
                    f"'{dep_type}', expected '{depends_on.output_model.__name__}'"
                )
        elif depends_on is None and params:
            context_param_name = params[0].name
            context_type = hints.get(context_param_name)
            if context_type is not None and context_type is not InputContext:
                raise TypeValidationError(
                    "input_map first arg is "
                    f"'{context_type}', expected '{InputContext.__name__}'"
                )

        return_type = hints.get("return")
        if return_type is None:
            return
        if return_type is expected_input_model:
            return
        if return_type is dict or return_type is dict[str, Any]:
            return
        raise TypeValidationError(
            "input_map return type "
            f"'{return_type}' is incompatible with '{expected_input_model.__name__}'"
        )
