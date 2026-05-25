from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

import pytest
from pydantic import BaseModel

from saga_orchestrator import BaseStep, TypeValidationError


class OutboxEvent(BaseModel):
    topic: str
    payload: dict


class InputModelA(BaseModel):
    a: int


class OutputModelA(BaseModel):
    b: str


class InputModelB(BaseModel):
    x: float


class OutputModelB(BaseModel):
    y: bool


InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)


@dataclass(frozen=True)
class StepAwaitEvent:
    event_types: tuple[str, ...] | None = None


IntermediateInputT = TypeVar("IntermediateInputT", bound=BaseModel)
IntermediateOutputT = TypeVar("IntermediateOutputT", bound=BaseModel)


class AbstractIntermediateStep(
    BaseStep[IntermediateInputT, IntermediateOutputT],
    ABC,
    Generic[IntermediateInputT, IntermediateOutputT],
):
    @abstractmethod
    def some_abstract_method(self):
        pass

    async def execute(
        self, inp: IntermediateInputT
    ) -> IntermediateOutputT | StepAwaitEvent:
        pass


class TestTypeResolution:
    def test_indirect_inheritance_resolves_types_correctly(self):
        """
        GIVEN a concrete step class inheriting from a generic abstract step
        WHEN the concrete class is defined
        THEN BaseStep.__init_subclass__ should correctly resolve input and output models.
        """

        class ConcreteStep(AbstractIntermediateStep[InputModelA, OutputModelA]):
            def some_abstract_method(self):
                pass

        assert hasattr(ConcreteStep, "input_model")
        assert hasattr(ConcreteStep, "output_model")
        assert ConcreteStep.input_model is InputModelA
        assert ConcreteStep.output_model is OutputModelA

    def test_direct_inheritance_resolves_types_correctly(self):
        """
        GIVEN a concrete step class inheriting directly from BaseStep
        WHEN the concrete class is defined
        THEN input and output models should be resolved from its own `execute` method.
        """

        class DirectStep(BaseStep[InputModelB, OutputModelB]):
            async def execute(self, inp: InputModelB) -> OutputModelB:
                pass

        assert DirectStep.input_model is InputModelB
        assert DirectStep.output_model is OutputModelB

    def test_missing_annotation_on_execute_raises_error(self):
        """
        GIVEN a step class with a missing type annotation on `execute`
        WHEN the class is defined
        THEN TypeValidationError should be raised.
        """
        with pytest.raises(TypeValidationError, match="must type annotate execute"):

            class FaultyStep(BaseStep[InputModelA, OutputModelA]):
                # `inp` не аннотирован
                async def execute(self, inp) -> OutputModelA:
                    pass

    def test_wrong_parameterization_raises_error(self):
        """
        GIVEN a step class parameterized with a non-BaseModel type
        WHEN the class is defined
        THEN a TypeError or TypeValidationError should be raised.
        """

        @dataclass
        class NotAModel:
            pass

        with pytest.raises(TypeValidationError, match="Could not resolve type hints"):

            class FaultyStep(BaseStep[InputModelA, OutputModelA]):
                async def execute(self, inp: NotAModel) -> OutputModelA:
                    pass

    def test_unparameterized_generic_step_raises_error(self):
        """
        GIVEN a concrete step inheriting from a generic abstract step without parameters
        WHEN the class is defined
        THEN TypeValidationError should be raised by our custom logic.
        """
        with pytest.raises(
            TypeValidationError,
            match="inherits from a generic Step but was not parameterized with concrete Input/Output models",
        ):

            class UnparameterizedStep(AbstractIntermediateStep):
                def some_abstract_method(self):
                    pass
