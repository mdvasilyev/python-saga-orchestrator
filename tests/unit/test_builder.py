import pytest
from pydantic import BaseModel

from src.core import SagaBuilder
from src.domain.exceptions import SagaDefinitionError, TypeValidationError
from src.domain.models import BaseStep, InputContext


class InOne(BaseModel):
    value: int


class OutOne(BaseModel):
    value: int


class InTwo(BaseModel):
    value: int


class OutTwo(BaseModel):
    value: int


class StepOne(BaseStep[InOne, OutOne]):
    async def execute(self, inp: InOne) -> OutOne:
        return OutOne(value=inp.value + 1)


class StepTwo(BaseStep[InTwo, OutTwo]):
    async def execute(self, inp: InTwo) -> OutTwo:
        return OutTwo(value=inp.value + 1)


def test_builder_rejects_empty_saga() -> None:
    builder = SagaBuilder()
    with pytest.raises(SagaDefinitionError):
        builder.build()


def test_builder_accepts_well_typed_maps() -> None:
    builder = SagaBuilder()
    ref = builder.add_step(
        step=StepOne(),
        input_map=lambda ctx: InOne(value=ctx.initial_data["value"]),
    )

    def step_two_map(out: OutOne) -> InTwo:
        return InTwo(value=out.value)

    builder.add_step(step=StepTwo(), input_map=step_two_map, depends_on=ref)
    definition = builder.build()
    assert len(definition.steps) == 2


def test_builder_rejects_wrong_dep_type() -> None:
    builder = SagaBuilder()
    ref = builder.add_step(
        step=StepOne(),
        input_map=lambda ctx: InOne(value=ctx.initial_data["value"]),
    )

    def wrong_map(dep: InOne) -> InTwo:
        return InTwo(value=dep.value)

    with pytest.raises(TypeValidationError):
        builder.add_step(step=StepTwo(), input_map=wrong_map, depends_on=ref)


def test_builder_rejects_wrong_root_context_type() -> None:
    builder = SagaBuilder()

    def wrong_map(dep: OutOne) -> InOne:
        return InOne(value=dep.value)

    with pytest.raises(TypeValidationError):
        builder.add_step(step=StepOne(), input_map=wrong_map)


def test_builder_accepts_annotated_input_context() -> None:
    builder = SagaBuilder()

    def root_map(ctx: InputContext) -> InOne:
        return InOne(value=ctx.initial_data["value"])

    builder.add_step(step=StepOne(), input_map=root_map)


def test_builder_rejects_duplicate_step_id() -> None:
    builder = SagaBuilder()
    builder.add_step(
        step=StepOne(),
        step_id="reserve",
        input_map=lambda ctx: InOne(value=ctx.initial_data["value"]),
    )

    with pytest.raises(SagaDefinitionError):
        builder.add_step(
            step=StepTwo(),
            step_id="reserve",
            input_map=lambda ctx: InTwo(value=ctx.initial_data["value"]),
        )


def test_builder_rejects_non_callable_input_map() -> None:
    builder = SagaBuilder()

    with pytest.raises(SagaDefinitionError):
        builder.add_step(step=StepOne(), input_map=123)  # type: ignore[arg-type]


def test_builder_accepts_untyped_dict_return_annotation() -> None:
    builder = SagaBuilder()

    def root_map(ctx: InputContext) -> dict:
        return {"value": ctx.initial_data["value"]}

    builder.add_step(step=StepOne(), input_map=root_map)


def test_builder_rejects_typed_dict_return_annotation() -> None:
    builder = SagaBuilder()

    def root_map(ctx: InputContext) -> dict[str, int]:
        return {"value": ctx.initial_data["value"]}

    with pytest.raises(TypeValidationError):
        builder.add_step(step=StepOne(), input_map=root_map)


def test_build_preserves_compensation_flag() -> None:
    builder = SagaBuilder(compensate_on_failure=False)
    builder.add_step(
        step=StepOne(),
        input_map=lambda ctx: InOne(value=ctx.initial_data["value"]),
    )

    definition = builder.build()
    assert definition.compensate_on_failure is False
