from datetime import timedelta

from pydantic import BaseModel

from saga_orchestrator.core import SagaBuilder
from saga_orchestrator.core.engine import SagaEngine
from saga_orchestrator.domain.models import BaseStep, InputContext


class RootInput(BaseModel):
    value: int
    event_value: int | None = None


class RootOutput(BaseModel):
    value: int


class RootStep(BaseStep[RootInput, RootOutput]):
    async def execute(self, inp: RootInput) -> RootOutput:
        return RootOutput(value=inp.value)


def test_build_step_input_exposes_latest_event() -> None:
    builder = SagaBuilder()

    def root_map(ctx: InputContext) -> RootInput:
        latest_event = ctx.latest_event or {}
        return RootInput(
            value=ctx.initial_data["value"],
            event_value=latest_event.get("value"),
        )

    builder.add_step(step=RootStep(), input_map=root_map, timeout=timedelta(seconds=5))
    definition = builder.build()

    step_input = SagaEngine._build_step_input(
        definition.steps[0],
        {
            "initial_data": {"value": 7},
            "step_outputs": {},
            "latest_event": {"value": 99},
            "events": [{"value": 99}],
        },
    )

    assert step_input == RootInput(value=7, event_value=99)
