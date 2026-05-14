import uuid
from datetime import timedelta
from unittest.mock import Mock

from pydantic import BaseModel

from saga_orchestrator import SagaStateMixin
from saga_orchestrator.core import SagaBuilder
from saga_orchestrator.core.engine import SagaEngine
from saga_orchestrator.domain.models import BaseStep, InputContext


class RootInput(BaseModel):
    value: int
    event_value: int | None = None
    saga_id: uuid.UUID | None = None


class RootOutput(BaseModel):
    value: int


class RootStep(BaseStep[RootInput, RootOutput]):
    async def execute(self, inp: RootInput) -> RootOutput:
        return RootOutput(value=inp.value)

class MockSaga(SagaStateMixin):
    """A mock saga object that satisfies the interface needed by _build_step_input."""
    def __init__(self, id: uuid.UUID, context: dict):
        self.id = id
        self.context = context


def test_build_step_input_exposes_latest_event() -> None:
    builder = SagaBuilder()

    def root_map(ctx: InputContext) -> RootInput:
        latest_event = ctx.latest_event or {}
        return RootInput(
            value=ctx.initial_data["value"],
            event_value=latest_event.get("value"),
            saga_id=ctx.saga_id,
        )

    builder.add_step(step=RootStep(), input_map=root_map, timeout=timedelta(seconds=5))
    definition = builder.build()

    engine = SagaEngine(model_class=MockSaga, session_maker=Mock())

    test_saga_id = uuid.uuid4()
    mock_saga_instance = MockSaga(
        id=test_saga_id,
        context={
            "initial_data": {"value": 7},
            "step_outputs": {},
            "latest_event": {"value": 99},
            "events": [{"value": 99}],
        },
    )

    step_input = engine._build_step_input(
        step_def=definition.steps[0],
        saga=mock_saga_instance,
    )

    assert isinstance(step_input, RootInput)
    assert step_input.value == 7
    assert step_input.event_value == 99
    assert step_input.saga_id == test_saga_id
