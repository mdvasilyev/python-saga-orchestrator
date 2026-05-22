from uuid import uuid4

import pytest

from saga_orchestrator import InputContext
from saga_orchestrator.domain.models.context import SagaContext


@pytest.mark.parametrize(
    "test_id, saga_context_config, expected_type, expected_payload",
    [
        (
            "happy_path",
            {
                "latest_event": {"user_id": 123},
                "latest_event_meta": {
                    "event_type": "user.created",
                    "event_id": "evt-1",
                },
            },
            "user.created",
            {"user_id": 123},
        ),
        (
            "no_event_data",
            {},
            None,
            None,
        ),
        (
            "meta_without_type",
            {
                "latest_event": {"data": "something"},
                "latest_event_meta": {"source": "legacy_system"},
            },
            None,
            {"data": "something"},
        ),
        (
            "payload_is_none",
            {
                "latest_event": None,
                "latest_event_meta": {"event_type": "user.deleted"},
            },
            "user.deleted",
            None,
        ),
        (
            "payload_is_primitive",
            {
                "latest_event": "simple_string_payload",
                "latest_event_meta": {"event_type": "notification.sent"},
            },
            "notification.sent",
            "simple_string_payload",
        ),
    ],
)
def test_input_context_properties_parametrized(
    test_id, saga_context_config, expected_type, expected_payload
):
    """
    Параметризованный тест для проверки свойств InputContext.
    """
    saga_id = uuid4()
    initial_data = {"key": "value"}

    saga_context = SagaContext(
        saga_id=saga_id,
        saga_name="test_saga",
        initial_data=initial_data,
        **saga_context_config,
    )

    input_ctx = InputContext(
        saga_id=saga_id,
        initial_data=initial_data,
        context=saga_context,
        step_outputs={},
        latest_event=saga_context.latest_event,
    )

    assert input_ctx.latest_event_type == expected_type, f"Failed on test: {test_id}"
    assert (
        input_ctx.latest_event_payload == expected_payload
    ), f"Failed on test: {test_id}"
