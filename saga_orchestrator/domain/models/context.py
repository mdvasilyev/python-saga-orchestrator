from __future__ import annotations

from typing import Any, NotRequired, TypedDict

from .enums import SagaStepPhase, SagaStepStatus

class NotifyLogEntry(TypedDict):
    """A record of a notification attempt."""
    timestamp: str  # ISO 8601 format
    result: str     # Value from NotifyResult enum
    event_id: str | None
    event_type: str | None
    correlation_id: str | None


class SagaStepHistoryEntry(TypedDict):
    """A record of one step execution or compensation attempt."""
    timestamp: str  # ISO 8601 format
    phase: SagaStepPhase
    status: SagaStepStatus
    step_id: str
    step_name: str
    attempt: int
    token: str
    input: dict[str, Any]  # Serialized input model
    output: dict[str, Any] | None # Serialized output model
    error: str | None
    skipped: NotRequired[bool]


class SagaContext(TypedDict):
    """
    The internal context of a saga instance, stored as a JSON object in the database.
    """
    # -- Core data --
    saga_name: str
    initial_data: Any
    step_outputs: dict[str, dict[str, Any]] # step_id -> serialized output model

    # -- Event handling --
    events: NotRequired[list[Any]] # Payloads of events received during a wait
    latest_event: NotRequired[Any] # Payload of the very last event
    latest_event_meta: NotRequired[dict[str, Any]] # Full serialized NotifyEvent
    processed_event_ids: NotRequired[list[str]] # For idempotency

    # -- Awaiting state --
    awaiting_event_type: NotRequired[str | None]
    awaiting_event_types: NotRequired[list[str] | None]
    awaiting_correlation_id: NotRequired[str | None]
    awaiting_until: NotRequired[str | None] # ISO 8601 format

    # -- Internal logging --
    notify_inbox: NotRequired[list[NotifyLogEntry]]
