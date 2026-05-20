from __future__ import annotations

from typing import Any, NotRequired, TypedDict
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .enums import SagaStepPhase, SagaStepStatus


class NotifyLogEntry(TypedDict):
    """A record of a notification attempt."""

    timestamp: str  # ISO 8601 format
    result: str  # Value from NotifyResult enum
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
    output: dict[str, Any] | None  # Serialized output model
    error: str | None
    skipped: NotRequired[bool]


class SagaContext(BaseModel):
    """
    The internal context of a saga instance, stored as a JSON object in the database.
    """

    model_config = ConfigDict(from_attributes=True)

    # -- Core data --
    saga_id: UUID
    saga_name: str
    initial_data: Any
    step_outputs: dict[str, dict[str, Any]] = Field(
        default_factory=dict
    )  # step_id -> serialized output model

    # -- Event handling --
    events: list[Any] = Field(
        default_factory=list
    )  # Payloads of events received during a wait
    latest_event: Any = None  # Payload of the very last event
    latest_event_meta: dict[str, Any] = Field(
        default_factory=dict
    )  # Full serialized NotifyEvent
    processed_event_ids: list[str] = Field(default_factory=list)  # For idempotency

    # -- Awaiting state --
    awaiting_event_type: str | None = None
    awaiting_event_types: list[str] = Field(default_factory=list)
    awaiting_correlation_id: str | None = None
    awaiting_until: str | None = None  # ISO 8601 format

    # -- Internal logging --
    notify_inbox: list[NotifyLogEntry] = Field(default_factory=list)
