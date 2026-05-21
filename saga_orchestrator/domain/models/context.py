from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, NotRequired, TypedDict
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, WrapValidator

from .enums import SagaStepPhase, SagaStepStatus


def _wrap_mapping(v: Any, handler: Any) -> MappingProxyType:
    if isinstance(v, MappingProxyType):
        return v

    parsed_dict = handler(v)
    return MappingProxyType(parsed_dict)


ImmutableDict = Annotated[
    Mapping[str, Any],
    WrapValidator(_wrap_mapping),
    PlainSerializer(lambda v: dict(v), return_type=dict),
]


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

    model_config = ConfigDict(from_attributes=True, validate_assignment=True)

    # -- Core data --
    saga_id: UUID
    saga_name: str
    initial_data: Any
    step_outputs: ImmutableDict = Field(
        default_factory=dict
    )  # step_id -> serialized output model

    # -- Event handling --
    events: tuple[Any, ...] = Field(
        default_factory=tuple
    )  # Payloads of events received during a wait
    latest_event: Any = None  # Payload of the very last event
    latest_event_meta: ImmutableDict = Field(
        default_factory=dict
    )  # Full serialized NotifyEvent
    processed_event_ids: tuple[str, ...] = Field(
        default_factory=tuple
    )  # For idempotency

    # -- Awaiting state --
    awaiting_event_type: str | None = None
    awaiting_event_types: tuple[str, ...] = Field(default_factory=tuple)
    awaiting_correlation_id: str | None = None
    awaiting_until: str | None = None  # ISO 8601 format

    # -- Internal logging --
    notify_inbox: tuple[NotifyLogEntry, ...] = Field(default_factory=tuple)

    def add_processed_event(self, event_id: str) -> None:
        if event_id not in self.processed_event_ids:
            self.processed_event_ids = self.processed_event_ids + (event_id,)

    def add_event(self, payload: Any, meta: dict[str, Any]) -> None:
        self.events = self.events + (payload,)
        self.latest_event = payload
        self.latest_event_meta = meta

    def clear_latest_event(self) -> None:
        self.latest_event = None
        self.latest_event_meta = {}

    def save_step_output(self, step_id: str, output: dict[str, Any]) -> None:
        new_outputs = dict(self.step_outputs)
        new_outputs[step_id] = output
        self.step_outputs = new_outputs

    def set_awaiting_state(
        self,
        event_types: tuple[str, ...],
        correlation_id: str | None,
        until: str | None,
    ) -> None:
        self.awaiting_event_types = event_types
        self.awaiting_event_type = event_types[0] if event_types else None
        self.awaiting_correlation_id = correlation_id
        self.awaiting_until = until

    def clear_awaiting_state(self) -> None:
        self.awaiting_event_type = None
        self.awaiting_event_types = ()
        self.awaiting_correlation_id = None
        self.awaiting_until = None

    def append_notify_log(self, entry: NotifyLogEntry) -> None:
        self.notify_inbox = self.notify_inbox + (entry,)
