from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel

from .contracts import OutboxWriteMessage
from .event import OutboxEvent
from .serialization import OutboxSerializer


class OutboxMessageFactory(Protocol):
    """Define conversion from step outbox events to persisted outbox messages."""

    def build_messages(
        self,
        *,
        saga_id: UUID,
        aggregation_id: str,
        step_id: str,
        trace_id: str,
        step_input: BaseModel,
        step_output: BaseModel,
        events: Sequence[OutboxEvent],
        now: datetime,
        serializer: OutboxSerializer,
    ) -> list[OutboxWriteMessage]: ...


class DefaultOutboxMessageFactory:
    """Build default outbox messages from mapped step events."""

    def build_messages(
        self,
        *,
        saga_id: UUID,
        aggregation_id: str,
        step_id: str,
        trace_id: str,
        step_input: BaseModel,
        step_output: BaseModel,
        events: Sequence[OutboxEvent],
        now: datetime,
        serializer: OutboxSerializer,
    ) -> list[OutboxWriteMessage]:
        return [
            OutboxWriteMessage(
                saga_id=saga_id,
                aggregation_id=aggregation_id,
                step_id=step_id,
                trace_id=trace_id,
                topic=event.topic,
                key=event.key,
                payload=serializer.serialize_payload(event.payload),
                headers=serializer.serialize_headers(event.headers),
                next_attempt_at=now,
            )
            for event in events
        ]
