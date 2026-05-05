from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True)
class InboxWriteMessage:
    """Represent one inbound event ready to be persisted."""

    event_id: str
    saga_id: UUID | None
    aggregation_id: str | None
    event_type: str | None
    correlation_id: str | None
    payload: dict[str, Any]
    source: str | None
    occurred_at: datetime | None = None
    next_attempt_at: datetime | None = None


@dataclass(frozen=True)
class ClaimedInboxMessage:
    """Represent one claimed inbound event ready for processing."""

    id: UUID
    event_id: str
    saga_id: UUID | None
    aggregation_id: str | None
    event_type: str | None
    correlation_id: str | None
    payload: dict[str, Any]
    source: str | None
    occurred_at: datetime | None
    attempts: int = 0


class InboxWriter(Protocol):
    """Define persistence operations required by inbox ingestion and processing."""

    async def save(
        self,
        session,
        message: InboxWriteMessage,
    ) -> bool: ...

    async def claim_due(
        self,
        session,
        *,
        now: datetime,
        limit: int,
    ) -> list[ClaimedInboxMessage]: ...

    async def mark_applied(
        self,
        session,
        message_id: UUID,
        *,
        processed_at: datetime,
    ) -> bool: ...

    async def mark_ignored(
        self,
        session,
        message_id: UUID,
        *,
        processed_at: datetime,
        reason: str,
    ) -> bool: ...

    async def mark_failed(
        self,
        session,
        message_id: UUID,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> bool: ...
