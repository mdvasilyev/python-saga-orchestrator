from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from .models import OutboxStatus


@dataclass(frozen=True)
class OutboxWriteMessage:
    """Represent one outbox message ready to be persisted."""

    saga_id: UUID
    aggregation_id: str
    step_id: str
    trace_id: str
    topic: str
    payload: dict[str, Any]
    key: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)
    status: OutboxStatus = OutboxStatus.PENDING
    next_attempt_at: datetime | None = None


@dataclass(frozen=True)
class ClaimedOutboxMessage:
    """Represent one claimed outbox message ready to be dispatched."""

    id: UUID
    topic: str
    payload: dict[str, Any]
    key: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0


class OutboxWriter(Protocol):
    """Define persistence operations required by engine and dispatcher."""

    async def save(
        self,
        session: AsyncSession,
        messages: Sequence[OutboxWriteMessage],
    ) -> None: ...

    async def claim_due(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        limit: int,
    ) -> list[ClaimedOutboxMessage]: ...

    async def mark_sent(
        self,
        session: AsyncSession,
        message_id: UUID,
        *,
        sent_at: datetime,
    ) -> bool: ...

    async def mark_failed(
        self,
        session: AsyncSession,
        message_id: UUID,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> bool: ...


class OutboxPublisher(Protocol):
    """Define transport publish operation for outbox dispatch."""

    async def publish(
        self,
        *,
        topic: str,
        payload: dict[str, Any],
        key: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None: ...
