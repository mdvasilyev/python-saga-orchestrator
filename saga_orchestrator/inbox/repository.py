from __future__ import annotations

from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from .contracts import ClaimedInboxMessage, InboxWriteMessage
from .models import InboxMessageMixin, InboxStatus

InboxModelT = TypeVar("InboxModelT", bound=InboxMessageMixin)


class InboxRepository(Generic[InboxModelT]):
    """Provide persistence operations for inbox rows."""

    def __init__(self, model_class: type[InboxModelT]) -> None:
        self.model_class = model_class

    async def save(
        self,
        session: AsyncSession,
        message: InboxWriteMessage,
    ) -> bool:
        row = self.model_class(
            event_id=message.event_id,
            saga_id=message.saga_id,
            aggregation_id=message.aggregation_id,
            event_type=message.event_type,
            correlation_id=message.correlation_id,
            payload=message.payload,
            source=message.source,
            occurred_at=message.occurred_at,
            status=InboxStatus.PENDING,
            next_attempt_at=message.next_attempt_at or datetime.now(UTC),
        )
        async with session.begin_nested():
            session.add(row)
            try:
                await session.flush()
            except IntegrityError:
                return False
        return True

    async def due_for_processing(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        limit: int,
    ) -> list[InboxModelT]:
        stmt: Select[tuple[InboxModelT]] = (
            select(self.model_class)
            .where(
                self.model_class.status.in_((InboxStatus.PENDING, InboxStatus.FAILED)),
                self.model_class.next_attempt_at <= now,
            )
            .order_by(
                self.model_class.next_attempt_at.asc(),
                self.model_class.created_at.asc(),
            )
            .limit(limit)
        )
        if self._supports_skip_locked(session):
            stmt = stmt.with_for_update(skip_locked=True)
        else:
            stmt = stmt.with_for_update(nowait=False)
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def claim_due(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        limit: int,
    ) -> list[ClaimedInboxMessage]:
        due = await self.due_for_processing(session, now=now, limit=limit)
        claimed: list[ClaimedInboxMessage] = []
        for message in due:
            message.status = InboxStatus.PROCESSING
            claimed.append(
                ClaimedInboxMessage(
                    id=message.id,
                    event_id=message.event_id,
                    saga_id=message.saga_id,
                    aggregation_id=message.aggregation_id,
                    event_type=message.event_type,
                    correlation_id=message.correlation_id,
                    payload=message.payload,
                    source=message.source,
                    occurred_at=message.occurred_at,
                    attempts=message.attempts,
                )
            )
        return claimed

    async def get_for_update(
        self,
        session: AsyncSession,
        message_id: UUID,
    ) -> InboxModelT | None:
        stmt: Select[tuple[InboxModelT]] = (
            select(self.model_class)
            .where(self.model_class.id == message_id)
            .with_for_update(nowait=False)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def mark_applied(
        self,
        session: AsyncSession,
        message_id: UUID,
        *,
        processed_at: datetime,
    ) -> bool:
        row = await self.get_for_update(session, message_id)
        if row is None or row.status != InboxStatus.PROCESSING:
            return False
        row.status = InboxStatus.APPLIED
        row.processed_at = processed_at
        row.last_error = None
        return True

    async def mark_ignored(
        self,
        session: AsyncSession,
        message_id: UUID,
        *,
        processed_at: datetime,
        reason: str,
    ) -> bool:
        row = await self.get_for_update(session, message_id)
        if row is None or row.status != InboxStatus.PROCESSING:
            return False
        row.status = InboxStatus.IGNORED
        row.processed_at = processed_at
        row.last_error = reason
        return True

    async def mark_failed(
        self,
        session: AsyncSession,
        message_id: UUID,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> bool:
        row = await self.get_for_update(session, message_id)
        if row is None or row.status != InboxStatus.PROCESSING:
            return False
        row.status = InboxStatus.FAILED
        row.attempts += 1
        row.last_error = error
        row.next_attempt_at = next_attempt_at
        return True

    @staticmethod
    def _supports_skip_locked(session: AsyncSession) -> bool:
        bind = session.get_bind()
        return bind is not None and bind.dialect.name == "postgresql"
