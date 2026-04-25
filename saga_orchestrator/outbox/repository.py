from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from .contracts import ClaimedOutboxMessage, OutboxWriteMessage
from .models import OutboxMessageMixin, OutboxStatus

OutboxModelT = TypeVar("OutboxModelT", bound=OutboxMessageMixin)


class OutboxRepository(Generic[OutboxModelT]):
    """Provide persistence operations for outbox rows."""

    def __init__(self, model_class: type[OutboxModelT]) -> None:
        self.model_class = model_class

    async def create_many(
        self,
        session: AsyncSession,
        messages: list[OutboxModelT],
    ) -> None:
        session.add_all(messages)
        await session.flush()

    async def save(
        self,
        session: AsyncSession,
        messages: Sequence[OutboxWriteMessage],
    ) -> None:
        if not messages:
            return
        rows = [
            self.model_class(
                saga_id=message.saga_id,
                aggregation_id=message.aggregation_id,
                step_id=message.step_id,
                trace_id=message.trace_id,
                topic=message.topic,
                message_key=message.key,
                payload=message.payload,
                headers=message.headers,
                status=message.status,
                next_attempt_at=message.next_attempt_at or datetime.now(UTC),
            )
            for message in messages
        ]
        await self.create_many(session, rows)

    async def due_for_dispatch(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        limit: int,
    ) -> list[OutboxModelT]:
        stmt: Select[tuple[OutboxModelT]] = (
            select(self.model_class)
            .where(
                self.model_class.status.in_(
                    (OutboxStatus.PENDING, OutboxStatus.FAILED),
                ),
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
    ) -> list[ClaimedOutboxMessage]:
        due = await self.due_for_dispatch(session, now=now, limit=limit)
        claimed: list[ClaimedOutboxMessage] = []
        for message in due:
            message.status = OutboxStatus.DISPATCHING
            claimed.append(
                ClaimedOutboxMessage(
                    id=message.id,
                    topic=message.topic,
                    payload=message.payload,
                    key=message.message_key,
                    headers=message.headers,
                    attempts=message.attempts,
                )
            )
        return claimed

    async def get_for_update(
        self,
        session: AsyncSession,
        message_id: UUID,
    ) -> OutboxModelT | None:
        stmt: Select[tuple[OutboxModelT]] = (
            select(self.model_class)
            .where(self.model_class.id == message_id)
            .with_for_update(nowait=False)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def mark_sent(
        self,
        session: AsyncSession,
        message_id: UUID,
        *,
        sent_at: datetime,
    ) -> bool:
        row = await self.get_for_update(session, message_id)
        if row is None or row.status != OutboxStatus.DISPATCHING:
            return False
        row.status = OutboxStatus.SENT
        row.sent_at = sent_at
        row.last_error = None
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
        if row is None or row.status != OutboxStatus.DISPATCHING:
            return False
        row.status = OutboxStatus.FAILED
        row.attempts += 1
        row.last_error = error
        row.next_attempt_at = next_attempt_at
        return True

    @staticmethod
    def _supports_skip_locked(session: AsyncSession) -> bool:
        bind = session.get_bind()
        return bind is not None and bind.dialect.name == "postgresql"
