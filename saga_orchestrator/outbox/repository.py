from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

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

    @staticmethod
    def _supports_skip_locked(session: AsyncSession) -> bool:
        bind = session.get_bind()
        return bind is not None and bind.dialect.name == "postgresql"
