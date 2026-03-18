from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..domain.exceptions import ActiveSagaAlreadyExistsError, SagaNotFoundError
from ..domain.mixins import SagaStateMixin
from ..domain.models.enums import SagaStatus

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaRepository(Generic[ModelT]):
    ACTIVE_STATUSES: tuple[SagaStatus, ...] = (
        SagaStatus.RUNNING,
        SagaStatus.SUSPENDED,
        SagaStatus.COMPENSATING,
    )

    def __init__(self, model_class: type[ModelT]) -> None:
        self.model_class = model_class

    async def get_for_update(self, session: AsyncSession, saga_id: UUID) -> ModelT:
        stmt: Select[tuple[ModelT]] = (
            select(self.model_class)
            .where(self.model_class.id == saga_id)
            .with_for_update(nowait=False)
        )
        result = await session.execute(stmt)
        saga = result.scalar_one_or_none()
        if saga is None:
            raise SagaNotFoundError(f"Saga '{saga_id}' not found")
        return saga

    async def get_active_by_aggregation_id_for_update(
        self,
        session: AsyncSession,
        aggregation_id: str,
    ) -> ModelT | None:
        stmt: Select[tuple[ModelT]] = (
            select(self.model_class)
            .where(
                self.model_class.aggregation_id == aggregation_id,
                self.model_class.status.in_(self.ACTIVE_STATUSES),
            )
            .with_for_update(nowait=False)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def ensure_no_active_aggregation_conflict(
        self,
        session: AsyncSession,
        aggregation_id: str,
    ) -> None:
        existing = await self.get_active_by_aggregation_id_for_update(
            session,
            aggregation_id,
        )
        if existing is not None:
            raise ActiveSagaAlreadyExistsError(
                "Active saga already exists for aggregation_id "
                f"'{aggregation_id}' (saga_id={existing.id})"
            )

    async def create(self, session: AsyncSession, saga: ModelT) -> ModelT:
        session.add(saga)
        await session.flush()
        return saga

    async def due_suspended(
        self,
        session: AsyncSession,
        now: datetime,
        limit: int,
    ) -> list[ModelT]:
        return await self._due_by_status(
            session=session,
            status=SagaStatus.SUSPENDED,
            now=now,
            limit=limit,
        )

    async def due_running(
        self,
        session: AsyncSession,
        now: datetime,
        limit: int,
    ) -> list[ModelT]:
        return await self._due_by_status(
            session=session,
            status=SagaStatus.RUNNING,
            now=now,
            limit=limit,
        )

    async def _due_by_status(
        self,
        *,
        session: AsyncSession,
        status: SagaStatus,
        now: datetime,
        limit: int,
    ) -> list[ModelT]:
        stmt = (
            select(self.model_class)
            .where(
                self.model_class.status == status,
                self.model_class.deadline_at.is_not(None),
                self.model_class.deadline_at <= now,
            )
            .order_by(self.model_class.deadline_at.asc())
            .limit(limit)
        )
        if self._supports_skip_locked(session):
            stmt = stmt.with_for_update(skip_locked=True)
        else:
            stmt = stmt.with_for_update(nowait=False)

        result = await session.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    def _supports_skip_locked(session: AsyncSession) -> bool:
        bind = session.get_bind()
        return bind is not None and bind.dialect.name == "postgresql"
