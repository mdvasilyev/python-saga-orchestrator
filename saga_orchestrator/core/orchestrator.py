from __future__ import annotations

from datetime import timedelta
from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..domain.mixins import SagaStateMixin
from ..domain.models import (
    AwaitingEvent,
    NotifyEvent,
    NotifyResult,
    SagaDefinition,
    SagaSnapshot,
)
from ..outbox.models import OutboxMessageMixin
from ..outbox.repository import OutboxRepository
from .engine import SagaEngine
from .repository import SagaRepository

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaOrchestrator(Generic[ModelT]):
    """Provide the public runtime API for saga execution."""

    def __init__(
        self,
        *,
        model_class: type[ModelT],
        session_maker: async_sessionmaker[AsyncSession],
        outbox_model_class: type[OutboxMessageMixin] | None = None,
        execution_lease: timedelta = timedelta(minutes=5),
    ) -> None:
        """Initialize the orchestrator facade."""
        self._engine = SagaEngine(
            model_class=model_class,
            session_maker=session_maker,
            outbox_model_class=outbox_model_class,
            execution_lease=execution_lease,
        )

    @property
    def engine(self) -> SagaEngine[ModelT]:
        """Return the engine used by the orchestrator."""
        return self._engine

    @property
    def repository(self) -> SagaRepository[ModelT]:
        """Return the repository used by the engine."""
        return self._engine.repository

    @property
    def outbox_repository(self) -> OutboxRepository[OutboxMessageMixin] | None:
        """Return the outbox repository used by the engine."""
        return self._engine.outbox_repository

    def register(self, name: str, saga_definition: SagaDefinition) -> None:
        """Register a saga definition under a runtime name."""
        self._engine.register(name, saga_definition)

    async def start(
        self,
        *,
        saga_name: str,
        initial_data: BaseModel | dict[str, Any] | Any,
        aggregation_id: str,
        trace_id: str | None = None,
    ) -> UUID:
        """Create a new saga instance and start executing it."""
        return await self._engine.start(
            saga_name=saga_name,
            initial_data=initial_data,
            aggregation_id=aggregation_id,
            trace_id=trace_id,
        )

    async def notify(
        self,
        *,
        saga_id: UUID,
        token: UUID,
        event: NotifyEvent | dict[str, Any] | Any | None = None,
    ) -> bool:
        """Resume a suspended saga when the provided execution token matches."""
        return await self._engine.notify(saga_id=saga_id, token=token, event=event)

    async def notify_detailed(
        self,
        *,
        saga_id: UUID,
        token: UUID,
        event: NotifyEvent | dict[str, Any] | Any | None = None,
    ) -> NotifyResult:
        """Resume a suspended saga and return a detailed notify outcome."""
        return await self._engine.notify_detailed(
            saga_id=saga_id,
            token=token,
            event=event,
        )

    async def await_event(
        self,
        *,
        saga_id: UUID,
        event: AwaitingEvent,
    ) -> UUID:
        """Configure a suspended saga to wait for an external event."""
        return await self._engine.await_event(saga_id=saga_id, event=event)

    async def run_due(self, *, limit: int = 100) -> int:
        """Resume due running, suspended, and compensating sagas."""
        return await self._engine.run_due(limit=limit)

    async def get_snapshot(self, saga_id: UUID) -> SagaSnapshot:
        """Return the snapshot view of one saga."""
        return await self._engine.get_snapshot(saga_id)

    async def resume(self, saga_id: UUID) -> None:
        """Resume forward execution of one saga."""
        await self._engine.resume(saga_id)
