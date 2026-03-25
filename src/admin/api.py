from __future__ import annotations

import uuid
from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..core import SagaOrchestrator, SagaRepository
from ..domain.mixins import SagaStateMixin
from ..domain.models import SagaAdminSnapshot
from ..domain.models.enums import SagaStatus

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaAdmin(Generic[ModelT]):
    """Provide administrative operations for persisted saga instances."""

    def __init__(
        self,
        *,
        model_class: type[ModelT],
        session_maker: async_sessionmaker[AsyncSession],
        orchestrator: SagaOrchestrator[ModelT],
    ) -> None:
        """Initialize the admin API dependencies."""
        self._model_class = model_class
        self._session_maker = session_maker
        self._repository = SagaRepository(model_class)
        self._orchestrator = orchestrator

    async def get_saga(self, saga_id: UUID) -> SagaAdminSnapshot:
        """Return the current persisted state of one saga."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get(session, saga_id)
                return SagaAdminSnapshot(
                    id=saga.id,
                    aggregation_id=saga.aggregation_id,
                    trace_id=saga.trace_id,
                    status=saga.status,
                    current_step_index=saga.current_step_index,
                    step_execution_token=saga.step_execution_token,
                    retry_counter=saga.retry_counter,
                    deadline_at=saga.deadline_at,
                    last_error=saga.last_error,
                    context=saga.context,
                    step_history=saga.step_history,
                )

    async def retry_step(self, saga_id: UUID) -> None:
        """Retry the current failed step of a saga."""
        await self._orchestrator.resume_from_admin_retry(saga_id)

    async def skip_step(
        self,
        saga_id: UUID,
        mock_output: BaseModel | dict[str, Any] | None = None,
    ) -> None:
        """Skip the current suspended step using a provided output value."""
        await self._orchestrator.skip_current_step(saga_id, mock_output)

    async def abort(self, saga_id: UUID) -> None:
        """Mark a saga as failed and invalidate its current execution token."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                saga.status = SagaStatus.FAILED
                saga.deadline_at = None
                saga.last_error = saga.last_error or "Aborted by admin"
                saga.step_execution_token = uuid.uuid4()
