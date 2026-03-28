from __future__ import annotations

from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel

from ..core import SagaOrchestrator
from ..domain.mixins import SagaStateMixin
from ..domain.models import SagaAdminSnapshot

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaAdmin(Generic[ModelT]):
    """Provide administrative operations for persisted saga instances."""

    def __init__(
        self,
        orchestrator: SagaOrchestrator[ModelT],
    ) -> None:
        """Initialize the admin API facade."""
        self._orchestrator = orchestrator

    async def get_saga(self, saga_id: UUID) -> SagaAdminSnapshot:
        """Return the current persisted state of one saga."""
        return await self._orchestrator.get_admin_snapshot(saga_id)

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

    async def compensate_step(self, saga_id: UUID) -> None:
        """Start or resume compensation for one saga."""
        await self._orchestrator.start_compensation_from_admin(saga_id)

    async def abort(self, saga_id: UUID) -> None:
        """Mark a saga as failed and invalidate its current execution token."""
        await self._orchestrator.abort(saga_id)
