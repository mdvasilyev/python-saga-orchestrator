from __future__ import annotations

from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel

from ..core.engine import SagaEngine
from ..domain.mixins import SagaStateMixin
from ..domain.models import SagaAdminSnapshot

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaAdmin(Generic[ModelT]):
    """Provide administrative operations for persisted saga instances."""

    def __init__(
        self,
        engine: SagaEngine[ModelT],
    ) -> None:
        """Initialize the admin API facade."""
        self._engine = engine

    async def get_saga(self, saga_id: UUID) -> SagaAdminSnapshot:
        """Return the current persisted state of one saga."""
        return await self._engine.get_admin_snapshot(saga_id)

    async def retry_step(self, saga_id: UUID) -> None:
        """Retry the current failed step of a saga."""
        await self._engine.retry_step(saga_id)

    async def skip_step(
        self,
        saga_id: UUID,
        mock_output: BaseModel | dict[str, Any] | None = None,
    ) -> None:
        """Skip the current suspended step using a provided output value."""
        await self._engine.skip_step(saga_id, mock_output)

    async def compensate_step(self, saga_id: UUID) -> None:
        """Start or resume compensation for one saga."""
        await self._engine.compensate_step(saga_id)

    async def abort(self, saga_id: UUID) -> None:
        """Mark a saga as failed and invalidate its current execution token."""
        await self._engine.abort(saga_id)
