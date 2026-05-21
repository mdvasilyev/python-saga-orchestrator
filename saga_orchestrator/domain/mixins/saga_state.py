from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, mapped_column

from ..models.context import SagaContext
from ..models.enums import SagaStatus
from .types import MutableModel


@declarative_mixin
class SagaStateMixin:
    """Mixin for saga state table.

    Внимание: Пользователь обязан переопределить `step_history`.
    """

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    aggregation_id: Mapped[str] = mapped_column(String(255), index=True)
    trace_id: Mapped[str] = mapped_column(String(255), index=True)
    saga_name: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[SagaStatus] = mapped_column(
        Enum(SagaStatus), default=SagaStatus.RUNNING, index=True
    )
    current_step_index: Mapped[int] = mapped_column(Integer, default=0)
    step_execution_token: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    context: Mapped[SagaContext] = mapped_column(
        MutableModel(SagaContext), nullable=False
    )

    @declared_attr
    def step_history(cls) -> Mapped[list[Any]]:
        """Relationship to the saga history table."""
        raise NotImplementedError(
            f"Model '{cls.__name__}' inherits from SagaStateMixin "
            f"but does not define 'step_history'. You must implement it. "
            f"Example: step_history: Mapped[list['MySagaHistory']] = relationship(cascade='all, delete-orphan')"
        )

    deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    retry_counter: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
