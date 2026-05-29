from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Enum, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, mapped_column

from ..models.enums import SagaStepPhase, SagaStepStatus
from .types import json_type


@declarative_mixin
class SagaStepHistoryMixin:
    """Mixin for saga step history table.

    Внимание: Пользователь обязан переопределить `saga_id`.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    @declared_attr
    def saga_id(cls) -> Mapped[uuid.UUID]:
        """Foreign key to the saga state table."""
        raise NotImplementedError(
            f"Model '{cls.__name__}' inherits from SagaStepHistoryMixin "
            f"but does not define 'saga_id'. You must implement it. "
            f"Example: saga_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('sagas.id', ondelete='CASCADE'))"
        )

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    phase: Mapped[SagaStepPhase] = mapped_column(Enum(SagaStepPhase), nullable=False)
    status: Mapped[SagaStepStatus] = mapped_column(Enum(SagaStepStatus), nullable=False)
    step_id: Mapped[str] = mapped_column(String(255), nullable=False)
    step_name: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    token: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    input: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    output: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    skipped: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
