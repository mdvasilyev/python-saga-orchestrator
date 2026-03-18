from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Enum, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, declarative_mixin, mapped_column

from src.domain.models.enums import SagaStatus

JSON_TYPE = JSON().with_variant(JSONB, "postgresql")


@declarative_mixin
class SagaStateMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    aggregation_id: Mapped[str] = mapped_column(String(255), index=True)
    trace_id: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[SagaStatus] = mapped_column(
        Enum(SagaStatus),
        default=SagaStatus.RUNNING,
        index=True,
    )
    current_step_index: Mapped[int] = mapped_column(Integer, default=0)
    step_execution_token: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )
    context: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(JSON_TYPE), default=dict
    )
    step_history: Mapped[list[dict[str, Any]]] = mapped_column(
        MutableList.as_mutable(JSON_TYPE), default=list
    )
    deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
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
