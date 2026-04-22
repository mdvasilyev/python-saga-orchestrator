from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import JSON, DateTime
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, declarative_mixin, mapped_column


def _json_type() -> JSON:
    return JSON().with_variant(JSONB, "postgresql")


class OutboxStatus(str, Enum):
    PENDING = "PENDING"
    DISPATCHING = "DISPATCHING"
    SENT = "SENT"
    FAILED = "FAILED"


@declarative_mixin
class OutboxMessageMixin:
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    saga_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    aggregation_id: Mapped[str] = mapped_column(String(255), index=True)
    step_id: Mapped[str] = mapped_column(String(255), index=True)
    trace_id: Mapped[str] = mapped_column(String(255), index=True)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(_json_type()),
        default=dict,
    )
    headers: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(_json_type()),
        default=dict,
    )
    status: Mapped[OutboxStatus] = mapped_column(
        SqlEnum(OutboxStatus),
        default=OutboxStatus.PENDING,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
