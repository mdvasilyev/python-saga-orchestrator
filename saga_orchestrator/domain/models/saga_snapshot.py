from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from .enums import SagaStatus


@dataclass(frozen=True)
class SagaSnapshot:
    id: UUID
    aggregation_id: str
    status: SagaStatus
    current_step_index: int
    retry_counter: int
    deadline_at: datetime | None
    trace_id: str
    step_execution_token: UUID | None
    last_error: str | None


@dataclass(frozen=True)
class SagaAdminSnapshot:
    id: UUID
    aggregation_id: str
    trace_id: str
    status: SagaStatus
    current_step_index: int
    step_execution_token: UUID | None
    retry_counter: int
    deadline_at: datetime | None
    last_error: str | None
    context: dict[str, Any]
    step_history: list[dict[str, Any]]
