from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
