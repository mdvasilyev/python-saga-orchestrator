from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class NotifyResult(str, Enum):
    ACCEPTED = "ACCEPTED"
    NOT_SUSPENDED = "NOT_SUSPENDED"
    STALE_TOKEN = "STALE_TOKEN"
    DUPLICATE = "DUPLICATE"
    EVENT_TYPE_MISMATCH = "EVENT_TYPE_MISMATCH"
    CORRELATION_MISMATCH = "CORRELATION_MISMATCH"
    EXPIRED = "EXPIRED"


class NotifyEvent(BaseModel):
    event_id: str | None = None
    event_type: str | None = None
    correlation_id: str | None = None
    payload: Any = None
    source: str | None = None
    occurred_at: datetime | None = None


class AwaitingEvent(BaseModel):
    event_type: str | None = None
    correlation_id: str | None = None
    until: datetime | None = None
