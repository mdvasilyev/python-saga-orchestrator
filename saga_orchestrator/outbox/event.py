from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class OutboxEvent:
    topic: str
    payload: dict[str, Any]
    key: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)
