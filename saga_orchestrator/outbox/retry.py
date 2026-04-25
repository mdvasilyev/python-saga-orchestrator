from __future__ import annotations

from datetime import timedelta
from typing import Protocol


class OutboxDispatchRetryPolicy(Protocol):
    """Define scheduling of the next outbox dispatch attempt after failure."""

    def next_delay(self, attempt: int, error: Exception) -> timedelta: ...


class FixedOutboxDispatchRetry:
    """Return a fixed delay for each failed dispatch attempt."""

    def __init__(self, *, delay: timedelta = timedelta(seconds=30)) -> None:
        self._delay = delay

    def next_delay(self, attempt: int, error: Exception) -> timedelta:
        return self._delay
