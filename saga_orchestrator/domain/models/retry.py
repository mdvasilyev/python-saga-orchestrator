from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int

    def next_delay(self, attempt_number: int) -> timedelta | None:
        if attempt_number > self.max_attempts:
            return None
        return self._delay_for_attempt(attempt_number)

    def _delay_for_attempt(self, attempt_number: int) -> timedelta:
        raise NotImplementedError


@dataclass(frozen=True)
class NoRetry(RetryPolicy):
    def __init__(self) -> None:
        super().__init__(max_attempts=0)

    def _delay_for_attempt(self, attempt_number: int) -> timedelta:
        return timedelta(seconds=0)


@dataclass(frozen=True)
class FixedRetry(RetryPolicy):
    delay: timedelta

    def _delay_for_attempt(self, attempt_number: int) -> timedelta:
        return self.delay


@dataclass(frozen=True)
class ExponentialRetry(RetryPolicy):
    base_delay: timedelta
    multiplier: float = 2.0
    max_delay: timedelta | None = None

    def _delay_for_attempt(self, attempt_number: int) -> timedelta:
        seconds = self.base_delay.total_seconds() * (
            self.multiplier ** max(attempt_number - 1, 0)
        )
        delay = timedelta(seconds=seconds)
        if self.max_delay is None:
            return delay
        return min(delay, self.max_delay)
