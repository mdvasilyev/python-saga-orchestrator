from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Protocol, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .contracts import ClaimedInboxMessage, InboxWriter
from .models import InboxMessageMixin
from .repository import InboxRepository
from .retry import FixedInboxRetry, InboxRetryPolicy

InboxModelT = TypeVar("InboxModelT", bound=InboxMessageMixin)


class InboxProcessStatus(str, Enum):
    APPLIED = "APPLIED"
    IGNORED = "IGNORED"
    RETRY = "RETRY"


@dataclass(frozen=True)
class InboxProcessOutcome:
    status: InboxProcessStatus
    reason: str | None = None


class InboxProcessor(Protocol):
    """Define one inbox event processing operation."""

    async def process(self, message: ClaimedInboxMessage) -> InboxProcessOutcome: ...


class InboxDispatcher:
    """Process inbox rows by invoking a user-provided inbox processor."""

    def __init__(
        self,
        *,
        session_maker: async_sessionmaker[AsyncSession],
        processor: InboxProcessor,
        writer: InboxWriter | None = None,
        model_class: type[InboxModelT] | None = None,
        retry_policy: InboxRetryPolicy | None = None,
        failure_backoff: timedelta = timedelta(seconds=30),
    ) -> None:
        self._session_maker = session_maker
        self._processor = processor
        if writer is None:
            if model_class is None:
                raise ValueError("Either writer or model_class must be provided")
            self._writer: InboxWriter = InboxRepository(model_class)
        else:
            self._writer = writer
        self._retry_policy = retry_policy or FixedInboxRetry(delay=failure_backoff)

    async def run_once(self, *, limit: int = 100) -> int:
        """Claim due inbox messages and process them once."""
        now = datetime.now(UTC)

        async with self._session_maker() as session:
            async with session.begin():
                claimed = await self._writer.claim_due(
                    session,
                    now=now,
                    limit=limit,
                )

        for message in claimed:
            try:
                outcome = await self._processor.process(message)
            except Exception as exc:  # noqa: BLE001
                delay = self._retry_policy.next_delay(message.attempts + 1, exc)
                async with self._session_maker() as session:
                    async with session.begin():
                        await self._writer.mark_failed(
                            session,
                            message.id,
                            error=repr(exc),
                            next_attempt_at=datetime.now(UTC) + delay,
                        )
                continue

            if outcome.status == InboxProcessStatus.APPLIED:
                async with self._session_maker() as session:
                    async with session.begin():
                        await self._writer.mark_applied(
                            session,
                            message.id,
                            processed_at=datetime.now(UTC),
                        )
                continue

            if outcome.status == InboxProcessStatus.IGNORED:
                async with self._session_maker() as session:
                    async with session.begin():
                        await self._writer.mark_ignored(
                            session,
                            message.id,
                            processed_at=datetime.now(UTC),
                            reason=outcome.reason or "ignored",
                        )
                continue

            delay = self._retry_policy.next_delay(
                message.attempts + 1,
                RuntimeError(outcome.reason or "retry requested"),
            )
            async with self._session_maker() as session:
                async with session.begin():
                    await self._writer.mark_failed(
                        session,
                        message.id,
                        error=outcome.reason or "retry requested",
                        next_attempt_at=datetime.now(UTC) + delay,
                    )

        return len(claimed)
