from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .contracts import OutboxPublisher, OutboxWriter
from .models import OutboxMessageMixin
from .repository import OutboxRepository
from .retry import FixedOutboxDispatchRetry, OutboxDispatchRetryPolicy
from .serialization import JsonOutboxSerializer, OutboxSerializer

OutboxModelT = TypeVar("OutboxModelT", bound=OutboxMessageMixin)


class OutboxDispatcher:
    """Dispatch outbox rows to an external transport."""

    def __init__(
        self,
        *,
        session_maker: async_sessionmaker[AsyncSession],
        publisher: OutboxPublisher,
        writer: OutboxWriter | None = None,
        model_class: type[OutboxModelT] | None = None,
        serializer: OutboxSerializer | None = None,
        retry_policy: OutboxDispatchRetryPolicy | None = None,
        failure_backoff: timedelta = timedelta(seconds=30),
    ) -> None:
        self._session_maker = session_maker
        self._publisher = publisher
        if writer is None:
            if model_class is None:
                raise ValueError("Either writer or model_class must be provided")
            self._writer: OutboxWriter = OutboxRepository(model_class)
        else:
            self._writer = writer
        self._serializer = serializer or JsonOutboxSerializer()
        self._retry_policy = retry_policy or FixedOutboxDispatchRetry(
            delay=failure_backoff
        )

    async def run_once(self, *, limit: int = 100) -> int:
        """Claim due outbox messages and attempt to publish them once."""
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
                await self._publisher.publish(
                    topic=message.topic,
                    payload=self._serializer.deserialize_payload(message.payload),
                    key=message.key,
                    headers=self._serializer.deserialize_headers(message.headers),
                )
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

            async with self._session_maker() as session:
                async with session.begin():
                    await self._writer.mark_sent(
                        session,
                        message.id,
                        sent_at=datetime.now(UTC),
                    )

        return len(claimed)
