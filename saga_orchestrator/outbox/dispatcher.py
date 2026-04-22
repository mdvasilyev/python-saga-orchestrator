from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .models import OutboxMessageMixin, OutboxStatus
from .repository import OutboxRepository

OutboxModelT = TypeVar("OutboxModelT", bound=OutboxMessageMixin)


class OutboxPublisher(Protocol):
    async def publish(
        self,
        *,
        topic: str,
        payload: dict[str, Any],
        key: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None: ...


class OutboxDispatcher:
    """Dispatch outbox rows to an external transport."""

    def __init__(
        self,
        *,
        session_maker: async_sessionmaker[AsyncSession],
        model_class: type[OutboxModelT],
        publisher: OutboxPublisher,
        failure_backoff: timedelta = timedelta(seconds=30),
    ) -> None:
        self._session_maker = session_maker
        self._repository = OutboxRepository(model_class)
        self._publisher = publisher
        self._failure_backoff = failure_backoff

    async def run_once(self, *, limit: int = 100) -> int:
        """Claim due outbox messages and attempt to publish them once."""
        now = datetime.now(UTC)
        claimed: list[tuple[UUID, str, dict[str, Any], str | None, dict[str, Any]]] = []

        async with self._session_maker() as session:
            async with session.begin():
                due = await self._repository.due_for_dispatch(
                    session,
                    now=now,
                    limit=limit,
                )
                for message in due:
                    message.status = OutboxStatus.DISPATCHING
                    claimed.append(
                        (
                            message.id,
                            message.topic,
                            message.payload,
                            message.message_key,
                            message.headers,
                        )
                    )

        for message_id, topic, payload, key, headers in claimed:
            try:
                await self._publisher.publish(
                    topic=topic,
                    payload=payload,
                    key=key,
                    headers=headers,
                )
            except Exception as exc:  # noqa: BLE001
                async with self._session_maker() as session:
                    async with session.begin():
                        row = await self._repository.get_for_update(session, message_id)
                        if row is None or row.status != OutboxStatus.DISPATCHING:
                            continue
                        row.status = OutboxStatus.FAILED
                        row.attempts += 1
                        row.last_error = repr(exc)
                        row.next_attempt_at = datetime.now(UTC) + self._failure_backoff
                continue

            async with self._session_maker() as session:
                async with session.begin():
                    row = await self._repository.get_for_update(session, message_id)
                    if row is None or row.status != OutboxStatus.DISPATCHING:
                        continue
                    row.status = OutboxStatus.SENT
                    row.sent_at = datetime.now(UTC)
                    row.last_error = None

        return len(claimed)
