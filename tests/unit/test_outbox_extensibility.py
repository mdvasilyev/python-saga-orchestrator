from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from saga_orchestrator.outbox import ClaimedOutboxMessage, OutboxDispatcher
from saga_orchestrator.outbox.contracts import OutboxWriteMessage


class _DummySession:
    @asynccontextmanager
    async def begin(self):
        yield


class _DummySessionMaker:
    @asynccontextmanager
    async def __call__(self):
        yield _DummySession()


class _MemoryWriter:
    def __init__(self, claimed: list[ClaimedOutboxMessage]) -> None:
        self._claimed = claimed
        self.saved: list[OutboxWriteMessage] = []
        self.failed: list[tuple[str, datetime]] = []
        self.sent: list[str] = []

    async def save(self, session: Any, messages: list[OutboxWriteMessage]) -> None:
        self.saved.extend(messages)

    async def claim_due(
        self,
        session: Any,
        *,
        now: datetime,
        limit: int,
    ) -> list[ClaimedOutboxMessage]:
        return self._claimed[:limit]

    async def mark_sent(
        self,
        session: Any,
        message_id,
        *,
        sent_at: datetime,
    ) -> bool:
        self.sent.append(str(message_id))
        return True

    async def mark_failed(
        self,
        session: Any,
        message_id,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> bool:
        self.failed.append((error, next_attempt_at))
        return True


class _TracingSerializer:
    def __init__(self) -> None:
        self.payload_deserialized = False
        self.headers_deserialized = False

    def deserialize_payload(self, payload: Any) -> dict[str, Any]:
        self.payload_deserialized = True
        return {"decoded": payload["encoded"]}

    def deserialize_headers(self, headers: Any) -> dict[str, Any]:
        self.headers_deserialized = True
        return {"h": "ok"} if headers else {}


class _TracingRetryPolicy:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str]] = []

    def next_delay(self, attempt: int, error: Exception) -> timedelta:
        self.calls.append((attempt, type(error).__name__))
        return timedelta(seconds=7)


@pytest.mark.asyncio
async def test_dispatcher_uses_injected_writer_and_marks_sent() -> None:
    message = ClaimedOutboxMessage(
        id=uuid4(),
        topic="topic",
        payload={"encoded": 42},
        key="k1",
        headers={"x": 1},
        attempts=0,
    )
    writer = _MemoryWriter([message])
    serializer = _TracingSerializer()

    class _Publisher:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def publish(
            self,
            *,
            topic: str,
            payload: dict[str, Any],
            key: str | None = None,
            headers: dict[str, Any] | None = None,
        ) -> None:
            self.calls.append(
                {
                    "topic": topic,
                    "payload": payload,
                    "key": key,
                    "headers": headers or {},
                }
            )

    publisher = _Publisher()
    dispatcher = OutboxDispatcher(
        session_maker=_DummySessionMaker(),  # type: ignore[arg-type]
        publisher=publisher,
        writer=writer,
        serializer=serializer,
    )

    processed = await dispatcher.run_once(limit=10)

    assert processed == 1
    assert serializer.payload_deserialized is True
    assert serializer.headers_deserialized is True
    assert len(publisher.calls) == 1
    assert publisher.calls[0]["payload"] == {"decoded": 42}
    assert publisher.calls[0]["headers"] == {"h": "ok"}
    assert writer.sent == [str(message.id)]
    assert writer.failed == []


@pytest.mark.asyncio
async def test_dispatcher_uses_injected_retry_policy_on_failure() -> None:
    message = ClaimedOutboxMessage(
        id=uuid4(),
        topic="topic",
        payload={"encoded": 1},
        key="k2",
        headers={},
        attempts=2,
    )
    writer = _MemoryWriter([message])
    retry_policy = _TracingRetryPolicy()
    before = datetime.now(UTC)

    class _FailingPublisher:
        async def publish(
            self,
            *,
            topic: str,
            payload: dict[str, Any],
            key: str | None = None,
            headers: dict[str, Any] | None = None,
        ) -> None:
            raise RuntimeError("transport down")

    dispatcher = OutboxDispatcher(
        session_maker=_DummySessionMaker(),  # type: ignore[arg-type]
        publisher=_FailingPublisher(),
        writer=writer,
        retry_policy=retry_policy,
    )

    processed = await dispatcher.run_once(limit=10)

    assert processed == 1
    assert retry_policy.calls == [(3, "RuntimeError")]
    assert writer.sent == []
    assert len(writer.failed) == 1
    assert "RuntimeError" in writer.failed[0][0]
    assert writer.failed[0][1] >= before + timedelta(seconds=7)
