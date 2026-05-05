from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from saga_orchestrator.inbox import (
    ClaimedInboxMessage,
    InboxDispatcher,
    InboxProcessOutcome,
    InboxProcessStatus,
)


class _DummySession:
    @asynccontextmanager
    async def begin(self):
        yield


class _DummySessionMaker:
    @asynccontextmanager
    async def __call__(self):
        yield _DummySession()


class _MemoryWriter:
    def __init__(self, claimed: list[ClaimedInboxMessage]) -> None:
        self._claimed = claimed
        self.applied: list[str] = []
        self.ignored: list[tuple[str, str]] = []
        self.failed: list[tuple[str, datetime]] = []

    async def save(self, session, message) -> bool:
        return True

    async def claim_due(
        self,
        session,
        *,
        now: datetime,
        limit: int,
    ) -> list[ClaimedInboxMessage]:
        return self._claimed[:limit]

    async def mark_applied(
        self,
        session,
        message_id,
        *,
        processed_at: datetime,
    ) -> bool:
        self.applied.append(str(message_id))
        return True

    async def mark_ignored(
        self,
        session,
        message_id,
        *,
        processed_at: datetime,
        reason: str,
    ) -> bool:
        self.ignored.append((str(message_id), reason))
        return True

    async def mark_failed(
        self,
        session,
        message_id,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> bool:
        self.failed.append((error, next_attempt_at))
        return True


@pytest.mark.asyncio
async def test_inbox_dispatcher_marks_applied_and_ignored() -> None:
    applied_message = ClaimedInboxMessage(
        id=uuid4(),
        event_id="evt-1",
        saga_id=uuid4(),
        aggregation_id=None,
        event_type="ok",
        correlation_id=None,
        payload={},
        source=None,
        occurred_at=None,
        attempts=0,
    )
    ignored_message = ClaimedInboxMessage(
        id=uuid4(),
        event_id="evt-2",
        saga_id=uuid4(),
        aggregation_id=None,
        event_type="noop",
        correlation_id=None,
        payload={},
        source=None,
        occurred_at=None,
        attempts=0,
    )
    writer = _MemoryWriter([applied_message, ignored_message])

    class _Processor:
        async def process(self, message: ClaimedInboxMessage) -> InboxProcessOutcome:
            if message.event_id == "evt-1":
                return InboxProcessOutcome(status=InboxProcessStatus.APPLIED)
            return InboxProcessOutcome(
                status=InboxProcessStatus.IGNORED,
                reason="not applicable",
            )

    dispatcher = InboxDispatcher(
        session_maker=_DummySessionMaker(),  # type: ignore[arg-type]
        writer=writer,
        processor=_Processor(),
    )
    processed = await dispatcher.run_once(limit=10)

    assert processed == 2
    assert writer.applied == [str(applied_message.id)]
    assert writer.ignored == [(str(ignored_message.id), "not applicable")]
    assert writer.failed == []


@pytest.mark.asyncio
async def test_inbox_dispatcher_retries_on_exception() -> None:
    claimed = ClaimedInboxMessage(
        id=uuid4(),
        event_id="evt-fail",
        saga_id=uuid4(),
        aggregation_id=None,
        event_type="fail",
        correlation_id=None,
        payload={},
        source=None,
        occurred_at=None,
        attempts=2,
    )
    writer = _MemoryWriter([claimed])
    before = datetime.now(UTC)

    class _BoomProcessor:
        async def process(self, message: ClaimedInboxMessage) -> InboxProcessOutcome:
            raise RuntimeError("boom")

    dispatcher = InboxDispatcher(
        session_maker=_DummySessionMaker(),  # type: ignore[arg-type]
        writer=writer,
        processor=_BoomProcessor(),
        failure_backoff=timedelta(seconds=9),
    )
    processed = await dispatcher.run_once(limit=10)

    assert processed == 1
    assert writer.applied == []
    assert writer.ignored == []
    assert len(writer.failed) == 1
    assert "RuntimeError" in writer.failed[0][0]
    assert writer.failed[0][1] >= before + timedelta(seconds=9)
