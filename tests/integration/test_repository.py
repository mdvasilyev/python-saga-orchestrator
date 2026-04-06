from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from saga_orchestrator.core import SagaRepository
from saga_orchestrator.domain.exceptions import (
    ActiveSagaAlreadyExistsError,
    SagaNotFoundError,
)
from saga_orchestrator.domain.models.enums import SagaStatus

from .models import IntegrationSagaState


def _make_saga(
    *,
    aggregation_id: str,
    status: SagaStatus,
    deadline_at: datetime | None = None,
) -> IntegrationSagaState:
    return IntegrationSagaState(
        id=uuid.uuid4(),
        aggregation_id=aggregation_id,
        trace_id=f"trace-{aggregation_id}",
        status=status,
        current_step_index=0,
        step_execution_token=uuid.uuid4(),
        context={"saga_name": "repo_test", "initial_data": {}, "step_outputs": {}},
        step_history=[],
        deadline_at=deadline_at,
        retry_counter=0,
    )


async def _insert_sagas(session_maker, *sagas: IntegrationSagaState) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add_all(list(sagas))


@pytest.mark.asyncio
async def test_get_for_update_raises_for_missing_saga(session_maker) -> None:
    repository = SagaRepository(IntegrationSagaState)

    async with session_maker() as session:
        async with session.begin():
            with pytest.raises(SagaNotFoundError):
                await repository.get_for_update(session, uuid.uuid4())


@pytest.mark.asyncio
async def test_get_for_update_blocks_other_transactions(session_maker) -> None:
    repository = SagaRepository(IntegrationSagaState)
    saga = _make_saga(aggregation_id="lock-test", status=SagaStatus.RUNNING)
    await _insert_sagas(session_maker, saga)

    first_session = session_maker()
    second_session = session_maker()
    try:
        first_tx = await first_session.begin()
        locked = await repository.get_for_update(first_session, saga.id)
        assert locked.id == saga.id

        async def acquire_same_row() -> IntegrationSagaState:
            async with second_session.begin():
                return await repository.get_for_update(second_session, saga.id)

        competing_task = asyncio.create_task(acquire_same_row())
        await asyncio.sleep(0.2)
        assert not competing_task.done()

        await first_tx.commit()
        acquired = await asyncio.wait_for(competing_task, timeout=2)
        assert acquired.id == saga.id
    finally:
        await first_session.close()
        await second_session.close()


@pytest.mark.asyncio
async def test_due_running_uses_skip_locked(session_maker) -> None:
    repository = SagaRepository(IntegrationSagaState)
    now = datetime.now(UTC)
    sagas = [
        _make_saga(
            aggregation_id=f"due-running-{index}",
            status=SagaStatus.RUNNING,
            deadline_at=now - timedelta(minutes=index + 1),
        )
        for index in range(3)
    ]
    await _insert_sagas(session_maker, *sagas)

    first_session = session_maker()
    second_session = session_maker()
    try:
        first_tx = await first_session.begin()
        first_batch = await repository.due_running(first_session, now=now, limit=2)
        assert len(first_batch) == 2
        first_ids = {saga.id for saga in first_batch}

        async with second_session.begin():
            second_batch = await repository.due_running(
                second_session, now=now, limit=3
            )

        second_ids = {saga.id for saga in second_batch}
        assert len(second_batch) == 1
        assert first_ids.isdisjoint(second_ids)
        assert first_ids | second_ids == {saga.id for saga in sagas}

        await first_tx.commit()
    finally:
        await first_session.close()
        await second_session.close()


@pytest.mark.asyncio
async def test_due_queries_filter_order_and_limit_by_status(session_maker) -> None:
    repository = SagaRepository(IntegrationSagaState)
    now = datetime.now(UTC)

    running_earliest = _make_saga(
        aggregation_id="running-earliest",
        status=SagaStatus.RUNNING,
        deadline_at=now - timedelta(minutes=5),
    )
    running_latest = _make_saga(
        aggregation_id="running-latest",
        status=SagaStatus.RUNNING,
        deadline_at=now - timedelta(minutes=1),
    )
    suspended_due = _make_saga(
        aggregation_id="suspended-due",
        status=SagaStatus.SUSPENDED,
        deadline_at=now - timedelta(minutes=2),
    )
    compensating_due = _make_saga(
        aggregation_id="compensating-due",
        status=SagaStatus.COMPENSATING,
        deadline_at=now - timedelta(minutes=3),
    )
    running_future = _make_saga(
        aggregation_id="running-future",
        status=SagaStatus.RUNNING,
        deadline_at=now + timedelta(minutes=1),
    )
    running_without_deadline = _make_saga(
        aggregation_id="running-without-deadline",
        status=SagaStatus.RUNNING,
        deadline_at=None,
    )

    await _insert_sagas(
        session_maker,
        running_earliest,
        running_latest,
        suspended_due,
        compensating_due,
        running_future,
        running_without_deadline,
    )

    async with session_maker() as session:
        async with session.begin():
            running = await repository.due_running(session, now=now, limit=1)
            suspended = await repository.due_suspended(session, now=now, limit=10)
            compensating = await repository.due_compensating(
                session,
                now=now,
                limit=10,
            )

    assert [saga.id for saga in running] == [running_earliest.id]
    assert [saga.id for saga in suspended] == [suspended_due.id]
    assert [saga.id for saga in compensating] == [compensating_due.id]


@pytest.mark.asyncio
async def test_active_aggregation_conflict_distinguishes_terminal_statuses(
    session_maker,
) -> None:
    repository = SagaRepository(IntegrationSagaState)
    active = _make_saga(
        aggregation_id="agg-conflict",
        status=SagaStatus.RUNNING,
    )
    failed = _make_saga(
        aggregation_id="agg-terminal",
        status=SagaStatus.FAILED,
    )
    completed = _make_saga(
        aggregation_id="agg-terminal",
        status=SagaStatus.COMPLETED,
    )
    await _insert_sagas(session_maker, active, failed, completed)

    async with session_maker() as session:
        async with session.begin():
            with pytest.raises(ActiveSagaAlreadyExistsError):
                await repository.ensure_no_active_aggregation_conflict(
                    session,
                    "agg-conflict",
                )

            await repository.ensure_no_active_aggregation_conflict(
                session,
                "agg-terminal",
            )
