from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from saga_orchestrator import (
    SagaAdmin,
    SagaAdminSnapshot,
    SagaBuilder,
    SagaStepPhase,
    SagaStepStatus,
)
from saga_orchestrator.core.orchestrator import SagaOrchestrator
from saga_orchestrator.domain.exceptions import ActiveSagaAlreadyExistsError
from saga_orchestrator.domain.models import ExponentialRetry
from saga_orchestrator.domain.models.enums import SagaStatus
from saga_orchestrator.domain.models.notify import NotifyEvent
from saga_orchestrator.outbox import OutboxDispatcher
from tests.integration.helpers import (
    ActivateQueueInput,
    ActivateQueueStep,
    AddOneStep,
    AlwaysFailStep,
    AsyncFailInput,
    AsyncFailStep,
    CompensateEventTrackerStep,
    CompensatingStep,
    FailsOnceStep,
    FlakyStep,
    HttpInput,
    HttpStep,
    LeakTrackInput,
    NextInput,
    RecoverableStep,
    ReserveQueueInput,
    ReserveQueueStep,
    RetryWaitInput,
    RetryWaitStep,
    StartInput,
    WaitingWithTimeoutStep,
)
from tests.integration.models import (
    IntegrationOutboxMessage,
    IntegrationSagaHistory,
    IntegrationSagaState,
)


@pytest.mark.asyncio
async def test_start_completes_saga(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("simple", builder.build())

    saga_id = await orchestrator.start(
        saga_name="simple",
        initial_data={"value": 1},
        aggregation_id="agg-simple",
    )

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.COMPLETED
    assert len(state.step_history) == 1


@pytest.mark.asyncio
async def test_retry_flow_with_run_due(session_maker):
    second = FailsOnceStep()
    builder = SagaBuilder()
    ref = builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=second,
        depends_on=ref,
        input_map=lambda out: StartInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=2, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("retry", builder.build())

    saga_id = await orchestrator.start(
        saga_name="retry",
        initial_data={"value": 1},
        aggregation_id="agg-retry",
    )

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.SUSPENDED

    resumed = await orchestrator.run_due()
    assert resumed == 1

    final = await admin.get_saga(saga_id)
    assert final.status == SagaStatus.COMPLETED


@pytest.mark.asyncio
async def test_start_rejects_duplicate_active_aggregation_id(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AlwaysFailStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
        retry_policy=ExponentialRetry(
            max_attempts=1,
            base_delay=timedelta(hours=1),
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("dedupe", builder.build())

    first_saga_id = await orchestrator.start(
        saga_name="dedupe",
        initial_data={"value": 1},
        aggregation_id="agg-dup",
    )
    state = await admin.get_saga(first_saga_id)
    assert state.status == SagaStatus.SUSPENDED

    with pytest.raises(ActiveSagaAlreadyExistsError):
        await orchestrator.start(
            saga_name="dedupe",
            initial_data={"value": 2},
            aggregation_id="agg-dup",
        )


@pytest.mark.asyncio
async def test_retry_and_run_due(session_maker):
    flaky = FlakyStep()
    builder = SagaBuilder()
    first_ref = builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=flaky,
        depends_on=first_ref,
        input_map=lambda out: NextInput(value=out.value),
        retry_policy=ExponentialRetry(max_attempts=2, base_delay=timedelta(seconds=0)),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    orchestrator.register("demo", builder.build())
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )

    saga_id = await orchestrator.start(
        saga_name="demo",
        initial_data={"value": 5},
        aggregation_id="agg-1",
        trace_id="trace-1",
    )

    state_after_first_attempt = await admin.get_saga(saga_id)
    assert state_after_first_attempt.status == SagaStatus.SUSPENDED
    assert state_after_first_attempt.retry_counter == 1

    resumed = await orchestrator.run_due(limit=10)
    assert resumed == 1

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPLETED
    assert flaky.calls == 2


@pytest.mark.asyncio
async def test_run_due_recovers_expired_running_step(session_maker):
    recoverable = RecoverableStep()
    builder = SagaBuilder()
    builder.add_step(
        step=recoverable,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
        execution_lease=timedelta(seconds=0),
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("recover", builder.build())

    start_task = asyncio.create_task(
        orchestrator.start(
            saga_name="recover",
            initial_data={"value": 1},
            aggregation_id="agg-recover",
        )
    )
    await recoverable.started.wait()
    await asyncio.sleep(0)
    start_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start_task

    async with session_maker() as session:
        saga = await orchestrator.repository.get_active_by_aggregation_id_for_update(
            session,
            "agg-recover",
        )
        assert saga is not None
        recovered_saga_id = saga.id

    state_before = await admin.get_saga(recovered_saga_id)
    assert state_before.status == SagaStatus.RUNNING

    resumed = await orchestrator.run_due(limit=10)
    assert resumed == 1

    state_after = await admin.get_saga(recovered_saga_id)
    assert state_after.status == SagaStatus.COMPLETED
    assert recoverable.calls == 2


@pytest.mark.asyncio
async def test_three_step_http_and_queue_style_flow(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=HttpStep(),
        input_map=lambda ctx: HttpInput(order_id=ctx.initial_data["order_id"]),
    )
    builder.add_step(
        step=ReserveQueueStep(),
        input_map=lambda ctx: ReserveQueueInput(
            order_id=ctx.initial_data["order_id"],
            gateway_url=ctx.step_outputs["step_0"]["gateway_url"],
            correlation_id=f"reserve-{ctx.initial_data['order_id']}",
        ),
    )
    builder.add_step(
        step=ActivateQueueStep(),
        input_map=lambda ctx: ActivateQueueInput(
            reservation_id=ctx.step_outputs["step_1"]["reservation_id"],
            correlation_id=f"activate-{ctx.step_outputs['step_1']['reservation_id']}",
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        outbox_model_class=IntegrationOutboxMessage,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("http-queue-3", builder.build())

    queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()

    class QueuePublisher:
        async def publish(
            self,
            *,
            topic: str,
            payload: dict[str, object],
            key: str | None = None,
            headers: dict[str, object] | None = None,
        ) -> None:
            await queue.put(
                {
                    "topic": topic,
                    "payload": payload,
                    "headers": headers or {},
                }
            )

    dispatcher = OutboxDispatcher(
        session_maker=session_maker,
        model_class=IntegrationOutboxMessage,
        publisher=QueuePublisher(),
    )

    saga_id = await orchestrator.start(
        saga_name="http-queue-3",
        initial_data={"order_id": "order-200"},
        aggregation_id="agg-http-queue-3",
    )

    state_after_start: SagaAdminSnapshot = await admin.get_saga(saga_id)
    assert state_after_start.status == SagaStatus.SUSPENDED
    assert state_after_start.current_step_index == 1
    assert len(state_after_start.context.awaiting_event_types) == 2
    assert "reserve.success" in state_after_start.context.awaiting_event_types
    assert "reserve.failed" in state_after_start.context.awaiting_event_types

    processed = await dispatcher.run_once(limit=10)
    assert processed == 1
    first_command = await queue.get()
    queue.task_done()
    first_headers = first_command["headers"]
    assert isinstance(first_headers, dict)

    reserve_token = (await admin.get_saga(saga_id)).step_execution_token
    await orchestrator.notify(
        saga_id=saga_id,
        token=reserve_token,
        event=NotifyEvent(
            event_id="evt-reserve-1",
            event_type="reserve.success",
            correlation_id=first_headers["correlation_id"],
            payload={"reservation_id": "res-200"},
        ),
    )

    state_after_reserve = await admin.get_saga(saga_id)
    assert state_after_reserve.status == SagaStatus.SUSPENDED
    assert state_after_reserve.current_step_index == 2
    assert len(state_after_start.context.awaiting_event_types) == 2
    assert "activate.success" in state_after_reserve.context.awaiting_event_types
    assert "activate.failed" in state_after_reserve.context.awaiting_event_types

    processed = await dispatcher.run_once(limit=10)
    assert processed == 1
    second_command = await queue.get()
    queue.task_done()
    second_headers = second_command["headers"]
    assert isinstance(second_headers, dict)

    activate_token = (await admin.get_saga(saga_id)).step_execution_token
    await orchestrator.notify(
        saga_id=saga_id,
        token=activate_token,
        event=NotifyEvent(
            event_id="evt-activate-1",
            event_type="activate.success",
            correlation_id=second_headers["correlation_id"],
            payload={"deployment_id": "dep-200"},
        ),
    )

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPLETED
    assert final_state.current_step_index == 3


@pytest.mark.asyncio
async def test_get_snapshot_returns(session_maker):
    builder = SagaBuilder()
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )

    saga_name = "test_snapshot_saga"
    orchestrator.register(saga_name, builder.build())

    aggregation_id = "agg-snap-1"
    trace_id = "trace-snap-1"

    saga_id = await orchestrator.start(
        saga_name=saga_name,
        initial_data={"value": 10},
        aggregation_id=aggregation_id,
        trace_id=trace_id,
    )

    snapshot = await orchestrator.get_snapshot(saga_id)

    assert snapshot is not None
    assert snapshot.id == saga_id
    assert snapshot.saga_name == saga_name
    assert snapshot.aggregation_id == aggregation_id
    assert snapshot.trace_id == trace_id
    assert snapshot.status == SagaStatus.COMPLETED
    assert snapshot.current_step_index == 1
    assert snapshot.deadline_at is None
    assert snapshot.last_error is None


@pytest.mark.asyncio
async def test_timed_out_status_on_await_event_deadline(session_maker):
    """
    Проверяет, что сага переходит в статус TIMEOUT, если истекает время ожидания события.
    """
    builder = SagaBuilder()
    builder.add_step(
        step=WaitingWithTimeoutStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("await-timeout", builder.build())
    saga_id = await orchestrator.start(
        saga_name="await-timeout",
        initial_data={"value": 1},
        aggregation_id="agg-await-timeout",
    )

    state_before = await admin.get_saga(saga_id)
    assert state_before.status == SagaStatus.SUSPENDED
    assert state_before.deadline_at is not None

    await asyncio.sleep(0.1)
    resumed = await orchestrator.run_due()
    assert resumed == 0
    state_after = await admin.get_saga(saga_id)
    assert state_after.status == SagaStatus.TIMEOUT
    assert "Timed out waiting for event" in state_after.last_error
    assert state_after.deadline_at is None

    assert len(state_after.step_history) == 2
    assert state_after.step_history[0].status == SagaStepStatus.WAITING
    assert state_after.step_history[0].phase == SagaStepPhase.EXECUTE
    assert not state_after.step_history[0].error
    assert state_after.step_history[1].status == SagaStepStatus.TIMEOUT
    assert state_after.step_history[1].phase == SagaStepPhase.EXECUTE
    assert "Timed out" in state_after.step_history[1].error

    assert state_after.context.awaiting_event_types == ()
    assert state_after.context.awaiting_correlation_id is None
    assert state_after.context.awaiting_until is None


@pytest.mark.asyncio
async def test_compensate_after_timeout_resolves_deadlock(session_maker):
    """
    Проверяет возможность запуска компенсации для саги, зависшей со статусом TIMEOUT.
    Убеждается, что админ может спасти сагу без правки БД.
    """
    first_step = CompensatingStep()
    builder = SagaBuilder()
    ref = builder.add_step(
        step=first_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=WaitingWithTimeoutStep(),
        depends_on=ref,
        input_map=lambda out: StartInput(value=out.value),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("comp-timeout", builder.build())

    saga_id = await orchestrator.start(
        saga_name="comp-timeout",
        initial_data={"value": 1},
        aggregation_id="agg-comp-timeout",
    )

    await asyncio.sleep(0.1)
    await orchestrator.run_due()

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.TIMEOUT

    await admin.compensate_step(saga_id)

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPENSATED
    assert first_step.compensated is True


@pytest.mark.asyncio
async def test_retry_after_timeout_processes_successfully(session_maker):
    """
    Проверяет успешный ретрай саги после таймаута. Убеждается, что нет мгновенного
    повторного таймаута из-за старого неочищенного кеша ожидания.
    """

    builder = SagaBuilder()
    builder.add_step(
        step=RetryWaitStep(),
        input_map=lambda ctx: RetryWaitInput(
            value=ctx.initial_data["value"],
        ),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("retry-timeout", builder.build())

    saga_id = await orchestrator.start(
        saga_name="retry-timeout",
        initial_data={"value": 1},
        aggregation_id="agg-retry-timeout",
    )

    await asyncio.sleep(0.1)
    await orchestrator.run_due()
    state1 = await admin.get_saga(saga_id)
    assert state1.status == SagaStatus.TIMEOUT

    await admin.retry_step(saga_id)
    state2 = await admin.get_saga(saga_id)

    assert state2.status == SagaStatus.SUSPENDED
    assert state2.context.awaiting_event_types == ("some.event",)

    await orchestrator.notify(
        saga_id=saga_id,
        token=state2.step_execution_token,
        event=NotifyEvent(
            event_id="evt-retry-123", event_type="some.event", payload={"value": 42}
        ),
    )

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPLETED


@pytest.mark.asyncio
async def test_event_does_not_leak_into_compensation_after_failure(session_maker):
    """
    Проверяет, что если шаг упал после пробуждения от события,
    это событие очищается и не 'протекает' в компенсацию предыдущих шагов.
    """
    tracker_step = CompensateEventTrackerStep()
    fail_step = AsyncFailStep()

    builder = SagaBuilder()
    step1_ref = builder.add_step(
        step=tracker_step,
        input_map=lambda ctx: LeakTrackInput(value=ctx.initial_data["correlation"]),
    )
    builder.add_step(
        step=fail_step,
        depends_on=step1_ref,
        input_map=lambda out: AsyncFailInput(value=out.value),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("leak_test_saga", builder.build())

    saga_id = await orchestrator.start(
        saga_name="leak_test_saga",
        initial_data={"correlation": "corr-leak-123"},
        aggregation_id="agg-leak-test",
    )

    state1 = await admin.get_saga(saga_id)
    assert state1.status == SagaStatus.SUSPENDED
    assert state1.current_step_index == 1

    await orchestrator.notify(
        saga_id=saga_id,
        token=state1.step_execution_token,
        event=NotifyEvent(
            event_id="evt-leak",
            event_type="fail.event",
            correlation_id="corr-leak-123",
            payload={"reason": "fatal error"},
        ),
    )

    final_state = await admin.get_saga(saga_id)
    assert final_state.status == SagaStatus.COMPENSATED

    assert tracker_step.compensate_event_type is None, (
        f"Event leaked into compensation! Expected None, got: {tracker_step.compensate_event_type}"
    )


@pytest.mark.asyncio
async def test_saga_fails_gracefully_on_input_map_error_forward(session_maker):
    """
    Проверяет, что если маппер input_map выбрасывает исключение,
    SagaEngine не крашится, а безопасно переводит сагу в статус FAILED
    с сохранением ошибки в step_history.
    """
    builder = SagaBuilder(compensate_on_failure=False)
    builder.add_step(
        step=AddOneStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    def crashing_map(ctx):
        raise KeyError("missing_crucial_key")

    builder.add_step(
        step=AddOneStep(),
        input_map=crashing_map,
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("map_fail_forward", builder.build())

    saga_id = await orchestrator.start(
        saga_name="map_fail_forward",
        initial_data={"value": 1},
        aggregation_id="agg-fail-forward",
    )

    state = await admin.get_saga(saga_id)

    assert state.status == SagaStatus.FAILED
    assert "missing_crucial_key" in state.last_error

    assert len(state.step_history) == 2
    error_entry = state.step_history[1]

    assert error_entry.phase == SagaStepPhase.EXECUTE
    assert error_entry.status == SagaStepStatus.ERROR
    assert "mapping_failed" in error_entry.input.get("_error", "")
    assert "missing_crucial_key" in error_entry.error


@pytest.mark.asyncio
async def test_saga_compensates_on_input_map_error(session_maker):
    """
    Проверяет, что если маппер падает, а у саги включена настройка compensate_on_failure=True,
    то она корректно переходит в COMPENSATING и откатывает предыдущие шаги.
    """
    builder = SagaBuilder(compensate_on_failure=True)
    first_step = CompensatingStep()
    builder.add_step(
        step=first_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    def bad_mapper(ctx):
        raise ValueError("Invalid mapping payload")

    builder.add_step(
        step=AddOneStep(),
        input_map=bad_mapper,
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("map_fail_compensate", builder.build())

    saga_id = await orchestrator.start(
        saga_name="map_fail_compensate",
        initial_data={"value": 10},
        aggregation_id="agg-fail-comp",
    )

    state = await admin.get_saga(saga_id)

    assert state.status == SagaStatus.COMPENSATED
    assert first_step.compensated is True

    assert len(state.step_history) == 3
    assert state.step_history[0].status == SagaStepStatus.SUCCESS
    assert state.step_history[1].status == SagaStepStatus.ERROR
    assert state.step_history[1].phase == SagaStepPhase.EXECUTE
    assert state.step_history[2].status == SagaStepStatus.SUCCESS
    assert state.step_history[2].phase == SagaStepPhase.COMPENSATE


@pytest.mark.asyncio
async def test_saga_handles_compensation_preparation_error(session_maker):
    """
    Проверяет, что если оркестратор не может восстановить модели входных/выходных данных
    во время отката (например, структура Pydantic поменялась, или данные в БД битые),
    """
    builder = SagaBuilder(compensate_on_failure=True)
    first_step = CompensatingStep()
    builder.add_step(
        step=first_step,
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )
    builder.add_step(
        step=WaitingWithTimeoutStep(),
        input_map=lambda ctx: StartInput(value=ctx.initial_data["value"]),
    )

    orchestrator = SagaOrchestrator[IntegrationSagaState, IntegrationSagaHistory](
        model_class=IntegrationSagaState,
        history_model_class=IntegrationSagaHistory,
        session_maker=session_maker,
    )
    admin = SagaAdmin[IntegrationSagaState, IntegrationSagaHistory](
        engine=orchestrator.engine
    )
    orchestrator.register("comp_prep_fail", builder.build())

    saga_id = await orchestrator.start(
        saga_name="comp_prep_fail",
        initial_data={"value": 5},
        aggregation_id="agg-comp-prep",
    )

    # Ломаем JSON в базе данных
    async with session_maker() as session:
        saga = await orchestrator.repository.get_for_update(session, saga_id)
        saga.step_history[0].output = {"value": "NOT_AN_INTEGER"}
        await session.commit()

    await admin.compensate_step(saga_id)

    state = await admin.get_saga(saga_id)
    assert state.status == SagaStatus.FAILED
    assert "Compensation input preparation failed" in state.last_error

    comp_error_entry = state.step_history[-1]
    assert comp_error_entry.phase == SagaStepPhase.COMPENSATE
    assert comp_error_entry.status == SagaStepStatus.ERROR
    assert "compensation_mapping_failed" in comp_error_entry.input.get("_error", "")
    assert first_step.compensated is False
