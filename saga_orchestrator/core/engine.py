from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Generic, TypeVar
from uuid import UUID

from loguru import logger
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..domain.exceptions import SagaDefinitionError, SagaNotFoundError, SagaStateError
from ..domain.mixins import SagaStateMixin
from ..domain.models import (
    AwaitingEvent,
    InputContext,
    NotifyEvent,
    NotifyResult,
    SagaAdminSnapshot,
    SagaDefinition,
    SagaSnapshot,
    StepAwaitEvent,
    StepDefinition,
)
from ..domain.models.context import SagaContext, SagaStepHistoryEntry
from ..domain.models.enums import SagaStatus, SagaStepPhase, SagaStepStatus
from ..inbox.contracts import ClaimedInboxMessage, InboxWriteMessage, InboxWriter
from ..inbox.dispatcher import InboxDispatcher, InboxProcessOutcome, InboxProcessStatus
from ..inbox.models import InboxMessageMixin
from ..inbox.repository import InboxRepository
from ..outbox.contracts import OutboxWriteMessage, OutboxWriter
from ..outbox.event import OutboxEvent
from ..outbox.factory import DefaultOutboxMessageFactory, OutboxMessageFactory
from ..outbox.models import OutboxMessageMixin
from ..outbox.repository import OutboxRepository
from ..outbox.serialization import JsonOutboxSerializer, OutboxSerializer
from .repository import SagaRepository

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaEngine(Generic[ModelT]):
    """Execute, resume, recover, and administrate saga instances."""

    def __init__(
        self,
        *,
        model_class: type[ModelT],
        session_maker: async_sessionmaker[AsyncSession],
        inbox_model_class: type[InboxMessageMixin] | None = None,
        inbox_writer: InboxWriter | None = None,
        outbox_model_class: type[OutboxMessageMixin] | None = None,
        outbox_writer: OutboxWriter | None = None,
        outbox_serializer: OutboxSerializer | None = None,
        outbox_message_factory: OutboxMessageFactory | None = None,
        execution_lease: timedelta = timedelta(minutes=5),
    ) -> None:
        """Initialize the engine dependencies and execution lease."""
        self._model_class = model_class
        self._session_maker = session_maker
        self._execution_lease = execution_lease
        self._repository = SagaRepository(model_class)
        self._inbox_repository: InboxRepository[InboxMessageMixin] | None = None
        if inbox_writer is not None:
            self._inbox_writer: InboxWriter | None = inbox_writer
        elif inbox_model_class is not None:
            self._inbox_repository = InboxRepository(inbox_model_class)
            self._inbox_writer = self._inbox_repository
        else:
            self._inbox_writer = None
        self._inbox_dispatcher: InboxDispatcher | None = None
        if self._inbox_writer is not None:
            self._inbox_dispatcher = InboxDispatcher(
                session_maker=session_maker,
                writer=self._inbox_writer,
                processor=self,
            )
        self._outbox_repository: OutboxRepository[OutboxMessageMixin] | None = None
        if outbox_writer is not None:
            self._outbox_writer: OutboxWriter | None = outbox_writer
        elif outbox_model_class is not None:
            self._outbox_repository = OutboxRepository(outbox_model_class)
            self._outbox_writer = self._outbox_repository
        else:
            self._outbox_writer = None
        self._outbox_serializer = outbox_serializer or JsonOutboxSerializer(
            normalize=self._serialize_value
        )
        self._outbox_message_factory = (
            outbox_message_factory or DefaultOutboxMessageFactory()
        )
        self._registry: dict[str, SagaDefinition] = {}

    @property
    def repository(self) -> SagaRepository[ModelT]:
        """Return the repository used by the engine."""
        return self._repository

    @property
    def inbox_repository(self) -> InboxRepository[InboxMessageMixin] | None:
        """Return the inbox repository used by the engine."""
        return self._inbox_repository

    @property
    def inbox_writer(self) -> InboxWriter | None:
        """Return the inbox writer used by the engine."""
        return self._inbox_writer

    @property
    def outbox_repository(self) -> OutboxRepository[OutboxMessageMixin] | None:
        """Return the outbox repository used by the engine."""
        return self._outbox_repository

    @property
    def outbox_writer(self) -> OutboxWriter | None:
        """Return the outbox writer used by the engine."""
        return self._outbox_writer

    def register(self, name: str, saga_definition: SagaDefinition) -> None:
        """Register a saga definition under a runtime name."""
        if name in self._registry:
            raise SagaDefinitionError(f"Saga '{name}' is already registered")
        self._registry[name] = saga_definition

    async def start(
        self,
        *,
        saga_name: str,
        initial_data: BaseModel | dict[str, Any] | Any,
        aggregation_id: str,
        trace_id: str | None = None,
    ) -> UUID:
        """Create a new saga instance and start executing it."""
        if saga_name not in self._registry:
            raise SagaDefinitionError(f"Saga '{saga_name}' is not registered")

        normalized_initial = self._serialize_value(initial_data)
        saga_trace_id = trace_id or str(uuid.uuid4())
        saga_id = uuid.uuid4()
        definition = self._registry[saga_name]
        initial_deadline = self._running_deadline_for_step(
            definition.steps[0],
            now=datetime.now(UTC),
        )
        context = SagaContext(
            saga_id=saga_id,
            saga_name=saga_name,
            initial_data=normalized_initial,
            step_outputs={},
        )
        async with self._session_maker() as session:
            async with session.begin():
                await self._repository.ensure_no_active_aggregation_conflict(
                    session,
                    aggregation_id,
                )
                saga = self._model_class(
                    id=saga_id,
                    aggregation_id=aggregation_id,
                    trace_id=saga_trace_id,
                    saga_name=saga_name,
                    status=SagaStatus.RUNNING,
                    current_step_index=0,
                    step_execution_token=uuid.uuid4(),
                    context=context,
                    step_history=[],
                    deadline_at=initial_deadline,
                    retry_counter=0,
                )
                await self._repository.create(session, saga)
                if definition.on_start_map:
                    if self._outbox_writer is None:
                        raise SagaStateError(
                            "on_start is configured, but outbox writer is not"
                        )

                    input_ctx = InputContext(
                        saga_id=saga.id,
                        initial_data=context.initial_data,
                        context=context,
                        step_outputs={},
                    )
                    outbox_events = definition.on_start_map(input_ctx) or []
                    if outbox_events:
                        messages = self._outbox_message_factory.build_messages(
                            saga_id=saga.id,
                            aggregation_id=saga.aggregation_id,
                            step_id="__saga_start__",
                            trace_id=saga.trace_id,
                            step_input=None,
                            step_output=None,
                            events=outbox_events,
                            now=datetime.now(UTC),
                            serializer=self._outbox_serializer,
                        )
                        await self._outbox_writer.save(session, messages)
        await self._drive(saga_id)
        return saga_id

    async def notify(
        self,
        *,
        saga_id: UUID,
        token: UUID,
        event: NotifyEvent | dict[str, Any] | Any | None = None,
    ) -> bool:
        """Resume a suspended saga when the provided execution token matches."""
        result = await self.notify_detailed(
            saga_id=saga_id,
            token=token,
            event=event,
        )
        return result == NotifyResult.ACCEPTED

    async def notify_detailed(
        self,
        *,
        saga_id: UUID,
        token: UUID,
        event: NotifyEvent | dict[str, Any] | Any | None = None,
    ) -> NotifyResult:
        """Resume a suspended saga and return a detailed notify outcome."""
        normalized_event, idempotency_key = self._normalize_notify_event(event)
        should_drive_forward = False

        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status not in {
                    SagaStatus.SUSPENDED,
                    SagaStatus.COMPENSATING_SUSPENDED,
                }:
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.NOT_SUSPENDED,
                    )
                    return NotifyResult.NOT_SUSPENDED
                if saga.step_execution_token != token:
                    logger.info("Ignoring stale notify for saga_id=%s", saga_id)
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.STALE_TOKEN,
                    )
                    return NotifyResult.STALE_TOKEN

                context: SagaContext = saga.context
                processed_ids = context.processed_event_ids
                if idempotency_key is not None and idempotency_key in processed_ids:
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.DUPLICATE,
                    )
                    return NotifyResult.DUPLICATE

                expected_types: tuple[str, ...] = context.awaiting_event_types
                expected_type: str | None = context.awaiting_event_type
                if normalized_event is None and (
                    expected_type is not None
                    or (
                        isinstance(expected_types, (list, tuple))
                        and len(expected_types) > 0
                    )
                ):
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.EVENT_TYPE_MISMATCH,
                    )
                    return NotifyResult.EVENT_TYPE_MISMATCH
                if (
                    normalized_event is not None
                    and isinstance(expected_types, (list, tuple))
                    and expected_types
                    and normalized_event.event_type not in expected_types
                ):
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.EVENT_TYPE_MISMATCH,
                    )
                    return NotifyResult.EVENT_TYPE_MISMATCH
                if (
                    expected_type is not None
                    and normalized_event is not None
                    and normalized_event.event_type != expected_type
                ):
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.EVENT_TYPE_MISMATCH,
                    )
                    return NotifyResult.EVENT_TYPE_MISMATCH

                expected_correlation: str | None = context.awaiting_correlation_id
                if (
                    expected_correlation is not None
                    and normalized_event is not None
                    and normalized_event.correlation_id != expected_correlation
                ):
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.CORRELATION_MISMATCH,
                    )
                    return NotifyResult.CORRELATION_MISMATCH

                awaiting_until = self._parse_iso_datetime(context.awaiting_until)
                if awaiting_until is not None and datetime.now(UTC) > awaiting_until:
                    self._append_notify_log(
                        saga=saga,
                        event=normalized_event,
                        result=NotifyResult.EXPIRED,
                    )
                    return NotifyResult.EXPIRED

                if normalized_event is not None:
                    payload: Any = self._serialize_value(normalized_event.payload)
                    meta: dict[str, Any] = self._serialize_value(
                        normalized_event.model_dump(mode="json")
                    )
                    context.add_event(payload, meta)
                    if idempotency_key is not None:
                        context.add_processed_event(idempotency_key)

                context.clear_awaiting_state()

                if saga.status == SagaStatus.SUSPENDED:
                    saga.status = SagaStatus.RUNNING
                    should_drive_forward = True
                else:  # saga.status == SagaStatus.COMPENSATING_SUSPENDED
                    saga.status = SagaStatus.COMPENSATING
                    should_drive_forward = False

                step_def = self._registry[context.saga_name].steps[
                    saga.current_step_index
                ]
                saga.deadline_at = self._running_deadline_for_step(
                    step_def,
                    now=datetime.now(UTC),
                )
                saga.step_execution_token = uuid.uuid4()
                self._append_notify_log(
                    saga=saga,
                    event=normalized_event,
                    result=NotifyResult.ACCEPTED,
                )

        if should_drive_forward:
            await self._drive(saga_id)
        else:
            await self._run_compensation(saga_id)

        return NotifyResult.ACCEPTED

    async def ingest_event(
        self,
        *,
        event: NotifyEvent | dict[str, Any] | Any,
        saga_id: UUID | None = None,
        aggregation_id: str | None = None,
    ) -> bool:
        """Persist one inbound event into inbox storage for asynchronous processing."""
        if self._inbox_writer is None:
            raise SagaStateError("Inbox writer is not configured in SagaEngine")

        normalized_event, idempotency_key = self._normalize_notify_event(event)
        if normalized_event is None:
            raise SagaStateError("Inbox ingestion requires a non-empty event payload")
        if idempotency_key is None:
            raise SagaStateError("Inbox ingestion requires event.event_id")
        if saga_id is None and aggregation_id is None:
            raise SagaStateError(
                "Inbox ingestion requires saga_id or aggregation_id for routing"
            )

        message = InboxWriteMessage(
            event_id=idempotency_key,
            saga_id=saga_id,
            aggregation_id=aggregation_id,
            event_type=normalized_event.event_type,
            correlation_id=normalized_event.correlation_id,
            payload=self._serialize_value(normalized_event.payload),
            source=normalized_event.source,
            occurred_at=normalized_event.occurred_at,
        )

        async with self._session_maker() as session:
            async with session.begin():
                return await self._inbox_writer.save(session, message)

    async def run_inbox_due(self, *, limit: int = 100) -> int:
        """Process due inbox events through the configured inbox dispatcher."""
        if self._inbox_dispatcher is None:
            return 0
        return await self._inbox_dispatcher.run_once(limit=limit)

    async def await_event(
        self,
        *,
        saga_id: UUID,
        event: AwaitingEvent,
    ) -> UUID:
        """Configure a suspended saga to wait for a specific external event."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status != SagaStatus.SUSPENDED:
                    raise SagaStateError(
                        "Cannot configure external wait unless saga is suspended "
                        f"(status={saga.status})"
                    )
                context: SagaContext = saga.context

                event_types: tuple[str, ...] = (
                    tuple(event.event_types) if event.event_types else ()
                )
                if event.event_type and not event_types:
                    event_types = (event.event_type,)

                context.set_awaiting_state(
                    event_types=event_types,
                    correlation_id=event.correlation_id,
                    until=event.until.isoformat() if event.until else None,
                )

                # External event waits should not be auto-resumed by run_due.
                saga.deadline_at = None
                saga.step_execution_token = uuid.uuid4()
                return saga.step_execution_token

    async def run_due(self, *, limit: int = 100) -> int:
        """Resume due running, suspended, and compensating sagas."""
        now = datetime.now(UTC)
        ready_ids: list[UUID] = []
        compensation_ids: list[UUID] = []

        async with self._session_maker() as session:
            async with session.begin():
                due_running = await self._repository.due_running(
                    session, now=now, limit=limit
                )
                remaining = max(limit - len(due_running), 0)
                due_suspended = await self._repository.due_suspended(
                    session, now=now, limit=remaining
                )
                remaining -= len(due_suspended)
                due_compensating = await self._repository.due_compensating(
                    session, now=now, limit=remaining
                )
                remaining -= len(due_compensating)

                due_compensating_suspended = (
                    await self._repository.due_compensating_suspended(
                        session, now=now, limit=max(remaining, 0)
                    )
                )

                for saga in [*due_running, *due_suspended]:
                    saga.status = SagaStatus.RUNNING
                    saga.step_execution_token = uuid.uuid4()
                    saga.deadline_at = now + self._execution_lease
                    ready_ids.append(saga.id)

                for saga in [*due_compensating, *due_compensating_suspended]:
                    saga.status = SagaStatus.COMPENSATING
                    saga.step_execution_token = uuid.uuid4()
                    saga.deadline_at = now + self._execution_lease
                    compensation_ids.append(saga.id)

        for saga_id in ready_ids:
            await self._drive(saga_id)
        for saga_id in compensation_ids:
            await self._run_compensation(saga_id)
        return len(ready_ids) + len(compensation_ids)

    async def get_snapshot(self, saga_id: UUID) -> SagaSnapshot:
        """Return the snapshot view of one saga."""
        async with self._session_maker() as session:
            saga = await self._repository.get_for_update(session, saga_id)
            return self._to_snapshot(saga)

    async def get_admin_snapshot(self, saga_id: UUID) -> SagaAdminSnapshot:
        """Return the administrative view of one saga."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get(session, saga_id)
                return SagaAdminSnapshot(
                    id=saga.id,
                    aggregation_id=saga.aggregation_id,
                    trace_id=saga.trace_id,
                    saga_name=saga.saga_name,
                    status=saga.status,
                    current_step_index=saga.current_step_index,
                    step_execution_token=saga.step_execution_token,
                    retry_counter=saga.retry_counter,
                    deadline_at=saga.deadline_at,
                    last_error=saga.last_error,
                    context=saga.context,
                    step_history=saga.step_history,
                )

    async def resume(self, saga_id: UUID) -> None:
        """Resume forward execution of one saga."""
        await self._drive(saga_id)

    async def retry_step(self, saga_id: UUID) -> None:
        """Reset the current step state and retry it."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status not in {SagaStatus.SUSPENDED, SagaStatus.FAILED}:
                    raise SagaStateError(
                        f"Cannot retry step when saga status is {saga.status}"
                    )

                saga_name = saga.context.saga_name
                definition = self._registry[saga_name]
                if saga.current_step_index >= len(definition.steps):
                    raise SagaStateError(
                        "Cannot retry saga because there is no current step to resume"
                    )
                if saga.status == SagaStatus.FAILED and self._has_compensation_history(
                    saga.step_history
                ):
                    raise SagaStateError(
                        "Cannot retry saga after compensation has already started"
                    )
                step_def = definition.steps[saga.current_step_index]

                saga.status = SagaStatus.RUNNING
                saga.retry_counter = 0
                saga.last_error = None
                saga.step_execution_token = uuid.uuid4()
                saga.deadline_at = self._running_deadline_for_step(
                    step_def,
                    now=datetime.now(UTC),
                )

        await self._drive(saga_id)

    async def compensate_step(self, saga_id: UUID) -> None:
        """Switch one saga into compensation and execute rollback."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status not in {
                    SagaStatus.SUSPENDED,
                    SagaStatus.FAILED,
                    SagaStatus.COMPENSATING,
                    SagaStatus.COMPENSATING_SUSPENDED,
                }:
                    raise SagaStateError(
                        "Cannot start compensation unless saga is suspended, failed, "
                        f"or already compensating (status={saga.status})"
                    )
                if saga.current_step_index <= 0:
                    raise SagaStateError(
                        "Cannot compensate saga because there is no completed step to roll back"
                    )

                saga.status = SagaStatus.COMPENSATING
                saga.retry_counter = 0
                saga.step_execution_token = uuid.uuid4()
                saga.deadline_at = datetime.now(UTC) + self._execution_lease

        await self._run_compensation(saga_id)

    async def abort(self, saga_id: UUID) -> None:
        """Mark a saga as failed and invalidate its current execution token."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                saga.status = SagaStatus.FAILED
                saga.deadline_at = None
                saga.last_error = saga.last_error or "Aborted by admin"
                saga.step_execution_token = uuid.uuid4()

    async def skip_step(
        self,
        saga_id: UUID,
        mock_output: BaseModel | dict[str, Any] | None = None,
    ) -> None:
        """Mark the current step as successful and continue execution."""
        should_resume = False

        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status != SagaStatus.SUSPENDED:
                    raise SagaStateError(
                        "Cannot skip step unless saga is suspended on the current step "
                        f"(status={saga.status})"
                    )

                saga_name = saga.context.saga_name
                definition = self._registry[saga_name]
                if saga.current_step_index >= len(definition.steps):
                    raise SagaStateError("No step available for skipping")

                step_def = definition.steps[saga.current_step_index]
                if mock_output is None:
                    output_payload: dict[str, Any] = {}
                elif isinstance(mock_output, BaseModel):
                    output_payload = mock_output.model_dump(mode="json")
                else:
                    output_payload = mock_output

                output_model = step_def.output_model.model_validate(output_payload)
                token = saga.step_execution_token or uuid.uuid4()

                step_history: SagaStepHistoryEntry = {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "phase": SagaStepPhase.EXECUTE,
                    "status": SagaStepStatus.SUCCESS,
                    "step_id": step_def.step_id,
                    "step_name": type(step_def.step).__name__,
                    "attempt": 0,
                    "token": str(token),
                    "input": {"_admin": "skip_step"},
                    "output": output_model.model_dump(mode="json"),
                    "error": None,
                    "skipped": True,
                }
                saga.step_history.append(step_history)

                saga.context.save_step_output(
                    step_def.step_id, output_model.model_dump(mode="json")
                )

                saga.current_step_index += 1
                saga.retry_counter = 0
                saga.last_error = None
                saga.step_execution_token = uuid.uuid4()

                if saga.current_step_index >= len(definition.steps):
                    saga.status = SagaStatus.COMPLETED
                    saga.deadline_at = None
                else:
                    next_step = definition.steps[saga.current_step_index]
                    saga.status = SagaStatus.RUNNING
                    saga.deadline_at = self._running_deadline_for_step(
                        next_step,
                        now=datetime.now(UTC),
                    )
                    should_resume = True

        if should_resume:
            await self._drive(saga_id)

    async def _handle_terminal_state(
        self,
        session: AsyncSession,
        saga: ModelT,
        definition: SagaDefinition,
    ) -> None:
        """Generate outbox events for terminal state hooks."""
        hook_map: Callable[..., Any] | None = None
        events: list[OutboxEvent] = []

        if saga.status == SagaStatus.COMPLETED and definition.on_completed_map:
            hook_map = definition.on_completed_map
            events = hook_map(saga.context) or []
        elif saga.status == SagaStatus.FAILED and definition.on_failed_map:
            hook_map = definition.on_failed_map
            events = hook_map(saga.context, saga.last_error) or []
        elif saga.status == SagaStatus.COMPENSATED and definition.on_compensated_map:
            hook_map = definition.on_compensated_map
            events = hook_map(saga.context) or []

        if not events:
            return

        if self._outbox_writer is None:
            raise SagaStateError(
                f"{saga.status} hook is configured, but outbox writer is not"
            )

        messages: list[OutboxWriteMessage] = (
            self._outbox_message_factory.build_messages(
                saga_id=saga.id,
                aggregation_id=saga.aggregation_id,
                step_id=f"__saga_{saga.status.lower()}__",  # e.g. __saga_completed__
                trace_id=saga.trace_id,
                step_input=None,
                step_output=None,
                events=events,
                now=datetime.now(UTC),
                serializer=self._outbox_serializer,
            )
        )
        await self._outbox_writer.save(session, messages)

    async def _drive(self, saga_id: UUID) -> None:
        """Execute forward steps until the saga stops progressing."""
        while True:
            prep = await self._prepare_step(saga_id)
            if prep is None:
                return

            step_def = prep["step_def"]
            step_token = prep["step_token"]
            step_input = prep["step_input"]
            attempt_number = prep["attempt_number"]

            success = False
            step_output: BaseModel | None = None
            wait_spec: StepAwaitEvent | None = None
            error: Exception | None = None

            try:
                if step_def.timeout is None:
                    step_result = await step_def.step.execute(step_input)
                else:
                    step_result = await asyncio.wait_for(
                        step_def.step.execute(step_input),
                        timeout=step_def.timeout.total_seconds(),
                    )
                if isinstance(step_result, StepAwaitEvent):
                    wait_spec = step_result
                else:
                    step_output = step_result
                success = True
            except Exception as exc:  # noqa: BLE001
                error = exc

            should_continue = await self._finalize_step(
                saga_id=saga_id,
                step_def=step_def,
                token=step_token,
                step_input=step_input,
                step_output=step_output,
                wait_spec=wait_spec,
                error=error,
                attempt_number=attempt_number,
            )
            if not should_continue:
                return
            if not success:
                return

    async def _prepare_step(self, saga_id: UUID) -> dict[str, Any] | None:
        """Load saga state and prepare the current forward step for execution."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                saga_name = saga.context.saga_name
                if not saga_name or saga_name not in self._registry:
                    raise SagaDefinitionError(
                        f"Unknown saga registered name in state: '{saga_name}'"
                    )

                definition = self._registry[saga_name]
                if saga.status != SagaStatus.RUNNING:
                    return None
                if saga.current_step_index >= len(definition.steps):
                    saga.status = SagaStatus.COMPLETED
                    saga.deadline_at = None
                    saga.last_error = None
                    return None

                step_def = definition.steps[saga.current_step_index]
                step_token = saga.step_execution_token or uuid.uuid4()
                saga.step_execution_token = step_token
                saga.deadline_at = self._running_deadline_for_step(
                    step_def,
                    now=datetime.now(UTC),
                )
                attempt_number = saga.retry_counter + 1
                step_input = self._build_step_input(step_def, saga)

                return {
                    "step_def": step_def,
                    "step_token": step_token,
                    "step_input": step_input,
                    "attempt_number": attempt_number,
                }

    async def _finalize_step(
        self,
        *,
        saga_id: UUID,
        step_def: StepDefinition[Any, Any],
        token: UUID,
        step_input: BaseModel,
        step_output: BaseModel | None,
        wait_spec: StepAwaitEvent | None,
        error: Exception | None,
        attempt_number: int,
    ) -> bool:
        """Persist one forward step result and return whether execution continues."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if (
                    saga.step_execution_token != token
                    or saga.status != SagaStatus.RUNNING
                ):
                    logger.info("Stale step result ignored for saga_id=%s", saga_id)
                    return False
                context: SagaContext = saga.context
                if error is None and wait_spec is not None:
                    if wait_spec.outbox_events:
                        if self._outbox_writer is None:
                            raise SagaStateError(
                                "Step returned StepAwaitEvent with outbox_events, "
                                "but outbox writer is not configured in SagaEngine"
                            )
                        now = datetime.now(UTC)
                        await_messages = [
                            OutboxWriteMessage(
                                saga_id=saga.id,
                                aggregation_id=saga.aggregation_id,
                                step_id=step_def.step_id,
                                trace_id=saga.trace_id,
                                topic=event.topic,
                                key=event.key,
                                payload=self._outbox_serializer.serialize_payload(
                                    event.payload
                                ),
                                headers=self._outbox_serializer.serialize_headers(
                                    event.headers
                                ),
                                next_attempt_at=now,
                            )
                            for event in wait_spec.outbox_events
                        ]
                        await self._outbox_writer.save(session, await_messages)

                    saga.step_history.append(
                        self._history_entry(
                            phase=SagaStepPhase.EXECUTE,
                            status=SagaStepStatus.WAITING,
                            step_def=step_def,
                            token=token,
                            attempt=attempt_number,
                            step_input=step_input,
                            step_output=None,
                            error=None,
                        )
                    )

                    event_types: tuple[str, ...] = (
                        tuple(wait_spec.event_types) if wait_spec.event_types else ()
                    )
                    until_iso: str | None = None
                    if wait_spec.until:
                        until = datetime.now(UTC) + wait_spec.until
                        until_iso = until.isoformat()
                        saga.deadline_at = until
                    else:
                        saga.deadline_at = None

                    context.set_awaiting_state(
                        event_types=event_types,
                        correlation_id=wait_spec.correlation_id,
                        until=until_iso,
                    )

                    saga.status = SagaStatus.SUSPENDED
                    saga.last_error = None
                    context.clear_latest_event()
                    saga.step_execution_token = uuid.uuid4()
                    return False

                if error is None and step_output is not None:
                    if step_def.outbox_map is not None:
                        if self._outbox_writer is None:
                            raise SagaStateError(
                                "outbox_map is configured for step "
                                f"'{step_def.step_id}', but outbox writer is not configured in SagaEngine"
                            )
                        outbox_events: list[OutboxEvent] = (
                            step_def.outbox_map(step_input, step_output) or []
                        )
                        if outbox_events:
                            now = datetime.now(UTC)
                            outbox_messages = (
                                self._outbox_message_factory.build_messages(
                                    saga_id=saga.id,
                                    aggregation_id=saga.aggregation_id,
                                    step_id=step_def.step_id,
                                    trace_id=saga.trace_id,
                                    step_input=step_input,
                                    step_output=step_output,
                                    events=outbox_events,
                                    now=now,
                                    serializer=self._outbox_serializer,
                                )
                            )
                            await self._outbox_writer.save(session, outbox_messages)
                    saga.step_history.append(
                        self._history_entry(
                            phase=SagaStepPhase.EXECUTE,
                            status=SagaStepStatus.SUCCESS,
                            step_def=step_def,
                            token=token,
                            attempt=attempt_number,
                            step_input=step_input,
                            step_output=step_output,
                            error=None,
                        )
                    )

                    context.save_step_output(
                        step_def.step_id, self._serialize_value(step_output)
                    )
                    context.clear_latest_event()

                    saga.current_step_index += 1
                    saga.retry_counter = 0
                    saga.deadline_at = None
                    saga.last_error = None
                    saga.step_execution_token = uuid.uuid4()

                    saga_name: str = saga.context.saga_name
                    definition = self._registry[saga_name]
                    if saga.current_step_index >= len(definition.steps):
                        saga.status = SagaStatus.COMPLETED
                        await self._handle_terminal_state(session, saga, definition)
                    return saga.status == SagaStatus.RUNNING

                if error is None:
                    raise SagaStateError(
                        "Step finalization expected either a successful output "
                        "or an execution error"
                    )
                saga.step_history.append(
                    self._history_entry(
                        phase=SagaStepPhase.EXECUTE,
                        status=SagaStepStatus.ERROR,
                        step_def=step_def,
                        token=token,
                        attempt=attempt_number,
                        step_input=step_input,
                        step_output=None,
                        error=error,
                    )
                )

                saga.last_error = repr(error)
                next_attempt = saga.retry_counter + 1
                delay = step_def.retry_policy.next_delay(next_attempt)
                saga.retry_counter = next_attempt
                if delay is not None:
                    saga.status = SagaStatus.SUSPENDED
                    saga.deadline_at = datetime.now(UTC) + delay
                    saga.step_execution_token = uuid.uuid4()
                    return False

                saga_name = context.saga_name
                definition = self._registry[saga_name]
                if definition.compensate_on_failure and saga.current_step_index > 0:
                    saga.status = SagaStatus.COMPENSATING
                    saga.retry_counter = 0
                    saga.deadline_at = datetime.now(UTC) + self._execution_lease
                else:
                    saga.status = SagaStatus.FAILED
                    saga.deadline_at = None
                    await self._handle_terminal_state(session, saga, definition)
                    return False

        await self._run_compensation(saga_id)
        return False

    async def _run_compensation(self, saga_id: UUID) -> None:
        """Execute compensation steps until rollback stops."""
        while True:
            comp_prep = await self._prepare_compensation(saga_id)
            if comp_prep is None:
                return

            step_def = comp_prep["step_def"]
            token = comp_prep["token"]
            original_input = comp_prep["original_input"]
            original_output = comp_prep["original_output"]
            attempt_number = comp_prep["attempt_number"]

            error: Exception | None = None
            wait_spec: StepAwaitEvent | None = None

            try:
                comp_result = await step_def.step.compensate(
                    original_input, original_output
                )
                if isinstance(comp_result, StepAwaitEvent):
                    wait_spec = comp_result
            except Exception as exc:  # noqa: BLE001
                error = exc

            should_continue = await self._finalize_compensation(
                saga_id=saga_id,
                step_def=step_def,
                token=token,
                original_input=original_input,
                original_output=original_output,
                wait_spec=wait_spec,
                error=error,
                attempt_number=attempt_number,
            )
            if not should_continue:
                return

    async def _prepare_compensation(self, saga_id: UUID) -> dict[str, Any] | None:
        """Load saga state and prepare the next compensation step."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status != SagaStatus.COMPENSATING:
                    return None

                saga_name = saga.context.saga_name
                definition = self._registry[saga_name]
                if saga.current_step_index <= 0:
                    return None

                step_idx = saga.current_step_index - 1
                step_def = definition.steps[step_idx]

                execution_entry = None
                for entry in reversed(saga.step_history):
                    if (
                        entry.get("phase") == SagaStepPhase.EXECUTE
                        and entry.get("status") == SagaStepStatus.SUCCESS
                        and entry.get("step_id") == step_def.step_id
                    ):
                        execution_entry = entry
                        break

                if execution_entry is None:
                    saga.status = SagaStatus.FAILED
                    saga.last_error = f"Missing successful execution entry for step '{step_def.step_id}'"
                    saga.deadline_at = None
                    return None

                token = uuid.uuid4()
                saga.step_execution_token = token
                saga.deadline_at = datetime.now(UTC) + self._execution_lease
                attempt_number = saga.retry_counter + 1
                return {
                    "step_def": step_def,
                    "token": token,
                    "attempt_number": attempt_number,
                    "original_input": step_def.input_model.model_validate(
                        execution_entry["input"]
                    ),
                    "original_output": step_def.output_model.model_validate(
                        execution_entry["output"]
                    ),
                }

    async def _finalize_compensation(
        self,
        *,
        saga_id: UUID,
        step_def: StepDefinition[Any, Any],
        token: UUID,
        original_input: BaseModel,
        original_output: BaseModel,
        wait_spec: StepAwaitEvent | None,
        error: Exception | None,
        attempt_number: int,
    ) -> bool:
        """Persist one compensation result and return whether rollback continues."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if (
                    saga.status != SagaStatus.COMPENSATING
                    or saga.step_execution_token != token
                ):
                    return False
                context: SagaContext = saga.context
                if error is None and wait_spec is not None:
                    saga.step_history.append(
                        self._history_entry(
                            phase=SagaStepPhase.COMPENSATE,
                            status=SagaStepStatus.WAITING,
                            step_def=step_def,
                            token=token,
                            attempt=attempt_number,
                            step_input=original_input,
                            step_output=original_output,
                            error=None,
                        )
                    )

                    event_types: tuple[str, ...] = (
                        tuple(wait_spec.event_types) if wait_spec.event_types else ()
                    )
                    until_iso = None
                    if wait_spec.until:
                        until = datetime.now(UTC) + wait_spec.until
                        until_iso = until.isoformat()
                        saga.deadline_at = until
                    else:
                        saga.deadline_at = None

                    context.set_awaiting_state(
                        event_types=event_types,
                        correlation_id=wait_spec.correlation_id,
                        until=until_iso,
                    )

                    saga.status = SagaStatus.COMPENSATING_SUSPENDED
                    saga.last_error = None
                    saga.step_execution_token = uuid.uuid4()
                    return False

                if error is not None:
                    saga.step_history.append(
                        self._history_entry(
                            phase=SagaStepPhase.COMPENSATE,
                            status=SagaStepStatus.ERROR,
                            step_def=step_def,
                            token=token,
                            attempt=attempt_number,
                            step_input=original_input,
                            step_output=original_output,
                            error=error,
                        )
                    )
                    saga.last_error = (
                        f"Compensation failed for step '{step_def.step_id}': {error!r}"
                    )

                    next_attempt = saga.retry_counter + 1
                    delay = step_def.retry_policy.next_delay(next_attempt)
                    saga.retry_counter = next_attempt

                    if delay is not None:
                        saga.status = SagaStatus.COMPENSATING_SUSPENDED
                        saga.deadline_at = datetime.now(UTC) + delay
                        saga.step_execution_token = uuid.uuid4()
                        return False

                    saga.status = SagaStatus.FAILED
                    saga.deadline_at = None
                    definition = self._registry[saga.context.saga_name]
                    await self._handle_terminal_state(session, saga, definition)
                    return False

                saga.step_history.append(
                    self._history_entry(
                        phase=SagaStepPhase.COMPENSATE,
                        status=SagaStepStatus.SUCCESS,
                        step_def=step_def,
                        token=token,
                        attempt=attempt_number,
                        step_input=original_input,
                        step_output=original_output,
                        error=None,
                    )
                )
                saga.current_step_index -= 1
                saga.retry_counter = 0
                saga.last_error = None
                saga.step_execution_token = uuid.uuid4()

                if saga.current_step_index <= 0:
                    saga.status = SagaStatus.COMPENSATED
                    saga.last_error = "Compensation completed successfully"
                    saga.deadline_at = None
                    definition = self._registry[saga.context.saga_name]
                    await self._handle_terminal_state(session, saga, definition)
                    return False

                saga.deadline_at = datetime.now(UTC) + self._execution_lease
                return True

    async def process(self, message: ClaimedInboxMessage) -> InboxProcessOutcome:
        """Process one claimed inbox message and map outcome to dispatcher semantics."""
        saga_id = message.saga_id
        token: UUID | None = None

        if saga_id is not None:
            async with self._session_maker() as session:
                async with session.begin():
                    try:
                        saga = await self._repository.get_for_update(session, saga_id)
                    except SagaNotFoundError:
                        return InboxProcessOutcome(
                            status=InboxProcessStatus.IGNORED,
                            reason="Saga not found",
                        )
                    token = saga.step_execution_token
        elif message.aggregation_id is not None:
            async with self._session_maker() as session:
                async with session.begin():
                    saga = (
                        await self._repository.get_active_by_aggregation_id_for_update(
                            session,
                            message.aggregation_id,
                        )
                    )
                    if saga is None:
                        return InboxProcessOutcome(
                            status=InboxProcessStatus.RETRY,
                            reason="Active saga for aggregation_id not found",
                        )
                    saga_id = saga.id
                    token = saga.step_execution_token
        else:
            return InboxProcessOutcome(
                status=InboxProcessStatus.IGNORED,
                reason="Inbox message has no saga_id or aggregation_id",
            )

        if saga_id is None or token is None:
            return InboxProcessOutcome(
                status=InboxProcessStatus.RETRY,
                reason="Saga execution token is not available yet",
            )

        notify_result = await self.notify_detailed(
            saga_id=saga_id,
            token=token,
            event=NotifyEvent(
                event_id=message.event_id,
                event_type=message.event_type,
                correlation_id=message.correlation_id,
                payload=message.payload,
                source=message.source,
                occurred_at=message.occurred_at,
            ),
        )
        if notify_result in {NotifyResult.ACCEPTED, NotifyResult.DUPLICATE}:
            return InboxProcessOutcome(status=InboxProcessStatus.APPLIED)
        if notify_result == NotifyResult.NOT_SUSPENDED:
            return InboxProcessOutcome(
                status=InboxProcessStatus.IGNORED,
                reason=notify_result.value,
            )
        if notify_result in {
            NotifyResult.STALE_TOKEN,
            NotifyResult.EVENT_TYPE_MISMATCH,
            NotifyResult.CORRELATION_MISMATCH,
            NotifyResult.EXPIRED,
        }:
            return InboxProcessOutcome(
                status=InboxProcessStatus.IGNORED,
                reason=notify_result.value,
            )
        return InboxProcessOutcome(
            status=InboxProcessStatus.IGNORED,
            reason=notify_result.value,
        )

    @staticmethod
    def _build_step_input(
        step_def: StepDefinition[Any, Any],
        saga: ModelT,
    ) -> BaseModel:
        """Build the input model for one step from saga context."""
        context: SagaContext = saga.context

        if step_def.depends_on is not None:
            dep_payload = context.step_outputs.get(step_def.depends_on.step_id)
            if dep_payload is None:
                raise SagaStateError(
                    f"Missing dependency output for step '{step_def.depends_on.step_id}'"
                )
            dep_model = step_def.depends_on.output_model.model_validate(dep_payload)
            mapped = step_def.input_map(dep_model)
        else:
            mapped = step_def.input_map(
                InputContext(
                    saga_id=saga.id,
                    initial_data=context.initial_data,
                    context=context,
                    step_outputs=context.step_outputs,
                    latest_event=context.latest_event,
                    events=context.events,
                )
            )

        if isinstance(mapped, step_def.input_model):
            return mapped
        if isinstance(mapped, dict):
            return step_def.input_model.model_validate(mapped)
        raise SagaStateError(
            f"input_map for step '{step_def.step_id}' "
            f"must return {step_def.input_model.__name__} or dict"
        )

    def _serialize_value(self, value: Any) -> Any:
        """Convert values into JSON-serializable structures."""
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, dict):
            return {key: self._serialize_value(val) for key, val in value.items()}
        if isinstance(value, list):
            return [self._serialize_value(item) for item in value]
        return value

    def _normalize_notify_event(
        self,
        event: NotifyEvent | dict[str, Any] | Any | None,
    ) -> tuple[NotifyEvent | None, str | None]:
        if event is None:
            return None, None
        if isinstance(event, NotifyEvent):
            return event, event.event_id
        if isinstance(event, dict):
            envelope_keys = {
                "event_id",
                "event_type",
                "correlation_id",
                "payload",
                "source",
                "occurred_at",
            }
            if any(key in event for key in envelope_keys):
                notify_event = NotifyEvent.model_validate(event)
                return notify_event, notify_event.event_id
            return NotifyEvent(payload=self._serialize_value(event)), None
        return NotifyEvent(payload=self._serialize_value(event)), None

    @staticmethod
    def _parse_iso_datetime(value: Any) -> datetime | None:
        if not isinstance(value, str):
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed

    def _append_notify_log(
        self,
        *,
        saga: ModelT,
        event: NotifyEvent | None,
        result: NotifyResult,
    ) -> None:
        saga.context.append_notify_log(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "result": result.value,
                "event_id": event.event_id if event is not None else None,
                "event_type": event.event_type if event is not None else None,
                "correlation_id": (event.correlation_id if event is not None else None),
            }
        )

    def _history_entry(
        self,
        *,
        phase: SagaStepPhase,
        status: SagaStepStatus,
        step_def: StepDefinition[Any, Any],
        token: UUID,
        attempt: int,
        step_input: BaseModel,
        step_output: BaseModel | None,
        error: Exception | None,
    ) -> SagaStepHistoryEntry:
        """Return one step history record."""
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "phase": phase,
            "status": status,
            "step_id": step_def.step_id,
            "step_name": type(step_def.step).__name__,
            "attempt": attempt,
            "token": str(token),
            "input": self._serialize_value(step_input),
            "output": (
                self._serialize_value(step_output) if step_output is not None else None
            ),
            "error": repr(error) if error is not None else None,
        }

    @staticmethod
    def _has_compensation_history(step_history: list[dict[str, Any]]) -> bool:
        """Return whether step history contains a compensation entry."""
        return any(
            entry.get("phase") == SagaStepPhase.COMPENSATE for entry in step_history
        )

    def _running_deadline_for_step(
        self,
        step_def: StepDefinition[Any, Any],
        *,
        now: datetime,
    ) -> datetime:
        """Return the deadline for the current step execution."""
        if step_def.timeout is not None:
            return now + step_def.timeout
        return now + self._execution_lease

    @staticmethod
    def _to_snapshot(saga: ModelT) -> SagaSnapshot:
        """Convert a saga ORM object into a snapshot model."""
        return SagaSnapshot(
            id=saga.id,
            aggregation_id=saga.aggregation_id,
            saga_name=saga.saga_name,
            status=saga.status,
            current_step_index=saga.current_step_index,
            retry_counter=saga.retry_counter,
            deadline_at=saga.deadline_at,
            trace_id=saga.trace_id,
            step_execution_token=saga.step_execution_token,
            last_error=saga.last_error,
        )
