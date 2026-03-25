from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Generic, TypeVar
from uuid import UUID

from loguru import logger
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..domain.exceptions import SagaDefinitionError, SagaStateError
from ..domain.mixins import SagaStateMixin
from ..domain.models import InputContext, SagaDefinition, SagaSnapshot, StepDefinition
from ..domain.models.enums import SagaStatus
from .repository import SagaRepository

ModelT = TypeVar("ModelT", bound=SagaStateMixin)


class SagaOrchestrator(Generic[ModelT]):
    """Execute, resume, and recover saga instances stored in the database."""

    def __init__(
        self,
        *,
        model_class: type[ModelT],
        session_maker: async_sessionmaker[AsyncSession],
        execution_lease: timedelta = timedelta(minutes=5),
    ) -> None:
        """Initialize the orchestrator dependencies and execution lease."""
        self._model_class = model_class
        self._session_maker = session_maker
        self._execution_lease = execution_lease
        self._repository = SagaRepository(model_class)
        self._registry: dict[str, SagaDefinition] = {}

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
                    status=SagaStatus.RUNNING,
                    current_step_index=0,
                    step_execution_token=uuid.uuid4(),
                    context={
                        "saga_name": saga_name,
                        "initial_data": normalized_initial,
                        "step_outputs": {},
                    },
                    step_history=[],
                    deadline_at=initial_deadline,
                    retry_counter=0,
                )
                await self._repository.create(session, saga)

        await self._drive(saga_id)
        return saga_id

    async def notify(
        self, *, saga_id: UUID, token: UUID, event: Any | None = None
    ) -> bool:
        """Resume a suspended saga when the provided execution token matches."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status != SagaStatus.SUSPENDED:
                    return False
                if saga.step_execution_token != token:
                    logger.info("Ignoring stale notify for saga_id=%s", saga_id)
                    return False
                if event is not None:
                    events = saga.context.setdefault("events", [])
                    events.append(self._serialize_value(event))
                    saga.context["latest_event"] = self._serialize_value(event)
                saga.status = SagaStatus.RUNNING
                step_def = self._registry[saga.context["saga_name"]].steps[
                    saga.current_step_index
                ]
                saga.deadline_at = self._running_deadline_for_step(
                    step_def,
                    now=datetime.now(UTC),
                )
                saga.step_execution_token = uuid.uuid4()

        await self._drive(saga_id)
        return True

    async def run_due(self, *, limit: int = 100) -> int:
        """Resume due running, suspended, and compensating sagas."""
        now = datetime.now(UTC)
        ready_ids: list[UUID] = []
        compensation_ids: list[UUID] = []

        async with self._session_maker() as session:
            async with session.begin():
                due_running = await self._repository.due_running(
                    session,
                    now=now,
                    limit=limit,
                )
                remaining = max(limit - len(due_running), 0)
                due_suspended = await self._repository.due_suspended(
                    session,
                    now=now,
                    limit=remaining,
                )
                remaining -= len(due_suspended)
                due_compensating = await self._repository.due_compensating(
                    session,
                    now=now,
                    limit=max(remaining, 0),
                )
                for saga in [*due_running, *due_suspended]:
                    saga.status = SagaStatus.RUNNING
                    saga.step_execution_token = uuid.uuid4()
                    saga.deadline_at = now + self._execution_lease
                    ready_ids.append(saga.id)
                for saga in due_compensating:
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

    async def resume(self, saga_id: UUID) -> None:
        """Resume forward execution of one saga."""
        await self._drive(saga_id)

    async def resume_from_admin_retry(self, saga_id: UUID) -> None:
        """Reset the current step state and retry it."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status not in {SagaStatus.SUSPENDED, SagaStatus.FAILED}:
                    raise SagaStateError(
                        f"Cannot retry step when saga status is {saga.status.value}"
                    )

                saga_name = saga.context["saga_name"]
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

    async def start_compensation_from_admin(self, saga_id: UUID) -> None:
        """Switch one saga into compensation and execute rollback."""
        async with self._session_maker() as session:
            async with session.begin():
                saga = await self._repository.get_for_update(session, saga_id)
                if saga.status not in {
                    SagaStatus.SUSPENDED,
                    SagaStatus.FAILED,
                    SagaStatus.COMPENSATING,
                }:
                    raise SagaStateError(
                        "Cannot start compensation unless saga is suspended, failed, "
                        f"or already compensating (status={saga.status.value})"
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

    async def skip_current_step(
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
                        f"(status={saga.status.value})"
                    )

                saga_name = saga.context["saga_name"]
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

                saga.step_history.append(
                    {
                        "phase": "execute",
                        "status": "SUCCESS",
                        "step_id": step_def.step_id,
                        "step_name": type(step_def.step).__name__,
                        "attempt": 0,
                        "token": str(token),
                        "input": {"_admin": "skip_step"},
                        "output": output_model.model_dump(mode="json"),
                        "error": None,
                        "skipped": True,
                    }
                )
                outputs = saga.context.setdefault("step_outputs", {})
                outputs[step_def.step_id] = output_model.model_dump(mode="json")
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
            error: Exception | None = None

            try:
                if step_def.timeout is None:
                    step_output = await step_def.step.execute(step_input)
                else:
                    step_output = await asyncio.wait_for(
                        step_def.step.execute(step_input),
                        timeout=step_def.timeout.total_seconds(),
                    )
                success = True
            except Exception as exc:  # noqa: BLE001
                error = exc

            should_continue = await self._finalize_step(
                saga_id=saga_id,
                step_def=step_def,
                token=step_token,
                step_input=step_input,
                step_output=step_output,
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
                saga_name = saga.context.get("saga_name")
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
                step_input = self._build_step_input(step_def, saga.context)

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

                if error is None and step_output is not None:
                    saga.step_history.append(
                        self._history_entry(
                            phase="execute",
                            status="SUCCESS",
                            step_def=step_def,
                            token=token,
                            attempt=attempt_number,
                            step_input=step_input,
                            step_output=step_output,
                            error=None,
                        )
                    )
                    outputs = saga.context.setdefault("step_outputs", {})
                    outputs[step_def.step_id] = self._serialize_value(step_output)
                    saga.context.pop("latest_event", None)
                    saga.current_step_index += 1
                    saga.retry_counter = 0
                    saga.deadline_at = None
                    saga.last_error = None
                    saga.step_execution_token = uuid.uuid4()

                    saga_name = saga.context["saga_name"]
                    definition = self._registry[saga_name]
                    if saga.current_step_index >= len(definition.steps):
                        saga.status = SagaStatus.COMPLETED
                    return saga.status == SagaStatus.RUNNING

                assert error is not None
                saga.step_history.append(
                    self._history_entry(
                        phase="execute",
                        status="ERROR",
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

                saga_name = saga.context["saga_name"]
                definition = self._registry[saga_name]
                if definition.compensate_on_failure and saga.current_step_index > 0:
                    saga.status = SagaStatus.COMPENSATING
                    saga.deadline_at = datetime.now(UTC) + self._execution_lease
                else:
                    saga.status = SagaStatus.FAILED
                    saga.deadline_at = None
                    return False

        await self._run_compensation(saga_id)
        return False

    async def _run_compensation(self, saga_id: UUID) -> None:
        """Execute compensation steps until rollback stops."""
        while True:
            comp = await self._prepare_compensation(saga_id)
            if comp is None:
                return

            step_def = comp["step_def"]
            token = comp["token"]
            original_input = comp["original_input"]
            original_output = comp["original_output"]

            error: Exception | None = None
            try:
                await step_def.step.compensate(original_input, original_output)
            except Exception as exc:  # noqa: BLE001
                error = exc

            should_continue = await self._finalize_compensation(
                saga_id=saga_id,
                step_def=step_def,
                token=token,
                original_input=original_input,
                original_output=original_output,
                error=error,
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

                saga_name = saga.context["saga_name"]
                definition = self._registry[saga_name]
                if saga.current_step_index <= 0:
                    saga.status = SagaStatus.FAILED
                    return None

                step_idx = saga.current_step_index - 1
                step_def = definition.steps[step_idx]

                execution_entry = None
                for entry in reversed(saga.step_history):
                    if (
                        entry.get("phase") == "execute"
                        and entry.get("status") == "SUCCESS"
                        and entry.get("step_id") == step_def.step_id
                    ):
                        execution_entry = entry
                        break

                if execution_entry is None:
                    saga.status = SagaStatus.FAILED
                    saga.last_error = f"Missing successful execution entry for step '{step_def.step_id}'"
                    return None

                token = uuid.uuid4()
                saga.step_execution_token = token
                saga.deadline_at = datetime.now(UTC) + self._execution_lease
                return {
                    "step_def": step_def,
                    "token": token,
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
        error: Exception | None,
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

                if error is not None:
                    saga.step_history.append(
                        self._history_entry(
                            phase="compensate",
                            status="ERROR",
                            step_def=step_def,
                            token=token,
                            attempt=1,
                            step_input=original_input,
                            step_output=original_output,
                            error=error,
                        )
                    )
                    saga.last_error = (
                        f"Compensation failed for step '{step_def.step_id}': {error!r}"
                    )
                    saga.status = SagaStatus.FAILED
                    saga.deadline_at = None
                    return False

                saga.step_history.append(
                    self._history_entry(
                        phase="compensate",
                        status="SUCCESS",
                        step_def=step_def,
                        token=token,
                        attempt=1,
                        step_input=original_input,
                        step_output=original_output,
                        error=None,
                    )
                )
                saga.current_step_index -= 1
                saga.step_execution_token = uuid.uuid4()
                if saga.current_step_index <= 0:
                    saga.status = SagaStatus.FAILED
                    saga.deadline_at = None
                    return False
                saga.deadline_at = datetime.now(UTC) + self._execution_lease
                return True

    @staticmethod
    def _build_step_input(
        step_def: StepDefinition[Any, Any],
        context: dict[str, Any],
    ) -> BaseModel:
        """Build the input model for one step from saga context."""
        if step_def.depends_on is not None:
            dep_payload = context.get("step_outputs", {}).get(
                step_def.depends_on.step_id
            )
            if dep_payload is None:
                raise SagaStateError(
                    f"Missing dependency output for step '{step_def.depends_on.step_id}'"
                )
            dep_model = step_def.depends_on.output_model.model_validate(dep_payload)
            mapped = step_def.input_map(dep_model)
        else:
            mapped = step_def.input_map(
                InputContext(
                    initial_data=context.get("initial_data"),
                    context=context,
                    step_outputs=context.get("step_outputs", {}),
                    latest_event=context.get("latest_event"),
                    events=context.get("events"),
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

    def _history_entry(
        self,
        *,
        phase: str,
        status: str,
        step_def: StepDefinition[Any, Any],
        token: UUID,
        attempt: int,
        step_input: BaseModel,
        step_output: BaseModel | None,
        error: Exception | None,
    ) -> dict[str, Any]:
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
        return any(entry.get("phase") == "compensate" for entry in step_history)

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
            status=saga.status,
            current_step_index=saga.current_step_index,
            retry_counter=saga.retry_counter,
            deadline_at=saga.deadline_at,
            trace_id=saga.trace_id,
            step_execution_token=saga.step_execution_token,
            last_error=saga.last_error,
        )
