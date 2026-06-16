# python-saga-orchestrator

Lightweight embedded saga orchestration for `asyncio` Python services.

The library implements the Saga pattern for long-running business processes that:
- span multiple steps,
- call external systems,
- need retry and compensation,
- must survive worker crashes and process restarts.

Unlike external workflow platforms, this library runs inside your service and stores saga state in your application's database through SQLAlchemy.

## What it provides

- typed step definitions with `Pydantic` models
- saga construction with `SagaBuilder` and `StepRef`
- persisted saga state through `SagaStateMixin`
- runtime execution through `SagaOrchestrator` and `SagaEngine`
- retry, timeout, recovery, and compensation
- async queue-style steps through `StepAwaitEvent` and `notify(...)`
- administrative operations through `SagaAdmin`
- PostgreSQL-first reliability using `SELECT ... FOR UPDATE`
- strict CQRS-style separation of historical step data (`InputModel`) and async triggers (`Events`)

## Installation

Requirements:
- Python 3.12+
- PostgreSQL for production-grade execution semantics

Install from PyPI:

```bash
pip install python-saga-orchestrator
```

Or with `uv`:

```bash
uv pip install python-saga-orchestrator
```

Install from the repository source:

```bash
pip install .
```

For local development:

```bash
pip install '.[dev]'
```

## Core concepts

### `BaseStep`

Each saga step is a class with:
- `execute(self, inp, event_type=None, event_payload=None) -> out`
- optional `compensate(self, inp, out, event_type=None, event_payload=None) -> None`

The `inp` object represents the immutable historical command parameters (mapped via `input_map`), while `event_type` and `event_payload` receive dynamic asynchronous continuation signals.

Steps are regular Python objects. In practice they are created once at application startup and reused.

### `SagaBuilder`

`SagaBuilder` creates an immutable `SagaDefinition`.  
Each added step includes:
- the step object
- `input_map`
- optional timeout
- retry policy
- optional dependency on a previous step via `StepRef`

### `SagaStateMixin`

Your SQLAlchemy model inherits `SagaStateMixin` to store:
- current status
- current step index
- execution token
- context
- step history
- deadline
- retry counter

### `SagaOrchestrator`

`SagaOrchestrator` is the public runtime API used by application code:
- `register(...)`
- `start(...)`
- `notify(...)`
- `run_due(...)`
- `get_snapshot(...)`

### `SagaAdmin`

`SagaAdmin` exposes operational controls:
- `get_saga(...)`
- `retry_step(...)`
- `skip_step(...)`
- `compensate_step(...)`
- `abort(...)`

## Quick start

```python
import uuid
from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from saga_orchestrator import (
    BaseStep,
    ExponentialRetry,
    SagaAdmin,
    SagaBuilder,
    SagaOrchestrator,
    SagaStateMixin,
    SagaStepHistoryMixin,
)


class Base(DeclarativeBase):
    pass


class OrderSagaHistory(Base, SagaStepHistoryMixin):
    __tablename__ = "order_saga_history"

    saga_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("order_saga_state.id", ondelete="CASCADE"),
        index=True,
    )


class OrderSagaState(Base, SagaStateMixin):
    __tablename__ = "order_saga_state"

    step_history: Mapped[list[OrderSagaHistory]] = relationship(
        "OrderSagaHistory",
        cascade="all, delete-orphan",
        order_by="OrderSagaHistory.id",
    )


class ReserveInput(BaseModel):
    order_id: str


class ReserveOutput(BaseModel):
    reservation_id: str


class ChargeInput(BaseModel):
    reservation_id: str


class ChargeOutput(BaseModel):
    payment_id: str


class ReserveInventoryStep(BaseStep[ReserveInput, ReserveOutput]):
    async def execute(
        self, inp: ReserveInput, event_type: str | None = None, event_payload: Any | None = None
    ) -> ReserveOutput:
        return ReserveOutput(reservation_id=f"res-{inp.order_id}")

    async def compensate(
        self, inp: ReserveInput, out: ReserveOutput, event_type: str | None = None, event_payload: Any | None = None
    ) -> None:
        return None


class ChargePaymentStep(BaseStep[ChargeInput, ChargeOutput]):
    async def execute(
        self, inp: ChargeInput, event_type: str | None = None, event_payload: Any | None = None
    ) -> ChargeOutput:
        return ChargeOutput(payment_id=f"pay-{inp.reservation_id}")


def build_order_saga():
    builder = SagaBuilder()

    reserve_ref = builder.add_step(
        step=ReserveInventoryStep(),
        input_map=lambda ctx: ReserveInput(order_id=ctx.initial_data["order_id"]),
    )

    builder.add_step(
        step=ChargePaymentStep(),
        depends_on=reserve_ref,
        input_map=lambda out: ChargeInput(reservation_id=out.reservation_id),
        retry_policy=ExponentialRetry(
            max_attempts=3,
            base_delay=timedelta(seconds=5),
        ),
    )

    return builder.build()


def setup_saga(
    session_maker: async_sessionmaker,
) -> tuple[
    SagaOrchestrator[OrderSagaState, OrderSagaHistory], 
    SagaAdmin[OrderSagaState, OrderSagaHistory]
]:
    orchestrator = SagaOrchestrator[OrderSagaState, OrderSagaHistory](
        model_class=OrderSagaState,
        history_model_class=OrderSagaHistory,
        session_maker=session_maker,
    )
    orchestrator.register("create_order_v1", build_order_saga())

    admin = SagaAdmin[OrderSagaState, OrderSagaHistory](engine=orchestrator.engine)
    
    return orchestrator, admin
```

Start a saga:

```python
orchestrator, admin = setup_saga(session_maker)

saga_id = await orchestrator.start(
    saga_name="create_order_v1",
    initial_data={"order_id": "order-123"},
    aggregation_id="order-123",
)
```

## Recovery model

The library persists enough state to recover work after failures:

- `RUNNING` sagas with expired execution leases can be reclaimed
- `SUSPENDED` sagas with expired retry deadlines can be resumed
- `COMPENSATING` sagas can continue rollback after a crash

The recovery entry point is:

```python
await orchestrator.run_due(limit=100)
```

In production this should be called by a background worker or scheduled job.

## Notifications and external events

Use `notify(...)` when a suspended saga should resume because of an external signal:

```python
accepted = await orchestrator.notify(
    saga_id=saga_id,
    token=current_token,
    event={"approved": True},
)
```

Configure explicit event expectations through a public API:

```python
token = await orchestrator.await_event(
    saga_id=saga_id,
    event=AwaitingEvent(
        event_type="model.approved",
        correlation_id="corr-123",
    ),
)
```

When a suspended saga is awakened by an event, the orchestrator passes the event directly to the step's `execute` or `compensate` method via the `event_type` and `event_payload` arguments. This strictly separates historical context (`InputModel`) from dynamic asynchronous triggers (`Events`).

For distributed consumers, use transactional inbox ingestion first, then process inbox rows:

```python
stored = await orchestrator.ingest_event(
    aggregation_id="order-123",
    event={
        "event_id": "evt-123",
        "event_type": "payment.completed",
        "correlation_id": "corr-123",
        "payload": {"payment_id": "pay-1"},
    },
)

if stored:
    await orchestrator.run_inbox_due(limit=100)
```

## Administrative operations

Get the full persisted state:

```python
snapshot = await admin.get_saga(saga_id)
print(snapshot.status)
print(snapshot.step_history)
```

Retry the current failed step:

```python
await admin.retry_step(saga_id)
```

Skip the current suspended step:

```python
await admin.skip_step(
    saga_id,
    mock_output={"payment_id": "manual-payment"},
)
```

Start compensation manually:

```python
await admin.compensate_step(saga_id)
```

Abort the saga:

```python
await admin.abort(saga_id)
```

## Persistence expectations

The library is PostgreSQL-first.

Important implementation details:
- state transitions are performed inside database transactions
- mutating reads use `SELECT ... FOR UPDATE`
- JSON state is stored in `context` and `step_history`
- `step_execution_token` is used to reject stale events and stale step completions

SQLite may be sufficient for local experiments, but PostgreSQL should be used for integration testing and production use.

## Example workflow

A runnable end-to-end example is available in:

- [`examples/llm_deploy.py`](./examples/llm_deploy.py)
- [`examples/retry_recovery.py`](./examples/retry_recovery.py)
- [`examples/compensation_flow.py`](./examples/compensation_flow.py)
- [`examples/admin_skip.py`](./examples/admin_skip.py)
- [`examples/http_and_queue.py`](./examples/http_and_queue.py)

These examples demonstrate:
- basic model deployment
- retry and recovery through `run_due()`
- compensation after failure
- admin-driven step skipping

## Running tests

Run unit tests:

```bash
pytest -q tests/unit
```

Run PostgreSQL integration tests:

```bash
export TEST_DATABASE_URL='postgresql+asyncpg://postgres:postgres@localhost:5432/saga_test_db'
pytest -q tests/integration
```

The integration fixture creates an isolated schema per test, so it does not require a dedicated empty database schema.

## Current limitations

- the implementation is optimized for sequential saga execution, not parallel DAG execution
- PostgreSQL is the intended reliability target
- tracing integration is not implemented yet

## License

MIT. See [`LICENSE`](./LICENSE).
