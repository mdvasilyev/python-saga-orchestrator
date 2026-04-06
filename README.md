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
- administrative operations through `SagaAdmin`
- PostgreSQL-first reliability using `SELECT ... FOR UPDATE`

## Installation

Requirements:
- Python 3.12+
- PostgreSQL for production-grade execution semantics

Install from source:

```bash
pip install .
```

Or install development dependencies:

```bash
pip install .[dev]
```

## Core concepts

### `BaseStep`

Each saga step is a class with:
- `execute(inp) -> out`
- optional `compensate(inp, out) -> None`

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
from datetime import timedelta

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from saga_orchestrator import (
    BaseStep,
    ExponentialRetry,
    SagaAdmin,
    SagaBuilder,
    SagaOrchestrator,
    SagaStateMixin,
)


class Base(DeclarativeBase):
    pass


class OrderSagaState(Base, SagaStateMixin):
    __tablename__ = "order_saga_state"


class ReserveInput(BaseModel):
    order_id: str


class ReserveOutput(BaseModel):
    reservation_id: str


class ChargeInput(BaseModel):
    reservation_id: str


class ChargeOutput(BaseModel):
    payment_id: str


class ReserveInventoryStep(BaseStep[ReserveInput, ReserveOutput]):
    async def execute(self, inp: ReserveInput) -> ReserveOutput:
        return ReserveOutput(reservation_id=f"res-{inp.order_id}")

    async def compensate(self, inp: ReserveInput, out: ReserveOutput) -> None:
        return None


class ChargePaymentStep(BaseStep[ChargeInput, ChargeOutput]):
    async def execute(self, inp: ChargeInput) -> ChargeOutput:
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
) -> tuple[SagaOrchestrator[OrderSagaState], SagaAdmin[OrderSagaState]]:
    orchestrator = SagaOrchestrator[OrderSagaState](
        model_class=OrderSagaState,
        session_maker=session_maker,
    )
    orchestrator.register("create_order_v1", build_order_saga())

    admin = SagaAdmin[OrderSagaState](engine=orchestrator.engine)
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

The event payload is stored in saga context and can be used by root-step `input_map` functions through `InputContext`.

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

- [`test.py`](./test.py)

It demonstrates:
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
