from __future__ import annotations

import os
from datetime import timedelta
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from saga_orchestrator import BaseStep, SagaAdmin, SagaOrchestrator, SagaStateMixin

DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/saga_db",
)


class Base(DeclarativeBase):
    pass


class DemoSagaState(Base, SagaStateMixin):
    __tablename__ = "demo_saga_state"


class StartInput(BaseModel):
    model_name: str


class StartOutput(BaseModel):
    model_name: str
    reservation_id: str


class DeployInput(BaseModel):
    model_name: str
    reservation_id: str


class DeployOutput(BaseModel):
    endpoint: str


class FinalizeInput(BaseModel):
    endpoint: str


class FinalizeOutput(BaseModel):
    status: str


class ReserveResourcesStep(BaseStep[StartInput, StartOutput]):
    async def execute(self, inp: StartInput) -> StartOutput:
        print(f"[reserve] reserving resources for {inp.model_name}")
        return StartOutput(
            model_name=inp.model_name,
            reservation_id=f"reservation-{inp.model_name}",
        )

    async def compensate(self, inp: StartInput, out: StartOutput) -> None:
        print(f"[reserve] compensating reservation {out.reservation_id}")


class DeployModelStep(BaseStep[DeployInput, DeployOutput]):
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, inp: DeployInput) -> DeployOutput:
        self.calls += 1
        print(f"[deploy] attempt={self.calls} model={inp.model_name}")
        if self.calls == 1:
            raise RuntimeError("temporary deploy failure")
        return DeployOutput(endpoint=f"https://models.local/{inp.model_name}")


class FinalizeStep(BaseStep[FinalizeInput, FinalizeOutput]):
    async def execute(self, inp: FinalizeInput) -> FinalizeOutput:
        print(f"[finalize] model is available at {inp.endpoint}")
        return FinalizeOutput(status="COMPLETED")


class FailingPublishStep(BaseStep[DeployInput, DeployOutput]):
    async def execute(self, inp: DeployInput) -> DeployOutput:
        print(f"[publish] forcing failure for {inp.model_name}")
        raise RuntimeError("publish failed")


class ManualApprovalStep(BaseStep[StartInput, DeployOutput]):
    async def execute(self, inp: StartInput) -> DeployOutput:
        print(f"[approval] waiting for manual approval for {inp.model_name}")
        raise RuntimeError("approval is pending")


async def create_runtime(
    *,
    execution_lease: timedelta = timedelta(seconds=5),
    reset_schema: bool = True,
) -> tuple[
    async_sessionmaker, SagaOrchestrator[DemoSagaState], SagaAdmin[DemoSagaState]
]:
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        if reset_schema:
            await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    orchestrator = SagaOrchestrator[DemoSagaState](
        model_class=DemoSagaState,
        session_maker=session_maker,
        execution_lease=execution_lease,
    )
    admin = SagaAdmin[DemoSagaState](engine=orchestrator.engine)
    return session_maker, orchestrator, admin


async def dispose_runtime(session_maker: async_sessionmaker) -> None:
    await session_maker.kw["bind"].dispose()


async def print_snapshot(
    admin: SagaAdmin[DemoSagaState],
    saga_id: UUID,
    *,
    title: str,
) -> None:
    snapshot = await admin.get_saga(saga_id)
    print(f"\n=== {title} ===")
    print(f"id: {snapshot.id}")
    print(f"status: {snapshot.status.value}")
    print(f"current_step_index: {snapshot.current_step_index}")
    print(f"retry_counter: {snapshot.retry_counter}")
    print(f"deadline_at: {snapshot.deadline_at}")
    print(f"last_error: {snapshot.last_error}")
    print(f"step_history entries: {len(snapshot.step_history)}")
