from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from saga_orchestrator import (
    BaseStep,
    ExponentialRetry,
    SagaBuilder,
    SagaOrchestrator,
    SagaStateMixin,
    SagaStepHistoryMixin,
)

DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/saga_db",
)


class Base(DeclarativeBase):
    pass


class ModelDeploySagaHistory(Base, SagaStepHistoryMixin):
    __tablename__ = "model_deploy_saga_history"

    saga_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("model_deploy_saga.id", ondelete="CASCADE"),
        index=True,
    )


class ModelDeploySagaState(Base, SagaStateMixin):
    __tablename__ = "model_deploy_saga"

    step_history: Mapped[list[ModelDeploySagaHistory]] = relationship(
        "ModelDeploySagaHistory",
        cascade="all, delete-orphan",
        order_by="ModelDeploySagaHistory.id",
    )


class CheckModelInput(BaseModel):
    model_name: str


class CheckModelOutput(BaseModel):
    exists: bool


class DeployInput(BaseModel):
    model_name: str


class DeployOutput(BaseModel):
    endpoint: str


class CheckModelStep(BaseStep[CheckModelInput, CheckModelOutput]):
    async def execute(
        self,
        inp: CheckModelInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> CheckModelOutput:
        return CheckModelOutput(exists=inp.model_name in {"llama-2"})


class DeployStep(BaseStep[DeployInput, DeployOutput]):
    async def execute(
        self,
        inp: DeployInput,
        event_type: str | None = None,
        event_payload: Any | None = None,
    ) -> DeployOutput:
        await asyncio.sleep(0.01)
        return DeployOutput(endpoint=f"https://models.local/{inp.model_name}")


async def main() -> None:
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    builder = SagaBuilder()
    check_ref = builder.add_step(
        step=CheckModelStep(),
        input_map=lambda ctx: CheckModelInput(model_name=ctx.initial_data["model"]),
    )
    builder.add_step(
        step=DeployStep(),
        depends_on=check_ref,
        input_map=lambda _: DeployInput(model_name="llama-2"),
        retry_policy=ExponentialRetry(max_attempts=3, base_delay=timedelta(seconds=1)),
    )

    orchestrator = SagaOrchestrator[ModelDeploySagaState, ModelDeploySagaHistory](
        model_class=ModelDeploySagaState,
        history_model_class=ModelDeploySagaHistory,
        session_maker=session_maker,
    )
    orchestrator.register("ai_deploy_v1", builder.build())

    saga_id = await orchestrator.start(
        saga_name="ai_deploy_v1",
        initial_data={"model": "llama-2"},
        aggregation_id="deploy-1",
    )
    print(f"Started saga: {saga_id}")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
