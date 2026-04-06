from __future__ import annotations

import asyncio
from datetime import timedelta

from examples.common import (
    DeployInput,
    FailingPublishStep,
    ReserveResourcesStep,
    StartInput,
    create_runtime,
    dispose_runtime,
    print_snapshot,
)
from saga_orchestrator import ExponentialRetry, SagaBuilder


async def main() -> None:
    session_maker, orchestrator, admin = await create_runtime()
    try:
        builder = SagaBuilder()
        reserve_ref = builder.add_step(
            step=ReserveResourcesStep(),
            input_map=lambda ctx: StartInput(model_name=ctx.initial_data["model_name"]),
        )
        builder.add_step(
            step=FailingPublishStep(),
            depends_on=reserve_ref,
            input_map=lambda out: DeployInput(
                model_name=out.model_name,
                reservation_id=out.reservation_id,
            ),
            retry_policy=ExponentialRetry(
                max_attempts=0,
                base_delay=timedelta(seconds=1),
            ),
        )

        orchestrator.register("compensation_demo", builder.build())
        saga_id = await orchestrator.start(
            saga_name="compensation_demo",
            initial_data={"model_name": "mistral-7b"},
            aggregation_id="compensation-demo",
        )
        await print_snapshot(admin, saga_id, title="After compensation flow")
    finally:
        await dispose_runtime(session_maker)


if __name__ == "__main__":
    asyncio.run(main())
