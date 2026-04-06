from __future__ import annotations

import asyncio
from datetime import timedelta

from examples.common import (
    DeployInput,
    DeployModelStep,
    FinalizeInput,
    FinalizeStep,
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
        deploy_step = DeployModelStep()
        builder = SagaBuilder()
        reserve_ref = builder.add_step(
            step=ReserveResourcesStep(),
            input_map=lambda ctx: StartInput(model_name=ctx.initial_data["model_name"]),
        )
        deploy_ref = builder.add_step(
            step=deploy_step,
            depends_on=reserve_ref,
            input_map=lambda out: DeployInput(
                model_name=out.model_name,
                reservation_id=out.reservation_id,
            ),
            retry_policy=ExponentialRetry(
                max_attempts=2,
                base_delay=timedelta(seconds=0),
            ),
        )
        builder.add_step(
            step=FinalizeStep(),
            depends_on=deploy_ref,
            input_map=lambda out: FinalizeInput(endpoint=out.endpoint),
        )

        orchestrator.register("retry_demo", builder.build())
        saga_id = await orchestrator.start(
            saga_name="retry_demo",
            initial_data={"model_name": "llama-2"},
            aggregation_id="retry-demo",
        )
        await print_snapshot(admin, saga_id, title="After first attempt")

        await orchestrator.run_due(limit=10)
        await print_snapshot(admin, saga_id, title="After retry recovery")
    finally:
        await dispose_runtime(session_maker)


if __name__ == "__main__":
    asyncio.run(main())
