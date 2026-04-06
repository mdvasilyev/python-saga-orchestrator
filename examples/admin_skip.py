from __future__ import annotations

import asyncio
from datetime import timedelta

from examples.common import (
    DeployOutput,
    FinalizeInput,
    FinalizeStep,
    ManualApprovalStep,
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
        approval_ref = builder.add_step(
            step=ManualApprovalStep(),
            input_map=lambda ctx: StartInput(model_name=ctx.initial_data["model_name"]),
            retry_policy=ExponentialRetry(
                max_attempts=1,
                base_delay=timedelta(hours=1),
            ),
        )
        builder.add_step(
            step=FinalizeStep(),
            depends_on=approval_ref,
            input_map=lambda out: FinalizeInput(endpoint=out.endpoint),
        )

        orchestrator.register("admin_skip_demo", builder.build())
        saga_id = await orchestrator.start(
            saga_name="admin_skip_demo",
            initial_data={"model_name": "qwen-2.5"},
            aggregation_id="admin-skip-demo",
        )
        await print_snapshot(admin, saga_id, title="Before admin skip")

        await admin.skip_step(
            saga_id,
            mock_output=DeployOutput(endpoint="https://models.local/qwen-2.5"),
        )
        await print_snapshot(admin, saga_id, title="After admin skip")
    finally:
        await dispose_runtime(session_maker)


if __name__ == "__main__":
    asyncio.run(main())
