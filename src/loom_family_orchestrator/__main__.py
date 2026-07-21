"""Entry point: ``python -m loom_family_orchestrator``.

Boots the long-running family-run adapter orchestrator loop. Uses the
same ``ControlPlaneSettings`` env prefix (``LOOM_CP_*``) so the
Deployment can share the CP's env block wholesale.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.config import ControlPlaneSettings
from loom_family_orchestrator.gateway_client import OrchestratorGatewayClient
from loom_family_orchestrator.main_loop import OrchestratorContext, run

logger = logging.getLogger(__name__)


async def _amain() -> None:
    settings = ControlPlaneSettings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("family_orchestrator_starting poll_sec=%s", settings.family_orchestrator_poll_sec)

    engine = create_async_engine(
        settings.db_engine_url,
        connect_args=settings.db_engine_connect_args,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # The state backend calls store.get_object(bucket=..., key=...)
    # — the ObjectStore Protocol shape, not raw boto3. Wrap the boto3
    # client in MinioObjectStore so the kwarg translation
    # (bucket→Bucket, key→Key) actually happens; otherwise every
    # download hits ``ParamValidationError: Unknown parameter in
    # input: "bucket"``.
    object_store = MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )

    # Which bucket the state backend seeds new empty prefixes into.
    # Not hardcoded to "artifacts": staging seeds
    # ``s3://loom-staging-artifacts/`` via LOOM_FAMILY_ARTIFACTS_BUCKET
    # so the orchestrator's initialize() lands in the same place the
    # service did at accept-time. Download/upload always parse the
    # bucket from the incoming state_uri (see PR #728), so this only
    # affects fresh initialize() calls; still, giving operators a knob
    # keeps the two paths coherent.
    artifacts_bucket = os.environ.get(
        "LOOM_FAMILY_ARTIFACTS_BUCKET", "artifacts",
    )

    stop_event = asyncio.Event()

    def _sigterm(*_: object) -> None:
        logger.info("family_orchestrator_stop_signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _sigterm)

    # Wire the skill_patcher_llm adapter with a dedicated family:evolve
    # credential. The client exchanges it at the Control Plane for a JWT bound
    # to the real completed trial, represented team, and evolver provider.
    gateway_token = os.environ.get(
        "LOOM_FAMILY_ORCHESTRATOR_GATEWAY_TOKEN", "",
    )
    gateway_client: OrchestratorGatewayClient | None = None
    if gateway_token:
        gateway_client = OrchestratorGatewayClient(
            base_url=str(settings.llm_gateway_url),
            control_plane_url=str(settings.control_plane_url),
            worker_token=gateway_token,
            timeout_sec=settings.family_adapter_call_timeout_sec,
        )
    else:
        logger.warning(
            "family_orchestrator_gateway_unconfigured — set "
            "LOOM_FAMILY_ORCHESTRATOR_GATEWAY_TOKEN to enable "
            "adapter LLM calls",
        )

    ctx = OrchestratorContext(
        session_factory=session_factory,
        gateway=gateway_client,
        object_store=object_store,
        artifacts_bucket=artifacts_bucket,
        state_backend_factory=None,
        settings_default_model=settings.skill_evolver_default_model,
        adapter_call_timeout_sec=settings.family_adapter_call_timeout_sec,
        poll_sec=settings.family_orchestrator_poll_sec,
    )

    try:
        await run(ctx, stop_event=stop_event)
    finally:
        if gateway_client is not None:
            await gateway_client.aclose()
        await engine.dispose()
    logger.info("family_orchestrator_stopped")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
