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

from loom.storage_credentials import build_s3_client
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

    object_store = build_s3_client(
        endpoint_url=settings.minio_endpoint,
        auth_kind=settings.storage_auth_kind,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )

    stop_event = asyncio.Event()

    def _sigterm(*_: object) -> None:
        logger.info("family_orchestrator_stop_signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _sigterm)

    # #672 PR-3: wire an httpx client to the LLM gateway so the
    # skill_patcher_llm adapter can call it. The orchestrator runs as
    # a service account — the team_id + token come from env vars set
    # on the Deployment. When either is missing, the adapter surfaces
    # a ValueError at evolve() time and the orchestrator's failure
    # policy takes over (retry / stall) rather than crashing the loop.
    gateway_team_id = os.environ.get(
        "LOOM_FAMILY_ORCHESTRATOR_GATEWAY_TEAM_ID", "",
    )
    gateway_token = os.environ.get(
        "LOOM_FAMILY_ORCHESTRATOR_GATEWAY_TOKEN", "",
    )
    gateway_client: OrchestratorGatewayClient | None = None
    if gateway_team_id and gateway_token:
        gateway_client = OrchestratorGatewayClient(
            base_url=str(settings.llm_gateway_url),
            team_id=gateway_team_id,
            token=gateway_token,
            timeout_sec=settings.family_adapter_call_timeout_sec,
        )
    else:
        logger.warning(
            "family_orchestrator_gateway_unconfigured — set "
            "LOOM_FAMILY_ORCHESTRATOR_GATEWAY_TEAM_ID + "
            "LOOM_FAMILY_ORCHESTRATOR_GATEWAY_TOKEN to enable "
            "adapter LLM calls",
        )

    ctx = OrchestratorContext(
        session_factory=session_factory,
        gateway=gateway_client,
        object_store=object_store,
        artifacts_bucket="artifacts",
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
