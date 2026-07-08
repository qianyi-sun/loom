"""Entry point: ``python -m loom_family_orchestrator``.

Boots the long-running family-run adapter orchestrator loop. Uses the
same ``ControlPlaneSettings`` env prefix (``LOOM_CP_*``) so the
Deployment can share the CP's env block wholesale.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.storage_credentials import build_s3_client
from loom_control_plane.config import ControlPlaneSettings
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

    ctx = OrchestratorContext(
        session_factory=session_factory,
        gateway=None,  # PR-3: wire up an httpx client to the LLM gateway.
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
        await engine.dispose()
    logger.info("family_orchestrator_stopped")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
