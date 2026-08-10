"""Entrypoint for ``python -m loom_pipeline_orchestrator``."""

from __future__ import annotations

import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_pipeline_orchestrator.health import start_health_server
from loom_pipeline_orchestrator.main_loop import OrchestratorContext, run
from loom_pipeline_orchestrator.reconciler import PipelineReconciler
from loom_pipeline_orchestrator.repository import PipelineRepository
from loom_pipeline_orchestrator.settings import PipelineOrchestratorSettings

logger = logging.getLogger(__name__)


async def _amain() -> None:
    settings = PipelineOrchestratorSettings()
    logging.basicConfig(level=logging.INFO)
    engine = create_async_engine(settings.db_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = PipelineRepository(sessions)
    reconciler = PipelineReconciler(repository)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    server = await start_health_server(
        host=settings.health_host,
        port=settings.health_port,
        healthy=lambda: not stop.is_set(),
    )
    ctx = OrchestratorContext(
        repository=repository,
        controller_id=settings.controller_id,
        reconcile=reconciler.reconcile,
        poll_seconds=settings.poll_seconds,
        picker_batch=settings.picker_batch,
    )
    try:
        await run(ctx, stop_event=stop)
    finally:
        server.close()
        await server.wait_closed()
        await engine.dispose()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
