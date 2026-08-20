"""Entrypoint for ``python -m loom_pipeline_orchestrator``."""

from __future__ import annotations

import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.pipeline.image_runtime import ImageRuntimeRegistry
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.trajectory.storage import MinioObjectStore
from loom_pipeline_orchestrator.fanout_runtime import FanoutExpansionRuntime
from loom_pipeline_orchestrator.health import start_health_server
from loom_pipeline_orchestrator.main_loop import OrchestratorContext, run
from loom_pipeline_orchestrator.reconciler import (
    CompositeReadinessRuntime,
    PairedReadinessRuntime,
    PipelineReconciler,
)
from loom_pipeline_orchestrator.repository import PipelineRepository
from loom_pipeline_orchestrator.settings import PipelineOrchestratorSettings
from loom_pipeline_orchestrator.stage1_runtime import (
    Stage1ReadinessResolver,
    Stage1RequestRenderer,
)
from loom_pipeline_orchestrator.terminalgen_publication import (
    TerminalGenCorpusPublicationRuntime,
)
from loom_pipeline_orchestrator.terminalgen_runtime import TerminalGenReadinessRuntime

logger = logging.getLogger(__name__)


async def _amain() -> None:
    settings = PipelineOrchestratorSettings()
    logging.basicConfig(level=logging.INFO)
    engine = create_async_engine(settings.db_url, pool_pre_ping=True)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repository = PipelineRepository(sessions)
    repo_root = settings.repo_root.resolve()
    resource_profiles = ResourceProfileRegistry.load(
        repo_root / settings.resource_profiles_path
    )
    image_runtime = ImageRuntimeRegistry.load(repo_root / settings.image_runtime_contracts_path)
    artifact_store = MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )
    reconciler = PipelineReconciler(
        repository,
        fanout_runtime=FanoutExpansionRuntime(
            repository=repository,
            store=artifact_store,
            bucket=settings.artifacts_bucket,
        ),
        publication_runtime=TerminalGenCorpusPublicationRuntime(
            repository=repository,
            store=artifact_store,
            bucket=settings.artifacts_bucket,
        ),
        readiness_runtime=CompositeReadinessRuntime(
            (
                PairedReadinessRuntime(
                    resolver=Stage1ReadinessResolver(
                        repo_root=repo_root,
                        resource_profiles=resource_profiles,
                        image_runtime=image_runtime,
                    ),
                    renderer=Stage1RequestRenderer(),
                ),
                TerminalGenReadinessRuntime(
                    repo_root=repo_root,
                    resource_profiles=resource_profiles,
                    image_runtime=image_runtime,
                ),
            )
        ),
    )
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
