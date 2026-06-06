"""Control Plane app factory."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import boto3
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.routes import health, state, trials, workers
from loom_control_plane.scheduler.crash_detector import run_crash_detector_loop


def create_app(settings: ControlPlaneSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(str(settings.db_url))
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        minio_client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key.get_secret_value(),
            aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
            region_name=settings.minio_region,
            config=Config(signature_version="s3v4"),
        )

        app.state.settings = settings
        app.state.session_factory = session_factory
        app.state.minio_client = minio_client

        crash_detector_task = asyncio.create_task(
            run_crash_detector_loop(
                session_factory=session_factory,
                expiry_sec=settings.worker_heartbeat_expiry_sec,
                interval_sec=settings.worker_reclaim_sweep_interval_sec,
            ),
            name="loom-cp-crash-detector",
        )
        try:
            yield
        finally:
            crash_detector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await crash_detector_task
            await engine.dispose()

    app = FastAPI(
        title="Loom Control Plane", version="0.0.1", lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(trials.router)
    app.include_router(workers.router)
    app.include_router(state.router)
    return app
