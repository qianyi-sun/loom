"""Control Plane app factory."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import boto3
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier, load_optional_admin_secret_verifier
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.routes import (
    admin,
    artifacts,
    health,
    state,
    step_tokens,
    tasks,
    trajectory,
    trials,
    workers,
)
from loom_control_plane.scheduler.crash_detector import run_crash_detector_loop


def _load_admin_secret_verifier(
    settings: ControlPlaneSettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for Control Plane startup."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


def create_app(settings: ControlPlaneSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(str(settings.db_url))
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        admin_secret_verifier = _load_admin_secret_verifier(settings)

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
        app.state.admin_secret_verifier = admin_secret_verifier
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
            # Bound the await so a stuck task (e.g. mid-DB call when
            # cancellation arrives, asyncpg connection takes a moment
            # to release) doesn't block the entire lifespan shutdown —
            # which then blocks `TestClient.__exit__`, which then
            # blocks the test. Five seconds is generous for a task
            # that should respond to cancel in microseconds.
            with contextlib.suppress(
                asyncio.CancelledError, asyncio.TimeoutError,
            ):
                await asyncio.wait_for(crash_detector_task, timeout=5.0)
            await engine.dispose()

    app = FastAPI(
        title="Loom Control Plane", version="0.0.1", lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(trials.router)
    app.include_router(workers.router)
    app.include_router(state.router)
    app.include_router(trajectory.router)
    app.include_router(artifacts.router)
    app.include_router(tasks.router)
    app.include_router(admin.router)
    app.include_router(step_tokens.router)
    return app
