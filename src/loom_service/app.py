"""FastAPI factory for loom_service (spec §2).

Stateless service. The lifespan opens a per-process async SQLAlchemy
engine, a boto3 S3 client (for Plan 18+ presigned URLs), and an httpx
AsyncClient pointed at the Control Plane (for Plan 18+ forwarders).
Routes pull these off `request.app.state`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import boto3
import httpx
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_service.batch_runner import run_loop
from loom_service.config import LoomServiceSettings
from loom_service.routes import (
    agents,
    atif,
    backends,
    batches,
    benchmarks,
    health,
    local_servers,
    models,
    rate_cards,
    tasks,
    teams,
    tokens,
    trajectory,
    trials,
    usage,
)


def create_app(settings: LoomServiceSettings) -> FastAPI:
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

        http_client = httpx.AsyncClient(
            base_url=str(settings.control_plane_url), timeout=10.0,
        )

        # Plan 20: separate httpx client for the Gateway. Rate-card
        # routes proxy to /admin/rate-cards on the Gateway; we keep
        # CP + Gateway clients independent so a slow CP doesn't
        # starve the rate-card surface.
        # Path-prefix sanity check: a base_url with a non-root path
        # (e.g. `https://gw/loom/`) silently strips the prefix when
        # the forwarder uses absolute paths like `/admin/rate-cards`.
        # Fail at startup rather than route to the wrong URL.
        gw_path = settings.gateway_url.path or "/"
        if gw_path not in ("", "/"):
            raise RuntimeError(
                f"LOOM_SVC_GATEWAY_URL must not include a path prefix "
                f"(got {settings.gateway_url!s}). Forwarders use "
                f"absolute paths; a prefix would be silently dropped."
            )
        gateway_client = httpx.AsyncClient(
            base_url=str(settings.gateway_url), timeout=10.0,
        )

        app.state.settings = settings
        app.state.session_factory = session_factory
        app.state.minio_client = minio_client
        app.state.http_client = http_client
        app.state.gateway_client = gateway_client

        # Plan 19: batch runner background task. Picks up
        # submitted/running batches on each poll, fans out trial
        # submissions to Control Plane via the shared http_client.
        # The runner's CP token is read from settings — without it
        # every CP submit 401s, so the loop short-circuits with a
        # warning per tick and waits for the operator to provision
        # the secret.
        runner_token = (
            settings.batch_runner_cp_token.get_secret_value()
            if settings.batch_runner_cp_token is not None
            else None
        )
        runner_authorization = (
            f"Bearer {runner_token}" if runner_token else None
        )
        runner_task = asyncio.create_task(
            run_loop(
                session_factory=session_factory,
                http_client=http_client,
                batch_size=settings.batch_runner_batch_size,
                submit_rate_per_sec=(
                    settings.batch_runner_submit_rate_per_sec
                ),
                poll_interval_sec=(
                    settings.batch_runner_poll_interval_sec
                ),
                cp_authorization=runner_authorization,
            ),
            name="loom-svc-batch-runner",
        )
        app.state.batch_runner_task = runner_task

        try:
            yield
        finally:
            runner_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner_task
            with contextlib.suppress(Exception):
                await gateway_client.aclose()
            with contextlib.suppress(Exception):
                await http_client.aclose()
            await engine.dispose()

    app = FastAPI(title="Loom Service", version="0.0.1", lifespan=lifespan)

    @app.get("/", include_in_schema=False)
    async def _root() -> dict[str, object]:
        """Root landing — every other surface lives under `/api/v1/*`
        or `/docs`. Without this handler users hitting the bare URL
        see FastAPI's `{"detail": "Not Found"}` and assume the
        service is broken; return a tiny manifest of where to go
        next instead."""
        return {
            "service": "loom-service",
            "version": app.version,
            "links": {
                "swagger_ui": "/docs",
                "openapi_schema": "/openapi.json",
                "health": "/api/v1/health",
            },
            "note": (
                "API surface lives under /api/v1/* (most routes "
                "require a Bearer token; mint one with `loom service "
                "up` or via the admin tooling)."
            ),
        }

    app.include_router(health.router, prefix="/api/v1")
    app.include_router(tokens.router, prefix="/api/v1")
    app.include_router(trials.router, prefix="/api/v1")
    app.include_router(trajectory.router, prefix="/api/v1")
    app.include_router(atif.router, prefix="/api/v1")
    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(benchmarks.router, prefix="/api/v1")
    app.include_router(batches.router, prefix="/api/v1")
    app.include_router(rate_cards.router, prefix="/api/v1")
    app.include_router(teams.router, prefix="/api/v1")
    app.include_router(usage.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(models.router, prefix="/api/v1")
    app.include_router(backends.router, prefix="/api/v1")
    app.include_router(local_servers.router, prefix="/api/v1")
    return app
