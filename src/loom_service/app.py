"""FastAPI factory for loom_service (spec §2).

Stateless service. The lifespan opens a per-process async SQLAlchemy
engine, a boto3 S3 client for internal object-store operations, and an
httpx AsyncClient pointed at the Control Plane (for Plan 18+ forwarders).
Routes pull these off `request.app.state`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom.admin_secret import (
    AdminSecretVerifier,
    load_optional_admin_secret_verifier,
)
from loom.db.schema_startup import assert_schema_at_head
from loom.security.secret_store import assert_existing_secrets_decryptable
from loom.startup_retry import retry_startup_dependency
from loom_service.batch_runner import run_loop
from loom_service.config import LoomServiceSettings
from loom_service.metrics import (
    HTTP_REQUEST_LATENCY_SEC,
    HTTP_REQUESTS_TOTAL,
)
from loom_service.routes import (
    admin_audit,
    agents,
    atif,
    auth,
    backends,
    batches,
    benchmarks,
    health,
    invites,
    local_servers,
    models,
    monitor,
    overview,
    provider_connections,
    rate_cards,
    run_library,
    secret_store_admin,
    tasksets,
    tasks,
    team_registrations,
    teams,
    tokens,
    trajectory,
    trials,
    usage,
)
from loom_service.storage import (
    create_minio_client,
)


def _load_admin_secret_verifier(
    settings: LoomServiceSettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for loom_service startup."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


async def _assert_secret_store_startup(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return await assert_existing_secrets_decryptable(session)


async def _assert_schema_startup(engine: AsyncEngine) -> int:
    return await assert_schema_at_head(engine, db_url_env_var="LOOM_SVC_DB_URL")


def create_app(settings: LoomServiceSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(str(settings.db_url))
        await _assert_schema_startup(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        await retry_startup_dependency(
            lambda: _assert_secret_store_startup(session_factory),
            operation_name="service secret-store startup validation",
        )
        admin_secret_verifier = _load_admin_secret_verifier(settings)

        minio_client = create_minio_client(
            settings, endpoint_url=settings.minio_endpoint,
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
        app.state.admin_secret_verifier = admin_secret_verifier
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
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(auth.admin_router, prefix="/api/v1")
    app.include_router(invites.router, prefix="/api/v1")
    app.include_router(tokens.router, prefix="/api/v1")
    app.include_router(trials.router, prefix="/api/v1")
    app.include_router(trajectory.router, prefix="/api/v1")
    app.include_router(atif.router, prefix="/api/v1")
    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(benchmarks.router, prefix="/api/v1")
    app.include_router(tasksets.router, prefix="/api/v1")
    app.include_router(batches.router, prefix="/api/v1")
    app.include_router(run_library.router, prefix="/api/v1")
    app.include_router(rate_cards.router, prefix="/api/v1")
    app.include_router(admin_audit.router, prefix="/api/v1")
    app.include_router(team_registrations.router, prefix="/api/v1")
    app.include_router(teams.router, prefix="/api/v1")
    app.include_router(usage.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(models.router, prefix="/api/v1")
    app.include_router(monitor.router, prefix="/api/v1")
    app.include_router(overview.router, prefix="/api/v1")
    app.include_router(backends.router, prefix="/api/v1")
    app.include_router(local_servers.router, prefix="/api/v1")
    app.include_router(provider_connections.router, prefix="/api/v1")
    app.include_router(secret_store_admin.router, prefix="/api/v1")

    @app.middleware("http")
    async def _metrics_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        """Observe every HTTP request once. Uses FastAPI's matched
        route template (e.g., `/api/v1/trials/{trial_id}`) as the
        label so cardinality is bounded by the route count, not the
        UUIDs in the URL."""
        import time as _time
        t0 = _time.perf_counter()
        response = await call_next(request)
        elapsed = _time.perf_counter() - t0
        # The matched route lands on request.scope["route"] for
        # APIRoute matches; missing for /metrics and 404s. Fall back
        # to the raw URL path in those cases (bounded — 404s typically
        # come from a small set of operator typos).
        route_obj = request.scope.get("route")
        route_path = getattr(route_obj, "path", None) or request.url.path
        status_class = f"{response.status_code // 100}xx"
        HTTP_REQUESTS_TOTAL.labels(
            route=route_path, method=request.method,
            status_class=status_class,
        ).inc()
        HTTP_REQUEST_LATENCY_SEC.labels(
            route=route_path, method=request.method,
        ).observe(elapsed)
        return response

    # /metrics: prometheus_client ASGI app. Note the Ingress only
    # routes `/api/v1/*` to loom-service (see
    # `src/loom_cli/templates/k8s/ingress.yaml.j2`), so `/metrics`
    # is NOT reachable from the public Internet — only from cluster
    # scrapers using the ClusterIP Service. The #78 slice C
    # NetworkPolicy on loom-service still allows any-namespace
    # ingress (the Ingress controller is in another namespace and
    # hard to label-select), so production scrapers should target
    # the Service's cluster DNS, not the public URL.
    app.mount("/metrics", make_asgi_app())
    return app
