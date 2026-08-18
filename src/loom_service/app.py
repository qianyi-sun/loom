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
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
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
from loom.data_lifecycle_capacity import StagingAdmissionError
from loom.db.schema_startup import assert_schema_at_head
from loom.personal_dev_activation import load_personal_dev_activation_verifier
from loom.personal_dev_candidate import PersonalDevCandidateLimits
from loom.personal_dev_environment import PersonalDevLifecycleLimits
from loom.security.secret_store import assert_existing_secrets_decryptable
from loom.startup_retry import retry_startup_dependency
from loom.system_identities import assert_pipeline_controller_identity
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom.workload_trust import WorkloadTrustContract
from loom_service.batch_runner import run_loop as batch_run_loop
from loom_service.behavior_pipeline_adapter import install_behavior_pipeline_public_adapter
from loom_service.config import LoomServiceSettings
from loom_service.dev_instance_runtime import build_personal_dev_preparation_runtime
from loom_service.metrics import (
    HTTP_REQUEST_LATENCY_SEC,
    HTTP_REQUESTS_TOTAL,
)
from loom_service.personal_dev_builder import (
    build_personal_dev_builder_runtime,
    personal_dev_builder_run_loop,
)
from loom_service.personal_dev_candidate_gc import (
    build_personal_dev_artifact_collector,
    personal_dev_artifact_gc_run_loop,
)
from loom_service.personal_dev_lifecycle import (
    build_personal_dev_capacity_runtime,
    personal_dev_reconcile_run_loop,
)
from loom_service.pipeline_control_bindings import SqlPipelineRecipeBindingResolver
from loom_service.pipeline_stage1_smoke_authority import (
    build_stage1_candidate_authority_from_environment,
)
from loom_service.pipeline_stage1_smoke_service import (
    load_stage1_smoke_signature_verifier,
)
from loom_service.routes import (
    admin_audit,
    agents,
    atif,
    auth,
    backends,
    batches,
    benchmarks,
    delivery_exports,
    dev_instances,
    health,
    invites,
    local_servers,
    models,
    monitor,
    overview,
    personal_dev_candidates,
    pipeline,
    pipeline_stage1_smoke,
    pipeline_stage1_smoke_prepare,
    provider_connections,
    rate_cards,
    run_library,
    secret_store_admin,
    tasks,
    tasksets,
    team_registrations,
    teams,
    tokens,
    trajectory,
    trials,
    usage,
)
from loom_service.session_auth import (
    is_staging_admin_browser_session,
    staging_admin_browser_request_allowed,
)
from loom_service.storage import (
    create_minio_client,
)
from loom_service.taskset_gc import run_loop as taskset_gc_run_loop
from loom_service.taskset_materializer import run_loop as taskset_materializer_run_loop


def _load_admin_secret_verifier(
    settings: LoomServiceSettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for loom_service startup."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


def _validated_v1_workload_contract(
    settings: LoomServiceSettings,
) -> WorkloadTrustContract:
    """Return the deployment workload contract or reject an invalid v1 startup."""
    contract = settings.workload_contract
    violations = contract.v1_violations()
    if violations:
        raise RuntimeError(
            "invalid v1 workload trust contract: " + "; ".join(violations),
        )
    return contract


async def _assert_secret_store_startup(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        count = await assert_existing_secrets_decryptable(session)
        await assert_pipeline_controller_identity(session)
        return count


async def _assert_schema_startup(engine: AsyncEngine) -> int:
    return await assert_schema_at_head(engine, db_url_env_var="LOOM_SVC_DB_URL")


def create_app(settings: LoomServiceSettings) -> FastAPI:
    workload_contract = _validated_v1_workload_contract(settings)
    if settings.pipeline_stage1_smoke_signature_max_age_sec <= 0:
        raise RuntimeError("Pipeline Stage 1 smoke signature max age must be positive")
    personal_dev_limits: PersonalDevLifecycleLimits | None = None
    personal_dev_candidate_limits: PersonalDevCandidateLimits | None = None
    if settings.dev_instances_enabled:
        personal_dev_candidate_limits = PersonalDevCandidateLimits(
            per_owner_retained_candidates=(settings.personal_dev_candidate_retained_count_limit),
            per_owner_retained_archive_bytes=(settings.personal_dev_candidate_retained_bytes_limit),
            global_active_builds=settings.personal_dev_builder_global_concurrency,
            per_owner_active_builds=(settings.personal_dev_builder_per_owner_concurrency),
        )
        personal_dev_limits = PersonalDevLifecycleLimits(
            global_live_instances=settings.personal_dev_global_live_instance_limit,
            per_owner_live_instances=settings.personal_dev_per_owner_live_instance_limit,
            per_owner_aggregate_min_slots=(settings.personal_dev_per_owner_aggregate_min_slots),
            per_owner_aggregate_max_slots=(settings.personal_dev_per_owner_aggregate_max_slots),
        )
        if settings.personal_dev_reconciler_lease_sec <= 0:
            raise RuntimeError("personal-dev reconciler lease must be positive")
        if settings.personal_dev_reconciler_poll_interval_sec <= 0:
            raise RuntimeError("personal-dev reconciler poll interval must be positive")
        if settings.personal_dev_activation_ack_max_age_sec <= 0:
            raise RuntimeError("personal-dev activation acknowledgement max age must be positive")
        if not 300 <= settings.personal_dev_builder_lease_sec <= 7200:
            raise RuntimeError("personal-dev builder lease must be between 300 and 7200 seconds")
        if settings.personal_dev_builder_poll_interval_sec <= 0:
            raise RuntimeError("personal-dev builder poll interval must be positive")
        if settings.personal_dev_candidate_gc_retention_sec < 0:
            raise RuntimeError("personal-dev artifact GC retention must be non-negative")
        if not 60 <= settings.personal_dev_candidate_gc_lease_sec <= 7200:
            raise RuntimeError("personal-dev artifact GC lease must be between 60 and 7200 seconds")
        if settings.personal_dev_candidate_gc_poll_interval_sec <= 0:
            raise RuntimeError("personal-dev artifact GC poll interval must be positive")

    @asynccontextmanager
    async def _service_lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Validate deterministic URL shape before opening database, mTLS, or
        # HTTP resources so a startup rejection cannot leak any of them.
        gw_path = settings.gateway_url.path or "/"
        if gw_path not in ("", "/"):
            raise RuntimeError(
                f"LOOM_SVC_GATEWAY_URL must not include a path prefix "
                f"(got {settings.gateway_url!s}). Forwarders use "
                f"absolute paths; a prefix would be silently dropped."
            )
        engine = create_async_engine(
            settings.db_engine_url,
            connect_args=settings.db_engine_connect_args,
        )
        app.state._owned_service_engine = engine
        await _assert_schema_startup(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        await retry_startup_dependency(
            lambda: _assert_secret_store_startup(session_factory),
            operation_name="service secret-store startup validation",
        )
        admin_secret_verifier = _load_admin_secret_verifier(settings)
        if settings.pipeline_stage1_smoke_public_key_file is not None:
            app.state.pipeline_stage1_smoke_verifier = load_stage1_smoke_signature_verifier(
                settings.pipeline_stage1_smoke_public_key_file,
                key_id=settings.pipeline_stage1_smoke_key_id,
                max_age_seconds=(settings.pipeline_stage1_smoke_signature_max_age_sec),
            )

        minio_client = create_minio_client(
            settings,
            endpoint_url=settings.minio_endpoint,
        )
        app.state._owned_service_minio_client = minio_client
        personal_dev_task: asyncio.Task[None] | None = None
        personal_dev_builder_task: asyncio.Task[None] | None = None
        personal_dev_artifact_gc_task: asyncio.Task[None] | None = None
        personal_dev_runtime = None
        personal_dev_builder_runtime = None
        personal_dev_artifact_collector = None
        personal_dev_capacity_runtime = None
        if personal_dev_limits is not None:
            personal_dev_capacity_runtime = build_personal_dev_capacity_runtime(settings)
            if personal_dev_capacity_runtime is None:  # pragma: no cover - guarded by limits
                raise RuntimeError("personal-dev capacity runtime is unavailable")
            app.state._owned_personal_dev_capacity_projector = (
                personal_dev_capacity_runtime.projector
            )
            if settings.personal_dev_activation_public_key_file is None:
                raise RuntimeError(
                    "LOOM_SVC_PERSONAL_DEV_ACTIVATION_PUBLIC_KEY_FILE is required "
                    "when dev instances are enabled",
                )
            app.state.personal_dev_activation_verifier = load_personal_dev_activation_verifier(
                settings.personal_dev_activation_public_key_file,
                key_id=settings.personal_dev_activation_key_id,
                max_age_seconds=settings.personal_dev_activation_ack_max_age_sec,
                expected_sha256=(
                    settings.personal_dev_activation_public_key_sha256
                    if settings.personal_dev_builder_enabled
                    else None
                ),
            )
            personal_dev_runtime = build_personal_dev_preparation_runtime(
                settings,
                minio_client=minio_client,
            )
            if personal_dev_runtime is None:  # pragma: no cover - guarded by limits
                raise RuntimeError("personal-dev preparation runtime is unavailable")
            personal_dev_builder_runtime = build_personal_dev_builder_runtime(
                settings,
                minio_client=minio_client,
            )
            personal_dev_artifact_collector = build_personal_dev_artifact_collector(
                settings,
                minio_client=minio_client,
            )
            if personal_dev_capacity_runtime.acceptance_interlock is not None:
                await personal_dev_capacity_runtime.acceptance_interlock.assert_ready(
                    now=datetime.now(UTC)
                )
                app.state.personal_dev_acceptance_interlock = (
                    personal_dev_capacity_runtime.acceptance_interlock
                )
        http_client = httpx.AsyncClient(
            base_url=str(settings.control_plane_url),
            timeout=10.0,
        )
        app.state._owned_service_http_client = http_client

        # Plan 20: separate httpx client for the Gateway. Rate-card
        # routes proxy to /admin/rate-cards on the Gateway; we keep
        # CP + Gateway clients independent so a slow CP doesn't
        # starve the rate-card surface.
        # Path-prefix sanity check: a base_url with a non-root path
        # (e.g. `https://gw/loom/`) silently strips the prefix when
        # the forwarder uses absolute paths like `/admin/rate-cards`.
        # Fail at startup rather than route to the wrong URL.
        gateway_client = httpx.AsyncClient(
            base_url=str(settings.gateway_url),
            timeout=10.0,
        )
        app.state._owned_service_gateway_client = gateway_client

        app.state.settings = settings
        app.state.session_factory = session_factory
        pipeline_binding_resolver = SqlPipelineRecipeBindingResolver(session_factory)
        app.state.pipeline_binding_resolver = pipeline_binding_resolver
        app.state.pipeline_judge_profile_reader = pipeline_binding_resolver
        app.state.admin_secret_verifier = admin_secret_verifier
        app.state.minio_client = minio_client
        app.state.http_client = http_client
        app.state.gateway_client = gateway_client
        app.state.personal_dev_candidate_limits = personal_dev_candidate_limits
        app.state.personal_dev_builder_available = personal_dev_builder_runtime is not None
        install_behavior_pipeline_public_adapter(app=app, settings=settings)
        if personal_dev_capacity_runtime is not None:
            app.state.personal_dev_capacity_status_reader = (
                personal_dev_capacity_runtime.status_reader
            )
        if personal_dev_builder_runtime is not None and personal_dev_candidate_limits is not None:
            personal_dev_builder_task = asyncio.create_task(
                personal_dev_builder_run_loop(
                    session_factory=session_factory,
                    source=personal_dev_builder_runtime.source,
                    executor=personal_dev_builder_runtime.executor,
                    limits=personal_dev_candidate_limits,
                    builder_id=f"loom-service:{socket.gethostname()}:{os.getpid()}",
                    lease_seconds=settings.personal_dev_builder_lease_sec,
                    registry_prefix=settings.personal_dev_builder_registry_prefix,
                    poll_interval_seconds=settings.personal_dev_builder_poll_interval_sec,
                ),
                name="loom-svc-personal-dev-builder",
            )
            app.state.personal_dev_builder_task = personal_dev_builder_task
        if (
            personal_dev_artifact_collector is not None
            and personal_dev_candidate_limits is not None
        ):
            personal_dev_artifact_gc_task = asyncio.create_task(
                personal_dev_artifact_gc_run_loop(
                    session_factory=session_factory,
                    collector=personal_dev_artifact_collector,
                    limits=personal_dev_candidate_limits,
                    collector_id=f"loom-service:{socket.gethostname()}:{os.getpid()}",
                    retention_seconds=settings.personal_dev_candidate_gc_retention_sec,
                    lease_seconds=settings.personal_dev_candidate_gc_lease_sec,
                    poll_interval_seconds=(settings.personal_dev_candidate_gc_poll_interval_sec),
                ),
                name="loom-svc-personal-dev-artifact-gc",
            )
            app.state.personal_dev_artifact_gc_task = personal_dev_artifact_gc_task
        if (
            personal_dev_runtime is not None
            and personal_dev_capacity_runtime is not None
            and personal_dev_limits is not None
        ):
            personal_dev_task = asyncio.create_task(
                personal_dev_reconcile_run_loop(
                    session_factory=session_factory,
                    executor=personal_dev_runtime,
                    capacity_installer=personal_dev_capacity_runtime.installer,
                    capacity_projector=personal_dev_capacity_runtime.projector,
                    limits=personal_dev_limits,
                    reconciler_id=f"loom-service:{socket.gethostname()}:{os.getpid()}",
                    lease_seconds=settings.personal_dev_reconciler_lease_sec,
                    poll_interval_seconds=(settings.personal_dev_reconciler_poll_interval_sec),
                ),
                name="loom-svc-personal-dev-reconciler",
            )
            app.state.personal_dev_reconciler_task = personal_dev_task

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
        runner_authorization = f"Bearer {runner_token}" if runner_token else None
        runner_task = asyncio.create_task(
            batch_run_loop(
                session_factory=session_factory,
                http_client=http_client,
                batch_size=settings.batch_runner_batch_size,
                submit_rate_per_sec=(settings.batch_runner_submit_rate_per_sec),
                poll_interval_sec=(settings.batch_runner_poll_interval_sec),
                cp_authorization=runner_authorization,
            ),
            name="loom-svc-batch-runner",
        )
        app.state.batch_runner_task = runner_task

        materializer_task = asyncio.create_task(
            taskset_materializer_run_loop(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=settings.artifacts_bucket,
                upstream_cache_root=settings.taskset_materializer_upstream_cache_root,
                batch_size=settings.taskset_materializer_batch_size,
                poll_interval_sec=settings.taskset_materializer_poll_interval_sec,
                claim_ttl_sec=settings.taskset_materializer_claim_ttl_sec,
                transform_config=TransformSandboxConfig(
                    enabled=settings.taskset_materializer_transforms_enabled,
                    network_isolated=settings.taskset_materializer_transform_network_isolated,
                    workload_contract=workload_contract,
                    wall_timeout_sec=settings.taskset_materializer_transform_wall_timeout_sec,
                    cpu_limit_sec=settings.taskset_materializer_transform_cpu_limit_sec,
                    memory_limit_mb=settings.taskset_materializer_transform_memory_limit_mb,
                ),
                max_bundle_bytes=settings.taskset_quota_max_bundle_bytes,
                max_team_storage_bytes=settings.taskset_quota_max_storage_bytes_per_team,
            ),
            name="loom-svc-taskset-materializer",
        )
        app.state.taskset_materializer_task = materializer_task

        gc_task = asyncio.create_task(
            taskset_gc_run_loop(
                session_factory=session_factory,
                minio_client=minio_client,
                artifacts_bucket=settings.artifacts_bucket,
                retention_days=settings.taskset_gc_retention_days,
                poll_interval_sec=settings.taskset_gc_poll_interval_sec,
            ),
            name="loom-svc-taskset-gc",
        )
        app.state.taskset_gc_task = gc_task

        try:
            yield
        finally:
            runner_task.cancel()
            materializer_task.cancel()
            gc_task.cancel()
            if personal_dev_task is not None:
                personal_dev_task.cancel()
            if personal_dev_builder_task is not None:
                personal_dev_builder_task.cancel()
            if personal_dev_artifact_gc_task is not None:
                personal_dev_artifact_gc_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner_task
            with contextlib.suppress(asyncio.CancelledError):
                await materializer_task
            with contextlib.suppress(asyncio.CancelledError):
                await gc_task
            if personal_dev_task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await personal_dev_task
            if personal_dev_builder_task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await personal_dev_builder_task
            if personal_dev_artifact_gc_task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await personal_dev_artifact_gc_task

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Close every owned client even when startup rejects before yielding."""

        try:
            async with _service_lifespan(app):
                yield
        finally:
            for attribute in (
                "_owned_service_gateway_client",
                "_owned_service_http_client",
                "_owned_personal_dev_capacity_projector",
            ):
                client = getattr(app.state, attribute, None)
                close = getattr(client, "aclose", None)
                if callable(close):
                    with contextlib.suppress(Exception):
                        await close()
            minio = getattr(app.state, "_owned_service_minio_client", None)
            close_minio = getattr(minio, "close", None)
            if callable(close_minio):
                with contextlib.suppress(Exception):
                    close_minio()
            owned_engine = getattr(app.state, "_owned_service_engine", None)
            dispose = getattr(owned_engine, "dispose", None)
            if callable(dispose):
                with contextlib.suppress(Exception):
                    await dispose()

    app = FastAPI(title="Loom Service", version="0.0.1", lifespan=lifespan)
    app.state.personal_dev_builder_available = False
    stage1_candidate_authority = build_stage1_candidate_authority_from_environment(
        repo_root=Path(__file__).resolve().parents[2]
    )
    if stage1_candidate_authority is not None:
        app.state.pipeline_stage1_candidate_authority = stage1_candidate_authority

    @app.exception_handler(StagingAdmissionError)
    async def _staging_admission_error(
        _request: Request,
        exc: StagingAdmissionError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={
                "detail": {
                    "reason": str(exc),
                    "retryable": True,
                }
            },
        )

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
    app.include_router(delivery_exports.router, prefix="/api/v1")
    app.include_router(dev_instances.router, prefix="/api/v1")
    app.include_router(dev_instances.internal_router, prefix="/api/v1/internal")
    app.include_router(personal_dev_candidates.router, prefix="/api/v1")
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
    app.include_router(pipeline.router, prefix="/api/v1")
    app.include_router(pipeline_stage1_smoke.router, prefix="/api/v1/internal")
    app.include_router(pipeline_stage1_smoke_prepare.router, prefix="/api/v1/internal")
    app.include_router(backends.router, prefix="/api/v1")
    app.include_router(local_servers.router, prefix="/api/v1")
    app.include_router(provider_connections.router, prefix="/api/v1")
    app.include_router(secret_store_admin.router, prefix="/api/v1")

    @app.middleware("http")
    async def _staging_admin_validation_session_middleware(  # type: ignore[no-untyped-def]
        request: Request,
        call_next,
    ):
        """Fail closed on every mutation except exact session cleanup."""
        raw_cookie = request.cookies.get(settings.auth_session_cookie_name)
        request_path = request.scope.get("path", request.url.path)
        hidden_bootstrap_probe = (
            os.environ.get("LOOM_ENV", "").strip().lower() != "staging"
            and request.method.upper() == "POST"
            and request_path == "/api/v1/auth/staging-admin-browser-session"
        )
        if (
            is_staging_admin_browser_session(raw_cookie)
            and not hidden_bootstrap_probe
            and not staging_admin_browser_request_allowed(
                method=request.method,
                path=request_path,
            )
        ):
            return JSONResponse(
                status_code=403,
                content={
                    "detail": ("staging admin browser session is validation-only"),
                },
                headers={"Cache-Control": "no-store"},
            )
        return await call_next(request)

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
            route=route_path,
            method=request.method,
            status_class=status_class,
        ).inc()
        HTTP_REQUEST_LATENCY_SEC.labels(
            route=route_path,
            method=request.method,
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
