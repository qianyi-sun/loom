"""Control Plane app factory."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier, load_optional_admin_secret_verifier
from loom.db.schema_startup import assert_schema_at_head
from loom.storage_credentials import build_s3_client
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.elastic_slurm_worker_controller import (
    SubprocessSlurmCommandRunner,
    build_controller_config,
    run_elastic_slurm_worker_controller_loop,
)
from loom_control_plane.metrics_refresher import run_metrics_refresher_loop
from loom_control_plane.retry_exhausted_sweeper import (
    run_retry_exhausted_sweeper_loop,
)
from loom_control_plane.routes import (
    admin,
    artifacts,
    health,
    state,
    step_tokens,
    tasks,
    trajectory,
    trial_cache,
    trials,
    workers,
)
from loom_control_plane.scheduler.crash_detector import run_crash_detector_loop
from loom_control_plane.worker_pool_autoscaler import (
    run_worker_pool_autoscaler_loop,
)


def _load_admin_secret_verifier(
    settings: ControlPlaneSettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for Control Plane startup."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


async def _assert_schema_startup(engine: AsyncEngine) -> int:
    return await assert_schema_at_head(engine, db_url_env_var="LOOM_CP_DB_URL")


def create_app(settings: ControlPlaneSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(
            settings.db_engine_url,
            connect_args=settings.db_engine_connect_args,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout_sec,
        )
        await _assert_schema_startup(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        admin_secret_verifier = _load_admin_secret_verifier(settings)

        minio_client = build_s3_client(
            endpoint_url=settings.minio_endpoint,
            auth_kind=settings.storage_auth_kind,
            access_key=settings.minio_access_key.get_secret_value(),
            secret_key=settings.minio_secret_key.get_secret_value(),
            region=settings.minio_region,
        )

        app.state.settings = settings
        app.state.session_factory = session_factory
        app.state.admin_secret_verifier = admin_secret_verifier
        app.state.minio_client = minio_client

        slurm_controller_config = build_controller_config(
            enabled=settings.slurm_worker_controller_enabled,
            environment=settings.slurm_worker_controller_environment,
            pool_name=settings.slurm_worker_controller_pool_name,
            allowed_nodes_csv=settings.slurm_worker_controller_allowed_nodes,
            env_file=settings.slurm_worker_controller_env_file,
            repo_dir=settings.slurm_worker_controller_repo_dir,
            partition=settings.slurm_worker_controller_partition,
            time_limit=settings.slurm_worker_controller_time_limit,
            requested_cpus=settings.slurm_worker_controller_requested_cpus,
            requested_memory_mib=settings.slurm_worker_controller_requested_memory_mib,
            requested_concurrency=(settings.slurm_worker_controller_requested_concurrency),
            max_jobs=settings.slurm_worker_controller_max_jobs,
            pending_job_cap=settings.slurm_worker_controller_pending_job_cap,
            min_queued_trials=settings.slurm_worker_controller_min_queued_trials,
            stale_after_seconds=settings.slurm_worker_controller_stale_after_seconds,
            sbatch_path=settings.slurm_worker_controller_sbatch_path,
            squeue_path=settings.slurm_worker_controller_squeue_path,
            sacct_path=settings.slurm_worker_controller_sacct_path,
            scancel_path=settings.slurm_worker_controller_scancel_path,
            command_timeout_seconds=(settings.slurm_worker_controller_command_timeout_seconds),
        )

        crash_detector_task = asyncio.create_task(
            run_crash_detector_loop(
                session_factory=session_factory,
                expiry_sec=settings.worker_heartbeat_expiry_sec,
                interval_sec=settings.worker_reclaim_sweep_interval_sec,
                claimed_without_start_expiry_sec=(settings.claimed_without_start_expiry_sec),
                running_stale_timeout_multiplier=(
                    settings.stale_running_trial_timeout_multiplier
                    if settings.stale_running_trial_reclaim_enabled
                    else None
                ),
                running_stale_grace_sec=settings.stale_running_trial_grace_sec,
                running_stale_silence_sec=settings.stale_running_trial_silence_sec,
            ),
            name="loom-cp-crash-detector",
        )
        # Background refresher for gauge metrics (workers_active,
        # queue_depth, trials_inflight). See metrics_refresher.py
        # for the cadence rationale.
        metrics_refresher_task = asyncio.create_task(
            run_metrics_refresher_loop(
                session_factory=session_factory,
                expiry_sec=settings.worker_heartbeat_expiry_sec,
                interval_sec=30,
            ),
            name="loom-cp-metrics-refresher",
        )
        # Background sweep that transitions queued trials with
        # attempt_count >= team_quotas.max_attempts_ceiling to state='failed' with
        # failure_reason='retry_exhausted'. Runs at the same cadence
        # as the crash detector so the two sweeps are in lock-step.
        retry_exhausted_task = asyncio.create_task(
            run_retry_exhausted_sweeper_loop(
                session_factory=session_factory,
                interval_sec=settings.worker_reclaim_sweep_interval_sec,
            ),
            name="loom-cp-retry-exhausted-sweeper",
        )
        worker_pool_autoscaler_task = asyncio.create_task(
            run_worker_pool_autoscaler_loop(
                session_factory=session_factory,
                environment=settings.slurm_worker_controller_environment,
                interval_sec=settings.worker_reclaim_sweep_interval_sec,
                freshness_sec=settings.worker_heartbeat_expiry_sec,
            ),
            name="loom-cp-worker-pool-autoscaler",
        )
        slurm_controller_task: asyncio.Task[None] | None = None
        if slurm_controller_config is not None:
            slurm_controller_task = asyncio.create_task(
                run_elastic_slurm_worker_controller_loop(
                    session_factory=session_factory,
                    config=slurm_controller_config,
                    runner=SubprocessSlurmCommandRunner().bind_config(
                        slurm_controller_config,
                    ),
                    interval_sec=settings.worker_reclaim_sweep_interval_sec,
                ),
                name="loom-cp-elastic-slurm-worker-controller",
            )
        try:
            yield
        finally:
            crash_detector_task.cancel()
            metrics_refresher_task.cancel()
            retry_exhausted_task.cancel()
            worker_pool_autoscaler_task.cancel()
            if slurm_controller_task is not None:
                slurm_controller_task.cancel()
            # Bound the await so a stuck task (e.g. mid-DB call when
            # cancellation arrives, asyncpg connection takes a moment
            # to release) doesn't block the entire lifespan shutdown —
            # which then blocks `TestClient.__exit__`, which then
            # blocks the test. Five seconds is generous for a task
            # that should respond to cancel in microseconds.
            for t in (
                crash_detector_task,
                metrics_refresher_task,
                retry_exhausted_task,
                worker_pool_autoscaler_task,
                slurm_controller_task,
            ):
                if t is None:
                    continue
                with contextlib.suppress(
                    asyncio.CancelledError,
                    asyncio.TimeoutError,
                ):
                    await asyncio.wait_for(t, timeout=5.0)
            await engine.dispose()

    app = FastAPI(
        title="Loom Control Plane",
        version="0.0.1",
        lifespan=lifespan,
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
    app.include_router(trial_cache.router)
    # /metrics: standard prometheus_client ASGI app. Mounted at the
    # top-level for prometheus scrapers (operator-supplied
    # ServiceMonitor / PodMonitor uses the default `/metrics` path).
    # The CP service is internal (not exposed via Ingress); scrapers
    # reach it through cluster DNS.
    app.mount("/metrics", make_asgi_app())
    return app
