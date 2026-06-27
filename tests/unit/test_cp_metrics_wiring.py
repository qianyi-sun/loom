"""Unit tests for #81 slice B-1 — verify the CP metrics declared in
metrics.py are wired into the actual handlers + the /metrics endpoint
is mounted. Integration with a real DB is covered by the existing
state-PATCH + crash-detector integration suite."""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def _isolate_env_from_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """ControlPlaneSettings has `extra=forbid` and the local dev
    `.env` ships several `LOOM_*` env vars that aren't ControlPlane
    fields (LOOM_WORKER_TOKEN, LOOM_TEAM_TOKEN, LOOM_ADMIN_TOKEN).
    Strip them so the settings constructor doesn't trip."""
    for k in (
        "LOOM_WORKER_TOKEN", "LOOM_TEAM_TOKEN", "LOOM_ADMIN_TOKEN",
    ):
        monkeypatch.delenv(k, raising=False)


def test_metrics_endpoint_mounted_on_app() -> None:
    """`/metrics` is a sub-app mounted by `create_app`. We don't boot
    the lifespan (would need Postgres); just inspect the app routes."""
    from loom_control_plane.app import create_app
    from loom_control_plane.config import ControlPlaneSettings

    # Minimal settings — values don't matter, we never run the
    # lifespan; we just verify the route table. _env_file=None tells
    # pydantic_settings to skip the dev `.env` (which carries
    # non-CP keys that would trip `extra=forbid`).
    settings = ControlPlaneSettings(
        _env_file=None,  # type: ignore[call-arg]
        db_url="postgresql+asyncpg://loom@x/y",
        minio_endpoint="http://x",
        minio_access_key="x",
        minio_secret_key="x",
        llm_gateway_url="http://gw.x",
    )
    app = create_app(settings)
    mounts = [r for r in app.routes if hasattr(r, "path") and r.path.startswith("/metrics")]
    assert mounts, "no /metrics route in CP app"


def test_metrics_objects_exposed_to_registry() -> None:
    """The metric objects (declared in metrics.py) MUST be registered
    on the default prometheus_client registry — otherwise scrapes
    return nothing."""
    # Import has the side effect of registering on REGISTRY.
    from loom_control_plane import metrics  # noqa: F401

    names = {m.name for m in REGISTRY.collect()}
    # prometheus_client suffixes counters with `_total`; check the
    # declared name (without that suffix) is present in some form.
    expected = {
        "loom_workers_active",
        "loom_queue_depth",
        "loom_trials_inflight",
        "loom_slurm_worker_desired_slots",
        "loom_slurm_worker_active_slots",
        "loom_slurm_worker_pending_slots",
        "loom_slurm_worker_stale_slots",
        "loom_slurm_worker_running_jobs",
        "loom_slurm_worker_pending_jobs",
        "loom_slurm_worker_stale_jobs",
        "loom_slurm_worker_failed_submissions",
        "loom_slurm_worker_cancelled_pending_jobs",
        "loom_slurm_worker_idle_exits",
        "loom_worker_pool_total_slots",
        "loom_worker_pool_occupied_slots",
        "loom_worker_pool_free_slots",
        "loom_worker_pool_workers",
        "loom_worker_pool_desired_slots",
        "loom_worker_pool_pending_slots",
        "loom_worker_pool_draining_slots",
        "loom_worker_pool_draining_workers",
        "loom_worker_pool_autoscaler_decision",
        "loom_worker_pool_autoscaler_error",
        "loom_worker_pool_autoscaler_idle_seconds",
        "loom_worker_reclaim",   # counter → exposed as ..._total
        "loom_state_patch",      # counter → exposed as ..._total
        "loom_trials_state",     # counter → exposed as ..._total
        "loom_claim_latency_sec",
    }
    for stem in expected:
        # Either the bare name (gauges + histograms) or its
        # `_total`-suffixed counter form is present.
        assert stem in names or f"{stem}_total" in names or any(
            n.startswith(stem) for n in names
        ), f"metric stem {stem!r} not in registered names"


def test_state_route_imports_state_patch_total() -> None:
    """The state-patch route must import the STATE_PATCH_TOTAL counter
    so its on-success / on-fenced branches can increment it."""
    import loom_control_plane.routes.state as state_module
    assert hasattr(state_module, "STATE_PATCH_TOTAL")


def test_workers_route_imports_claim_latency() -> None:
    import loom_control_plane.routes.workers as workers_module
    assert hasattr(workers_module, "CLAIM_LATENCY_SEC")


def test_crash_detector_imports_worker_reclaim_total() -> None:
    import loom_control_plane.scheduler.crash_detector as cd_module
    assert hasattr(cd_module, "WORKER_RECLAIM_TOTAL")


def test_metrics_refresher_imports_slurm_worker_metrics() -> None:
    import loom_control_plane.metrics_refresher as refresher_module

    assert hasattr(refresher_module, "SLURM_WORKER_DESIRED_SLOTS")
    assert hasattr(refresher_module, "SLURM_WORKER_ACTIVE_SLOTS")
    assert hasattr(refresher_module, "SLURM_WORKER_PENDING_SLOTS")
    assert hasattr(refresher_module, "SLURM_WORKER_STALE_SLOTS")
    assert hasattr(refresher_module, "SLURM_WORKER_STALE_JOBS")
    assert hasattr(refresher_module, "WORKER_POOL_TOTAL_SLOTS")
    assert hasattr(refresher_module, "WORKER_POOL_OCCUPIED_SLOTS")
    assert hasattr(refresher_module, "WORKER_POOL_DESIRED_SLOTS")
    assert hasattr(refresher_module, "WORKER_POOL_DRAINING_SLOTS")
    assert hasattr(refresher_module, "WORKER_POOL_AUTOSCALER_DECISION")
    assert hasattr(refresher_module, "WORKER_POOL_AUTOSCALER_IDLE_SECONDS")


def test_control_plane_app_imports_elastic_slurm_controller_loop() -> None:
    import loom_control_plane.app as app_module

    assert hasattr(app_module, "run_elastic_slurm_worker_controller_loop")
