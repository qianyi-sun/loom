from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient


class _FakeEngine:
    async def dispose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_background_task_drain_follow_up_cancels_all_pending_tasks_together() -> None:
    from loom_control_plane import app as control_plane_app

    drain_tasks = control_plane_app._cancel_and_drain_tasks
    second_cancellations: set[int] = set()
    all_second_cancellations = asyncio.Event()

    async def cancellation_resistant_task(index: int) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                second_cancellations.add(index)
                if len(second_cancellations) == 3:
                    all_second_cancellations.set()
                await all_second_cancellations.wait()
                raise

    tasks = tuple(
        asyncio.create_task(cancellation_resistant_task(index)) for index in range(3)
    )
    await asyncio.sleep(0)
    try:
        await asyncio.wait_for(
            drain_tasks(tasks, grace_seconds=0.01),
            timeout=0.5,
        )
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert second_cancellations == {0, 1, 2}
    assert all(task.done() for task in tasks)


def test_control_plane_lifespan_configures_db_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    create_engine_calls: list[dict[str, Any]] = []

    async def _schema_noop(_engine: object) -> int:
        return 0

    async def _background_noop(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    def _create_async_engine(_db_url: str, **kwargs: Any) -> _FakeEngine:
        create_engine_calls.append(kwargs)
        return _FakeEngine()

    monkeypatch.setattr(
        control_plane_app,
        "_assert_schema_startup",
        _schema_noop,
        raising=False,
    )
    monkeypatch.setattr(
        control_plane_app,
        "create_async_engine",
        _create_async_engine,
    )
    monkeypatch.setattr(
        control_plane_app,
        "build_s3_client",
        lambda **_: object(),
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_crash_detector_loop",
        _background_noop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_metrics_refresher_loop",
        _background_noop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_retry_exhausted_sweeper_loop",
        _background_noop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_service_execution_materializer_loop",
        _background_noop,
    )

    app = control_plane_app.create_app(
        ControlPlaneSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
            step_jwt_signing_key="test-step-jwt-signing-key",
            db_pool_size=33,
            db_max_overflow=44,
            db_pool_timeout_sec=55.5,
        ),
    )

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    assert create_engine_calls == [
        {
            "connect_args": {},
            "pool_pre_ping": True,
            "pool_size": 33,
            "max_overflow": 44,
            "pool_timeout": 55.5,
        }
    ]


def test_control_plane_lifespan_signals_materializer_before_cancelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    materializer_stop_states: list[bool] = []

    async def _schema_noop(_engine: object) -> int:
        return 0

    async def _background_noop(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    async def _materializer_background(**kwargs: object) -> None:
        stop_event = kwargs.get("stop_event")
        try:
            await asyncio.Event().wait()
        finally:
            materializer_stop_states.append(
                isinstance(stop_event, asyncio.Event) and stop_event.is_set()
            )

    monkeypatch.setattr(control_plane_app, "_assert_schema_startup", _schema_noop)
    monkeypatch.setattr(control_plane_app, "create_async_engine", lambda *_a, **_kw: _FakeEngine())
    monkeypatch.setattr(control_plane_app, "build_s3_client", lambda **_: object())
    monkeypatch.setattr(control_plane_app, "run_crash_detector_loop", _background_noop)
    monkeypatch.setattr(control_plane_app, "run_metrics_refresher_loop", _background_noop)
    monkeypatch.setattr(control_plane_app, "run_retry_exhausted_sweeper_loop", _background_noop)
    monkeypatch.setattr(
        control_plane_app,
        "run_service_execution_materializer_loop",
        _materializer_background,
    )

    app = control_plane_app.create_app(
        ControlPlaneSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
            step_jwt_signing_key="test-step-jwt-signing-key",
        ),
    )

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    assert materializer_stop_states == [True]


@pytest.mark.parametrize("trial_cutover", [False, True])
def test_control_plane_lifespan_proves_protected_runtime_before_serving(
    monkeypatch: pytest.MonkeyPatch, trial_cutover: bool,
) -> None:
    from pathlib import Path

    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    calls: list[str] = []

    class _ProtectedStore:
        def __init__(self, _session_factory: object) -> None:
            calls.append("store")

        async def assert_ready(self) -> None:
            calls.append("protected-ready")

    async def _schema_noop(_engine: object) -> int:
        calls.append("schema")
        return 0

    async def _background_noop(**_kwargs: object) -> None:
        calls.append("background")
        await asyncio.Event().wait()

    monkeypatch.setattr(control_plane_app, "_assert_schema_startup", _schema_noop)
    monkeypatch.setattr(
        control_plane_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
    )
    monkeypatch.setattr(
        control_plane_app,
        "load_protected_worker_runtime_db_url",
        lambda _path: "postgresql+psycopg://runtime:opaque@example/loom",
    )
    monkeypatch.setattr(control_plane_app, "ProtectedWorkerSessionStore", _ProtectedStore)
    monkeypatch.setattr(control_plane_app, "build_s3_client", lambda **_: object())
    monkeypatch.setattr(control_plane_app, "run_crash_detector_loop", _background_noop)
    monkeypatch.setattr(control_plane_app, "run_metrics_refresher_loop", _background_noop)
    monkeypatch.setattr(
        control_plane_app,
        "run_retry_exhausted_sweeper_loop",
        _background_noop,
    )

    for name in ("run_worker_pool_autoscaler_loop", "run_live_preview_reconciler_loop",
                 "run_elastic_slurm_worker_controller_loop", "run_service_execution_scheduler_loop",
                 "run_service_execution_materializer_loop"):
        monkeypatch.setattr(control_plane_app, name, _background_noop)

    app = control_plane_app.create_app(
        ControlPlaneSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
            step_jwt_signing_key="test-step-jwt-signing-key",
            protected_trial_cutover_enabled=trial_cutover,
            service_execution_scheduler_enabled=True,
            service_execution_materializer_enabled=True,
            protected_worker_runtime_db_url_file=Path("/run/loom/runtime/database-url"),
        )
    )

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    assert calls[:3] == ["schema", "store", "protected-ready"]
    assert calls.count("background") == (1 if trial_cutover else 7)


@pytest.mark.parametrize("method,path", [
    ("POST", "/trials"), ("POST", "/admin/worker-tokens"),
    ("DELETE", "/admin/worker-tokens"), ("POST", "/admin/slurm-worker-jobs"),
    ("POST", "/admin/slurm-worker-jobs/reconcile"),
    ("PUT", "/admin/worker-pool-autoscaler-policies/staging/gb10"),
    ("PUT", "/admin/gb10-worker-pools/staging/gb10/desired-state"),
    ("POST", "/admin/worker-pools/staging/gb10/prod-pressure"),
    ("POST", "/execution-attempts/00000000-0000-0000-0000-000000000001/started"),
    ("POST", "/admin/service-execution/commands/claim"),
    ("POST", "/trials/00000000-0000-0000-0000-000000000001/terminus/reclaim"),
])
def test_trial_cutover_rejects_legacy_writes_before_handler(method, path):
    from pathlib import Path

    from loom_control_plane.app import create_app
    from loom_control_plane.config import ControlPlaneSettings

    app = create_app(ControlPlaneSettings(
        _env_file=None, protected_trial_cutover_enabled=True,
        db_url="postgresql+psycopg://loom:loom@example/loom",
        minio_access_key="test", minio_secret_key="test",
        protected_worker_runtime_db_url_file=Path("/run/loom/runtime/database-url"),
    ))
    # Intentionally do not start lifespan: a rejected route cannot reach any SQL handler.
    client = TestClient(app, raise_server_exceptions=False)
    response = client.request(method, path, json={})
    assert response.status_code == 503, response.text
    assert response.json()["detail"] == "protected_trial_cutover_writer_disabled"
    assert client.get("/healthz").status_code == 200


def test_trial_cutover_requires_protected_runtime():
    from loom_control_plane.app import create_app
    from loom_control_plane.config import ControlPlaneSettings

    with pytest.raises(ValueError, match="requires protected worker runtime"):
        create_app(ControlPlaneSettings(_env_file=None, protected_trial_cutover_enabled=True,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            minio_access_key="test", minio_secret_key="test"))


def test_trial_cutover_admits_actual_gateway_step_token_route(monkeypatch):
    from pathlib import Path

    from fastapi import APIRouter

    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    # Preserve the actual composed production route; isolate its handler's SQL.
    route = next(route for route in control_plane_app.step_tokens.router.routes
                 if route.name == "issue_step_token")
    probe = APIRouter()

    async def issue_probe():
        return {"reached": True}

    probe.add_api_route(route.path, issue_probe, methods=list(route.methods))
    monkeypatch.setattr(control_plane_app.step_tokens, "router", probe)
    app = control_plane_app.create_app(ControlPlaneSettings(
        _env_file=None, protected_trial_cutover_enabled=True,
        db_url="postgresql+psycopg://loom:loom@example/loom",
        minio_access_key="test", minio_secret_key="test",
        protected_worker_runtime_db_url_file=Path("/run/loom/runtime/database-url"),
    ))
    client = TestClient(app)
    response = client.post("/admin/step-tokens", json={})
    assert response.status_code == 200, response.text
    assert response.json() == {"reached": True}
