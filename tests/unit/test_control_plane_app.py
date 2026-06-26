from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient


class _FakeEngine:
    async def dispose(self) -> None:
        pass


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
    monkeypatch.setattr(control_plane_app.boto3, "client", lambda *_, **__: object())
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

    assert create_engine_calls == [{
        "pool_pre_ping": True,
        "pool_size": 33,
        "max_overflow": 44,
        "pool_timeout": 55.5,
    }]
