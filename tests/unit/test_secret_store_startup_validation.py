from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient


class _FakeEngine:
    async def dispose(self) -> None:
        pass


def test_gateway_lifespan_validates_existing_secret_store_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_llm_gateway import app as gateway_app
    from loom_llm_gateway.config import GatewaySettings

    calls = 0

    async def _validate(_session_factory: object) -> int:
        nonlocal calls
        calls += 1
        return 0

    monkeypatch.setattr(
        gateway_app,
        "_assert_secret_store_startup",
        _validate,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "create_async_engine",
        lambda _db_url: _FakeEngine(),
    )

    app = gateway_app.create_app(
        GatewaySettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            step_jwt_signing_key="test-step-jwt-signing-key",
        ),
    )

    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200

    assert calls == 1


def test_service_lifespan_validates_existing_secret_store_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service import app as service_app
    from loom_service.config import LoomServiceSettings

    calls = 0

    async def _validate(_session_factory: object) -> int:
        nonlocal calls
        calls += 1
        return 0

    async def _run_loop(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(
        service_app,
        "_assert_secret_store_startup",
        _validate,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "create_async_engine",
        lambda _db_url: _FakeEngine(),
    )
    monkeypatch.setattr(service_app, "run_loop", _run_loop)

    app = service_app.create_app(
        LoomServiceSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            gateway_url="http://gw.example",
            control_plane_url="http://cp.example",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
        ),
    )

    with TestClient(app) as client:
        assert client.get("/api/v1/health").status_code == 200

    assert calls == 1
