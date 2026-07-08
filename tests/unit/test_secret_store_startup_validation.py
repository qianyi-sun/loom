from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient


class _FakeEngine:
    async def dispose(self) -> None:
        pass


async def _schema_noop(_engine: object) -> int:
    return 0


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
        "_assert_schema_startup",
        _schema_noop,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
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


def test_gateway_lifespan_validates_schema_before_secret_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_llm_gateway import app as gateway_app
    from loom_llm_gateway.config import GatewaySettings

    calls: list[str] = []

    async def _validate_schema(_engine: object) -> None:
        calls.append("schema")

    async def _validate_secrets(_session_factory: object) -> int:
        calls.append("secrets")
        return 0

    monkeypatch.setattr(
        gateway_app,
        "_assert_schema_startup",
        _validate_schema,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "_assert_secret_store_startup",
        _validate_secrets,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
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

    assert calls == ["schema", "secrets"]


def test_gateway_lifespan_wraps_secret_validation_in_startup_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_llm_gateway import app as gateway_app
    from loom_llm_gateway.config import GatewaySettings

    calls: list[str] = []

    async def _validate_secrets(_session_factory: object) -> int:
        calls.append("secrets")
        return 0

    async def _retry(run, *, operation_name: str, **_kwargs: object) -> int:
        calls.append(operation_name)
        return await run()

    monkeypatch.setattr(
        gateway_app,
        "_assert_schema_startup",
        _schema_noop,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "_assert_secret_store_startup",
        _validate_secrets,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "retry_startup_dependency",
        _retry,
        raising=False,
    )
    monkeypatch.setattr(
        gateway_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
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

    assert calls == ["gateway secret-store startup validation", "secrets"]


async def _blocking_run_loop(**_kwargs: object) -> None:
    await asyncio.Event().wait()


def _patch_service_background_loops(
    monkeypatch: pytest.MonkeyPatch,
    service_app: object,
) -> None:
    monkeypatch.setattr(service_app, "batch_run_loop", _blocking_run_loop)
    monkeypatch.setattr(
        service_app,
        "taskset_materializer_run_loop",
        _blocking_run_loop,
    )


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

    monkeypatch.setattr(
        service_app,
        "_assert_secret_store_startup",
        _validate,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "_assert_schema_startup",
        _schema_noop,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
    )
    _patch_service_background_loops(monkeypatch, service_app)

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


def test_service_lifespan_validates_schema_before_secret_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service import app as service_app
    from loom_service.config import LoomServiceSettings

    calls: list[str] = []

    async def _validate_schema(_engine: object) -> None:
        calls.append("schema")

    async def _validate_secrets(_session_factory: object) -> int:
        calls.append("secrets")
        return 0

    monkeypatch.setattr(
        service_app,
        "_assert_schema_startup",
        _validate_schema,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "_assert_secret_store_startup",
        _validate_secrets,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
    )
    _patch_service_background_loops(monkeypatch, service_app)

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

    assert calls == ["schema", "secrets"]


def test_service_lifespan_wraps_secret_validation_in_startup_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service import app as service_app
    from loom_service.config import LoomServiceSettings

    calls: list[str] = []

    async def _validate_secrets(_session_factory: object) -> int:
        calls.append("secrets")
        return 0

    async def _retry(run, *, operation_name: str, **_kwargs: object) -> int:
        calls.append(operation_name)
        return await run()

    monkeypatch.setattr(
        service_app,
        "_assert_schema_startup",
        _schema_noop,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "_assert_secret_store_startup",
        _validate_secrets,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "retry_startup_dependency",
        _retry,
        raising=False,
    )
    monkeypatch.setattr(
        service_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
    )
    _patch_service_background_loops(monkeypatch, service_app)

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

    assert calls == ["service secret-store startup validation", "secrets"]


def test_control_plane_lifespan_validates_schema_before_background_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane import app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings

    calls: list[str] = []

    async def _validate_schema(_engine: object) -> None:
        calls.append("schema")

    async def _run_background_loop(**_kwargs: object) -> None:
        calls.append("background")
        await asyncio.Event().wait()

    monkeypatch.setattr(
        control_plane_app,
        "_assert_schema_startup",
        _validate_schema,
        raising=False,
    )
    monkeypatch.setattr(
        control_plane_app,
        "create_async_engine",
        lambda _db_url, **_kwargs: _FakeEngine(),
    )
    monkeypatch.setattr(
        control_plane_app,
        "build_s3_client",
        lambda **_: object(),
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_crash_detector_loop",
        _run_background_loop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_metrics_refresher_loop",
        _run_background_loop,
    )
    monkeypatch.setattr(
        control_plane_app,
        "run_retry_exhausted_sweeper_loop",
        _run_background_loop,
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

    assert calls[0] == "schema"
    assert calls.count("background") == 3
