from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.personal_dev_runtime import PersonalDevAcceptanceInterlockError
from loom_service.personal_dev_lifecycle import build_personal_dev_capacity_runtime
from loom_service.routes.health import router as health_router


def test_acceptance_binding_is_rejected_before_opening_capacity_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_projector(_connection: object) -> object:
        raise AssertionError("invalid acceptance configuration opened credentials")

    monkeypatch.setattr(
        "loom_service.personal_dev_lifecycle.CapacityManagerPersonalDevProjector.from_files",
        unexpected_projector,
    )
    settings = SimpleNamespace(
        dev_instances_enabled=True,
        personal_dev_builder_enabled=True,
        personal_dev_acceptance_binding_json="{}",
        personal_dev_acceptance_plan_sha256="a" * 64,
    )

    with pytest.raises(RuntimeError, match="acceptance binding"):
        build_personal_dev_capacity_runtime(settings)  # type: ignore[arg-type]


def test_acceptance_readiness_is_secret_free_and_fails_closed_on_drift() -> None:
    class _Interlock:
        async def assert_ready(self, *, now: datetime) -> None:
            assert now.tzinfo == UTC
            raise PersonalDevAcceptanceInterlockError("capacity-manager-binding-drift")

    app = FastAPI()
    app.state.personal_dev_acceptance_interlock = _Interlock()
    app.include_router(health_router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/health/personal-dev-acceptance")

    assert response.status_code == 503
    assert response.json() == {
        "blockers": ["capacity-manager-binding-drift"],
        "status": "not-ready",
    }


def test_acceptance_readiness_rejects_missing_runtime_interlock() -> None:
    app = FastAPI()
    app.include_router(health_router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/health/personal-dev-acceptance")

    assert response.status_code == 503
    assert response.json() == {
        "blockers": ["acceptance-interlock-unavailable"],
        "status": "not-ready",
    }


def test_acceptance_readiness_reports_ready_only_after_a_fresh_manager_check() -> None:
    calls = 0

    class _Interlock:
        async def assert_ready(self, *, now: datetime) -> None:
            nonlocal calls
            calls += 1
            assert now.tzinfo == UTC

    app = FastAPI()
    app.state.personal_dev_acceptance_interlock = _Interlock()
    app.include_router(health_router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/health/personal-dev-acceptance")

    assert calls == 1
    assert response.status_code == 200
    assert response.json() == {"blockers": [], "status": "ready"}


def test_service_startup_closes_owned_projector_when_interlock_rejects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service import app as service_app
    from loom_service.config import LoomServiceSettings

    calls: list[str] = []

    class _Engine:
        async def dispose(self) -> None:
            calls.append("engine-closed")

    class _Projector:
        async def aclose(self) -> None:
            calls.append("projector-closed")

    class _Interlock:
        async def assert_ready(self, *, now: datetime) -> None:
            calls.append("interlock")
            raise PersonalDevAcceptanceInterlockError("capacity-manager-binding-drift")

    async def _schema_noop(_engine: object) -> int:
        return 0

    async def _secrets_noop(_session_factory: object) -> int:
        return 0

    key_file = tmp_path / "activation.pub"
    key_file.write_bytes(b"k" * 32)
    key_file.chmod(0o600)
    projector = _Projector()
    interlock = _Interlock()
    capacity_runtime = SimpleNamespace(
        acceptance_interlock=interlock,
        installer=object(),
        projector=projector,
        status_reader=object(),
    )

    monkeypatch.setattr(service_app, "_assert_schema_startup", _schema_noop)
    monkeypatch.setattr(service_app, "_assert_secret_store_startup", _secrets_noop)
    monkeypatch.setattr(
        service_app,
        "create_async_engine",
        lambda *_args, **_kwargs: _Engine(),
    )
    monkeypatch.setattr(service_app, "create_minio_client", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        service_app,
        "load_personal_dev_activation_verifier",
        lambda _path, **kwargs: (
            calls.append(f"key-digest:{kwargs.get('expected_sha256')}") or object()
        ),
    )
    monkeypatch.setattr(
        service_app,
        "build_personal_dev_preparation_runtime",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        service_app,
        "build_personal_dev_builder_runtime",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        service_app,
        "build_personal_dev_artifact_collector",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        service_app,
        "build_personal_dev_capacity_runtime",
        lambda _settings: capacity_runtime,
    )

    app = service_app.create_app(
        LoomServiceSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            gateway_url="http://gw.example",
            control_plane_url="http://cp.example",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
            dev_instances_enabled=True,
            personal_dev_builder_enabled=True,
            personal_dev_activation_public_key_file=key_file,
            personal_dev_activation_public_key_sha256="b" * 64,
        )
    )

    with pytest.raises(PersonalDevAcceptanceInterlockError):
        with TestClient(app):
            pass

    assert calls == [
        "key-digest:" + "b" * 64,
        "interlock",
        "projector-closed",
        "engine-closed",
    ]


def test_service_startup_closes_every_owned_http_client_after_late_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service import app as service_app
    from loom_service.config import LoomServiceSettings

    calls: list[str] = []

    class _Engine:
        async def dispose(self) -> None:
            calls.append("engine-closed")

    class _Minio:
        def close(self) -> None:
            calls.append("minio-closed")

    class _HTTPClient:
        def __init__(self, *, base_url: str, timeout: float) -> None:
            assert timeout == 10.0
            self.name = "gateway" if "gw.example" in base_url else "control-plane"

        async def aclose(self) -> None:
            calls.append(f"{self.name}-closed")

    async def _schema_noop(_engine: object) -> int:
        return 0

    async def _secrets_noop(_session_factory: object) -> int:
        return 0

    monkeypatch.setattr(service_app, "_assert_schema_startup", _schema_noop)
    monkeypatch.setattr(service_app, "_assert_secret_store_startup", _secrets_noop)
    monkeypatch.setattr(
        service_app,
        "create_async_engine",
        lambda *_args, **_kwargs: _Engine(),
    )
    monkeypatch.setattr(
        service_app,
        "create_minio_client",
        lambda *_args, **_kwargs: _Minio(),
    )
    monkeypatch.setattr(service_app.httpx, "AsyncClient", _HTTPClient)
    monkeypatch.setattr(
        service_app,
        "install_behavior_pipeline_public_adapter",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("late startup rejection")),
    )
    app = service_app.create_app(
        LoomServiceSettings(
            _env_file=None,
            db_url="postgresql+psycopg://loom:loom@example/loom",
            gateway_url="http://gw.example",
            control_plane_url="http://cp.example",
            minio_endpoint="http://minio.example",
            minio_access_key="minio-access",
            minio_secret_key="minio-secret",
        )
    )

    with pytest.raises(RuntimeError, match="late startup rejection"):
        with TestClient(app):
            pass

    assert calls == [
        "gateway-closed",
        "control-plane-closed",
        "minio-closed",
        "engine-closed",
    ]
