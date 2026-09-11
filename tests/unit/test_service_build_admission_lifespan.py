"""Private admission is owned by service lifespan without enabling build intake."""

import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from loom_service import app as service_app
from loom_service.config import LoomServiceSettings


@pytest.mark.parametrize("boundary", ["normal", "later-failure", "admission-failure", "unconfigured"])
def test_service_owns_private_admission_through_startup_and_shutdown(monkeypatch, boundary):
    events = []

    class Engine:
        async def dispose(self):
            events.append("service-closed")

    async def close():
        events.append("admission-closed")

    runtime = SimpleNamespace(sessions=object(), verifier=object(), aclose=close)

    async def build(settings):
        events.append("admission-built")
        if boundary == "admission-failure":
            raise RuntimeError("admission rejected")
        return None if boundary == "unconfigured" else runtime

    async def noop(*args, **kwargs):
        pass

    async def idle(**kwargs):
        events.append("task-started")
        try:
            await asyncio.Event().wait()
        finally:
            events.append("task-stopped")

    def configure(**kwargs):
        if boundary == "later-failure":
            raise RuntimeError("later startup rejected")

    monkeypatch.setattr(service_app, "_assert_schema_startup", noop)
    monkeypatch.setattr(service_app, "_assert_secret_store_startup", noop)
    monkeypatch.setattr(service_app, "create_async_engine", lambda *a, **k: Engine())
    monkeypatch.setattr(service_app, "create_minio_client", lambda *a, **k: object())
    monkeypatch.setattr(service_app, "build_personal_build_admission_runtime", build, raising=False)
    monkeypatch.setattr(service_app, "install_behavior_pipeline_public_adapter", configure)
    for name in ("batch_run_loop", "taskset_materializer_run_loop", "taskset_gc_run_loop"):
        monkeypatch.setattr(service_app, name, idle)
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql+psycopg://svc:pw@db/management")
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "test-access")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "test-secret")
    app = service_app.create_app(LoomServiceSettings(_env_file=None))
    if boundary in {"later-failure", "admission-failure"}:
        with pytest.raises(RuntimeError, match="rejected"):
            with TestClient(app):
                pytest.fail("failed startup must not serve requests")
    else:
        with TestClient(app) as client:
            assert client.get("/api/v1/health").status_code == 200
            assert app.state.personal_dev_builder_available is False
            assert getattr(app.state, "personal_dev_builder_task", None) is None
            if boundary == "normal":
                assert app.state.personal_dev_build_admission_sessions is runtime.sessions
                assert app.state.personal_dev_build_admission_verifier is runtime.verifier
            else:
                assert getattr(app.state, "personal_dev_build_admission_sessions", None) is None
                assert getattr(app.state, "personal_dev_build_admission_verifier", None) is None
    assert events[0] == "admission-built"
    assert events[-1] == "service-closed"
    if boundary in {"normal", "later-failure"}:
        assert events.count("admission-closed") == 1
        if boundary == "normal":
            assert events.count("task-stopped") == 3
            assert max(i for i, event in enumerate(events) if event == "task-stopped") < events.index("admission-closed")
    else:
        assert "admission-closed" not in events
    if boundary.endswith("failure"):
        assert "task-started" not in events
    assert getattr(app.state, "personal_dev_build_admission_sessions", None) is None
    assert getattr(app.state, "personal_dev_build_admission_verifier", None) is None
