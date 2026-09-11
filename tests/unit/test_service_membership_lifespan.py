"""App startup owns every active runtime client even before background tasks start."""

import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from loom.personal_dev_membership_admission import PersonalDevMembershipAdmissionError
from loom_service import app as service_app
from tests.unit.test_service_membership_runtime import _configured


def test_active_startup_closes_all_clients_on_later_configuration_failure(tmp_path, monkeypatch):
    events = []

    class Engine:
        async def dispose(self):
            events.append("engine-closed")

    class Client:
        def __init__(self, name):
            self.name = name

        async def aclose(self):
            events.append(self.name + "-closed")

    async def noop(*args):
        return 0

    async def runtime(settings):
        events.append("membership-built")
        return SimpleNamespace(
            projector=Client("projector"),
            owned_membership_clients=(Client("delegate"), Client("observer")),
            membership=SimpleNamespace(admission=object()),
        )

    monkeypatch.setattr(service_app, "_assert_schema_startup", noop)
    monkeypatch.setattr(service_app, "_assert_secret_store_startup", noop)
    monkeypatch.setattr(service_app, "create_async_engine", lambda *a, **k: Engine())
    monkeypatch.setattr(service_app, "create_minio_client", lambda *a, **k: object())
    monkeypatch.setattr(service_app, "build_personal_dev_membership_runtime", runtime, raising=False)
    settings = _configured(tmp_path)
    settings.personal_dev_activation_public_key_file = None
    app = service_app.create_app(settings)
    with pytest.raises(RuntimeError, match="ACTIVATION_PUBLIC_KEY_FILE"):
        with TestClient(app):
            pass
    assert events == ["membership-built", "projector-closed", "delegate-closed", "observer-closed", "engine-closed"]
    assert app.state.personal_dev_enablement_required is True


def test_expired_membership_starts_recovery_loop_and_stops_it_before_closing_clients(tmp_path, monkeypatch):
    events = []

    class Engine:
        async def dispose(self):
            events.append("engine-closed")

    class Client:
        def __init__(self, name):
            self.name = name

        async def aclose(self):
            events.append(self.name + "-closed")

    class Expired:
        async def assert_admission_ready(self, *, now):
            events.append("admission-expired")
            raise PersonalDevMembershipAdmissionError("expired")

    membership = SimpleNamespace(admission=Expired())
    runtime = SimpleNamespace(
        projector=Client("projector"), installer=object(), status_reader=object(),
        owned_membership_clients=(Client("delegate"), Client("observer")),
        membership=membership, acceptance_interlock=None, operational_interlock=None,
    )

    async def build(_settings):
        return runtime

    async def noop(*args, **kwargs):
        return 0

    async def idle(**kwargs):
        await asyncio.Event().wait()

    async def recovery_loop(**kwargs):
        assert kwargs["membership"] is membership
        events.append("recovery-started")
        try:
            await asyncio.Event().wait()
        finally:
            events.append("recovery-stopped")

    monkeypatch.setattr(service_app, "_assert_schema_startup", noop)
    monkeypatch.setattr(service_app, "_assert_secret_store_startup", noop)
    monkeypatch.setattr(service_app, "create_async_engine", lambda *a, **k: Engine())
    monkeypatch.setattr(service_app, "create_minio_client", lambda *a, **k: object())
    monkeypatch.setattr(service_app, "build_personal_dev_membership_runtime", build)
    monkeypatch.setattr(service_app, "build_personal_dev_preparation_runtime", lambda *a, **k: object())
    monkeypatch.setattr(service_app, "build_personal_dev_artifact_collector", lambda *a, **k: None)
    monkeypatch.setattr(service_app, "load_personal_dev_activation_verifier", lambda *a, **k: object())
    for name in ("configure_personal_dev_native_builder_verifier", "configure_personal_dev_native_builder_storage", "install_behavior_pipeline_public_adapter"):
        monkeypatch.setattr(service_app, name, lambda *a, **k: None)
    for name in ("batch_run_loop", "taskset_materializer_run_loop", "taskset_gc_run_loop", "personal_dev_builder_run_loop"):
        monkeypatch.setattr(service_app, name, idle)
    monkeypatch.setattr(service_app, "personal_dev_reconcile_run_loop", recovery_loop)
    settings = _configured(tmp_path)
    settings.personal_dev_activation_public_key_file = tmp_path / "unused-verifier"
    app = service_app.create_app(settings)
    with TestClient(app) as client:
        assert client.get("/api/v1/health").status_code == 200
        assert app.state.personal_dev_membership_admission is membership.admission
        assert app.state.personal_dev_builder_available is False
        assert getattr(app.state, "personal_dev_builder_task", None) is None
        assert events == ["recovery-started"]
    assert events == ["recovery-started", "recovery-stopped", "projector-closed", "delegate-closed", "observer-closed", "engine-closed"]
