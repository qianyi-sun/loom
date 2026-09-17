"""Release-owned public roots and fixed signer transport, never claim-installed trust."""

import base64
import importlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def document(tmp_path, *, admission=False):
    now = datetime.now(UTC).replace(microsecond=0)
    root = dict(key_id="execution-1", environment="production",
        public_key=base64.urlsafe_b64encode(Ed25519PrivateKey.generate().public_key().public_bytes_raw()).rstrip(b"=").decode(),
        activated_at=(now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        expires_at=(now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    result = dict(schema="loom.task-image-execution-admission/v1" if admission else "loom.task-image-execution-reader/v1",
                  root=root, purpose="production")
    if admission:
        result["signer"] = dict(origin="https://signer.example:8447", ca_file=str(tmp_path / "ca.pem"),
                                client_cert_file=str(tmp_path / "client.pem"), client_key_file=str(tmp_path / "client.key"))
    return result


def save(tmp_path, data):
    path = tmp_path / "execution.json"
    path.write_text(json.dumps(data))
    path.chmod(0o600)
    return path


def module():
    name = "loom_task_image_authority.execution_config"
    assert importlib.util.find_spec(name) is not None, "release execution configuration missing"
    return importlib.import_module(name)


@pytest.mark.parametrize("admission", [False, True])
def test_owner_only_release_configuration_preserves_public_root_and_fixed_purpose(tmp_path, admission):
    data = document(tmp_path, admission=admission)
    path = save(tmp_path, data)
    settings = (module().load_execution_admission_settings if admission else module().load_execution_reader_settings)(path)
    assert settings.root.trust_root().key_id == "execution-1"
    assert settings.purpose == "production" and settings.shadow_campaign_id is None
    assert not hasattr(settings.root, "seed_file")
    if admission:
        assert settings.signer.origin == data["signer"]["origin"]


@pytest.mark.parametrize("case", ["private-key", "unknown", "shadow", "relative", "permissions", "symlink", "duplicate", "oversized", "expired", "http", "signer-path"])
def test_invalid_release_configuration_fails_closed(tmp_path, case):
    data = document(tmp_path, admission=case in {"http", "signer-path"})
    if case == "private-key":
        data["root"]["seed_file"] = "/secret/private.seed"
    elif case == "unknown":
        data["fetch_root_from_claim"] = True
    elif case == "shadow":
        data["purpose"] = "shadow"
    elif case == "expired":
        data["root"]["expires_at"] = (datetime.now(UTC) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif case == "http":
        data["signer"]["origin"] = "http://signer.example"
    elif case == "signer-path":
        data["signer"]["origin"] += "/arbitrary-operation"
    path = save(tmp_path, data)
    if case == "permissions":
        path.chmod(0o644)
    elif case == "symlink":
        link = tmp_path / "link.json"
        link.symlink_to(path)
        path = link
    elif case == "duplicate":
        path.write_text(json.dumps(data)[:-1] + ',"purpose":"production"}')
    elif case == "oversized":
        path.write_bytes(b"x" * (65536 + 1))
    elif case == "relative":
        path = type(path)("relative.json")
    loader = module().load_execution_admission_settings if case in {"http", "signer-path"} else module().load_execution_reader_settings
    with pytest.raises(ValueError):
        loader(path)


async def test_standard_worker_loads_release_file_before_registration_or_effects(tmp_path, monkeypatch):
    from loom_worker import main_loop

    path = save(tmp_path, document(tmp_path))
    settings = SimpleNamespace(task_image_execution_config_file=path, control_plane_url="http://cp.example")
    effects = Mock(side_effect=AssertionError("must reject before worker effects"))
    monkeypatch.setattr(main_loop, "install_signal_handlers", effects)
    with pytest.raises(ValueError, match="HTTPS"):
        await main_loop.run_worker(settings)
    effects.assert_not_called()


def test_standard_control_plane_rejects_conflicting_or_missing_release_config(tmp_path, monkeypatch):
    from loom_control_plane.app import create_app
    from loom_control_plane.config import ControlPlaneSettings

    for name, value in {"DB_URL": "postgresql+psycopg://test:test@localhost/test", "MINIO_ACCESS_KEY": "x", "MINIO_SECRET_KEY": "y"}.items():
        monkeypatch.setenv("LOOM_CP_" + name, value)
    path = save(tmp_path, document(tmp_path, admission=True))
    settings = ControlPlaneSettings(_env_file=None, task_image_execution_config_file=path,
                                   protected_worker_runtime_db_url_file=tmp_path / "protected-db")
    with pytest.raises(ValueError, match="protected"):
        create_app(settings)
    settings = settings.model_copy(update={"protected_worker_runtime_db_url_file": None})
    with pytest.raises(ValueError, match="configuration"):
        create_app(settings, task_image_execution_factory=Mock())
    path.unlink()
    with pytest.raises(ValueError):
        create_app(settings)


@pytest.mark.parametrize("case", ["normal", "later-startup-failure", "partial-background-failure", "signer-open-failure", "schema-failure"])
def test_configured_control_plane_owns_signer_and_engine_on_all_exits(tmp_path, monkeypatch, case):
    import asyncio

    from fastapi.testclient import TestClient

    from loom_control_plane import app as cp_app
    from loom_control_plane import task_image_execution as admission
    from loom_control_plane.config import ControlPlaneSettings

    data = document(tmp_path, admission=True)
    path = save(tmp_path, data)
    disposed = []
    engine = SimpleNamespace(dispose=AsyncMock(side_effect=lambda: disposed.append("database")))
    arguments = []

    class Signer:
        def __init__(self, **kwargs):
            arguments.append(kwargs)
            if case == "signer-open-failure":
                raise ValueError("fixture signer TLS failure")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            disposed.append("signer")

    class KeysetSigner(Signer):
        async def __aexit__(self, *_):
            assert not any(task.get_name() == "loom-cp-task-image-keyset-renewal" and not task.done() for task in asyncio.all_tasks())
            disposed.append("keyset-signer")

    class Publisher:
        ready = True

        def __init__(self, *args, **kwargs):
            pass

        async def run(self):
            await asyncio.Event().wait()

        def stop(self):
            self.ready = False

    async def idle(**_):
        await asyncio.Event().wait()

    monkeypatch.setattr(admission, "HTTPSExecutionSigner", Signer)
    monkeypatch.setattr(admission, "HTTPSKeysetSigner", KeysetSigner)
    monkeypatch.setattr(admission, "TaskImageKeysetPublisher", Publisher)
    monkeypatch.setattr(cp_app, "create_async_engine", lambda *a, **k: engine)
    monkeypatch.setattr(cp_app, "_assert_schema_startup", AsyncMock(side_effect=ValueError("fixture schema failure") if case == "schema-failure" else None))
    monkeypatch.setattr(cp_app, "build_s3_client", lambda **_: object())
    for name in ("run_crash_detector_loop", "run_metrics_refresher_loop", "run_retry_exhausted_sweeper_loop",
                 "run_live_preview_reconciler_loop", "run_service_execution_materializer_loop"):
        monkeypatch.setattr(cp_app, name, idle)
    if case == "later-startup-failure":
        monkeypatch.setattr(cp_app, "SqlArtifactCommitRepository", Mock(side_effect=ValueError("fixture later failure")))
    elif case == "partial-background-failure":
        monkeypatch.setattr(cp_app, "ServiceExecutionMaterializer", Mock(side_effect=ValueError("fixture partial background failure")))
    app = cp_app.create_app(ControlPlaneSettings(_env_file=None, db_url="postgresql+psycopg://test:test@localhost/test",
        minio_access_key="x", minio_secret_key="y", task_image_execution_config_file=path))

    def enter():
        with TestClient(app):
            service = app.state.task_image_execution
            assert isinstance(service, admission.TaskImageExecutionService)
            assert service._engine is engine and service.native_ready_enabled
            assert service._root.public_key == base64.urlsafe_b64decode(data["root"]["public_key"] + "=")

    if case == "normal":
        enter()
    else:
        with pytest.raises(ValueError, match="fixture"):
            enter()
    assert disposed == (["keyset-signer", "signer", "database"] if case in {"normal", "later-startup-failure", "partial-background-failure"} else ["database"])
    assert len(arguments) == (0 if case == "schema-failure" else 1 if case == "signer-open-failure" else 2)
    if arguments:
        assert arguments[0]["origin"] == data["signer"]["origin"]
        assert arguments[0]["client_key_file"].name == "client.key"
