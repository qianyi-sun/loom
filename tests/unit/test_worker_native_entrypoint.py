"""Native worker settings come only from the one-use approved launch handoff."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_capacity_executor.native_worker_bootstrap import (
    NativeBootstrapError,
    NativeWorkerBootstrap,
    encode_native_bootstrap,
    native_bootstrap_pipe,
    read_native_bootstrap,
)
from tests.unit.test_native_worker_bootstrap import _bootstrap


def _configured_bootstrap(**overrides: object) -> NativeWorkerBootstrap:
    bootstrap = _bootstrap()
    now = datetime.now(UTC).replace(microsecond=0)
    native = bootstrap.native_execution.model_copy(update={
        "root_activated_at": (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "root_expires_at": (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    settings: dict[str, object] = {
        "token": "disposable-worker-api-token",
        "minio_access_key": "disposable-storage-access",
        "minio_secret_key": "disposable-storage-secret",
        "control_plane_url": "http://approved-control-plane:8080",
        "max_concurrent": 1,
    }
    settings.update(overrides)
    return NativeWorkerBootstrap(
        native_execution=native,
        worker_credential=bootstrap.worker_credential,
        canonical_worker_settings=json.dumps(settings, sort_keys=True, separators=(",", ":")),
    )


def test_approved_settings_roundtrip_in_memory_without_secret_repr() -> None:
    import os

    original = _configured_bootstrap()
    descriptor = native_bootstrap_pipe(original)
    try:
        assert read_native_bootstrap(descriptor) == original
    finally:
        os.close(descriptor)
    assert "disposable-storage-secret" not in repr(original)
    assert len(encode_native_bootstrap(original)) <= 4096


def test_native_settings_ignore_environment_and_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_worker.native_main import native_worker_settings

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "LOOM_WORKER_CONTROL_PLANE_URL=http://dotenv-override:8080\n"
        "LOOM_WORKER_MAX_CONCURRENT=999\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://environment-override:8080")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "environment-secret")
    monkeypatch.setenv("LOOM_EXECUTOR_WORKER_CREDENTIAL", "environment-credential")
    monkeypatch.setenv("LOOM_WORKER_METRICS_PORT", "9999")
    bootstrap = _configured_bootstrap()
    settings = native_worker_settings(bootstrap)
    assert str(settings.control_plane_url) == "http://approved-control-plane:8080/"
    assert settings.max_concurrent == 1
    assert settings.metrics_port == 9090
    assert settings.minio_secret_key.get_secret_value() == "disposable-storage-secret"
    assert settings.executor_worker_credential is not None
    assert settings.executor_worker_credential.get_secret_value() == bootstrap.worker_credential
    assert settings.native_execution == bootstrap.native_execution


def test_native_settings_never_fill_missing_secrets_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_worker.native_main import native_worker_settings

    monkeypatch.setenv("LOOM_WORKER_TOKEN", "environment-token")
    bootstrap = _bootstrap()
    configured = NativeWorkerBootstrap(
        native_execution=bootstrap.native_execution,
        worker_credential=bootstrap.worker_credential,
        canonical_worker_settings='{}',
    )
    with pytest.raises(NativeBootstrapError) as caught:
        native_worker_settings(configured)
    assert "environment-token" not in str(caught.value)


@pytest.mark.parametrize("key", [
    "executor_worker_credential", "LOOM_EXECUTOR_WORKER_CREDENTIAL", "native_execution",
    "_env_file", "_secrets_dir", "_cli_parse_args",
])
def test_settings_cannot_override_bootstrap_authority_or_load_external_sources(key: str) -> None:
    with pytest.raises(NativeBootstrapError):
        _configured_bootstrap(**{key: "untrusted-override"})


def test_missing_settings_refuses_before_worker_start() -> None:
    from loom_worker.native_main import native_worker_settings

    with pytest.raises(NativeBootstrapError):
        native_worker_settings(_bootstrap())


def test_invalid_worker_settings_error_never_echoes_configuration() -> None:
    from loom_worker.native_main import native_worker_settings

    with pytest.raises(NativeBootstrapError) as caught:
        native_worker_settings(_configured_bootstrap(max_concurrent="private-invalid-value"))
    assert "private-invalid-value" not in str(caught.value)
    assert "disposable-storage-secret" not in str(caught.value)


def test_native_entrypoint_passes_memory_settings_into_existing_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_worker import native_main

    bootstrap = _configured_bootstrap()
    events: list[str] = []

    def consume() -> NativeWorkerBootstrap:
        events.append("consume")
        return bootstrap

    async def worker(settings: object) -> None:
        assert isinstance(settings, native_main.NativeWorkerSettings)
        assert settings.native_execution == bootstrap.native_execution
        assert settings.executor_worker_credential is not None
        assert settings.executor_worker_credential.get_secret_value() == bootstrap.worker_credential
        events.append("worker")

    monkeypatch.setattr(native_main, "consume_native_worker_bootstrap", consume)
    monkeypatch.setattr(native_main, "_configure_logging", lambda _: events.append("logging"))
    monkeypatch.setattr(native_main, "start_http_server", lambda _: events.append("metrics"))
    monkeypatch.setattr(native_main, "run_worker", worker)
    assert native_main.main(()) == 0
    assert events == ["consume", "logging", "metrics", "worker"]


@pytest.mark.parametrize("argv", [(), ("--env-file", "/tmp/unapproved-settings")])
def test_native_entrypoint_failure_starts_no_metrics_or_worker(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], argv: tuple[str, ...],
) -> None:
    from loom_worker import native_main

    def reject_bootstrap() -> NativeWorkerBootstrap:
        raise NativeBootstrapError("private-error-text")

    def forbidden(*args: object) -> None:
        raise AssertionError("worker side effects before accepted startup")

    monkeypatch.setattr(native_main, "consume_native_worker_bootstrap", reject_bootstrap)
    monkeypatch.setattr(native_main, "_configure_logging", forbidden)
    monkeypatch.setattr(native_main, "start_http_server", forbidden)
    monkeypatch.setattr(native_main, "run_worker", forbidden)
    assert native_main.main(argv) == 65
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.strip() == "native worker bootstrap unavailable or malformed"


@pytest.mark.parametrize("raw", [
    '{"token":"first","token":"second"}', '{ "token": "pretty" }',
    "[]", "null", '{"container_cpus":NaN}', '{"hostname":"' + "x" * 2048 + '"}',
])
def test_settings_subdocument_is_canonical_closed_and_bounded(raw: str) -> None:
    bootstrap = _bootstrap()
    with pytest.raises(NativeBootstrapError):
        NativeWorkerBootstrap(
            native_execution=bootstrap.native_execution,
            worker_credential=bootstrap.worker_credential,
            canonical_worker_settings=raw,
        )


@pytest.mark.parametrize("missing", [False, True])
def test_real_native_entrypoint_process_accepts_only_its_stdin_handoff(missing: bool) -> None:
    # The worker loop is replaced at its boundary, not the real bootstrap,
    # settings loader or kernel hardening. This is not registration acceptance.
    script = """
import ctypes, os, resource, sys
from loom_worker import native_main
native_main.start_http_server = lambda port: None
async def worker(settings):
    assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
    assert ctypes.CDLL(None).prctl(3, 0, 0, 0, 0) == 0
    assert os.read(0, 1) == b''
    assert str(settings.control_plane_url) == 'http://approved-control-plane:8080/'
    assert settings.executor_worker_credential is not None
    print('accepted')
native_main.run_worker = worker
raise SystemExit(native_main.main(()))
"""
    bootstrap = _configured_bootstrap()
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        input=b"" if missing else encode_native_bootstrap(bootstrap),
        capture_output=True, check=False, timeout=30,
    )
    assert completed.returncode == (65 if missing else 0), completed.stderr.decode()
    assert completed.stdout.strip() == (b"" if missing else b"accepted")
    assert completed.stderr.strip() == (
        b"native worker bootstrap unavailable or malformed" if missing else b""
    )
    assert bootstrap.worker_credential.encode() not in completed.stdout + completed.stderr
