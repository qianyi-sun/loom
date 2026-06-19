from pathlib import Path

import pytest
from pydantic import ValidationError

from loom_worker.config import WorkerSettings

_LOOM_WORKER_ENVS = [
    "LOOM_WORKER_CONTROL_PLANE_URL",
    "LOOM_WORKER_GATEWAY_URL",
    "LOOM_WORKER_TOKEN",
    "LOOM_WORKER_MINIO_ENDPOINT",
    "LOOM_WORKER_MINIO_ACCESS_KEY",
    "LOOM_WORKER_MINIO_SECRET_KEY",
    "LOOM_WORKER_MAX_CONCURRENT",
    "LOOM_WORKER_BLOCKING_IO_MAX_WORKERS",
    "LOOM_WORKER_TRAJECTORY_CACHE_DIR",
]


@pytest.fixture(autouse=True)
def _clear_envs(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in _LOOM_WORKER_ENVS:
        monkeypatch.delenv(k, raising=False)


def test_required_fields_missing_raises() -> None:
    with pytest.raises(ValidationError):
        WorkerSettings(_env_file=None)


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://cp:8080")
    monkeypatch.setenv("LOOM_WORKER_GATEWAY_URL", "http://gw:9100")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "loom_w_test")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "y")
    monkeypatch.setenv("LOOM_WORKER_MAX_CONCURRENT", "10")
    monkeypatch.setenv("LOOM_WORKER_BLOCKING_IO_MAX_WORKERS", "40")
    monkeypatch.setenv("LOOM_WORKER_TRAJECTORY_CACHE_DIR", str(tmp_path))
    s = WorkerSettings(_env_file=None)
    assert s.max_concurrent == 10
    assert s.blocking_io_max_workers == 40
    assert s.drain_timeout_sec == 600
    assert s.claim_poll_interval_sec == 1.0
    assert s.heartbeat_interval_sec == 5.0
    assert s.trajectory_cache_dir == tmp_path
    assert s.token.get_secret_value() == "loom_w_test"


def test_token_is_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """SecretStr means the token doesn't leak into repr/log lines."""
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://cp:8080")
    monkeypatch.setenv("LOOM_WORKER_GATEWAY_URL", "http://gw:9100")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "supersecret")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ENDPOINT", "http://m:9000")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "y")
    s = WorkerSettings(_env_file=None)
    assert "supersecret" not in repr(s)
    assert s.token.get_secret_value() == "supersecret"
