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
    "LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS",
    "LOOM_WORKER_MINIO_CONNECT_TIMEOUT_SEC",
    "LOOM_WORKER_MINIO_READ_TIMEOUT_SEC",
    "LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC",
    "LOOM_WORKER_MINIO_OPERATION_ATTEMPTS",
    "LOOM_WORKER_DOCKER_API_TIMEOUT_SEC",
    "LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT",
    "LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC",
    "LOOM_WORKER_MAX_CONCURRENT",
    "LOOM_WORKER_POOL_NAME",
    "LOOM_WORKER_BLOCKING_IO_MAX_WORKERS",
    "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS",
    "LOOM_WORKER_TRAJECTORY_CACHE_DIR",
    "LOOM_WORKER_SUBPROCESS_GATEWAY_URL",
    "LOOM_WORKER_HOSTNAME",
    "LOOM_WORKER_HUGGINGFACE_API_KEY",
    "LOOM_WORKER_SETUP_HEALTH_GUARD_ENABLED",
    "LOOM_WORKER_SETUP_HEALTH_IO_FULL_AVG10_MAX",
    "LOOM_WORKER_SETUP_HEALTH_MIN_SWAP_FREE_MB",
    "LOOM_WORKER_SETUP_HEALTH_DSTATE_MAX",
    "LOOM_WORKER_SETUP_HEALTH_WAIT_TIMEOUT_SEC",
    "LOOM_WORKER_SETUP_HEALTH_POLL_INTERVAL_SEC",
    "LOOM_WORKER_CONTAINER_CPUS",
    "LOOM_WORKER_CONTAINER_MEMORY_MIB",
    "LOOM_WORKER_CONTAINER_PIDS",
    "HF_TOKEN",
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
    monkeypatch.setenv("LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS", "512")
    monkeypatch.setenv("LOOM_WORKER_MINIO_CONNECT_TIMEOUT_SEC", "7.5")
    monkeypatch.setenv("LOOM_WORKER_MINIO_READ_TIMEOUT_SEC", "180")
    monkeypatch.setenv("LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC", "600")
    monkeypatch.setenv("LOOM_WORKER_MINIO_OPERATION_ATTEMPTS", "4")
    monkeypatch.setenv("LOOM_WORKER_DOCKER_API_TIMEOUT_SEC", "900")
    monkeypatch.setenv("LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT", "2")
    monkeypatch.setenv("LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC", "12.5")
    monkeypatch.setenv("LOOM_WORKER_MAX_CONCURRENT", "10")
    monkeypatch.setenv("LOOM_WORKER_POOL_NAME", "gb10")
    monkeypatch.setenv("LOOM_WORKER_BLOCKING_IO_MAX_WORKERS", "40")
    monkeypatch.setenv("LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS", "300")
    monkeypatch.setenv("LOOM_WORKER_TRAJECTORY_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("LOOM_WORKER_HOSTNAME", "trt-gb10-7")
    monkeypatch.setenv("LOOM_WORKER_SETUP_HEALTH_GUARD_ENABLED", "true")
    monkeypatch.setenv("LOOM_WORKER_SETUP_HEALTH_IO_FULL_AVG10_MAX", "45.5")
    monkeypatch.setenv("LOOM_WORKER_SETUP_HEALTH_MIN_SWAP_FREE_MB", "2048")
    monkeypatch.setenv("LOOM_WORKER_SETUP_HEALTH_DSTATE_MAX", "12")
    monkeypatch.setenv("LOOM_WORKER_SETUP_HEALTH_WAIT_TIMEOUT_SEC", "60")
    monkeypatch.setenv("LOOM_WORKER_SETUP_HEALTH_POLL_INTERVAL_SEC", "2")
    monkeypatch.setenv(
        "LOOM_WORKER_SUBPROCESS_GATEWAY_URL",
        "http://host.docker.internal:30443/openai/v1",
    )
    s = WorkerSettings(_env_file=None)
    assert s.max_concurrent == 10
    assert s.pool_name == "gb10"
    assert s.minio_max_pool_connections == 512
    assert s.minio_connect_timeout_sec == 7.5
    assert s.minio_read_timeout_sec == 180
    assert s.minio_operation_timeout_sec == 600
    assert s.minio_operation_attempts == 4
    assert s.docker_api_timeout_sec == 900
    assert s.trial_cache_build_max_concurrent == 2
    assert s.task_materialize_timeout_sec == 12.5
    assert s.blocking_io_max_workers == 40
    assert s.idle_exit_after_seconds == 300
    assert s.drain_timeout_sec == 600
    assert s.claim_poll_interval_sec == 1.0
    assert s.heartbeat_interval_sec == 5.0
    assert s.trajectory_cache_dir == tmp_path
    assert s.hostname == "trt-gb10-7"
    assert s.setup_health_guard_enabled is True
    assert s.setup_health_io_full_avg10_max == 45.5
    assert s.setup_health_min_swap_free_mb == 2048
    assert s.setup_health_dstate_max == 12
    assert s.setup_health_wait_timeout_sec == 60
    assert s.setup_health_poll_interval_sec == 2
    assert str(s.subprocess_gateway_url) == ("http://host.docker.internal:30443/openai/v1")
    assert s.token.get_secret_value() == "loom_w_test"


def test_idle_exit_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://cp:8080")
    monkeypatch.setenv("LOOM_WORKER_GATEWAY_URL", "http://gw:9100")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "loom_w_test")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "y")

    s = WorkerSettings(_env_file=None)

    assert s.idle_exit_after_seconds is None
    assert s.trial_cache_build_max_concurrent == 1
    assert s.setup_health_guard_enabled is True
    assert s.setup_health_io_full_avg10_max == 50.0
    assert s.setup_health_min_swap_free_mb == 1024
    assert s.setup_health_dstate_max == 32
    assert s.setup_health_wait_timeout_sec == 300.0
    assert s.setup_health_poll_interval_sec == 5.0


def test_container_caps_parse_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # #896: per-container caps parse from LOOM_WORKER_CONTAINER_* env.
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://cp:8080")
    monkeypatch.setenv("LOOM_WORKER_GATEWAY_URL", "http://gw:9100")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "loom_w_test")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ENDPOINT", "http://m:9000")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "y")
    monkeypatch.setenv("LOOM_WORKER_CONTAINER_CPUS", "4.0")
    monkeypatch.setenv("LOOM_WORKER_CONTAINER_MEMORY_MIB", "512")
    monkeypatch.setenv("LOOM_WORKER_CONTAINER_PIDS", "256")

    s = WorkerSettings(_env_file=None)

    assert s.container_cpus == 4.0
    assert s.container_memory_mib == 512
    assert s.container_pids == 256


def test_container_caps_default_unbounded(monkeypatch: pytest.MonkeyPatch) -> None:
    # #896: absent env → 0/unbounded so exclusive pools are unchanged.
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://cp:8080")
    monkeypatch.setenv("LOOM_WORKER_GATEWAY_URL", "http://gw:9100")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "loom_w_test")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ENDPOINT", "http://m:9000")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "y")

    s = WorkerSettings(_env_file=None)

    assert s.container_cpus == 0.0
    assert s.container_memory_mib == 0
    assert s.container_pids == 0


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


def test_worker_settings_do_not_accept_hf_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """HF_TOKEN belongs to catalog/service provisioning, not workers."""
    monkeypatch.setenv("LOOM_WORKER_CONTROL_PLANE_URL", "http://cp:8080")
    monkeypatch.setenv("LOOM_WORKER_GATEWAY_URL", "http://gw:9100")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "loom_w_test")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ENDPOINT", "http://m:9000")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "y")
    monkeypatch.setenv("HF_TOKEN", "hf_abcdefghijklmnopqrstuvwxyz1234567890")

    s = WorkerSettings(_env_file=None)

    assert "huggingface_api_key" not in WorkerSettings.model_fields
    assert "hf_abcdefghijklmnopqrstuvwxyz1234567890" not in repr(s)
