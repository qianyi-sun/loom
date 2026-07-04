from pathlib import Path

import pytest
from pydantic import ValidationError

from loom_control_plane.config import ControlPlaneSettings


def test_required_fields(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("LOOM_CP_DB_URL", raising=False)
    monkeypatch.delenv("LOOM_CP_MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("LOOM_CP_LLM_GATEWAY_URL", raising=False)
    with pytest.raises(ValidationError):
        ControlPlaneSettings(_env_file=None)


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOOM_CP_DB_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("LOOM_CP_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("LOOM_CP_LLM_GATEWAY_URL", "http://gateway:9100")
    monkeypatch.setenv("LOOM_CP_BIND_PORT", "8080")
    s = ControlPlaneSettings(_env_file=None)
    assert s.bind_port == 8080
    assert s.worker_heartbeat_expiry_sec == 120
    assert s.worker_reclaim_sweep_interval_sec == 30
    assert s.claimed_without_start_expiry_sec == 3600
    assert s.db_pool_size == 20
    assert s.db_max_overflow == 40
    assert s.db_pool_timeout_sec == 30.0
    assert s.worker_heartbeat_expiry_sec >= (
        # The worker heartbeat thread uses a 5s synchronous HTTP timeout and
        # a 5s interval. High-I/O benchmark hosts must tolerate several
        # transient timeout cycles plus one reclaim sweep before losing claims.
        (5 + 5) * 4 + s.worker_reclaim_sweep_interval_sec
    )
    assert s.slurm_worker_controller_enabled is False
    assert s.slurm_worker_controller_pool_name == "oldlab"
    assert s.slurm_worker_controller_requested_concurrency == 6
    assert s.minio_access_key.get_secret_value() == "ak"


def test_elastic_slurm_controller_settings(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOOM_CP_DB_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("LOOM_CP_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("LOOM_CP_LLM_GATEWAY_URL", "http://gateway:9100")
    monkeypatch.setenv("LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED", "true")
    monkeypatch.setenv("LOOM_CP_SLURM_WORKER_CONTROLLER_ENVIRONMENT", "production")
    monkeypatch.setenv("LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES", "oldlab-1,oldlab-2")
    monkeypatch.setenv("LOOM_CP_SLURM_WORKER_CONTROLLER_ENV_FILE", "/secure/prod.env")
    monkeypatch.setenv("LOOM_CP_SLURM_WORKER_CONTROLLER_REPO_DIR", "/opt/loom")
    monkeypatch.setenv("LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS", "2")

    s = ControlPlaneSettings(_env_file=None)

    assert s.slurm_worker_controller_enabled is True
    assert s.slurm_worker_controller_environment == "production"
    assert s.slurm_worker_controller_allowed_nodes == "oldlab-1,oldlab-2"
    assert s.slurm_worker_controller_env_file == "/secure/prod.env"
    assert s.slurm_worker_controller_repo_dir == "/opt/loom"
    assert s.slurm_worker_controller_max_jobs == 2


def test_admin_secret_file_env_var(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOOM_CP_DB_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("LOOM_CP_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("LOOM_CP_LLM_GATEWAY_URL", "http://gateway:9100")
    monkeypatch.setenv(
        "LOOM_CP_ADMIN_SECRET_FILE",
        "/var/run/loom/secrets/admin/secrets.toml",
    )

    s = ControlPlaneSettings(_env_file=None)

    assert s.admin_secret_file == Path("/var/run/loom/secrets/admin/secrets.toml")
