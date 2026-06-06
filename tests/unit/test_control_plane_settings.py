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
    assert s.worker_heartbeat_expiry_sec == 15
    assert s.worker_reclaim_sweep_interval_sec == 30
    assert s.minio_access_key.get_secret_value() == "ak"
