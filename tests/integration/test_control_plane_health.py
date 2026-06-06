import pytest
from fastapi.testclient import TestClient

from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str):
    monkeypatch.setenv("LOOM_CP_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_CP_MINIO_ENDPOINT", "http://minio.invalid:9000")
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "ak")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "sk")
    monkeypatch.setenv("LOOM_CP_LLM_GATEWAY_URL", "http://gateway:9100")
    return create_app(ControlPlaneSettings(_env_file=None))


def test_healthz(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
