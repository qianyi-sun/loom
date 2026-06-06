import pytest
from fastapi.testclient import TestClient

from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str):
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    return create_app(GatewaySettings(_env_file=None))


def test_healthz_returns_ok(app):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
