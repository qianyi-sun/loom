"""Root URL `/` of loom_service returns a JSON landing manifest.

Without the handler, FastAPI returns `{"detail": "Not Found"}` for
every undeclared route — including the bare `http://localhost:8090/`
URL users hit first after `loom service up`. The landing manifest
points them at `/docs`, `/openapi.json`, and `/api/v1/health` so the
service-is-up signal is visible without curl-and-Bearer-token gymnastics.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _base_env() -> dict[str, str]:
    return {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }


def test_root_returns_landing_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    app = create_app(LoomServiceSettings(_env_file=None))

    # NB: don't enter the lifespan — the `/` handler doesn't touch
    # app.state, so we exercise it without spinning up DB/MinIO/CP
    # fixtures. Calling TestClient without `with` skips the lifespan.
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["service"] == "loom-service"
    assert body["links"]["swagger_ui"] == "/docs"
    assert body["links"]["openapi_schema"] == "/openapi.json"
    assert body["links"]["health"] == "/api/v1/health"


def test_root_not_in_openapi_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The landing manifest is operational scaffolding — exclude it
    from the OpenAPI surface so SDK generators don't tag it as
    'production API'."""
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    app = create_app(LoomServiceSettings(_env_file=None))

    schema = app.openapi()
    assert "/" not in schema.get("paths", {})
