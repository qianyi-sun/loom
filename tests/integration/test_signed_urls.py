import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Team, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def artifact_seed(postgres_url: str) -> Iterator[tuple[UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    raw = f"team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"a-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield team_id, raw
    finally:
        with session_factory() as s:
            s.execute(delete(Token))
            s.execute(delete(Team))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    artifact_seed: tuple[UUID, str],
):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_signed_url_format(app, artifact_seed):  # type: ignore[no-untyped-def]
    team_id, raw = artifact_seed
    trial_id = uuid4()
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            headers={"Authorization": f"Bearer {raw}"},
            json={"trial_id": str(trial_id), "key": "step/out.json"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["url"].startswith("http")
        assert body["expires_in_sec"] > 0
        assert body["key"] == f"{team_id}/{trial_id}/step/out.json"


def test_signed_url_rejects_unauth(app, artifact_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/artifacts/upload-url",
            json={"trial_id": str(uuid4()), "key": "x"},
        )
        assert r.status_code == 401
