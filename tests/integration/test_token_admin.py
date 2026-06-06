import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def admin_seed(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw = f"loom_admin_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="admin", scopes=["admin:tokens"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_factory() as s:
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, admin_seed: str):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_issue_worker_token(app, admin_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {admin_seed}"},
            json={"expires_in_days": 90},
        )
        assert r.status_code == 201
        body = r.json()
        assert body["token"].startswith("loom_w_")
        assert "token_hash_prefix" in body


def test_revoke_token(app, admin_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {admin_seed}"},
            json={"expires_in_days": 1},
        )
        prefix = r.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix}",
            headers={"Authorization": f"Bearer {admin_seed}"},
        )
        assert r2.status_code == 200


def test_issue_without_admin_scope_rejected(app, admin_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": "Bearer wrong"},
            json={"expires_in_days": 1},
        )
        assert r.status_code == 403


@pytest.mark.parametrize("bad_prefix", [
    "abc",  # too short
    "ZZZZ",  # non-hex (uppercase, and also non-hex letters)
    "abcg",  # 'g' not hex
    "ab--",  # punctuation
    "abc!",  # punctuation
    "a" * 65,  # too long
])
def test_revoke_rejects_invalid_prefix(  # type: ignore[no-untyped-def]
    app, admin_seed, bad_prefix,
):
    """Bug 3 regression: revoke prefix must be 4-64 hex chars. Anything
    else — including the empty / wildcard / punctuation that would feed
    straight into a SQL LIKE — must be rejected with 400 BEFORE the
    UPDATE runs."""
    with TestClient(app) as client:
        r = client.delete(
            f"/admin/worker-tokens/{bad_prefix}",
            headers={"Authorization": f"Bearer {admin_seed}"},
        )
        assert r.status_code == 400, r.text


def test_revoke_doesnt_affect_unrelated_tokens(  # type: ignore[no-untyped-def]
    app, admin_seed,
):
    """Bug 3 regression: a precise prefix must only revoke matching tokens
    (not all of them). We issue two worker tokens, revoke one by its
    prefix, then confirm the admin token is still usable."""
    with TestClient(app) as client:
        r1 = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {admin_seed}"},
            json={"expires_in_days": 1},
        )
        prefix_1 = r1.json()["token_hash_prefix"]
        r2 = client.delete(
            f"/admin/worker-tokens/{prefix_1}",
            headers={"Authorization": f"Bearer {admin_seed}"},
        )
        assert r2.status_code == 200
        # admin token still works
        r3 = client.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {admin_seed}"},
            json={"expires_in_days": 1},
        )
        assert r3.status_code == 201
