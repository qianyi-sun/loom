"""Integration tests for /api/v1/internal/trial-cache/* (#317 Phase 1).

Exercises the 4 CP HTTP routes (claim, exists, release, refresh)
against a real Postgres testcontainer, with the alembic head applied
(includes migration 0032). Verifies:

- Atomic claim works; second claimer of held slot returns False
- TTL-based expired-slot stealing
- Cheap exists probe doesn't mutate
- Release is idempotent + worker-id-gated
- Refresh extends TTL; refresh-by-non-owner returns False
- Worker bearer-token auth required
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import ActiveTrialCacheBuild, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def key_ns() -> str:
    """Per-test cache_key namespace prefix. Lets parallel test
    processes (xdist) share the Postgres testcontainer without
    colliding on cache_key values."""
    return f"t{uuid4().hex[:10]}-"


@pytest.fixture
def worker_token(postgres_url: str, key_ns: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    raw = f"w_{uuid4().hex}"
    now = datetime.now(UTC)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker",
            scopes=["worker:claim", "worker:report"],
            team_id=None,
            issued_at=now,
            expires_at=None,
        ))
        s.commit()
    yield raw
    with sl() as s:
        s.execute(delete(Token).where(
            Token.token_hash == hashlib.sha256(raw.encode()).digest(),
        ))
        # Scope deletion to this test's key_ns so xdist-parallel
        # processes don't wipe each other's in-flight rows.
        s.execute(delete(ActiveTrialCacheBuild).where(
            ActiveTrialCacheBuild.cache_key.startswith(key_ns),
        ))
        s.commit()


@pytest.fixture
def client(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    app = create_app(ControlPlaneSettings(_env_file=None))
    with TestClient(app) as c:
        yield c


def _claim(
    client: TestClient, *,
    cache_key: str, worker_id: str, ttl_sec: float, token: str,
) -> dict:
    r = client.post(
        "/api/v1/internal/trial-cache/claim",
        headers={"Authorization": f"Bearer {token}"},
        json={"cache_key": cache_key, "worker_id": worker_id, "ttl_sec": ttl_sec},
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_claim_first_call_returns_true(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    j = _claim(
        client, cache_key=key_ns + "key1", worker_id=str(uuid4()),
        ttl_sec=30, token=worker_token,
    )
    assert j == {"i_am_builder": True}


def test_claim_second_call_returns_false(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    k = key_ns + "key2"
    j1 = _claim(
        client, cache_key=k, worker_id=str(uuid4()),
        ttl_sec=30, token=worker_token,
    )
    assert j1["i_am_builder"] is True
    j2 = _claim(
        client, cache_key=k, worker_id=str(uuid4()),
        ttl_sec=30, token=worker_token,
    )
    assert j2["i_am_builder"] is False


def test_claim_steals_expired_slot(
    client: TestClient, worker_token: str, postgres_url: str, key_ns: str,
) -> None:
    k = key_ns + "exp-key"
    j1 = _claim(
        client, cache_key=k, worker_id=str(uuid4()),
        ttl_sec=30, token=worker_token,
    )
    assert j1["i_am_builder"] is True

    # Force-expire the slot directly in the DB.
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(update(ActiveTrialCacheBuild).where(
            ActiveTrialCacheBuild.cache_key == k,
        ).values(expires_at=datetime.now(UTC) - timedelta(seconds=10)))
        s.commit()

    j2 = _claim(
        client, cache_key=k, worker_id=str(uuid4()),
        ttl_sec=30, token=worker_token,
    )
    assert j2["i_am_builder"] is True


def test_exists_returns_false_when_no_slot(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    r = client.get(
        f"/api/v1/internal/trial-cache/{key_ns}never-claimed",
        headers={"Authorization": f"Bearer {worker_token}"},
    )
    assert r.json() == {"exists": False}


def test_exists_returns_true_when_held(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    k = key_ns + "held"
    _claim(
        client, cache_key=k, worker_id=str(uuid4()),
        ttl_sec=30, token=worker_token,
    )
    r = client.get(
        f"/api/v1/internal/trial-cache/{k}",
        headers={"Authorization": f"Bearer {worker_token}"},
    )
    assert r.json() == {"exists": True}


def test_release_owner_deletes_row(
    client: TestClient, worker_token: str, postgres_url: str, key_ns: str,
) -> None:
    worker_id = uuid4()
    k = key_ns + "rel-key"
    _claim(
        client, cache_key=k, worker_id=str(worker_id),
        ttl_sec=30, token=worker_token,
    )
    r = client.delete(
        f"/api/v1/internal/trial-cache/{k}",
        headers={"Authorization": f"Bearer {worker_token}"},
        params={"worker_id": str(worker_id)},
    )
    assert r.status_code == 204

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        cnt = s.execute(text(
            "SELECT count(*) FROM active_trial_cache_builds "
            "WHERE cache_key = :k"
        ), {"k": k}).scalar_one()
        assert cnt == 0


def test_release_by_non_owner_is_noop(
    client: TestClient, worker_token: str, postgres_url: str, key_ns: str,
) -> None:
    owner = uuid4()
    intruder = uuid4()
    k = key_ns + "owned"
    _claim(
        client, cache_key=k, worker_id=str(owner),
        ttl_sec=30, token=worker_token,
    )
    r = client.delete(
        f"/api/v1/internal/trial-cache/{k}",
        headers={"Authorization": f"Bearer {worker_token}"},
        params={"worker_id": str(intruder)},
    )
    assert r.status_code == 204
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        row = s.execute(text(
            "SELECT builder_worker_id FROM active_trial_cache_builds "
            "WHERE cache_key = :k"
        ), {"k": k}).scalar_one()
        assert str(row) == str(owner)


def test_refresh_owner_extends_ttl_returns_true(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    worker_id = uuid4()
    k = key_ns + "ref-key"
    _claim(
        client, cache_key=k, worker_id=str(worker_id),
        ttl_sec=30, token=worker_token,
    )
    r = client.post(
        f"/api/v1/internal/trial-cache/{k}/refresh",
        headers={"Authorization": f"Bearer {worker_token}"},
        json={"worker_id": str(worker_id), "ttl_sec": 60},
    )
    assert r.json() == {"refreshed": True}


def test_refresh_by_non_owner_returns_false(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    owner = uuid4()
    intruder = uuid4()
    k = key_ns + "ref-other"
    _claim(
        client, cache_key=k, worker_id=str(owner),
        ttl_sec=30, token=worker_token,
    )
    r = client.post(
        f"/api/v1/internal/trial-cache/{k}/refresh",
        headers={"Authorization": f"Bearer {worker_token}"},
        json={"worker_id": str(intruder), "ttl_sec": 60},
    )
    assert r.json() == {"refreshed": False}


def test_concurrent_claims_only_one_wins(
    client: TestClient, worker_token: str, key_ns: str,
) -> None:
    """B3: 5 workers racing on the same cache_key — exactly one wins
    the builder slot, the other 4 see i_am_builder=False. This is the
    central correctness claim of the active_trial_cache_builds design."""
    import concurrent.futures
    k = key_ns + "race"
    workers = [str(uuid4()) for _ in range(5)]

    def claim_one(worker_id: str) -> bool:
        return _claim(
            client, cache_key=k, worker_id=worker_id,
            ttl_sec=30, token=worker_token,
        )["i_am_builder"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        results = list(ex.map(claim_one, workers))

    assert sum(results) == 1, (
        f"expected exactly one builder, got {sum(results)}: {results}"
    )


def test_routes_require_worker_token(client: TestClient) -> None:
    r = client.post(
        "/api/v1/internal/trial-cache/claim",
        json={"cache_key": "k", "worker_id": str(uuid4()), "ttl_sec": 30},
    )
    assert r.status_code == 401
