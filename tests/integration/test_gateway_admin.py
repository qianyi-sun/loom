import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import RateCard, Token
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings


@pytest.fixture
def admin_app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> Iterator[tuple[object, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw_admin = f"loom_admin_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_admin.encode()).digest(),
            type="admin", scopes=["admin:rate_cards"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub")
    app = create_app(GatewaySettings(_env_file=None))
    try:
        yield app, raw_admin
    finally:
        with session_factory() as s:
            s.execute(delete(Token))
            s.execute(delete(RateCard))
            s.commit()
        engine.dispose()


def test_admin_can_upsert_rate_card(admin_app):  # type: ignore[no-untyped-def]
    app, raw_admin = admin_app
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw_admin}"},
            json={
                "id": "card-upsert-1",
                "entries": [{
                    "provider": "anthropic",
                    "model": "claude-opus-4-7",
                    "input_per_mtok": 3.0, "output_per_mtok": 15.0,
                    "cache_read_per_mtok": 0.3, "cache_write_per_mtok": 3.75,
                }],
            },
        )
        assert r.status_code == 201
        assert r.json()["id"] == "card-upsert-1"


def test_admin_upsert_is_idempotent(admin_app):  # type: ignore[no-untyped-def]
    """Second upsert of the same id updates rather than failing on PK."""
    app, raw_admin = admin_app
    payload = {
        "id": "card-upsert-2",
        "entries": [{
            "provider": "anthropic", "model": "claude-opus-4-7",
            "input_per_mtok": 1.0, "output_per_mtok": 1.0,
            "cache_read_per_mtok": 0.0, "cache_write_per_mtok": 0.0,
        }],
    }
    with TestClient(app) as client:
        r1 = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw_admin}"},
            json=payload,
        )
        assert r1.status_code == 201
        payload["entries"][0]["input_per_mtok"] = 2.0  # type: ignore[index]
        r2 = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw_admin}"},
            json=payload,
        )
        assert r2.status_code == 201


def test_non_admin_rejected(admin_app):  # type: ignore[no-untyped-def]
    app, _ = admin_app
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": "Bearer wrong"},
            json={"id": "x", "entries": []},
        )
        assert r.status_code in (401, 403)


def test_missing_scope_rejected(admin_app, postgres_url: str):  # type: ignore[no-untyped-def]
    """A valid bearer token without admin:rate_cards scope → 403."""
    app, _ = admin_app
    engine = create_engine(postgres_url)
    raw = f"loom_team_{uuid4().hex}"
    with sessionmaker(engine)() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    engine.dispose()
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw}"},
            json={"id": "x", "entries": []},
        )
        assert r.status_code == 403
