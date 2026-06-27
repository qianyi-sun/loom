import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import RateCard, Token
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings

RAW_ADMIN_TOKEN = "loom_admin_" + "G" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        f'[admin]\ntoken = "{RAW_ADMIN_TOKEN}"\ncreated_at = "2026-06-17T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    path.chmod(0o600)


@pytest.fixture
def admin_app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
) -> Iterator[tuple[object, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub")
    monkeypatch.setenv("LOOM_GW_ADMIN_SECRET_FILE", str(secret_file))
    app = create_app(GatewaySettings(_env_file=None))
    try:
        yield app, RAW_ADMIN_TOKEN
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
                "entries": [
                    {
                        "provider": "anthropic",
                        "model": "claude-opus-4-7",
                        "input_per_mtok": 3.0,
                        "output_per_mtok": 15.0,
                        "cache_read_per_mtok": 0.3,
                        "cache_write_per_mtok": 3.75,
                    }
                ],
            },
        )
        assert r.status_code == 201
        assert r.json()["id"] == "card-upsert-1"


def test_admin_can_sync_yibuapi_rate_card(
    admin_app,
    postgres_url: str,
) -> None:  # type: ignore[no-untyped-def]
    app, raw_admin = admin_app

    async def _fake_fetcher(source_url: str) -> dict[str, object]:
        assert source_url == "https://yibuapi.test/api/pricing"
        return {
            "success": True,
            "pricing_version": "pricing-v1",
            "group_ratio": {"default": 1},
            "data": [
                {
                    "model_name": "qwen3.6-35b-a3b",
                    "quota_type": 0,
                    "model_ratio": 0.36,
                    "completion_ratio": 6,
                }
            ],
        }

    app.state.yibuapi_pricing_fetcher = _fake_fetcher
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards/sync/yibuapi",
            headers={"Authorization": f"Bearer {raw_admin}"},
            json={"source_url": "https://yibuapi.test/api/pricing"},
        )
    assert r.status_code == 201, r.text
    assert r.json() == {
        "id": "yibuapi-pricing-v1",
        "source_url": "https://yibuapi.test/api/pricing",
        "pricing_version": "pricing-v1",
        "entry_count": 1,
        "skipped_model_count": 0,
    }

    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as s:
        row = s.get(RateCard, "yibuapi-pricing-v1")
        assert row is not None
        assert row.table["source_url"] == "https://yibuapi.test/api/pricing"
        assert row.table["currency"] == "USD"
        assert row.table["entries"][0]["model"] == "qwen3.6-35b-a3b"
        assert row.table["entries"][0]["input_per_mtok"] == 0.72
    engine.dispose()


def test_admin_upsert_is_idempotent(admin_app):  # type: ignore[no-untyped-def]
    """Second upsert of the same id updates rather than failing on PK."""
    app, raw_admin = admin_app
    payload = {
        "id": "card-upsert-2",
        "entries": [
            {
                "provider": "anthropic",
                "model": "claude-opus-4-7",
                "input_per_mtok": 1.0,
                "output_per_mtok": 1.0,
                "cache_read_per_mtok": 0.0,
                "cache_write_per_mtok": 0.0,
            }
        ],
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


def test_admin_rejects_malformed_payload(admin_app):  # type: ignore[no-untyped-def]
    """Regression for Bug 2: a payload that won't validate as RateCardTable
    must be rejected with 400 BEFORE it lands in the DB. Otherwise the
    rate-card cache breaks for every chat request until an admin uploads
    a valid card."""
    app, raw_admin = admin_app
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw_admin}"},
            json={
                "id": "card-malformed",
                "entries": [{"provider": "anthropic"}],  # missing model + rates
            },
        )
        assert r.status_code == 400


def test_non_admin_rejected(admin_app):  # type: ignore[no-untyped-def]
    app, _ = admin_app
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": "Bearer wrong"},
            json={"id": "x", "entries": []},
        )
        assert r.status_code in (401, 403)


def test_legacy_db_admin_token_rejected(
    admin_app,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    app, _raw_admin = admin_app
    raw_db_admin = f"loom_admin_{uuid4().hex}"
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_db_admin.encode()).digest(),
                type="admin",
                scopes=["admin:rate_cards"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.commit()
    engine.dispose()
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw_db_admin}"},
            json={"id": "x", "entries": []},
        )
        assert r.status_code == 401


def test_missing_scope_rejected(admin_app, postgres_url: str):  # type: ignore[no-untyped-def]
    """A valid bearer token without admin:rate_cards scope → 403."""
    app, _ = admin_app
    engine = create_engine(postgres_url)
    raw = f"loom_team_{uuid4().hex}"
    with sessionmaker(engine)() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.commit()
    engine.dispose()
    with TestClient(app) as client:
        r = client.post(
            "/admin/rate-cards",
            headers={"Authorization": f"Bearer {raw}"},
            json={"id": "x", "entries": []},
        )
        assert r.status_code == 403
