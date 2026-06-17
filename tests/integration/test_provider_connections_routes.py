"""Route integration tests for provider_connections CRUD.

Covers the full request-response cycle: auth, SSRF validation (DNS
stubbed), team isolation, soft-delete, pricing validation. Master-key
load is patched so the SecretStore can construct without env vars.
"""

from __future__ import annotations

import base64
import hashlib
import socket
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    ProviderConnection,
    Secret,
    Team,
    TeamQuota,
    Token,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

# Use a fixed test master key so tests are deterministic.
_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()
RAW_ADMIN_TOKEN = "loom_admin_" + "P" * 43


@pytest.fixture(autouse=True)
def _stub_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub `socket.getaddrinfo` AT THE MODULE the service imports it
    from (NOT globally — the global socket.getaddrinfo is still needed
    by psycopg, httpx, etc. to talk to the test postgres container).

    Returns mapped IPs for the test hostnames the routes will see;
    falls through to real DNS for anything else (a deliberate
    pass-through so this stub never accidentally affects database/
    HTTP infrastructure)."""
    test_hosts = {
        "api.openai.com": ["104.18.0.1"],
        "api.anthropic.com": ["104.18.0.2"],
        "vllm.lab.local": ["10.0.5.42"],
        "localhost-svc.test": ["127.0.0.1"],
        "metadata.aws.test": ["169.254.169.254"],
    }

    real_getaddrinfo = socket.getaddrinfo

    def _stub(host, port, *args, **kwargs):
        if host in test_hosts:
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 0, "", (ip, 0))
                for ip in test_hosts[host]
            ]
        return real_getaddrinfo(host, port, *args, **kwargs)

    monkeypatch.setattr(
        "loom_service.provider_connections_service.socket.getaddrinfo",
        _stub,
    )


@pytest.fixture
async def app_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    """Boot a real FastAPI app with two teams + tokens, returning the
    app + the raw token strings (header value) + the team UUIDs."""
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SECRET_STORE_MASTER_KEY": _TEST_MASTER_KEY,
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine, expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    # Seed two teams + per-team tokens. Admin access comes from the singleton
    # admin secret verifier wired above, not from DB-backed admin rows.
    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name=f"team-a-{team_a}"))
        s.execute(insert(Team).values(id=team_b, name=f"team-b-{team_b}"))
        s.execute(insert(TeamQuota).values(team_id=team_a))
        s.execute(insert(TeamQuota).values(team_id=team_b))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_a.encode()).digest(),
            type="team", scopes=["read:own", "write:own"],
            team_id=team_a,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_b.encode()).digest(),
            type="team", scopes=["read:own", "write:own"],
            team_id=team_b,
            issued_at=datetime.now(UTC),
        ))
        s.commit()

    tokens = {"team_a": raw_a, "team_b": raw_b, "admin": RAW_ADMIN_TOKEN}
    team_ids = {"a": team_a, "b": team_b}
    try:
        yield app, tokens, team_ids
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ──────────────────────────────────────────────────────────────────────
# Create
# ──────────────────────────────────────────────────────────────────────


def test_create_returns_201_with_public_response(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "openai-prod",
                   "type": "openai-compatible",
                   "base_url": "https://api.openai.com/v1",
                   "api_key": "sk-test-XXXX",
               })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["name"] == "openai-prod"
    assert body["type"] == "openai-compatible"
    assert body["base_url"] == "https://api.openai.com/v1"
    assert body["upstream_host"] == "api.openai.com"
    assert body["resolved_egress_ips"] == ["104.18.0.1"]
    assert body["status"] == "pending"
    # openai-compatible defaults to tokens-only.
    assert body["pricing_source"] == "tokens-only"
    assert body["pricing_data"] is None
    # openai-compatible is ambiguous at the protocol layer, but the
    # default rate-card provider preserves the common OpenAI-hosted path
    # and can be corrected per connection for Together/Fireworks/etc.
    assert body["rate_card_provider"] == "openai"
    # api_key MUST NOT round-trip; the response only carries opaque
    # public fields.
    assert "api_key" not in body
    assert "encrypted_api_key_ref" not in body


def test_create_anthropic_defaults_to_rate_card(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "anthropic-prod",
                   "type": "anthropic",
                   "base_url": "https://api.anthropic.com/",
                   "api_key": "sk-ant-XXXX",
               })
    assert r.status_code == 201
    assert r.json()["pricing_source"] == "rate-card"
    assert r.json()["rate_card_provider"] == "anthropic"


def test_create_custom_defaults_to_no_rate_card_provider(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "lab-vllm",
                   "type": "custom",
                   "base_url": "https://api.openai.com/",
                   "api_key": "sk-custom-XXXX",
               })
    assert r.status_code == 201
    body = r.json()
    assert body["pricing_source"] == "tokens-only"
    assert body["rate_card_provider"] is None


def test_create_accepts_explicit_rate_card_provider(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "together-prod",
                   "type": "openai-compatible",
                   "base_url": "https://api.openai.com/",
                   "api_key": "sk-together-XXXX",
                   "rate_card_provider": "together",
               })
    assert r.status_code == 201
    assert r.json()["rate_card_provider"] == "together"


def test_create_operator_supplied_requires_pricing_data(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "n", "type": "openai-compatible",
                   "base_url": "https://api.openai.com/",
                   "api_key": "k",
                   "pricing_source": "operator-supplied",
               })
    assert r.status_code == 400
    assert "requires pricing_data" in r.json()["detail"]


def test_create_operator_supplied_negative_price_rejected(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "n", "type": "custom",
                   "base_url": "https://api.openai.com/",
                   "api_key": "k",
                   "pricing_source": "operator-supplied",
                   "pricing_data": {
                       "input_usd_per_1m": -1.0,
                       "output_usd_per_1m": 1.0,
                   },
               })
    assert r.status_code == 400
    assert ">= 0" in r.json()["detail"]


def test_create_rejects_private_ip_by_default(app_setup) -> None:
    """team.allow_private_endpoints defaults to False — RFC1918 host
    rejected with a 400 mentioning the policy flag."""
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "n", "type": "openai-compatible",
                   "base_url": "https://vllm.lab.local:8000/v1",
                   "api_key": "k",
               })
    assert r.status_code == 400
    assert "private range" in r.json()["detail"]
    assert "allow_private_endpoints" in r.json()["detail"]


def test_create_allows_private_ip_when_team_flag_on(app_setup) -> None:
    app, tokens, team_ids = app_setup
    # Flip the flag via direct DB write (the admin verb lands in a
    # follow-up PR; tests bypass).
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            TeamQuota.__table__.update()
            .where(TeamQuota.team_id == team_ids["a"])
            .values(allow_private_endpoints=True),
        )
        s.commit()
    sync_engine.dispose()

    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "n", "type": "openai-compatible",
                   "base_url": "https://vllm.lab.local:8000/v1",
                   "api_key": "k",
               })
    assert r.status_code == 201, r.text
    assert r.json()["resolved_egress_ips"] == ["10.0.5.42"]


def test_create_rejects_loopback_even_with_flag(app_setup) -> None:
    """allow_private opens RFC1918 + ULA only — loopback stays
    rejected unconditionally on any team."""
    app, tokens, team_ids = app_setup
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            TeamQuota.__table__.update()
            .where(TeamQuota.team_id == team_ids["a"])
            .values(allow_private_endpoints=True),
        )
        s.commit()
    sync_engine.dispose()

    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "n", "type": "openai-compatible",
                   "base_url": "https://localhost-svc.test/v1",
                   "api_key": "k",
               })
    assert r.status_code == 400
    assert "127.0.0" in r.json()["detail"]


def test_create_rejects_metadata_endpoint_even_with_flag(app_setup) -> None:
    """169.254.169.254 (AWS/GCP metadata) stays rejected even with
    allow_private. Documented carve-out."""
    app, tokens, team_ids = app_setup
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            TeamQuota.__table__.update()
            .where(TeamQuota.team_id == team_ids["a"])
            .values(allow_private_endpoints=True),
        )
        s.commit()
    sync_engine.dispose()

    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
               json={
                   "name": "n", "type": "openai-compatible",
                   "base_url": "https://metadata.aws.test/",
                   "api_key": "k",
               })
    assert r.status_code == 400
    assert "169.254" in r.json()["detail"]


def test_create_duplicate_active_name_returns_409(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    payload = {
        "name": "dup", "type": "openai-compatible",
        "base_url": "https://api.openai.com/",
        "api_key": "k",
    }
    assert c.post("/api/v1/provider-connections",
                  headers=_auth(tokens["team_a"]), json=payload).status_code == 201
    r2 = c.post("/api/v1/provider-connections",
                headers=_auth(tokens["team_a"]), json=payload)
    assert r2.status_code == 409
    assert "already exists" in r2.json()["detail"]


def test_create_requires_team_scoped_token(app_setup) -> None:
    """An admin-only token (team_id is None) can't create — the row
    needs a team_id FK."""
    app, tokens, _ = app_setup
    c = _client(app)
    r = c.post("/api/v1/provider-connections", headers=_auth(tokens["admin"]),
               json={
                   "name": "n", "type": "openai-compatible",
                   "base_url": "https://api.openai.com/",
                   "api_key": "k",
               })
    assert r.status_code == 400
    assert "team-scoped" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# List / Get
# ──────────────────────────────────────────────────────────────────────


def test_list_filters_to_calling_team(app_setup) -> None:
    """team_b creates a connection; team_a should not see it via list."""
    app, tokens, _ = app_setup
    c = _client(app)
    c.post("/api/v1/provider-connections", headers=_auth(tokens["team_b"]),
           json={"name": "team-b-conn", "type": "openai-compatible",
                 "base_url": "https://api.openai.com/", "api_key": "k"})

    r = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_a"]))
    assert r.status_code == 200
    assert r.json()["items"] == []

    r = c.get("/api/v1/provider-connections", headers=_auth(tokens["team_b"]))
    assert r.status_code == 200
    assert len(r.json()["items"]) == 1
    assert r.json()["items"][0]["name"] == "team-b-conn"


def test_list_admin_sees_all_teams(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    c.post("/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
           json={"name": "a-conn", "type": "openai-compatible",
                 "base_url": "https://api.openai.com/", "api_key": "k"})
    c.post("/api/v1/provider-connections", headers=_auth(tokens["team_b"]),
           json={"name": "b-conn", "type": "openai-compatible",
                 "base_url": "https://api.openai.com/", "api_key": "k"})

    r = c.get("/api/v1/provider-connections", headers=_auth(tokens["admin"]))
    assert r.status_code == 200
    names = {item["name"] for item in r.json()["items"]}
    assert names == {"a-conn", "b-conn"}


def test_get_cross_team_returns_404_not_403(app_setup) -> None:
    """Existence-leak prevention: team_a probing for team_b's row
    gets the same 404 as a nonexistent ID."""
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_b"]),
        json={"name": "b-only", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    b_id = create.json()["id"]

    r = c.get(f"/api/v1/provider-connections/{b_id}", headers=_auth(tokens["team_a"]))
    assert r.status_code == 404
    # Should NOT be 403 — that would leak existence.

    # Same response for a nonexistent ID.
    r_nope = c.get(
        f"/api/v1/provider-connections/{uuid4()}",
        headers=_auth(tokens["team_a"]),
    )
    assert r_nope.status_code == 404
    assert r.json()["detail"] == r_nope.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Update
# ──────────────────────────────────────────────────────────────────────


def test_update_base_url_re_resolves_and_re_pendings(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"base_url": "https://api.anthropic.com/"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["base_url"] == "https://api.anthropic.com/"
    assert body["upstream_host"] == "api.anthropic.com"
    assert body["resolved_egress_ips"] == ["104.18.0.2"]
    # Re-validation triggers status='pending'.
    assert body["status"] == "pending"


def test_update_api_key_rotates_ref(app_setup) -> None:
    """PATCH with api_key encrypts the new value and swaps the
    encrypted_api_key_ref. Both old and new refs persist (Phase 5
    cleanup walker reclaims orphaned old refs)."""
    app, tokens, _team_ids = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "old-key"},
    )
    conn_id = create.json()["id"]

    # Look up the original ref via direct DB query.
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        orig_ref = s.execute(
            ProviderConnection.__table__.select().where(
                ProviderConnection.id == UUID(conn_id),
            ),
        ).one().encrypted_api_key_ref

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"api_key": "new-key"},
    )
    assert r.status_code == 200

    with sl() as s:
        new_ref = s.execute(
            ProviderConnection.__table__.select().where(
                ProviderConnection.id == UUID(conn_id),
            ),
        ).one().encrypted_api_key_ref
        secret_count = s.execute(
            Secret.__table__.select(),
        ).all()
    sync_engine.dispose()

    assert new_ref != orig_ref
    # Both refs are in the secrets table (Phase 5 will GC the orphan).
    assert len(secret_count) == 2


def test_update_rate_card_provider(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]
    assert create.json()["rate_card_provider"] == "openai"

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"rate_card_provider": "together"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["rate_card_provider"] == "together"


def test_update_cross_team_returns_404(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_b"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"base_url": "https://api.anthropic.com/"},
    )
    assert r.status_code == 404


def test_update_to_invalid_pricing_returns_400(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]

    r = c.patch(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
        json={"pricing_source": "operator-supplied"},  # no pricing_data
    )
    assert r.status_code == 400


# ──────────────────────────────────────────────────────────────────────
# Delete
# ──────────────────────────────────────────────────────────────────────


def test_delete_soft_deletes_and_returns_204(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]

    r = c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 204

    # GET / LIST should now treat it as nonexistent.
    g = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert g.status_code == 404

    listed = c.get(
        "/api/v1/provider-connections",
        headers=_auth(tokens["team_a"]),
    )
    assert listed.json()["items"] == []


def test_delete_then_recreate_with_same_name(app_setup) -> None:
    """Soft-delete frees the display_name for reuse via the partial
    UNIQUE index. Catches a regression to a full UNIQUE."""
    app, tokens, _ = app_setup
    c = _client(app)
    payload = {
        "name": "reusable", "type": "openai-compatible",
        "base_url": "https://api.openai.com/", "api_key": "k",
    }
    create_1 = c.post("/api/v1/provider-connections",
                      headers=_auth(tokens["team_a"]), json=payload)
    assert create_1.status_code == 201
    c.delete(f"/api/v1/provider-connections/{create_1.json()['id']}",
             headers=_auth(tokens["team_a"]))

    create_2 = c.post("/api/v1/provider-connections",
                      headers=_auth(tokens["team_a"]), json=payload)
    assert create_2.status_code == 201, create_2.text
    assert create_2.json()["id"] != create_1.json()["id"]


def test_delete_cross_team_returns_404(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_b"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]

    r = c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# Test (probe)
# ──────────────────────────────────────────────────────────────────────


def test_test_valid_persists_status_and_returns_summary(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: probe succeeds → row.status='valid',
    last_validation_error cleared, last_validated_at set."""
    from loom_service.provider_connections_service import ProbeResult

    captured_args: dict[str, object] = {}

    async def _fake_probe(
        provider_type: str, base_url: str, api_key: str,
        *, _client_factory: object = None,
    ) -> ProbeResult:
        captured_args["provider_type"] = provider_type
        captured_args["base_url"] = base_url
        captured_args["api_key"] = api_key
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "openai-prod", "type": "openai-compatible",
              "base_url": "https://api.openai.com/v1",
              "api_key": "sk-XYZ"},
    )
    conn_id = create.json()["id"]
    assert create.json()["status"] == "pending"

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["connection_id"] == conn_id
    assert body["status"] == "valid"
    assert body["http_status"] == 200
    assert body["last_validation_error"] is None

    # The decrypted secret reached the probe — not the encrypted ref.
    assert captured_args["api_key"] == "sk-XYZ"
    assert captured_args["provider_type"] == "openai-compatible"
    assert captured_args["base_url"] == "https://api.openai.com/v1"

    # Row state now reflects the probe outcome — GET /show should
    # surface 'valid' + last_validated_at.
    show = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert show["status"] == "valid"
    assert show["last_validated_at"] is not None
    assert show["last_validation_error"] is None


def test_test_invalid_persists_error_message(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import ProbeResult

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        return ProbeResult(
            status="invalid", http_status=401,
            error="HTTP 401 from .../models; body excerpt: 'bad key'",
        )

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "wrong"},
    )
    conn_id = create.json()["id"]

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "invalid"
    assert body["http_status"] == 401
    assert "bad key" in body["last_validation_error"]

    show = c.get(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert show["status"] == "invalid"
    assert "bad key" in show["last_validation_error"]


def test_test_cross_team_returns_404(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-team /test mirrors the GET / PATCH / DELETE behavior: 404,
    not 403 — and crucially BEFORE we decrypt the api_key. The fake
    probe asserts it was never called."""
    from loom_service.provider_connections_service import ProbeResult

    called = {"count": 0}

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        called["count"] += 1
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_b"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404
    assert called["count"] == 0


def test_test_returns_404_for_soft_deleted_connection(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import ProbeResult

    async def _fake_probe(*args, **kwargs) -> ProbeResult:  # type: ignore[no-untyped-def]
        return ProbeResult(status="valid", http_status=200, error=None)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.probe_connection",
        _fake_probe,
    )

    app, tokens, _ = app_setup
    c = _client(app)
    create = c.post(
        "/api/v1/provider-connections", headers=_auth(tokens["team_a"]),
        json={"name": "n", "type": "openai-compatible",
              "base_url": "https://api.openai.com/", "api_key": "k"},
    )
    conn_id = create.json()["id"]
    c.delete(
        f"/api/v1/provider-connections/{conn_id}",
        headers=_auth(tokens["team_a"]),
    )

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/test",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# Models cache: list / refresh / hide / unhide
# ──────────────────────────────────────────────────────────────────────


def _stub_fetch_upstream_models(
    monkeypatch: pytest.MonkeyPatch, returns: list[str] | None = None,
    *, raises: Exception | None = None,
) -> dict[str, object]:
    """Patch fetch_upstream_models on the routes module + record the
    call. Returns a dict the test can inspect for call_count / last_args."""
    state: dict[str, object] = {"call_count": 0, "last_args": None}

    async def _fake(
        provider_type: str, base_url: str, api_key: str,
        *, _client_factory: object = None,
    ) -> list[str]:
        state["call_count"] = int(state["call_count"]) + 1  # type: ignore[arg-type]
        state["last_args"] = (provider_type, base_url, api_key)
        if raises is not None:
            raise raises
        assert returns is not None
        return list(returns)

    monkeypatch.setattr(
        "loom_service.routes.provider_connections.fetch_upstream_models",
        _fake,
    )
    return state


def _create_conn(c, token: str, name: str = "openai-prod") -> str:  # type: ignore[no-untyped-def]
    r = c.post(
        "/api/v1/provider-connections", headers=_auth(token),
        json={"name": name, "type": "openai-compatible",
              "base_url": "https://api.openai.com/v1", "api_key": "sk-XYZ"},
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]  # type: ignore[no-any-return]


def test_models_list_initially_empty(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    r = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200
    assert r.json() == {"items": []}


def test_models_manual_entry_is_tied_to_connection_with_metadata(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "my-lab-checkpoint-20260616"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["model_id"] == "my-lab-checkpoint-20260616"
    assert body["source"] == "manual"
    assert body["visible"] is True
    assert body["upstream_present"] is False
    assert body["agent_capable"] is True
    assert body["recommended"] is True
    assert body["visibility"] == "default"
    assert body["hidden_reason"] is None

    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    )
    assert listed.status_code == 200
    assert listed.json()["items"] == [body]


def test_models_manual_entry_survives_refresh_when_absent_upstream(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    created = c.post(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
        json={"model_id": "my-offline-vllm-model"},
    )
    assert created.status_code == 201, created.text

    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    refreshed = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    assert refreshed.status_code == 200, refreshed.text
    rows = {it["model_id"]: it for it in refreshed.json()["items"]}
    manual = rows["my-offline-vllm-model"]
    assert manual["source"] == "manual"
    assert manual["visible"] is True
    assert manual["upstream_present"] is False
    assert manual["hidden_reason"] is None
    assert manual["recommended"] is True


def test_models_refresh_populates_cache_then_list_returns_it(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _stub_fetch_upstream_models(
        monkeypatch, returns=["gpt-4o", "gpt-3.5-turbo"],
    )
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["added"] == 2
    assert body["refreshed"] == 0
    assert body["missing"] == 0
    ids = [it["model_id"] for it in body["items"]]
    assert sorted(ids) == ["gpt-3.5-turbo", "gpt-4o"]
    # All entries start visible + upstream-present.
    for it in body["items"]:
        assert it["visible"] is True
        assert it["upstream_present"] is True
        assert it["hidden_reason"] is None

    # Decrypted api_key reached the fetcher (not the opaque ref).
    last_args = state["last_args"]
    assert isinstance(last_args, tuple)
    assert last_args[2] == "sk-XYZ"

    # GET /models surfaces the same set.
    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert sorted(it["model_id"] for it in listed["items"]) == [
        "gpt-3.5-turbo", "gpt-4o",
    ]


def test_models_refresh_marks_missing_models_unhidden_to_missing(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A previously-cached model not in the new upstream response →
    visible=false, hidden_reason='missing-upstream', upstream_present=false."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(
        monkeypatch, returns=["gpt-4o", "gpt-3.5-turbo"],
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    # Upstream drops gpt-3.5-turbo.
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    body = r.json()
    assert body["added"] == 0
    assert body["refreshed"] == 1
    assert body["missing"] == 1
    rows = {it["model_id"]: it for it in body["items"]}
    assert rows["gpt-4o"]["visible"] is True
    assert rows["gpt-4o"]["upstream_present"] is True
    assert rows["gpt-3.5-turbo"]["visible"] is False
    assert rows["gpt-3.5-turbo"]["upstream_present"] is False
    assert rows["gpt-3.5-turbo"]["hidden_reason"] == "missing-upstream"


def test_models_refresh_preserves_operator_hide_across_refresh(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row hidden by the operator stays hidden after refresh even
    when the upstream still returns it. hidden_reason stays
    'operator-hidden', not flipped to NULL or 'missing-upstream'."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(
        monkeypatch, returns=["gpt-4o", "gpt-3.5-turbo"],
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-3.5-turbo/hide",
        headers=_auth(tokens["team_a"]),
    )

    # Refresh again with same upstream — operator-hidden persists.
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    rows = {it["model_id"]: it for it in r.json()["items"]}
    assert rows["gpt-3.5-turbo"]["visible"] is False
    assert rows["gpt-3.5-turbo"]["hidden_reason"] == "operator-hidden"
    # upstream_present should still be True — we saw it.
    assert rows["gpt-3.5-turbo"]["upstream_present"] is True


def test_models_refresh_upstream_502_does_not_partially_commit(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_service.provider_connections_service import (
        UpstreamModelFetchError,
    )
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    # Populate with one model first so we can verify cache isn't wiped.
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    # Now an upstream error.
    _stub_fetch_upstream_models(
        monkeypatch,
        raises=UpstreamModelFetchError("auth failure", http_status=401),
    )
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 502
    assert "upstream HTTP 401" in r.json()["detail"]

    # Cache state still has the prior gpt-4o row, untouched.
    listed = c.get(
        f"/api/v1/provider-connections/{conn_id}/models",
        headers=_auth(tokens["team_a"]),
    ).json()
    assert [it["model_id"] for it in listed["items"]] == ["gpt-4o"]


def test_models_hide_404_for_uncached_model(app_setup) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-never-cached/hide",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 404
    assert "run POST /models/refresh first" in r.json()["detail"]


def test_models_unhide_keeps_missing_upstream_invisible(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unhiding a model that's no longer upstream MUST NOT make it
    visible — we can't run a trial against a model the provider
    doesn't serve. hidden_reason flips back to 'missing-upstream'."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    # Drop gpt-4o upstream — it becomes missing-upstream.
    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-3.5"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )

    # Operator tries to unhide it.
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/unhide",
        headers=_auth(tokens["team_a"]),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["visible"] is False
    assert body["hidden_reason"] == "missing-upstream"
    assert body["upstream_present"] is False


def test_models_unhide_makes_operator_hidden_visible(
    app_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    _stub_fetch_upstream_models(monkeypatch, returns=["gpt-4o"])
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        headers=_auth(tokens["team_a"]),
    )
    c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/hide",
        headers=_auth(tokens["team_a"]),
    )
    r = c.post(
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/unhide",
        headers=_auth(tokens["team_a"]),
    )
    body = r.json()
    assert body["visible"] is True
    assert body["hidden_reason"] is None


def test_models_routes_cross_team_return_404(app_setup) -> None:
    """team_a owns the connection; team_b can't list / refresh / hide /
    unhide it. 404 (not 403) so existence isn't leaked."""
    app, tokens, _ = app_setup
    c = _client(app)
    conn_id = _create_conn(c, tokens["team_a"])

    for path in [
        f"/api/v1/provider-connections/{conn_id}/models",
    ]:
        r = c.get(path, headers=_auth(tokens["team_b"]))
        assert r.status_code == 404, path

    for path in [
        f"/api/v1/provider-connections/{conn_id}/models/refresh",
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/hide",
        f"/api/v1/provider-connections/{conn_id}/models/gpt-4o/unhide",
    ]:
        r = c.post(path, headers=_auth(tokens["team_b"]))
        assert r.status_code == 404, path
