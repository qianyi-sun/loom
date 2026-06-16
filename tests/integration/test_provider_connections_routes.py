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

    # Seed two teams + an admin token + per-team tokens.
    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    raw_admin = f"loom_admin_{uuid4().hex}"

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
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_admin.encode()).digest(),
            type="admin", scopes=["admin:tokens", "admin:rate_cards"],
            team_id=None,
            issued_at=datetime.now(UTC),
        ))
        s.commit()

    tokens = {"team_a": raw_a, "team_b": raw_b, "admin": raw_admin}
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
