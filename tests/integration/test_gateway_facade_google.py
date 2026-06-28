"""POST /google/v1beta/models/{model}:generateContent — provider-
connection facade. PR B of #70.

Mirror of test_gateway_facade_anthropic.py with Google-specific
assertions: query-string auth (`?key=...`), `usageMetadata` token
extraction (camelCase per Gemini convention), `countTokens` exemption
from cost attribution, streaming variant rejected with 501.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import (
    LlmCall,
    ProviderConnection,
    RateCard,
    Secret,
    Team,
    TeamQuota,
    Token,
)
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.rate_card import RateCardCache

_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
async def facade_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID, UUID, dict[str, object]]]:
    """Yields (app, step_jwt, team_id, trial_id, connection_id, captures).

    Seeds a google-typed connection with operator-supplied pricing
    ($0.075/1M in, $0.30/1M out — Gemini Flash actuals).
    """
    for k, v in {
        "LOOM_GW_DB_URL": postgres_url,
        "LOOM_SECRET_STORE_MASTER_KEY": _TEST_MASTER_KEY,
    }.items():
        monkeypatch.setenv(k, v)
    settings = GatewaySettings(_env_file=None)
    app = create_app(settings)

    async_engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        async_engine, expire_on_commit=False,
    )
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory,
        ttl_sec=settings.rate_card_cache_ttl_sec,
    )

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    team_id = uuid4()
    raw_team_token = f"loom_team_{uuid4().hex}"
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_team_token.encode()).digest(),
            type="team", scopes=["submit", "read:own"],
            team_id=team_id, issued_at=datetime.now(UTC),
        ))
        s.commit()

    async_session_factory = async_sessionmaker(
        async_engine, expire_on_commit=False,
    )
    async with async_session_factory() as ses:
        store = LocalEncryptedSecretStore(ses)
        # Realistic-ish Google API key shape (`AIza...`).
        ref = await store.put(
            namespace=f"team:{team_id}", value="AIzaSy-google-XYZ-1234",
        )
        await ses.commit()

    connection_id = uuid4()
    with session_local() as s:
        s.execute(insert(ProviderConnection).values(
            id=connection_id, team_id=team_id,
            provider_type="google",
            display_name="google-prod",
            base_url="https://generativelanguage.googleapis.com",
            upstream_host="generativelanguage.googleapis.com",
            resolved_egress_ips=["172.217.0.10"],
            encrypted_api_key_ref=ref,
            pricing_source="operator-supplied",
            pricing_data={
                "input_usd_per_1m": 0.075,
                "output_usd_per_1m": 0.30,
            },
            created_by="admin:fixture",
        ))
        s.commit()

    canned_default = httpx.Response(200, json={
        "candidates": [{
            "content": {"role": "model",
                        "parts": [{"text": "hello"}]},
            "finishReason": "STOP",
        }],
        "usageMetadata": {
            "promptTokenCount": 200,
            "candidatesTokenCount": 80,
            "totalTokenCount": 280,
            "cachedContentTokenCount": 40,
        },
    })
    captures: dict[str, object] = {
        "requests": [],
        "response": canned_default,
    }

    def _handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)  # type: ignore[union-attr]
        return captures["response"]  # type: ignore[return-value]

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url="",
        upstream_timeout_sec=settings.upstream_timeout_sec,
    )

    trial_id = uuid4()
    step_jwt = mint_step_jwt(
        team_id=team_id, trial_id=trial_id, step_id="step-1",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )

    try:
        yield app, step_jwt, team_id, trial_id, connection_id, captures  # type: ignore[misc]
    finally:
        await app.state.egress_client_pool.aclose()
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(RateCard))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _post(
    app: object, jwt: str,
    *, path_suffix: str = "gemini-2.5-flash:generateContent",
    **headers: str,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    body = {
        "contents": [{
            "role": "user",
            "parts": [{"text": "hi"}],
        }],
    }
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        return await client.post(
            f"/google/v1beta/models/{path_suffix}",
            headers={
                "Authorization": f"Bearer {jwt}",
                **headers,
            },
            json=body,
        )


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


async def test_facade_forwards_with_query_string_key_and_records_llm_call(
    facade_setup, postgres_url: str,
) -> None:
    app, jwt, team_id, trial_id, conn_id, captures = facade_setup
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["candidates"][0]["content"]["parts"][0]["text"] == "hello"
    assert body["usageMetadata"]["promptTokenCount"] == 200

    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    up = requests[0]
    assert up.method == "POST"
    # Upstream URL targets the connection's base_url + the same model
    # path the caller supplied. ?key=... carries the decrypted key in
    # the query string (Google API convention).
    assert up.url.path == (
        "/v1beta/models/gemini-2.5-flash:generateContent"
    )
    assert up.url.params["key"] == "AIzaSy-google-XYZ-1234"
    # Authorization header to the upstream MUST NOT carry the step-JWT
    # (we strip everything except content-type + the `?key=` query).
    auth_header = up.headers.get("Authorization", "")
    assert "loom_step_" not in auth_header

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "step-1"
    assert row["dialect"] == "gemini_facade"
    # model_name extracted from path, not body.
    assert row["model"] == "gemini-2.5-flash"
    assert row["input_tokens"] == 200
    assert row["output_tokens"] == 80
    # cachedContentTokenCount round-trips into provider_extras.
    assert row["provider_extras"]["cachedContentTokenCount"] == 40
    # operator-supplied cost: 200/1M * $0.075 + 80/1M * $0.30
    #                       = 0.000015 + 0.000024 = 0.000039
    assert float(row["cost_usd"]) == pytest.approx(0.000039, abs=1e-8)
    assert "operator-supplied" in row["rate_card_hash"]


async def test_facade_rate_card_pricing_uses_google_provider(
    facade_setup, postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections "
                "SET pricing_source='rate-card', pricing_data=NULL "
                "WHERE id = :id"
            ),
            {"id": conn_id},
        )
        conn.execute(insert(RateCard).values(
            id="card-google",
            captured_at=datetime.now(UTC),
            table={
                "entries": [{
                    "provider": "google",
                    "model": "gemini-2.5-flash",
                    "input_per_mtok": 0.075,
                    "output_per_mtok": 0.30,
                    "cache_read_per_mtok": 0.01,
                    "cache_write_per_mtok": 0.0,
                }],
            },
        ))
    sync_engine.dispose()

    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    # 200 input, 80 output, 40 cachedContentTokenCount.
    assert float(row["cost_usd"]) == pytest.approx(0.000039, abs=1e-8)
    assert len(row["rate_card_hash"]) == 64


async def test_facade_rate_card_missing_entry_records_missing_marker(
    facade_setup, postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE provider_connections "
                "SET pricing_source='rate-card', pricing_data=NULL "
                "WHERE id = :id"
            ),
            {"id": conn_id},
        )
        conn.execute(insert(RateCard).values(
            id="card-google-missing",
            captured_at=datetime.now(UTC),
            table={
                "entries": [{
                    "provider": "google",
                    "model": "not-gemini-2.5-flash",
                    "input_per_mtok": 0.075,
                    "output_per_mtok": 0.30,
                    "cache_read_per_mtok": 0.01,
                    "cache_write_per_mtok": 0.0,
                }],
            },
        ))
    sync_engine.dispose()

    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "facade:rate-card:missing"


async def test_facade_count_tokens_action_returns_body_without_audit(
    facade_setup, postgres_url: str,
) -> None:
    """countTokens is a legitimate `usageMetadata`-less response;
    the facade returns the body but doesn't write llm_calls.
    Matches the legacy /v1beta route's behavior."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(200, json={  # type: ignore[index]
        "totalTokens": 27,
    })
    r = await _post(
        app, jwt,
        path_suffix="gemini-2.5-flash:countTokens",
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200
    assert r.json() == {"totalTokens": 27}

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert rows == []


# ──────────────────────────────────────────────────────────────────────
# Auth + header + path validation
# ──────────────────────────────────────────────────────────────────────


async def test_facade_rejects_non_step_token(facade_setup) -> None:
    app, _jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    bogus = f"loom_team_{uuid4().hex}"
    r = await _post(
        app, bogus, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code in (401, 403)


async def test_facade_rejects_missing_connection_id_header(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, _conn_id, _captures = facade_setup
    r = await _post(app, jwt)
    assert r.status_code == 400
    assert "x-loom-provider-connection-id" in r.json()["detail"]


async def test_facade_rejects_path_without_action(facade_setup) -> None:
    """Path must be `<model>:<action>`. Missing `:` → 400."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    r = await _post(
        app, jwt,
        path_suffix="gemini-2.5-flash",  # no colon
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 400
    assert "<model>:<action>" in r.json()["detail"]


async def test_facade_rejects_stream_variant(facade_setup) -> None:
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    r = await _post(
        app, jwt,
        path_suffix="gemini-2.5-flash:streamGenerateContent",
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 501
    assert "not yet supported" in r.json()["detail"]
    assert len(captures["requests"]) == 0  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# Connection resolution
# ──────────────────────────────────────────────────────────────────────


async def test_facade_returns_404_for_cross_team_connection(
    facade_setup, postgres_url: str,
) -> None:
    app, _jwt, _team_a, _trial_a, conn_id, captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    other_team = uuid4()
    other_jwt = mint_step_jwt(
        team_id=other_team, trial_id=uuid4(), step_id="s",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(insert(Team).values(id=other_team, name=f"t-{other_team}"))
        s.commit()
    sync_engine.dispose()

    r = await _post(
        app, other_jwt,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    sync_engine = create_engine(postgres_url)
    with session_local() as s:
        s.execute(delete(TeamQuota).where(TeamQuota.team_id == other_team))
        s.execute(delete(Team).where(Team.id == other_team))
        s.commit()
    sync_engine.dispose()
    assert r.status_code == 404
    # Upstream never called.
    assert len(captures["requests"]) == 0  # type: ignore[arg-type]


async def test_facade_rejects_openai_compatible_type_connection(
    facade_setup, postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(text(
            "UPDATE provider_connections SET provider_type='openai-compatible' "
            "WHERE id = :id",
        ), {"id": conn_id})
    sync_engine.dispose()

    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 400
    assert "openai-compatible" in r.json()["detail"]


async def test_facade_returns_404_for_soft_deleted_connection(
    facade_setup, postgres_url: str,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(text(
            "UPDATE provider_connections SET deleted_at = now() "
            "WHERE id = :id",
        ), {"id": conn_id})
    sync_engine.dispose()

    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# Upstream error mapping + redaction
# ──────────────────────────────────────────────────────────────────────


async def test_facade_surfaces_upstream_403_without_llm_call_row(
    facade_setup, postgres_url: str,
) -> None:
    """Google rejects bad keys with 403 (not 401). Match that shape."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        403, json={"error": {"code": 403, "message": "API key invalid"}},
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 403
    assert "API key invalid" in r.json()["detail"]
    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert rows == []


async def test_facade_redacts_api_key_from_upstream_error_body(
    facade_setup,
) -> None:
    """Google upstream may echo the `?key=` parameter in 4xx debug
    bodies. Redact it before surfacing."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        403,
        text=(
            "Forbidden. Request was: GET /v1beta/models?key=AIzaSy-google-XYZ-1234. "
            "Check key restrictions."
        ),
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert "AIzaSy-google-XYZ-1234" not in detail
    assert "[REDACTED]" in detail


async def test_facade_returns_502_on_missing_usage_metadata(
    facade_setup, postgres_url: str,
) -> None:
    """generateContent without usageMetadata is a contract violation;
    countTokens is exempt (separate test above)."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    facade_setup_captures: dict[str, object] = facade_setup[5]  # type: ignore[index]
    facade_setup_captures["response"] = httpx.Response(200, json={
        "candidates": [{
            "content": {"role": "model", "parts": [{"text": "x"}]},
        }],
        # no usageMetadata
    })
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 502
    assert "usageMetadata" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert rows == []


async def test_facade_returns_504_on_upstream_timeout(facade_setup) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup

    def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("read timeout")

    await app.state.egress_client_pool.aclose()  # type: ignore[attr-defined]
    await app.state.upstream_client.aclose()  # type: ignore[attr-defined]
    app.state.upstream_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(_raise),
    )
    app.state.egress_client_pool = EgressClientPool(  # type: ignore[attr-defined]
        upstream_client=app.state.upstream_client,  # type: ignore[attr-defined]
        proxy_url="",
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,  # type: ignore[attr-defined]
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 504
    assert "timeout" in r.json()["detail"]
