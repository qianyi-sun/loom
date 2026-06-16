"""POST /openai/v1/chat/completions — provider-connection facade.

httpx.MockTransport intercepts the outbound POST to the connection's
base_url; the route looks up + decrypts the api_key via SecretStore
and forwards. On success we assert the upstream got the right
Authorization header, the response body passes through verbatim,
and an `llm_calls` row lands with the JWT's trial_id/step_id +
operator-supplied cost.
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
    Secret,
    Team,
    TeamQuota,
    Token,
)
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache

# Same deterministic test key the provider_connections route tests
# use so the SecretStore decrypts the seeded ciphertext.
_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()


@pytest.fixture
async def facade_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID, UUID, dict[str, list[httpx.Request]]]]:
    """Yields (app, step_jwt, team_id, trial_id, connection_id, captures).

    The fixture seeds:
    - team + token + team_quota
    - a provider connection with type='openai-compatible',
      operator-supplied pricing ($5/1M in, $15/1M out), and a real
      SecretStore-encrypted api_key
    - the gateway app with a MockTransport upstream that records every
      outbound request and returns a canned OpenAI shape

    Tests can override `captures["response"]` to inject different
    canned bodies / statuses.
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

    # Seed team + token + provider connection.
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

    # SecretStore put runs against the async engine so the same row
    # is visible to the route.
    async_session_factory = async_sessionmaker(
        async_engine, expire_on_commit=False,
    )
    async with async_session_factory() as ses:
        store = LocalEncryptedSecretStore(ses)
        ref = await store.put(
            namespace=f"team:{team_id}", value="sk-upstream-XYZ",
        )
        await ses.commit()

    connection_id = uuid4()
    with session_local() as s:
        s.execute(insert(ProviderConnection).values(
            id=connection_id, team_id=team_id,
            provider_type="openai-compatible",
            display_name="openai-prod",
            base_url="https://api.openai.com/v1",
            upstream_host="api.openai.com",
            resolved_egress_ips=["104.18.0.1"],
            encrypted_api_key_ref=ref,
            pricing_source="operator-supplied",
            pricing_data={
                "input_usd_per_1m": 5.0,
                "output_usd_per_1m": 15.0,
            },
            created_by="admin:fixture",
        ))
        s.commit()

    # Upstream MockTransport with a settable canned response. Tests
    # mutate `captures["response"]` to change the canned body/status.
    canned_default = httpx.Response(200, json={
        "id": "chatcmpl_test",
        "object": "chat.completion",
        "model": "gpt-4o",
        "choices": [{
            "index": 0,
            "finish_reason": "stop",
            "message": {"role": "assistant", "content": "hello"},
        }],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
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

    trial_id = uuid4()
    step_jwt = mint_step_jwt(
        team_id=team_id, trial_id=trial_id, step_id="step-1",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )

    try:
        yield app, step_jwt, team_id, trial_id, connection_id, captures  # type: ignore[misc]
    finally:
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _post(app: object, jwt: str, **headers: str) -> httpx.Response:
    """Single-shot POST against the facade. Defaults match a sensible
    happy path so tests only need to override what they care about."""
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "hi"}],
    }
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        return await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {jwt}",
                **headers,
            },
            json=body,
        )


# ──────────────────────────────────────────────────────────────────────
# Happy path
# ──────────────────────────────────────────────────────────────────────


async def test_facade_forwards_with_decrypted_key_and_records_llm_call(
    facade_setup, postgres_url: str,
) -> None:
    app, jwt, team_id, trial_id, conn_id, captures = facade_setup
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Pass-through preserved.
    assert body["id"] == "chatcmpl_test"
    assert body["choices"][0]["message"]["content"] == "hello"

    # Upstream got the decrypted api_key in Bearer auth, NOT the
    # opaque ref or the user's loom_step_<jwt>.
    requests: list[httpx.Request] = captures["requests"]  # type: ignore[assignment]
    assert len(requests) == 1
    up = requests[0]
    assert up.method == "POST"
    assert str(up.url) == "https://api.openai.com/v1/chat/completions"
    assert up.headers["Authorization"] == "Bearer sk-upstream-XYZ"
    # The user's body is forwarded verbatim — same model + messages.
    import json
    sent = json.loads(up.content)
    assert sent["model"] == "gpt-4o"
    assert sent["messages"] == [{"role": "user", "content": "hi"}]

    # llm_calls row written from the JWT's scope.
    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["team_id"] == team_id
    assert row["trial_id"] == trial_id
    assert row["step_id"] == "step-1"
    assert row["dialect"] == "openai_facade"
    assert row["model"] == "gpt-4o"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 50
    # operator-supplied cost: 100/1M * $5 + 50/1M * $15
    #                       = 0.0005 + 0.00075 = 0.00125
    assert float(row["cost_usd"]) == pytest.approx(0.00125, abs=1e-7)
    assert "operator-supplied" in row["rate_card_hash"]


# ──────────────────────────────────────────────────────────────────────
# Auth + header validation
# ──────────────────────────────────────────────────────────────────────


async def test_facade_rejects_non_step_token(facade_setup) -> None:
    """A team-token bearer (no trial_id in scope) must be rejected —
    cost attribution depends on the JWT's trial/step claims."""
    app, _jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    bogus = f"loom_team_{uuid4().hex}"
    r = await _post(
        app, bogus,
        **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code in (401, 403)


async def test_facade_rejects_missing_connection_id_header(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, _conn_id, _captures = facade_setup
    r = await _post(app, jwt)  # no x-loom-provider-connection-id
    assert r.status_code == 400
    assert "x-loom-provider-connection-id" in r.json()["detail"]


async def test_facade_rejects_malformed_connection_id_header(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, _conn_id, _captures = facade_setup
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": "not-a-uuid"},
    )
    assert r.status_code == 400
    assert "not a valid UUID" in r.json()["detail"]


async def test_facade_rejects_stream_true(facade_setup) -> None:
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        r = await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )
    assert r.status_code == 501
    assert "stream=true" in r.json()["detail"]
    # Upstream MUST not have been called.
    assert len(captures["requests"]) == 0  # type: ignore[arg-type]


async def test_facade_rejects_missing_model_field(facade_setup) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        r = await client.post(
            "/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {jwt}",
                "x-loom-provider-connection-id": str(conn_id),
            },
            json={"messages": [{"role": "user", "content": "hi"}]},
        )
    assert r.status_code == 400
    assert "model" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Connection resolution
# ──────────────────────────────────────────────────────────────────────


async def test_facade_returns_404_for_cross_team_connection(
    facade_setup, postgres_url: str,
) -> None:
    """A JWT scoped to team_a can't use team_b's connection — 404
    (not 403) so existence isn't leaked across teams."""
    app, _jwt_a, _team_a, _trial_a, conn_id, _captures = facade_setup
    settings: GatewaySettings = app.state.settings  # type: ignore[attr-defined]
    other_team = uuid4()
    other_jwt = mint_step_jwt(
        team_id=other_team, trial_id=uuid4(), step_id="s",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )
    # Need a Team row for the other team to satisfy any FK paths
    # — but the JWT-scope auth doesn't actually require it.
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
    # Clean up the extra team so the fixture's teardown stays simple.
    sync_engine = create_engine(postgres_url)
    with session_local() as s:
        s.execute(delete(Team).where(Team.id == other_team))
        s.commit()
    sync_engine.dispose()
    assert r.status_code == 404


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


async def test_facade_rejects_anthropic_type_connection(
    facade_setup, postgres_url: str,
) -> None:
    """Anthropic-typed connections don't speak the chat-completion
    shape — route 400s so operators get a clear hint."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(text(
            "UPDATE provider_connections SET provider_type='anthropic' "
            "WHERE id = :id",
        ), {"id": conn_id})
    sync_engine.dispose()

    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 400
    assert "anthropic" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Upstream error mapping
# ──────────────────────────────────────────────────────────────────────


async def test_facade_surfaces_upstream_401(
    facade_setup, postgres_url: str,
) -> None:
    """Upstream 401 → return 401 with the upstream body excerpt; no
    llm_calls row should be written."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(  # type: ignore[index]
        401, json={"error": {"message": "Invalid API key"}},
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 401
    assert "Invalid API key" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert rows == []


async def test_facade_redacts_api_key_from_upstream_error_body(
    facade_setup,
) -> None:
    """If the upstream echoes the api_key in a 4xx debug page, the
    facade MUST scrub it before surfacing — the sandbox caller is
    less trusted than the operator who supplied the key."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    # Fixture seeds api_key="sk-upstream-XYZ" — same string the
    # SecretStore round-trips. Have the upstream echo it.
    captures["response"] = httpx.Response(  # type: ignore[index]
        401,
        text=(
            "Auth failed. Token sent: Bearer sk-upstream-XYZ. "
            "Check Authorization header."
        ),
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 401
    detail = r.json()["detail"]
    assert "sk-upstream-XYZ" not in detail
    assert "[REDACTED]" in detail


async def test_facade_handles_missing_upstream_usage_block(
    facade_setup, postgres_url: str,
) -> None:
    """An operator endpoint that returns 200 without `usage` should
    still produce an llm_calls row with 0/0 + cost 0 — best-effort
    attribution beats silent loss."""
    app, jwt, _team_id, _trial_id, conn_id, captures = facade_setup
    captures["response"] = httpx.Response(200, json={  # type: ignore[index]
        "id": "no-usage",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "x"}}],
    })
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 200

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0


async def test_facade_returns_502_on_upstream_request_error(
    facade_setup,
) -> None:
    """Connect / DNS errors → 502 with the underlying error type
    in the detail."""
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup

    def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    await app.state.upstream_client.aclose()  # type: ignore[attr-defined]
    app.state.upstream_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(_raise),
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 502
    assert "ConnectError" in r.json()["detail"]


async def test_facade_returns_504_on_upstream_timeout(
    facade_setup,
) -> None:
    app, jwt, _team_id, _trial_id, conn_id, _captures = facade_setup

    def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("read timeout")

    await app.state.upstream_client.aclose()  # type: ignore[attr-defined]
    app.state.upstream_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(_raise),
    )
    r = await _post(
        app, jwt, **{"x-loom-provider-connection-id": str(conn_id)},
    )
    assert r.status_code == 504
    assert "timeout" in r.json()["detail"]
