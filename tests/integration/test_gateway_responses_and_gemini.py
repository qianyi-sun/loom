"""POST /v1/responses + POST /v1beta/models/...:generateContent
native passthrough (Plan 9 Tasks 8 + 9).

Shares the gateway_setup fixture pattern with test_gateway_messages.py;
the upstream MockTransport intercepts either OpenAI or Gemini depending
on the request URL host/path.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import LlmCall, RateCard
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache

_RATE_CARD_TABLE = {
    "id": "card-test",
    "entries": [
        {
            "provider": "openai", "model": "gpt-5",
            "input_per_mtok": 5.0, "output_per_mtok": 10.0,
            "cache_read_per_mtok": 0.5, "cache_write_per_mtok": 6.25,
        },
        {
            "provider": "google", "model": "gemini-2.0-flash",
            "input_per_mtok": 0.075, "output_per_mtok": 0.30,
            "cache_read_per_mtok": 0.019, "cache_write_per_mtok": 0.09,
        },
    ],
}


@pytest.fixture
async def gateway(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID]]:
    for k, v in {
        "LOOM_GW_DB_URL": postgres_url,
        "LOOM_GW_OPENAI_API_KEY": "test-openai-key",
        "LOOM_GW_GOOGLE_API_KEY": "test-google-key",
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

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.openai.com":
            assert (
                request.headers["authorization"]
                == "Bearer test-openai-key"
            )
            return httpx.Response(200, json={
                "id": "resp_test",
                "model": "gpt-5",
                "output": [{"type": "message", "content": [
                    {"type": "output_text", "text": "ok"},
                ]}],
                "usage": {
                    "input_tokens": 200, "output_tokens": 80,
                    "output_tokens_details": {"reasoning_tokens": 20},
                },
            })
        if request.url.host == "generativelanguage.googleapis.com":
            assert request.url.params["key"] == "test-google-key"
            return httpx.Response(200, json={
                "candidates": [{"content": {
                    "parts": [{"text": "ok"}], "role": "model",
                }}],
                "usageMetadata": {
                    "promptTokenCount": 150,
                    "candidatesTokenCount": 60,
                    "cachedContentTokenCount": 40,
                    "thoughtsTokenCount": 12,
                },
            })
        return httpx.Response(404)

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(insert(RateCard).values(
            id="card-test", captured_at=datetime.now(UTC),
            table=_RATE_CARD_TABLE,
        ))
        s.commit()

    team_id = uuid4()
    trial_id = uuid4()
    step_jwt = mint_step_jwt(
        team_id=team_id, trial_id=trial_id, step_id="main",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
    )

    try:
        yield app, step_jwt, team_id, trial_id
    finally:
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(RateCard))
            s.commit()
        sync_engine.dispose()


async def test_responses_native_passthrough(gateway, postgres_url):  # type: ignore[no-untyped-def]
    app, step_jwt, team_id, trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"model": "gpt-5", "input": "hi"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # Reasoning tokens detail preserved.
        assert body["usage"]["output_tokens_details"]["reasoning_tokens"] == 20

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["input_tokens"] == 200
    assert row["output_tokens"] == 80
    assert row["provider_extras"] == {"reasoning_tokens": 20}
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_gemini_native_passthrough(gateway, postgres_url):  # type: ignore[no-untyped-def]
    app, step_jwt, team_id, trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash:generateContent",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # usageMetadata preserved including thoughtsTokenCount.
        assert body["usageMetadata"]["thoughtsTokenCount"] == 12

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "gemini"
    assert row["model"] == "gemini-2.0-flash"
    assert row["input_tokens"] == 150
    assert row["output_tokens"] == 60
    assert row["provider_extras"]["cachedContentTokenCount"] == 40
    assert row["provider_extras"]["thoughtsTokenCount"] == 12
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_gemini_rejects_path_without_colon(gateway):  # type: ignore[no-untyped-def]
    app, step_jwt, _t, _tr = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={},
        )
        assert r.status_code == 400
