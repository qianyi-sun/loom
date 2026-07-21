"""End-to-end HttpLLMGatewayClient → Gateway (in-process via ASGITransport)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.agent.gateway_client import GatewayCallRequest
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.db.schema import RateCard, Team, Token
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.gateway_db import delete_all_teams_and_quotas


@pytest.fixture
async def gateway_app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncGenerator[tuple[Any, str, UUID], None]:
    sync_engine = create_engine(postgres_url)
    sync_factory = sessionmaker(sync_engine)
    team_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    with sync_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"th-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_token.encode()).digest(),
            type="team", scopes=["submit", "llm:call"], team_id=team_id,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(RateCard).values(
            id="card", captured_at=datetime.now(UTC),
            table={
                "id": "card",
                "entries": [{
                    "provider": "anthropic",
                    "model": "claude-opus-4-7",
                    "input_per_mtok": 1, "output_per_mtok": 1,
                    "cache_read_per_mtok": 0, "cache_write_per_mtok": 0,
                }],
            },
        ))
        s.commit()

    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "stub")
    settings = GatewaySettings(_env_file=None)
    app = create_app(settings)

    # ASGITransport does NOT run FastAPI lifespan — set up state manually
    # so the routes have the engine/session/cache they expect.
    async_engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(async_engine, expire_on_commit=False)
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory,
        ttl_sec=settings.rate_card_cache_ttl_sec,
    )

    async def stub(**kwargs: Any) -> dict[str, Any]:
        return {
            "id": "x", "model": kwargs.get("model"),
            "choices": [{
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        }
    monkeypatch.setattr("loom_llm_gateway.litellm_wrapper.acompletion", stub)

    try:
        yield app, raw_token, team_id
    finally:
        await async_engine.dispose()
        with sync_factory() as s:
            s.execute(delete(Token))
            delete_all_teams_and_quotas(s)
            s.execute(delete(RateCard))
            s.commit()
        sync_engine.dispose()


async def test_http_client_round_trip(gateway_app):  # type: ignore[no-untyped-def]
    app, raw_token, team_id = gateway_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://gateway") as client:
        gc = HttpLLMGatewayClient(
            base_url="http://gateway", token=raw_token, _client=client,
        )
        response = await gc.call(GatewayCallRequest(
            model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
            messages=[ChatMessage(role="user", content="hi")],
            system_prompt=None, tools=None, tool_choice=None,
            team_id=str(team_id), trial_id=str(uuid4()), step_id="main",
        ))
        assert response.response.content == "ok"
        assert response.input_tokens == 1
        assert response.output_tokens == 1
        assert response.cost_usd >= 0
        assert response.rate_card_hash


async def test_http_client_omits_unset_chat_message_fields() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "id": "stub",
                "model": "some-model",
                "choices": [{
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }],
                "loom": {
                    "input_tokens": 1,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 1,
                    "thinking_tokens": 0,
                    "provider_extras": {},
                    "cost_usd": 0.0,
                    "rate_card_hash": "test-card",
                    "finish_reason": "stop",
                    "duration_sec": 0.01,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "gateway_request_id": "gw-test",
                },
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway",
    ) as client:
        gc = HttpLLMGatewayClient(
            base_url="http://gateway",
            token="loom_team_test",
            _client=client,
        )
        await gc.call(GatewayCallRequest(
            model=ModelSpec(provider="openai", name="some-model"),
            messages=[ChatMessage(role="user", content="hi")],
            system_prompt="be brief",
            tools=None,
            tool_choice=None,
            team_id=str(uuid4()),
            trial_id=str(uuid4()),
            step_id="main",
        ))

    assert captured["body"]["messages"] == [
        {"role": "system", "content": "be brief"},
        {"role": "user", "content": "hi"},
    ]


async def test_http_client_forwards_model_output_limit() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "id": "stub",
                "model": "some-model",
                "choices": [{
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }],
                "loom": {
                    "input_tokens": 1,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 1,
                    "thinking_tokens": 0,
                    "provider_extras": {},
                    "cost_usd": 0.0,
                    "rate_card_hash": "test-card",
                    "finish_reason": "stop",
                    "duration_sec": 0.01,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "gateway_request_id": "gw-test",
                },
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway",
    ) as client:
        gc = HttpLLMGatewayClient(
            base_url="http://gateway",
            token="loom_team_test",
            _client=client,
        )
        await gc.call(GatewayCallRequest(
            model=ModelSpec(
                provider="openai",
                name="some-model",
                max_output_tokens=64,
            ),
            messages=[ChatMessage(role="user", content="hi")],
            system_prompt=None,
            tools=None,
            tool_choice=None,
            team_id=str(uuid4()),
            trial_id=str(uuid4()),
            step_id="main",
        ))

    assert captured["body"]["max_tokens"] == 64


async def test_http_client_forwards_sanitized_request_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "id": "stub",
                "model": "some-model",
                "choices": [{
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }],
                "loom": {
                    "input_tokens": 1,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 1,
                    "thinking_tokens": 0,
                    "provider_extras": {},
                    "cost_usd": 0.0,
                    "rate_card_hash": "test-card",
                    "finish_reason": "stop",
                    "duration_sec": 0.01,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "gateway_request_id": "gw-test",
                },
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway",
    ) as client:
        gc = HttpLLMGatewayClient(
            base_url="http://gateway",
            token="loom_team_test",
            _client=client,
        )
        await gc.call(GatewayCallRequest(
            model=ModelSpec(provider="openai", name="some-model"),
            messages=[ChatMessage(role="user", content="hi")],
            system_prompt=None,
            tools=None,
            tool_choice=None,
            team_id=str(uuid4()),
            trial_id=str(uuid4()),
            step_id="main",
            request_params={
                "temperature": 0,
                "top_p": 0.5,
                "seed": 1234,
                "messages": [{"role": "user", "content": "secret"}],
                "api_key": "sk-hidden",
                "extra_body": {"top_k": 40, "prompt": "secret"},
            },
        ))

    assert captured["body"]["temperature"] == 0
    assert captured["body"]["top_p"] == 0.5
    assert captured["body"]["seed"] == 1234
    assert captured["body"]["extra_body"] == {"top_k": 40}
    assert "api_key" not in captured["body"]
    assert captured["body"]["messages"] == [{"role": "user", "content": "hi"}]


async def test_http_client_propagates_401(gateway_app):  # type: ignore[no-untyped-def]
    """A bad bearer token surfaces as httpx.HTTPStatusError, not silent failure."""
    app, _raw_token, team_id = gateway_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://gateway") as client:
        gc = HttpLLMGatewayClient(
            base_url="http://gateway", token="bogus", _client=client,
        )
        with pytest.raises(httpx.HTTPStatusError):
            await gc.call(GatewayCallRequest(
                model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
                messages=[ChatMessage(role="user", content="hi")],
                system_prompt=None, tools=None, tool_choice=None,
                team_id=str(team_id), trial_id=str(uuid4()), step_id="main",
            ))
