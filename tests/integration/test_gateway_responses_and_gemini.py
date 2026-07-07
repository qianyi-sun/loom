"""POST /v1/responses + POST /v1beta/models/...:generateContent
native passthrough (Plan 9 Tasks 8 + 9).

Shares the gateway_setup fixture pattern with test_gateway_messages.py;
the upstream MockTransport intercepts either OpenAI or Gemini depending
on the request URL host/path.
"""

import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.auth import mint_step_jwt
from loom.db.schema import LlmCall, ProviderConnection, RateCard, Secret, Team
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.rate_card import RateCardCache
from tests.integration.gateway_db import delete_all_teams_and_quotas

_TEST_MASTER_KEY = base64.b64encode(bytes(range(32))).decode()

_RATE_CARD_TABLE = {
    "id": "card-test",
    "entries": [
        {
            "provider": "openai",
            "model": "gpt-5",
            "input_per_mtok": 5.0,
            "output_per_mtok": 10.0,
            "cache_read_per_mtok": 0.5,
            "cache_write_per_mtok": 6.25,
        },
        {
            "provider": "google",
            "model": "gemini-2.0-flash",
            "input_per_mtok": 0.075,
            "output_per_mtok": 0.30,
            "cache_read_per_mtok": 0.019,
            "cache_write_per_mtok": 0.09,
        },
    ],
}


@pytest.fixture
async def gateway(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
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
        async_engine,
        expire_on_commit=False,
    )
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory,
        ttl_sec=settings.rate_card_cache_ttl_sec,
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.openai.com":
            assert request.headers["authorization"] == "Bearer test-openai-key"
            return httpx.Response(
                200,
                json={
                    "id": "resp_test",
                    "model": "gpt-5",
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {"type": "output_text", "text": "ok"},
                            ],
                        }
                    ],
                    "usage": {
                        "input_tokens": 200,
                        "output_tokens": 80,
                        "output_tokens_details": {"reasoning_tokens": 20},
                    },
                },
            )
        if request.url.host == "generativelanguage.googleapis.com":
            assert request.url.params["key"] == "test-google-key"
            return httpx.Response(
                200,
                json={
                    "candidates": [
                        {
                            "content": {
                                "parts": [{"text": "ok"}],
                                "role": "model",
                            }
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 150,
                        "candidatesTokenCount": 60,
                        "cachedContentTokenCount": 40,
                        "thoughtsTokenCount": 12,
                    },
                },
            )
        return httpx.Response(404)

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(
            insert(RateCard).values(
                id="card-test",
                captured_at=datetime.now(UTC),
                table=_RATE_CARD_TABLE,
            )
        )
        s.commit()

    team_id = uuid4()
    trial_id = uuid4()
    step_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="main",
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


@pytest.fixture
async def gateway_with_provider_connection(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[object, str, UUID, UUID, UUID, dict[str, list[httpx.Request]]]]:
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
        async_engine,
        expire_on_commit=False,
    )
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory,
        ttl_sec=settings.rate_card_cache_ttl_sec,
    )

    captures: dict[str, list[httpx.Request]] = {"requests": []}

    def _handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)
        assert request.url.host == "provider.example"
        assert request.headers["authorization"] == "Bearer sk-provider-XYZ"
        body = json.loads(request.content)
        if body.get("stream") is True:
            payload = {
                "type": "response.completed",
                "response": {
                    "id": "resp_stream",
                    "model": body["model"],
                    "usage": {
                        "input_tokens": 12,
                        "output_tokens": 4,
                        "output_tokens_details": {"reasoning_tokens": 2},
                    },
                },
            }
            return httpx.Response(
                200,
                content=f"data: {json.dumps(payload)}\n\n".encode(),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            json={
                "id": "resp_provider",
                "model": body["model"],
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "ok"},
                        ],
                    }
                ],
                "usage": {
                    "input_tokens": 21,
                    "output_tokens": 7,
                    "output_tokens_details": {"reasoning_tokens": 3},
                },
            },
        )

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=settings.upstream_timeout_sec,
    )
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url=settings.egress_proxy_url,
        upstream_timeout_sec=settings.upstream_timeout_sec,
    )

    team_id = uuid4()
    trial_id = uuid4()
    connection_id = uuid4()

    async_session_factory = async_sessionmaker(
        async_engine,
        expire_on_commit=False,
    )
    async with async_session_factory() as ses:
        store = LocalEncryptedSecretStore(ses)
        ref = await store.put(
            namespace=f"team:{team_id}",
            value="sk-provider-XYZ",
        )
        await ses.commit()

    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(ProviderConnection).values(
                id=connection_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="provider",
                base_url="https://provider.example/v1",
                upstream_host="provider.example",
                resolved_egress_ips=["203.0.113.10"],
                encrypted_api_key_ref=ref,
                pricing_source="tokens-only",
                created_by="test",
                # #277 / responses-api-support-probe: these tests
                # exercise the NATIVE /v1/responses path and its 400-
                # signature fallback. Pre-seeding the probe-cached
                # bool to TRUE skips the inline probe (which would
                # otherwise send {} to the mock and get interpreted as
                # a rejected-body signal, dispatching straight to the
                # translator and short-circuiting the native path this
                # suite is here to test).
                responses_api_supported=True,
                responses_api_probed_at=datetime.now(UTC),
            )
        )
        s.commit()

    step_jwt = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id,
        step_id="main",
        ttl_sec=60,
        signing_key=settings.step_jwt_signing_key.get_secret_value(),
        provider_connection_id=connection_id,
    )

    try:
        yield app, step_jwt, team_id, trial_id, connection_id, captures
    finally:
        await app.state.egress_client_pool.aclose()
        await app.state.upstream_client.aclose()
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            delete_all_teams_and_quotas(s)
            s.commit()
        sync_engine.dispose()


async def test_responses_native_passthrough(gateway, postgres_url):  # type: ignore[no-untyped-def]
    app, step_jwt, team_id, trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
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


async def test_responses_upstream_500_records_failed_audit_row(  # type: ignore[no-untyped-def]
    gateway,
    postgres_url,
):
    app, step_jwt, team_id, trial_id = gateway

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.openai.com"
        return httpx.Response(500, text="internal provider error")

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"model": "gpt-5", "input": "hi", "temperature": 0.2},
        )
    assert r.status_code == 500
    assert "internal provider error" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "gpt-5"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_5xx",
        "_loom_failure_status_code": 500,
        "_loom_usage_status": "missing",
    }
    assert row["request_params"] == {
        "status": "available",
        "parameters": {"temperature": 0.2},
    }


@pytest.mark.parametrize("path", ["/v1/responses", "/openai/v1/responses"])
async def test_responses_routes_provider_connection_from_step_jwt(
    gateway_with_provider_connection,
    postgres_url: str,
    path: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    payload = {
        "model": "qwen2.5-coder-7b-instruct",
        "instructions": "You are Codex.",
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": "hi"}],
            }
        ],
        "parallel_tool_calls": True,
        "reasoning": {"effort": "low"},
        "store": False,
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            path,
            headers={"Authorization": f"Bearer {step_jwt}"},
            json=payload,
        )
    assert r.status_code == 200, r.text
    assert r.json()["id"] == "resp_provider"

    requests = captures["requests"]
    assert len(requests) == 1
    assert str(requests[0].url) == "https://provider.example/v1/responses"
    assert json.loads(requests[0].content) == payload

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "qwen2.5-coder-7b-instruct"
    assert row["input_tokens"] == 21
    assert row["output_tokens"] == 7
    assert row["provider_extras"]["reasoning_tokens"] == 3
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_responses_merges_query_request_params_into_provider_payload_and_audit(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    payload = {
        "model": "qwen2.5-coder-7b-instruct",
        "input": "hi",
        "store": False,
    }
    query_params = {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "messages": [{"role": "user", "content": "do not forward"}],
        "api_key": "sk-do-not-forward",
        "extra_body": {
            "top_k": 40,
            "prompt": "do not forward",
        },
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            params={"loom_request_params": json.dumps(query_params)},
            headers={"Authorization": f"Bearer {step_jwt}"},
            json=payload,
        )
    assert r.status_code == 200, r.text

    requests = captures["requests"]
    assert len(requests) == 1
    forwarded = json.loads(requests[0].content)
    assert forwarded == {
        **payload,
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "extra_body": {"top_k": 40},
    }
    rendered_forwarded = json.dumps(forwarded)
    assert "sk-do-not-forward" not in rendered_forwarded
    assert "do not forward" not in rendered_forwarded

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["request_params"] == {
        "status": "available",
        "parameters": {
            "temperature": 0,
            "top_p": 0.5,
            "seed": 1234,
            "top_k": 40,
        },
    }
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_responses_provider_connection_stream_passthrough(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    payload = {
        "model": "qwen2.5-coder-7b-instruct",
        "input": "hi",
        "stream": True,
        "store": False,
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1/responses",
            headers={
                "Authorization": f"Bearer {step_jwt}",
                "Accept": "text/event-stream",
            },
            json=payload,
        )
    assert r.status_code == 200, r.text
    assert "text/event-stream" in r.headers["content-type"]
    assert "response.completed" in r.text

    requests = captures["requests"]
    assert len(requests) == 1
    assert str(requests[0].url) == "https://provider.example/v1/responses"
    assert requests[0].headers["accept"] == "text/event-stream"
    assert json.loads(requests[0].content) == payload

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "qwen2.5-coder-7b-instruct"
    assert row["input_tokens"] == 12
    assert row["output_tokens"] == 4
    assert row["provider_extras"]["reasoning_tokens"] == 2
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_responses_facade_falls_back_to_chat_completions_for_chat_only_provider(
    gateway_with_provider_connection,
    postgres_url: str,
) -> None:
    app, step_jwt, team_id, trial_id, _conn_id, captures = gateway_with_provider_connection

    def _chat_only_handler(request: httpx.Request) -> httpx.Response:
        captures["requests"].append(request)
        assert request.url.host == "provider.example"
        assert request.headers["authorization"] == "Bearer sk-provider-XYZ"
        if request.url.path == "/v1/responses":
            return httpx.Response(
                400,
                json={
                    "error": {
                        "message": "you must provide a messages parameter",
                        "type": "invalid_request_error",
                        "param": "messages",
                        "code": "missing_required_parameter",
                    },
                },
            )
        assert request.url.path == "/v1/chat/completions"
        body = json.loads(request.content)
        assert body["model"] == "qwen3.6-35b-a3b"
        assert body["stream"] is False
        assert body["tool_choice"] == "auto"
        assert body["parallel_tool_calls"] is False
        assert body["messages"] == [
            {"role": "system", "content": "You are Codex."},
            {"role": "user", "content": "run echo"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_old",
                        "type": "function",
                        "function": {
                            "name": "exec_command",
                            "arguments": '{"cmd":"pwd"}',
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_old",
                "content": "ok",
            },
        ]
        assert body["tools"] == [
            {
                "type": "function",
                "function": {
                    "name": "exec_command",
                    "description": "run commands",
                    "parameters": {
                        "type": "object",
                        "properties": {"cmd": {"type": "string"}},
                        "required": ["cmd"],
                    },
                },
            }
        ]
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl_compat",
                "model": body["model"],
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call_new",
                                    "type": "function",
                                    "function": {
                                        "name": "exec_command",
                                        "arguments": '{"cmd":"echo hi"}',
                                    },
                                }
                            ],
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                },
            },
        )

    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_chat_only_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url=app.state.settings.egress_proxy_url,
        upstream_timeout_sec=app.state.settings.upstream_timeout_sec,
    )

    payload = {
        "model": "qwen3.6-35b-a3b",
        "instructions": "You are Codex.",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "run echo"}],
            },
            {
                "type": "function_call",
                "call_id": "call_old",
                "name": "exec_command",
                "arguments": '{"cmd":"pwd"}',
            },
            {
                "type": "function_call_output",
                "call_id": "call_old",
                "output": "ok",
            },
        ],
        "stream": True,
        "store": False,
        "tool_choice": "auto",
        "parallel_tool_calls": False,
        "tools": [
            {
                "type": "function",
                "name": "exec_command",
                "description": "run commands",
                "parameters": {
                    "type": "object",
                    "properties": {"cmd": {"type": "string"}},
                    "required": ["cmd"],
                },
            },
            {"type": "namespace", "name": "multi_agent_v1"},
        ],
    }
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/openai/v1/responses",
            headers={
                "Authorization": f"Bearer {step_jwt}",
                "Accept": "text/event-stream",
            },
            json=payload,
        )

    assert r.status_code == 200, r.text
    assert "text/event-stream" in r.headers["content-type"]
    assert "response.function_call_arguments.done" in r.text
    assert "call_new" in r.text
    assert "response.completed" in r.text

    requests = captures["requests"]
    assert [request.url.path for request in requests] == [
        "/v1/responses",
        "/v1/chat/completions",
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["dialect"] == "openai_responses"
    assert row["model"] == "qwen3.6-35b-a3b"
    assert row["input_tokens"] == 11
    assert row["output_tokens"] == 7
    assert row["provider_extras"]["_loom_cost_source"] == "tokens-only"
    assert row["provider_extras"]["_loom_cost_confidence"] == "not_applicable"
    assert row["provider_extras"]["_loom_pricing_source"] == "tokens-only"
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id


async def test_gemini_native_passthrough(gateway, postgres_url):  # type: ignore[no-untyped-def]
    app, step_jwt, team_id, trial_id = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
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


async def test_gemini_upstream_503_records_failed_audit_row(  # type: ignore[no-untyped-def]
    gateway,
    postgres_url,
):
    app, step_jwt, team_id, trial_id = gateway

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "generativelanguage.googleapis.com"
        return httpx.Response(503, json={"error": "temporarily unavailable"})

    await app.state.upstream_client.aclose()
    app.state.upstream_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
        timeout=app.state.settings.upstream_timeout_sec,
    )

    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash:generateContent",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={"contents": [{"role": "user", "parts": [{"text": "hi"}]}]},
        )
    assert r.status_code == 503
    assert "temporarily unavailable" in r.json()["detail"]

    sync_engine = create_engine(postgres_url)
    with sync_engine.connect() as conn:
        rows = list(conn.execute(text("SELECT * FROM llm_calls")))
    sync_engine.dispose()
    assert len(rows) == 1
    row = dict(rows[0]._mapping)
    assert row["trial_id"] == trial_id
    assert row["team_id"] == team_id
    assert row["step_id"] == "main"
    assert row["dialect"] == "gemini"
    assert row["model"] == "gemini-2.0-flash"
    assert row["input_tokens"] == 0
    assert row["output_tokens"] == 0
    assert float(row["cost_usd"]) == 0.0
    assert row["rate_card_hash"] == "failed-upstream"
    assert row["provider_extras"] == {
        "_loom_call_status": "failed",
        "_loom_failure_category": "upstream_http_5xx",
        "_loom_failure_status_code": 503,
        "_loom_usage_status": "missing",
    }
    assert "contents" not in row["request_params"]


async def test_gemini_rejects_path_without_colon(gateway):  # type: ignore[no-untyped-def]
    app, step_jwt, _t, _tr = gateway
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://gw",
    ) as client:
        r = await client.post(
            "/v1beta/models/gemini-2.0-flash",
            headers={"Authorization": f"Bearer {step_jwt}"},
            json={},
        )
        assert r.status_code == 400
