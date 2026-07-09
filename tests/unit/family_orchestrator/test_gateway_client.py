"""Unit tests for OrchestratorGatewayClient (#672 PR-3).

Covers the wire shape the ``skill_patcher_llm`` adapter depends on:
POST /v1/chat/completions with an OpenAI-shaped body plus the Loom
attribution block carrying ``dialect`` so the gateway can record the
llm_calls row under ``family_evolver``.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from loom_family_orchestrator.gateway_client import OrchestratorGatewayClient


@pytest.mark.asyncio
async def test_orchestrator_gateway_client_posts_openai_shape() -> None:
    """The client wraps httpx and speaks the same shape the family-run
    adapter expects: model + messages + max_tokens + loom.dialect."""
    captured: dict[str, Any] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["body"] = request.read().decode("utf-8")
        return httpx.Response(
            200,
            json={
                "choices": [
                    {"message": {"role": "assistant", "content": "OK"}},
                ],
            },
        )

    transport = httpx.MockTransport(_handler)
    client = OrchestratorGatewayClient(
        base_url="http://gateway.local",
        team_id="00000000-0000-0000-0000-000000000001",
        token="stub-token",
        _client=httpx.AsyncClient(transport=transport, base_url="http://gateway.local"),
    )

    resp = await client.chat_completion(
        model="anthropic/claude-sonnet-4-6",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=1024,
        timeout_sec=30.0,
    )
    assert resp["choices"][0]["message"]["content"] == "OK"

    import json

    body = json.loads(captured["body"])
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/v1/chat/completions")
    assert body["model"] == "anthropic/claude-sonnet-4-6"
    assert body["max_tokens"] == 1024
    assert body["loom"]["dialect"] == "family_evolver"
    assert body["loom"]["team_id"] == "00000000-0000-0000-0000-000000000001"
    assert body["loom"]["step_id"] == "family_evolver"
    assert captured["headers"]["authorization"] == "Bearer stub-token"


@pytest.mark.asyncio
async def test_orchestrator_gateway_client_omits_auth_when_token_empty() -> None:
    """Trusted-network deployments may not set a token — the client
    must skip the Authorization header rather than sending an empty
    Bearer that a strict gateway would 401."""
    captured: dict[str, Any] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        return httpx.Response(
            200, json={"choices": [{"message": {"content": ""}}]},
        )

    transport = httpx.MockTransport(_handler)
    client = OrchestratorGatewayClient(
        base_url="http://gateway.local",
        team_id="t",
        token="",
        _client=httpx.AsyncClient(transport=transport, base_url="http://gateway.local"),
    )
    await client.chat_completion(
        model="x",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=1,
        timeout_sec=1.0,
    )
    assert "authorization" not in {k.lower() for k in captured["headers"]}


@pytest.mark.asyncio
async def test_orchestrator_gateway_client_forwards_provider_connection_id() -> None:
    """#672 blocker #695: when the adapter spec carries a BYO
    provider_connection_id, the client MUST forward it both as the
    ``loom.provider_connection_id`` body field (the current gateway
    ``/v1/chat/completions`` shape) AND as the
    ``x-loom-provider-connection-id`` header (forward-compat with the
    facade auth path).
    """
    captured: dict[str, Any] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["body"] = request.read().decode("utf-8")
        return httpx.Response(
            200, json={"choices": [{"message": {"content": "OK"}}]},
        )

    transport = httpx.MockTransport(_handler)
    client = OrchestratorGatewayClient(
        base_url="http://gateway.local",
        team_id="00000000-0000-0000-0000-000000000001",
        token="stub-token",
        _client=httpx.AsyncClient(transport=transport, base_url="http://gateway.local"),
    )
    await client.chat_completion(
        model="anthropic/claude-haiku-4-5",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=64,
        timeout_sec=5.0,
        provider_connection_id="78964dda-638b-4ca1-ae19-6355d35e826c",
    )

    import json as _json

    body = _json.loads(captured["body"])
    assert body["loom"]["provider_connection_id"] == "78964dda-638b-4ca1-ae19-6355d35e826c"
    assert (
        captured["headers"]["x-loom-provider-connection-id"]
        == "78964dda-638b-4ca1-ae19-6355d35e826c"
    )


@pytest.mark.asyncio
async def test_orchestrator_gateway_client_omits_provider_connection_id_when_unset() -> None:
    """Default path (no BYO connection): no header + no body field, so
    the gateway falls back to the legacy platform-credentialed route."""
    captured: dict[str, Any] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["body"] = request.read().decode("utf-8")
        return httpx.Response(
            200, json={"choices": [{"message": {"content": "OK"}}]},
        )

    transport = httpx.MockTransport(_handler)
    client = OrchestratorGatewayClient(
        base_url="http://gateway.local",
        team_id="t",
        token="",
        _client=httpx.AsyncClient(transport=transport, base_url="http://gateway.local"),
    )
    await client.chat_completion(
        model="x",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=1,
        timeout_sec=1.0,
    )

    import json as _json

    body = _json.loads(captured["body"])
    assert "provider_connection_id" not in body["loom"]
    assert "x-loom-provider-connection-id" not in {k.lower() for k in captured["headers"]}


@pytest.mark.asyncio
async def test_orchestrator_gateway_client_raises_on_non_2xx() -> None:
    """A gateway 4xx/5xx must propagate as httpx.HTTPStatusError so
    the skill_patcher_llm adapter's failure_policy sees a plain
    exception it can classify."""

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    transport = httpx.MockTransport(_handler)
    client = OrchestratorGatewayClient(
        base_url="http://gateway.local",
        team_id="t",
        token="",
        _client=httpx.AsyncClient(transport=transport, base_url="http://gateway.local"),
    )
    with pytest.raises(httpx.HTTPStatusError):
        await client.chat_completion(
            model="x",
            messages=[{"role": "user", "content": "hi"}],
            dialect="family_evolver",
            max_tokens=1,
            timeout_sec=1.0,
        )
