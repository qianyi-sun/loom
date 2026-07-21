"""Unit tests for the family-orchestrator JWT-to-Gateway bridge."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from loom_family_orchestrator.gateway_client import OrchestratorGatewayClient

_TEAM_ID = "00000000-0000-4000-8000-000000000001"
_TRIAL_ID = "00000000-0000-4000-8000-000000000002"
_CONNECTION_ID = "78964dda-638b-4ca1-ae19-6355d35e826c"


def _client(
    gateway_handler: Callable[[httpx.Request], httpx.Response],
    *,
    control_plane_handler: Callable[[httpx.Request], httpx.Response] | None = None,
) -> OrchestratorGatewayClient:
    cp_handler = control_plane_handler or (
        lambda _request: httpx.Response(201, json={"token": "loom_step_test"})
    )
    return OrchestratorGatewayClient(
        base_url="http://gateway.local",
        control_plane_url="http://control-plane.local",
        worker_token="loom_fo_test",
        _client=httpx.AsyncClient(
            transport=httpx.MockTransport(gateway_handler),
            base_url="http://gateway.local",
        ),
        _control_plane_client=httpx.AsyncClient(
            transport=httpx.MockTransport(cp_handler),
            base_url="http://control-plane.local",
        ),
    )


@pytest.mark.asyncio
async def test_posts_real_attribution_with_control_plane_step_jwt() -> None:
    captured: dict[str, Any] = {}

    def cp_handler(request: httpx.Request) -> httpx.Response:
        captured["cp_headers"] = dict(request.headers)
        captured["cp_body"] = json.loads(request.content)
        return httpx.Response(201, json={"token": "loom_step_authoritative"})

    def gateway_handler(request: httpx.Request) -> httpx.Response:
        captured["gateway_headers"] = dict(request.headers)
        captured["gateway_body"] = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": "OK"}}]})

    client = _client(gateway_handler, control_plane_handler=cp_handler)
    response = await client.chat_completion(
        model="anthropic/claude-sonnet-4-6",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=1024,
        timeout_sec=30.0,
        team_id=_TEAM_ID,
        trial_id=_TRIAL_ID,
    )

    assert response["choices"][0]["message"]["content"] == "OK"
    assert captured["cp_headers"]["authorization"] == "Bearer loom_fo_test"
    assert captured["cp_body"] == {
        "team_id": _TEAM_ID,
        "trial_id": _TRIAL_ID,
        "step_id": "family_evolver",
        "ttl_sec": 90,
        "provider_connection_id": None,
    }
    assert captured["gateway_headers"]["authorization"] == (
        "Bearer loom_step_authoritative"
    )
    assert captured["gateway_body"]["loom"] == {
        "team_id": _TEAM_ID,
        "trial_id": _TRIAL_ID,
        "step_id": "family_evolver",
        "dialect": "family_evolver",
    }


@pytest.mark.asyncio
async def test_forwards_configured_provider_to_cp_body_and_header() -> None:
    captured: dict[str, Any] = {}

    def cp_handler(request: httpx.Request) -> httpx.Response:
        captured["cp_body"] = json.loads(request.content)
        return httpx.Response(201, json={"token": "loom_step_provider_bound"})

    def gateway_handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": "OK"}}]})

    await _client(gateway_handler, control_plane_handler=cp_handler).chat_completion(
        model="openai/model",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=64,
        timeout_sec=5.0,
        team_id=_TEAM_ID,
        trial_id=_TRIAL_ID,
        provider_connection_id=_CONNECTION_ID,
    )

    assert captured["cp_body"]["provider_connection_id"] == _CONNECTION_ID
    assert captured["body"]["loom"]["provider_connection_id"] == _CONNECTION_ID
    assert captured["headers"]["x-loom-provider-connection-id"] == _CONNECTION_ID


@pytest.mark.asyncio
async def test_unconfigured_provider_is_explicit_null_at_cp_and_omitted_at_gateway() -> None:
    captured: dict[str, Any] = {}

    def cp_handler(request: httpx.Request) -> httpx.Response:
        captured["cp_body"] = json.loads(request.content)
        return httpx.Response(201, json={"token": "loom_step_platform"})

    def gateway_handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": "OK"}}]})

    await _client(gateway_handler, control_plane_handler=cp_handler).chat_completion(
        model="anthropic/model",
        messages=[{"role": "user", "content": "hi"}],
        dialect="family_evolver",
        max_tokens=1,
        timeout_sec=1.0,
        team_id=_TEAM_ID,
        trial_id=_TRIAL_ID,
    )

    assert captured["cp_body"]["provider_connection_id"] is None
    assert "provider_connection_id" not in captured["body"]["loom"]
    assert "x-loom-provider-connection-id" not in captured["headers"]


@pytest.mark.asyncio
async def test_control_plane_rejection_stops_before_gateway() -> None:
    gateway_called = False

    def cp_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "provider_connection not found"})

    def gateway_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal gateway_called
        gateway_called = True
        return httpx.Response(200, json={})

    with pytest.raises(httpx.HTTPStatusError):
        await _client(
            gateway_handler,
            control_plane_handler=cp_handler,
        ).chat_completion(
            model="openai/model",
            messages=[{"role": "user", "content": "hi"}],
            dialect="family_evolver",
            max_tokens=1,
            timeout_sec=1.0,
            team_id=_TEAM_ID,
            trial_id=_TRIAL_ID,
            provider_connection_id=_CONNECTION_ID,
        )
    assert gateway_called is False


@pytest.mark.asyncio
async def test_gateway_non_2xx_propagates() -> None:
    client = _client(lambda _request: httpx.Response(500, json={"error": "boom"}))
    with pytest.raises(httpx.HTTPStatusError):
        await client.chat_completion(
            model="x",
            messages=[{"role": "user", "content": "hi"}],
            dialect="family_evolver",
            max_tokens=1,
            timeout_sec=1.0,
            team_id=_TEAM_ID,
            trial_id=_TRIAL_ID,
        )
