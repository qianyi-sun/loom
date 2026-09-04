from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import httpx
import pytest

from loom.agent.gateway_client import GatewayCallRequest
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.attempt_deadline import AttemptDeadline, AttemptDeadlineExceededError
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def _request(*, step_id: str = "solve") -> GatewayCallRequest:
    return GatewayCallRequest(
        model=ModelSpec(provider="openai", name="gpt-4"),
        messages=[ChatMessage(role="user", content="hi")],
        system_prompt=None,
        tools=None,
        tool_choice=None,
        team_id=str(uuid4()),
        trial_id=str(uuid4()),
        step_id=step_id,
    )


def _response(request: httpx.Request, *, status_code: int = 200) -> httpx.Response:
    if status_code != 200:
        return httpx.Response(status_code, request=request, json={"detail": "late"})
    return httpx.Response(
        200,
        request=request,
        json={
            "choices": [{"message": {"role": "assistant", "content": "done"}}],
            "loom": {
                "input_tokens": 1,
                "cached_input_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 1,
                "thinking_tokens": 0,
                "provider_extras": {},
                "cost_usd": 0.0,
                "finish_reason": "stop",
                "duration_sec": 0.01,
                "streamed": False,
                "time_to_first_token_sec": None,
                "rate_card_hash": "test",
                "gateway_request_id": "request-1",
            },
        },
    )


async def test_expired_attempt_does_not_dispatch_http_request() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _response(request)

    clock = _Clock(50.0)
    deadline = AttemptDeadline.after(0.0, clock=clock)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        gateway = HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="step-token",
            attempt_deadline=deadline,
            _client=http,
        )
        with pytest.raises(AttemptDeadlineExceededError):
            await gateway.call(_request())

    assert calls == 0


async def test_all_http_phase_timeouts_are_capped_to_attempt_remaining() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.extensions["timeout"])
        return _response(request)

    clock = _Clock(100.0)
    deadline = AttemptDeadline.after(3.25, clock=clock)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        gateway = HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="step-token",
            timeout_sec=120.0,
            attempt_deadline=deadline,
            _client=http,
        )
        await gateway.call(_request())

    assert set(captured) == {"connect", "read", "write", "pool"}
    assert all(timeout == 3.25 for timeout in captured.values())


async def test_total_http_wall_clock_is_capped_to_attempt_remaining() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1.0)
        return _response(request)

    deadline = AttemptDeadline.after(0.02)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        gateway = HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="step-token",
            attempt_deadline=deadline,
            _client=http,
        )
        with pytest.raises(AttemptDeadlineExceededError):
            await gateway.call(_request())


async def test_response_arriving_after_deadline_cannot_replace_timeout() -> None:
    clock = _Clock(100.0)
    deadline = AttemptDeadline.after(5.0, clock=clock)

    def handler(request: httpx.Request) -> httpx.Response:
        clock.now = 106.0
        return _response(request, status_code=401)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as http:
        gateway = HttpLLMGatewayClient(
            base_url="http://gateway.test",
            token="step-token",
            attempt_deadline=deadline,
            _client=http,
        )
        with pytest.raises(AttemptDeadlineExceededError):
            await gateway.call(_request())
