"""Unit coverage for BYO chat dispatch through the egress client pool."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import httpx

from loom_llm_gateway.routes.chat import _forward_openai_compatible_byo_chat


class _CapturingEgressPool:
    def __init__(self) -> None:
        self.connection_ids: list[Any] = []
        self.requests: list[httpx.Request] = []
        self.client = httpx.AsyncClient(
            transport=httpx.MockTransport(self._handle),
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl_test",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 7,
                    "completion_tokens": 3,
                    "total_tokens": 10,
                },
            },
        )

    async def get(self, connection_id: Any) -> httpx.AsyncClient:
        self.connection_ids.append(connection_id)
        return self.client


class _StubRetrySettings:
    """Minimal subset of GatewaySettings the retry helper reads (#298)."""

    llm_retry_max_attempts = 1
    llm_retry_base_backoff_sec = 0.0
    llm_retry_jitter_sec = 0.0
    llm_retry_max_backoff_sec = 0.0
    llm_retry_budget_sec = 30.0


def _unexpected_session_factory() -> Any:
    raise AssertionError("success path must not open an audit session")


async def test_openai_compatible_byo_chat_uses_egress_pool() -> None:
    pool = _CapturingEgressPool()
    connection_id = uuid4()
    team_id = uuid4()
    trial_id = uuid4()

    try:
        body, attempt = await _forward_openai_compatible_byo_chat(
            egress_client_pool=pool,
            session_factory=_unexpected_session_factory,
            connection_id=connection_id,
            base_url="https://byo.example.com/v1/",
            api_key="sk-real-byo-key",
            model_name="some-model",
            messages=[{"role": "user", "content": "hi"}],
            extra_kwargs={"temperature": 0},
            timeout=30.0,
            settings=_StubRetrySettings(),
            audit_team_id=team_id,
            audit_trial_id=trial_id,
            audit_step_id="main",
            audit_dialect="openai_chat",
            provider_label="openai-compatible",
            request_params={
                "status": "available",
                "parameters": {"temperature": 0},
            },
        )
    finally:
        await pool.client.aclose()

    assert body["choices"][0]["message"]["content"] == "ok"
    # First-attempt success on a single 200 response.
    assert attempt == 1
    assert pool.connection_ids == [connection_id]
    assert len(pool.requests) == 1
    upstream = pool.requests[0]
    assert str(upstream.url) == "https://byo.example.com/v1/chat/completions"
    assert upstream.headers["Authorization"] == "Bearer sk-real-byo-key"
    assert json.loads(upstream.content) == {
        "model": "some-model",
        "messages": [{"role": "user", "content": "hi"}],
        "temperature": 0,
    }
