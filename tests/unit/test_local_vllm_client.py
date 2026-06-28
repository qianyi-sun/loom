from __future__ import annotations

import json
from typing import Any

import httpx

from loom.agent.gateway_client import GatewayCallRequest
from loom.agent.local_vllm_client import LocalVLLMGatewayClient
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec


async def test_local_vllm_client_forwards_sanitized_request_params() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "choices": [{
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://vllm/v1",
    ) as client:
        gateway = LocalVLLMGatewayClient(
            base_url="http://vllm/v1",
            _client=client,
        )
        await gateway.call(GatewayCallRequest(
            model=ModelSpec(provider="huggingface", name="repo/model"),
            messages=[ChatMessage(role="user", content="hi")],
            system_prompt=None,
            tools=None,
            tool_choice=None,
            team_id="team",
            trial_id="trial",
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
    assert captured["body"]["messages"][0]["role"] == "user"
    assert captured["body"]["messages"][0]["content"] == "hi"
    assert "secret" not in json.dumps(captured["body"]["messages"])
