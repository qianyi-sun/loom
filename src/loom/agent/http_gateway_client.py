"""Real HTTP-backed LLMGatewayClient. Workers use this in production."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from loom.agent.gateway_client import (
    GatewayCallRequest,
    GatewayCallResponse,
)
from loom.models.trajectory import ChatMessage


@dataclass
class HttpLLMGatewayClient:
    """Talks HTTP to the Loom LLM Gateway. Constructed once per worker."""

    base_url: str
    token: str
    timeout_sec: float = 120.0
    # Optional pre-built client for tests (so ASGITransport / mocks can be injected).
    _client: httpx.AsyncClient | None = None

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        body: dict[str, Any] = {
            "model": request.model.to_gateway_model_string(),
            "messages": [m.model_dump() for m in request.messages],
            "loom": {
                "team_id": request.team_id,
                "trial_id": request.trial_id,
                "step_id": request.step_id,
            },
        }
        if request.system_prompt is not None:
            # OpenAI wire format: system prompt is the first chat message.
            body["messages"] = (
                [{"role": "system", "content": request.system_prompt}]
                + body["messages"]
            )
        if request.tools is not None:
            body["tools"] = [t.model_dump() for t in request.tools]
        if request.tool_choice is not None:
            body["tool_choice"] = request.tool_choice
        if request.model.tier:
            body["loom"]["tier"] = request.model.tier
        if request.model.region:
            body["loom"]["region"] = request.model.region
        # #178: forward BYO provider connection id so the gateway can
        # decrypt + use the team's stored credential.
        if request.provider_connection_id:
            body["loom"]["provider_connection_id"] = (
                request.provider_connection_id
            )

        client = self._client or httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout_sec,
        )
        try:
            r = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.token}"},
                json=body,
            )
            r.raise_for_status()
            payload = r.json()
        finally:
            if self._client is None:
                await client.aclose()

        loom_block = payload["loom"]
        return GatewayCallResponse(
            response=ChatMessage(**payload["choices"][0]["message"]),
            input_tokens=loom_block["input_tokens"],
            cached_input_tokens=loom_block["cached_input_tokens"],
            cache_write_tokens=loom_block["cache_write_tokens"],
            output_tokens=loom_block["output_tokens"],
            thinking_tokens=loom_block["thinking_tokens"],
            provider_extras=dict(loom_block["provider_extras"]),
            cost_usd=loom_block["cost_usd"],
            finish_reason=loom_block["finish_reason"],
            duration_sec=loom_block["duration_sec"],
            streamed=loom_block["streamed"],
            time_to_first_token_sec=loom_block.get("time_to_first_token_sec"),
            rate_card_hash=loom_block["rate_card_hash"],
            gateway_request_id=loom_block["gateway_request_id"],
        )
