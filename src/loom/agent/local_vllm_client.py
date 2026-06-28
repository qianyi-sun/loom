"""LLMGatewayClient backed by a worker-spawned vLLM subprocess.

Used when `ModelSpec.source="hf"` + `hf_execution="local-vllm"`. The
vLLM runs on the same host as the worker; this client POSTs directly
to its `/v1/chat/completions` endpoint, bypassing the gateway.

Cost is reported as 0 — operator pays for the GPU, not per-token. A
follow-up could wire per-server rate cards if/when that becomes
useful for budget tracking on local fleets.
"""

from __future__ import annotations

import time
import uuid as uuid_lib
from dataclasses import dataclass
from typing import Any

import httpx

from loom.agent.gateway_client import (
    GatewayCallRequest,
    GatewayCallResponse,
)
from loom.models.trajectory import ChatMessage
from loom.request_params import sanitize_request_extras


@dataclass
class LocalVLLMGatewayClient:
    """OpenAI-compatible client talking to a worker-spawned vLLM.

    Constructed per-trial by the trial runner when the trial's model
    selects `source=hf, hf_execution=local-vllm`. The `base_url` comes
    from `WorkerVLLMRegistry.get_or_launch(model.name)`.
    """

    base_url: str  # e.g. "http://127.0.0.1:8234/v1"
    timeout_sec: float = 600.0  # vLLM first-token latency can be high
    _client: httpx.AsyncClient | None = None

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        body: dict[str, Any] = {
            "model": request.model.name,
            "messages": [m.model_dump() for m in request.messages],
        }
        if request.system_prompt is not None:
            body["messages"] = (
                [{"role": "system", "content": request.system_prompt}]
                + body["messages"]
            )
        if request.tools is not None:
            body["tools"] = [t.model_dump() for t in request.tools]
        if request.tool_choice is not None:
            body["tool_choice"] = request.tool_choice
        body.update(sanitize_request_extras(request.request_params))

        client = self._client or httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout_sec,
        )
        try:
            started = time.monotonic()
            r = await client.post("/chat/completions", json=body)
            r.raise_for_status()
            payload = r.json()
            duration_sec = time.monotonic() - started
        finally:
            if self._client is None:
                await client.aclose()

        choice = payload["choices"][0]
        message = choice["message"]
        finish_reason = choice.get("finish_reason", "stop") or "stop"
        usage = payload.get("usage", {})

        return GatewayCallResponse(
            response=ChatMessage(**message),
            input_tokens=int(usage.get("prompt_tokens", 0)),
            cached_input_tokens=0,
            cache_write_tokens=0,
            output_tokens=int(usage.get("completion_tokens", 0)),
            thinking_tokens=0,
            provider_extras={},
            cost_usd=0.0,
            finish_reason=finish_reason,
            duration_sec=duration_sec,
            streamed=False,
            time_to_first_token_sec=None,
            rate_card_hash="local-vllm-no-card",
            gateway_request_id=str(uuid_lib.uuid4()),
        )
