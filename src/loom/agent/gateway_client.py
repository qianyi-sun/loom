"""LLMGatewayClient — the boundary between agents and the LLM Gateway service.

In Plan 4 a real client implementation is built that POSTs to the Gateway HTTP
service. For Plan 3 we only need the Protocol + an in-memory fake so agents
can be unit-tested without a running Gateway.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from loom.models.trajectory import ChatMessage, ToolSpec
from loom.models.types import ModelSpec


@dataclass(frozen=True)
class GatewayCallRequest:
    model: ModelSpec
    messages: list[ChatMessage]
    system_prompt: str | None
    tools: list[ToolSpec] | None
    tool_choice: str | dict[str, Any] | None
    team_id: str
    trial_id: str
    step_id: str
    cache_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class GatewayCallResponse:
    response: ChatMessage
    input_tokens: int
    cached_input_tokens: int
    cache_write_tokens: int
    output_tokens: int
    thinking_tokens: int
    provider_extras: dict[str, int]
    cost_usd: float
    finish_reason: str
    duration_sec: float
    streamed: bool
    time_to_first_token_sec: float | None
    rate_card_hash: str
    gateway_request_id: str


class LLMGatewayClient(Protocol):
    """Async client for an LLM Gateway. Plan 4 implements the HTTP backend;
    Plan 3 uses FakeLLMGatewayClient for unit + integration tests."""

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse: ...


@dataclass
class FakeLLMGatewayClient:
    """Returns scripted responses in order. Raises IndexError if asked for
    more calls than were scripted."""

    scripted: list[GatewayCallResponse]
    calls_recorded: list[GatewayCallRequest] = field(default_factory=list)

    async def call(self, request: GatewayCallRequest) -> GatewayCallResponse:
        self.calls_recorded.append(request)
        return self.scripted.pop(0)
