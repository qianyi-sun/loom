"""LiteLLMAgent — generic tool-loop using the LLM Gateway client.

v1 behaviour: send the instruction as a single user message; loop until the
gateway returns `finish_reason='stop'` (or max_turns hit). Each response is
emitted as an `llm_call` event. Tool dispatch (parsing tool_calls and
running them inside env.exec) is a v1.5 concern — Plan 4 + the agent
follow-ups extend this loop.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Literal
from uuid import UUID

from loom.agent.gateway_client import (
    GatewayCallRequest,
    GatewayCallResponse,
    LLMGatewayClient,
)
from loom.driver.base import Driver
from loom.errors import AgentError
from loom.models.mcp import MCPConnection
from loom.models.trajectory import ChatMessage, LLMCallEvent
from loom.models.types import OS, ModelSpec
from loom.trajectory.writer import TrajectoryWriter


@dataclass
class LiteLLMAgent:
    """Out-of-box tool-loop agent. All LLM calls go through the Gateway client."""

    model: ModelSpec
    gateway: LLMGatewayClient
    team_id: str
    trial_id: UUID
    system_prompt: str = "You are a helpful agent. Complete the task."
    max_turns: int = 8
    mode: Literal["out-of-box", "in-box"] = "out-of-box"
    name: str = "litellm-agent"
    version: str = "1.0"
    supports_os: frozenset[OS] = field(default_factory=lambda: frozenset({"linux"}))
    # #178: BYO provider connection. When set, forwarded to the gateway
    # client on every chat request so the call routes via the team's
    # stored credential + base_url rather than the platform default.
    provider_connection_id: str | None = None

    async def run(
        self,
        *,
        instruction: str,
        env: Driver,
        trajectory: TrajectoryWriter,
        mcp: Sequence[MCPConnection],
        skills_dir: PurePosixPath | None,
        step_id: str,
    ) -> None:
        messages: list[ChatMessage] = [
            ChatMessage(role="user", content=instruction),
        ]
        seq = 0

        for _turn in range(self.max_turns):
            request = GatewayCallRequest(
                model=self.model,
                messages=list(messages),
                system_prompt=self.system_prompt,
                tools=None,
                tool_choice=None,
                team_id=self.team_id,
                trial_id=str(self.trial_id),
                step_id=step_id,
                provider_connection_id=self.provider_connection_id,
            )
            response: GatewayCallResponse = await self.gateway.call(request)

            await trajectory.append(LLMCallEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self.trial_id,
                step_id=step_id,
                seq=seq,
                model=self.model,
                rate_card_hash=response.rate_card_hash,
                system_prompt=self.system_prompt,
                messages=list(messages),
                tools=None,
                tool_choice=None,
                response=response.response,
                finish_reason=response.finish_reason,
                input_tokens=response.input_tokens,
                cached_input_tokens=response.cached_input_tokens,
                cache_write_tokens=response.cache_write_tokens,
                output_tokens=response.output_tokens,
                thinking_tokens=response.thinking_tokens,
                provider_extras=response.provider_extras,
                cost_usd_snapshot=response.cost_usd,
                duration_sec=response.duration_sec,
                streamed=response.streamed,
                time_to_first_token_sec=response.time_to_first_token_sec,
                gateway_request_id=response.gateway_request_id,
                cache_keys=list(request.cache_keys),
            ))
            seq += 1

            messages.append(response.response)
            if response.finish_reason == "stop":
                return

        raise AgentError(
            f"LiteLLMAgent exhausted max_turns={self.max_turns} without 'stop'",
        )
