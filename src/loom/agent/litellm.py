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
    # Trial finalization must not project gateway llm_calls rows back into the
    # trajectory for this agent: the agent already writes rich LLMCallEvent
    # records from each GatewayCallResponse.
    emits_gateway_llm_call_events: bool = True
    # #178: BYO provider connection. When set, forwarded to the gateway
    # client on every chat request so the call routes via the team's
    # stored credential + base_url rather than the platform default.
    provider_connection_id: str | None = None
    # #184: paths (relative to sandbox /workspace) where the agent's
    # final LLM response should be written so file-artifact benchmarks'
    # verifiers (pytest etc.) can grade it. Set by the runner from
    # task_config.steps[*].artifacts when LiteLLMAgent is used. If
    # empty, no artifact write happens (chat-only tasks).
    artifact_paths: list[str] = field(default_factory=list)
    workdir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )

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

            await trajectory.append(
                LLMCallEvent(
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
                )
            )
            seq += 1

            messages.append(response.response)
            if response.finish_reason == "stop":
                raw_content = response.response.content
                content_str = (
                    raw_content
                    if isinstance(raw_content, str)
                    else ""
                    if raw_content is None
                    else "".join(
                        str(p.get("text", "")) if isinstance(p, dict) else str(p)
                        for p in raw_content
                    )
                )
                await self._write_artifacts(env, content_str)
                return

        raise AgentError(
            f"LiteLLMAgent exhausted max_turns={self.max_turns} without 'stop'",
        )

    async def _write_artifacts(self, env: Driver, content: str) -> None:
        """#184: write the LLM's final response into the declared
        artifact paths so file-artifact benchmarks (pytest etc.) can
        grade it. Extracts the first fenced code block when present;
        falls back to raw content. Each path is written individually
        to `/workspace/{path}` via the driver's upload.
        """
        if not self.artifact_paths:
            return
        body = _extract_first_code_block(content)
        import tempfile
        from pathlib import Path

        for rel_path in self.artifact_paths:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".out",
                delete=False,
                encoding="utf-8",
            ) as tf:
                tf.write(body)
                tmp = Path(tf.name)
            try:
                dst = self.workdir / rel_path
                await env.upload(tmp, dst)
            finally:
                tmp.unlink(missing_ok=True)


def _extract_first_code_block(text: str) -> str:
    """Return the first ```lang...``` fenced block's content, or the
    full text if no fence is present. Trims surrounding whitespace.

    The model is prompted to wrap solutions in fences; for safety
    against models that just return raw code, fall through to the
    full text.
    """
    import re

    m = re.search(r"```[a-zA-Z0-9_-]*\n(.*?)```", text, re.DOTALL)
    if m is None:
        return text.strip()
    return m.group(1).rstrip()
