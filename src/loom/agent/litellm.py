"""LiteLLMAgent — generic tool-loop using the LLM Gateway client.

v1 behaviour: send the instruction as a single user message; loop until the
gateway returns `finish_reason='stop'` (or max_turns hit). Each response is
emitted as an `llm_call` event. Tool dispatch (parsing tool_calls and
running them inside env.exec) is a v1.5 concern — Plan 4 + the agent
follow-ups extend this loop.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any, Literal
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
from loom.request_params import sanitize_request_extras
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
    request_params: dict[str, Any] = field(default_factory=dict)
    # #184: paths (relative to sandbox /workspace) where the agent's
    # final LLM response should be written so file-artifact benchmarks'
    # verifiers (pytest etc.) can grade it. Set by the runner from
    # task_config.steps[*].artifacts when LiteLLMAgent is used. If
    # empty, no artifact write happens (chat-only tasks). final_answer.txt
    # keeps verifier-facing answer text with helper code blocks removed;
    # strict answer.txt artifacts receive a single answer value. Code/data
    # artifacts still prefer fenced block extraction.
    artifact_paths: list[str] = field(default_factory=list)
    workdir: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )

    def __post_init__(self) -> None:
        self.request_params = sanitize_request_extras(self.request_params)

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
                request_params=self.request_params,
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
        grade it. Rendering is artifact-aware: final-answer artifacts keep
        answer text with helper code stripped, strict answer artifacts
        receive a normalized value, and code/data artifacts keep the
        historical fenced-block extraction. Each path is written
        individually to `/workspace/{path}` via the driver's upload.
        """
        if not self.artifact_paths:
            return
        import tempfile
        from pathlib import Path

        for rel_path in self.artifact_paths:
            body = _render_artifact_body(content, rel_path)
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


_FINAL_ANSWER_ARTIFACT_FILENAMES = frozenset({"final_answer.txt"})
_STRICT_ANSWER_ARTIFACT_FILENAMES = frozenset({"answer.txt"})


def _render_artifact_body(content: str, rel_path: str) -> str:
    """Render a model response for the declared artifact path.

    Most file-artifact tasks expect code or structured data and benefit
    from extracting a fenced block. final_answer.txt preserves answer prose
    for benchmark-owned extraction; strict answer.txt mirrors Harbor-style
    exact-match files by keeping only the answer value.
    """
    name = PurePosixPath(rel_path).name.lower()
    if name in _FINAL_ANSWER_ARTIFACT_FILENAMES:
        return _extract_final_answer_document(content)
    if name in _STRICT_ANSWER_ARTIFACT_FILENAMES:
        return _extract_final_answer_value(content)
    return _extract_first_code_block(content)


def _extract_first_code_block(text: str) -> str:
    """Return the first ```lang...``` fenced block's content, or the
    full text if no fence is present. Trims surrounding whitespace.

    The model is prompted to wrap solutions in fences; for safety
    against models that just return raw code, fall through to the
    full text.
    """
    m = re.search(r"```[a-zA-Z0-9_-]*\n(.*?)```", text, re.DOTALL)
    if m is None:
        return text.strip()
    return m.group(1).rstrip()


def _extract_final_answer_document(text: str) -> str:
    """Return answer prose for verifier-owned extraction.

    Official/reference final-answer evaluators commonly expect different
    surface forms: `Answer: X`, `Exact Answer: ...`, `\\boxed{...}`, or a
    raw exact-match string. Preserve non-code answer text so each benchmark
    verifier can apply its own extraction and normalization rules.
    """
    without_code = _strip_fenced_code_blocks(text).strip()
    if without_code:
        return without_code
    return _extract_final_answer_value(text)


def _extract_final_answer_value(text: str) -> str:
    """Extract a strict final-answer value from a model response.

    Final-answer benchmarks commonly instruct the model to reason in the
    response and put the answer at the end. Prefer non-code prose so helper
    snippets do not become the answer file. When the prose contains a
    boxed answer, return the boxed payload directly; AIME-style verifiers
    then see the single integer they expect.
    """
    without_code = _strip_fenced_code_blocks(text).strip()
    source = without_code or text.strip()
    boxed = _extract_last_boxed_answer(source)
    if boxed is not None:
        return _clean_answer_fragment(boxed)

    lines = [line.strip() for line in source.splitlines() if line.strip()]
    for line in reversed(lines):
        fragment = _answer_marker_fragment(line)
        if fragment:
            boxed = _extract_last_boxed_answer(fragment)
            if boxed is not None:
                return _clean_answer_fragment(boxed)
            return _clean_answer_fragment(fragment)

    if lines:
        return lines[-1]
    return _extract_first_code_block(text)


def _strip_fenced_code_blocks(text: str) -> str:
    return re.sub(r"```[a-zA-Z0-9_-]*\n.*?```", "\n", text, flags=re.DOTALL)


def _extract_last_boxed_answer(text: str) -> str | None:
    starts: list[tuple[int, str]] = []
    for command in (r"\boxed", r"\fbox"):
        start = 0
        while True:
            idx = text.find(command, start)
            if idx == -1:
                break
            starts.append((idx, command))
            start = idx + len(command)
    if not starts:
        return None

    idx, command = max(starts, key=lambda item: item[0])
    pos = idx + len(command)
    while pos < len(text) and text[pos].isspace():
        pos += 1
    if pos >= len(text):
        return None
    if text[pos] != "{":
        end = pos
        while end < len(text) and not text[end].isspace():
            end += 1
        return text[pos:end]

    depth = 1
    pos += 1
    start = pos
    while pos < len(text):
        char = text[pos]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:pos]
        pos += 1
    return None


def _answer_marker_fragment(line: str) -> str | None:
    patterns = (
        r"(?i)\b(?:the\s+)?(?:final\s+answer|exact\s+answer)\s*(?:is|=|:|：)\s*(.+)$",
        r"(?i)\banswer\s*(?:is|=|:|：)\s*(.+)$",
    )
    for pattern in patterns:
        match = re.search(pattern, line)
        if match is not None:
            return match.group(1)
    return None


def _clean_answer_fragment(fragment: str) -> str:
    value = fragment.strip()
    value = value.strip("`'\" \t")
    value = re.sub(r"(?i)^final\s+answer\s*(?:is|=|:|：)?\s*", "", value)
    value = value.strip()
    # Strict answer files should contain the answer, not sentence punctuation.
    while value.endswith((".", "。", ";")):
        value = value[:-1].strip()
    return value
