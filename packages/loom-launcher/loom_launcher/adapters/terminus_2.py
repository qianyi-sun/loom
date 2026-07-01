"""Terminus2Adapter — upstream Terminal-Bench-2 Terminus agent (#248).

Reuses upstream `terminal_bench.agents.terminus.Terminus` verbatim
(loop, prompt template, structured-output schema) so Loom's Layer 3
TB-2 numbers can be compared apples-to-apples against Anthropic /
Laude-Institute published references. A tiny `LocalContainer` shim in
`loom_launcher.terminus_2_runner` lets us run Terminus from INSIDE the
sandbox instead of upstream's docker-py-driven OUTSIDE-the-sandbox
flow.

Dialect: OpenAI-chat. The Loom gateway's openai-compatible facade
proxies to whatever provider the team's connection is configured with
(yibuapi-anthropic, claude-direct, etc.), so this adapter is permissive
about `supported_providers`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.adapters._terminus_2_runtime import (
    TERMINUS_2_INSTALL_SCRIPT,
    TERMINUS_2_PYTHON,
)
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter


@dataclass(frozen=True)
class Terminus2Adapter:
    name: str = "terminus-2"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "anthropic"
    api_key_env: str = "ANTHROPIC_API_KEY"
    base_url_env: str = "ANTHROPIC_BASE_URL"
    # LiteLLM dispatches `openai/<id>` via its openai-compatible client
    # to whatever `OPENAI_BASE_URL` points at — i.e. the Loom gateway.
    model_name_template: str = "anthropic/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = TERMINUS_2_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return [
            TERMINUS_2_PYTHON,
            "-m",
            "loom_launcher.terminus_2_runner",
            "--model",
            self.model_name_template.format(model_id=model.name),
            "--workdir",
            str(workdir),
            "--task",
            instruction,
        ]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in stream_stdout_jsonl(exec_handle):
            yield event


register_adapter(_cast(_AgentAdapter, Terminus2Adapter()))
