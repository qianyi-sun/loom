"""ClaudeCodeAdapter — Anthropic claude-code CLI in --print mode.

Per Amendment A12.1: instruction is passed via ``--print <text>``; there
is no ``--workdir`` flag, so we wrap in ``sh -c "cd <workdir> && claude ..."``.
Auto-update + telemetry are disabled via env vars, NOT CLI flags
(``CLAUDE_CODE_AUTO_UPDATE=false`` and ``DISABLE_TELEMETRY=1``).
"""

from __future__ import annotations

import shlex
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter


@dataclass(frozen=True)
class ClaudeCodeAdapter:
    name: str = "claude-code"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "anthropic"
    api_key_env: str = "ANTHROPIC_API_KEY"
    base_url_env: str = "ANTHROPIC_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = True
    additional_egress: frozenset[str] = frozenset()

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        env["DISABLE_TELEMETRY"] = "1"
        env["CLAUDE_CODE_AUTO_UPDATE"] = "false"
        return [
            "sh", "-c",
            (
                f"cd {shlex.quote(str(workdir))} && "
                f"claude --output-format stream-json "
                f"--model {shlex.quote(model.name)} "
                f"--print {shlex.quote(instruction)}"
            ),
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


register_adapter(_cast(_AgentAdapter, ClaudeCodeAdapter()))
