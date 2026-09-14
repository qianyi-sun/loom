"""HermesAdapter — thin hermes-agent ReAct runner; JSONL stdout."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.adapters._hermes_runtime import (
    HERMES_INSTALL_SCRIPT,
    HERMES_PYTHON,
)
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter


@dataclass(frozen=True)
class HermesAdapter:
    name: str = "hermes"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = HERMES_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return [
            HERMES_PYTHON,
            "-m",
            "loom_launcher.hermes_runner",
            "--model",
            self.model_name_template.format(model_id=model.name),
            "--workdir",
            str(workdir),
            "--output",
            "jsonl",
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


register_adapter(_cast(_AgentAdapter, HermesAdapter()))
