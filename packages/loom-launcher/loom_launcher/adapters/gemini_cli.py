"""GeminiCliAdapter — gemini CLI with structured JSON output."""

from __future__ import annotations

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
class GeminiCliAdapter:
    name: str = "gemini-cli"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "gemini"
    api_key_env: str = "GOOGLE_API_KEY"
    base_url_env: str = "GOOGLE_GEMINI_BASE_URL"
    model_name_template: str = "google/{model_id}"
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
        return [
            "gemini",
            "--model", self.model_name_template.format(model_id=model.name),
            "--output", "json",
            "--prompt", instruction,
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


register_adapter(_cast(_AgentAdapter, GeminiCliAdapter()))
