"""KimiCliAdapter — kimi TUI CLI; PTY scrape capture."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import tail_pty
from loom_launcher.registry import register_adapter


@dataclass(frozen=True)
class KimiCliAdapter:
    name: str = "kimi-cli"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return ["kimi", instruction]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in tail_pty(exec_handle):
            yield event


register_adapter(_cast(_AgentAdapter, KimiCliAdapter()))
