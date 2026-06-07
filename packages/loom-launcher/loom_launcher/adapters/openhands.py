"""OpenHandsAdapter — OpenHands server-mode CLI; events scraped over HTTP."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import poll_local_http
from loom_launcher.registry import register_adapter

_OPENHANDS_PORT = 9999


@dataclass(frozen=True)
class OpenHandsAdapter:
    name: str = "openhands"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "LLM_API_KEY"
    base_url_env: str = "LLM_BASE_URL"
    model_name_template: str = "openai/{model_id}"
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
            "python", "-m", "openhands.server",
            "--port", str(_OPENHANDS_PORT),
            "--workdir", str(workdir),
            "--task", instruction,
        ]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in poll_local_http(exec_handle, port=_OPENHANDS_PORT):
            yield event


register_adapter(_cast(_AgentAdapter, OpenHandsAdapter()))
