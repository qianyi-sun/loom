"""OpenHandsAdapter — legacy OpenHands name backed by SDK one-shot runner."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.adapters._openhands_runtime import (
    OPENHANDS_SDK_INSTALL_SCRIPT,
    OPENHANDS_SDK_PYTHON,
)
from loom_launcher.openhands_sdk_prompt import terminus_style_argv_suffix
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter

# The "openhands" compatibility slug invokes
# `python -m loom_launcher.openhands_sdk_runner`, which uses `openhands.sdk`
# from the `openhands-sdk` package. The install script creates
# a dedicated Python 3.12 venv because openhands-sdk 1.27.0 does not resolve
# against Python 3.11 task images.


@dataclass(frozen=True)
class OpenHandsAdapter:
    name: str = "openhands"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "LLM_API_KEY"
    base_url_env: str = "LLM_BASE_URL"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = OPENHANDS_SDK_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return [
            OPENHANDS_SDK_PYTHON,
            "-m",
            "loom_launcher.openhands_sdk_runner",
            "--model",
            self.model_name_template.format(model_id=model.name),
            "--workdir",
            str(workdir),
            "--output",
            "jsonl",
            "--task",
            instruction,
            *terminus_style_argv_suffix(env),
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


register_adapter(_cast(_AgentAdapter, OpenHandsAdapter()))
