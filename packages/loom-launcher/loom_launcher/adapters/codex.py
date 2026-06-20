"""CodexAdapter — OpenAI Codex CLI with JSONL stdout capture."""

from __future__ import annotations

import json
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
class CodexAdapter:
    name: str = "codex"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_responses"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "{model_id}"
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
        env["CODEX_HOME"] = "/tmp/loom-codex-home"
        model_name = self.model_name_template.format(model_id=model.name)
        provider_config = (
            'model_providers.loom={ name = "Loom", '
            f"base_url = {json.dumps(env[self.base_url_env])}, "
            f'env_key = "{self.api_key_env}", wire_api = "responses" }}'
        )
        script = (
            'mkdir -p "$CODEX_HOME" && '
            "exec codex exec --ignore-user-config --json "
            '--model "$1" --cd "$2" --skip-git-repo-check '
            "--sandbox danger-full-access --ignore-rules "
            '-c \'model_provider="loom"\' -c "$3" "$4" </dev/null'
        )
        return [
            "sh",
            "-c",
            script,
            "loom-codex",
            model_name,
            str(workdir),
            provider_config,
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


register_adapter(_cast(_AgentAdapter, CodexAdapter()))
