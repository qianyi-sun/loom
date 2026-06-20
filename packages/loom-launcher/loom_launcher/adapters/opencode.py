"""OpencodeAdapter — opencode CLI with JSONL stdout streaming."""

from __future__ import annotations

import json
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
class OpencodeAdapter:
    name: str = "opencode"
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
        env["HOME"] = "/tmp/loom-opencode-home"
        model_name = self.model_name_template.format(model_id=model.name)
        provider_id, _, model_id = model_name.partition("/")
        config = {
            "provider": {
                provider_id: {
                    "options": {
                        "baseURL": env[self.base_url_env],
                        "apiKey": env[self.api_key_env],
                    },
                    "models": {model_id: {"name": model_id}},
                }
            }
        }
        config_json = json.dumps(config, separators=(",", ":"))
        script = (
            'mkdir -p "$HOME/.config/opencode" && '
            f"printf '%s\\n' {shlex.quote(config_json)} > "
            '"$HOME/.config/opencode/opencode.json" && '
            f"exec opencode run --model {shlex.quote(model_name)} "
            "--format json --print-logs --log-level ERROR "
            f'--dir {shlex.quote(str(workdir))} "$1"'
        )
        return [
            "sh",
            "-c",
            script,
            "loom-opencode",
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


register_adapter(_cast(_AgentAdapter, OpencodeAdapter()))
