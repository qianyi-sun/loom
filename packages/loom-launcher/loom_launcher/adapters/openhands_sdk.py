"""OpenHandsSdkAdapter — openhands-sdk one-shot CLI; JSONL stdout."""

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

# #317 Phase 2: install openhands-sdk into system Python (matches
# deploy/agent-sandbox/python-requirements.txt). Also installs
# loom-launcher because the adapter invokes
# `python -m loom_launcher.openhands_sdk_runner`.
_OPENHANDS_SDK_VERSION = "1.27.0"
_LOOM_LAUNCHER_VERSION = "0.1.0"
_OPENHANDS_SDK_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 py3-pip git
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends python3 python3-pip git
else
  echo "no supported package manager (apk/apt-get); cannot install openhands-sdk" >&2
  exit 1
fi
pip install --no-cache-dir --break-system-packages \\
  "openhands-sdk=={_OPENHANDS_SDK_VERSION}" \\
  "loom-launcher=={_LOOM_LAUNCHER_VERSION}"
python -c "import openhands.sdk"
"""


@dataclass(frozen=True)
class OpenHandsSdkAdapter:
    name: str = "openhands-sdk"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "LLM_API_KEY"
    base_url_env: str = "LLM_BASE_URL"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _OPENHANDS_SDK_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        return [
            "python",
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


register_adapter(_cast(_AgentAdapter, OpenHandsSdkAdapter()))
