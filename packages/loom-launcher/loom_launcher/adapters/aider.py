"""AiderAdapter — aider CLI; captures the chat-history markdown log."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import tail_log_file
from loom_launcher.registry import register_adapter

# Install aider into its own venv (matches
# deploy/agent-sandbox/python-cli-requirements.txt). Pinned version.
_AIDER_INSTALL_SCRIPT = """\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 py3-pip py3-virtualenv build-base
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends python3 python3-pip python3-venv build-essential
else
  echo "no supported package manager (apk/apt-get); cannot install aider" >&2
  exit 1
fi
python3 -m venv /opt/loom-agents/aider
/opt/loom-agents/aider/bin/pip install --no-cache-dir aider-chat==0.86.2
ln -sf /opt/loom-agents/aider/bin/aider /usr/local/bin/aider
aider --version
"""


@dataclass(frozen=True)
class AiderAdapter:
    name: str = "aider"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_API_BASE"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = True
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _AIDER_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        # Telemetry off: aider checks this env var at startup.
        env["AIDER_NO_TELEMETRY"] = "1"
        return [
            "aider",
            "--yes-always",
            "--no-auto-commits",
            "--model", self.model_name_template.format(model_id=model.name),
            "--message", instruction,
        ]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        # aider writes its chat history to .aider.chat.history.md in the workdir.
        # The worker passes the sandboxed workdir via the handle's sandbox view;
        # we tail a fixed relative path inside the sandbox's CWD.
        path = PurePosixPath(".aider.chat.history.md")
        async for event in tail_log_file(exec_handle, path=path):
            yield event


register_adapter(_cast(_AgentAdapter, AiderAdapter()))
