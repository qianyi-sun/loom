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

# #317 Phase 1: install_script run inside the trial sandbox before
# the claude-code CLI is invoked. Version pinned to match
# deploy/agent-sandbox/npm-packages.txt so smoke + production agree.
# Multi-distro: detect apk vs apt-get for installing node + npm + procps
# (claude-code's node-tree-kill dep shells out to ps/pgrep).
_CLAUDE_CODE_PKG = "@anthropic-ai/claude-code"
_CLAUDE_CODE_VERSION = "2.1.183"
_CLAUDE_CODE_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache curl bash nodejs npm procps
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends curl ca-certificates procps gnupg
  # Pin to Node 22 LTS via NodeSource (matches deploy/agent-sandbox).
  mkdir -p /etc/apt/keyrings
  curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \\
    | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
  echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \\
    > /etc/apt/sources.list.d/nodesource.list
  apt-get update
  apt-get install -y --no-install-recommends nodejs
else
  echo "no supported package manager (apk/apt-get); cannot install node" >&2
  exit 1
fi
npm install -g "{_CLAUDE_CODE_PKG}@{_CLAUDE_CODE_VERSION}"
claude --version
"""


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
    install_script: str | None = _CLAUDE_CODE_INSTALL_SCRIPT

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
            "sh",
            "-c",
            (
                f"cd {shlex.quote(str(workdir))} && "
                f"claude --verbose --output-format stream-json "
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
