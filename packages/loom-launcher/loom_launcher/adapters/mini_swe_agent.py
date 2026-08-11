"""MiniSweAgentAdapter — mini-swe-agent CLI with JSONL stdout."""

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

# Install mini-swe-agent into its own venv (matches
# deploy/agent-sandbox/python-cli-requirements.txt). Pinned version.
_MINI_SWE_AGENT_VERSION = "2.4.2"
_MINI_SWE_AGENT_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 py3-pip py3-virtualenv build-base
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends python3 python3-pip python3-venv build-essential
else
  echo "no supported package manager (apk/apt-get); cannot install mini-swe-agent" >&2
  exit 1
fi
python3 -m venv /opt/loom-agents/mini-swe-agent
/opt/loom-agents/mini-swe-agent/bin/pip install --no-cache-dir mini-swe-agent=={_MINI_SWE_AGENT_VERSION}
ln -sf /opt/loom-agents/mini-swe-agent/bin/mini-swe-agent /usr/local/bin/mini-swe-agent
# mini-swe-agent v2.4.2 dropped `--version`; smoke-check via Python import.
/opt/loom-agents/mini-swe-agent/bin/python -c "import minisweagent"
"""


@dataclass(frozen=True)
class MiniSweAgentAdapter:
    name: str = "mini-swe-agent"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _MINI_SWE_AGENT_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        # mini-swe-agent v2 launches a first-run configuration wizard when
        # MSWEA_CONFIGURED is absent. Service-mode runs are non-interactive, so
        # provide the same values through the step environment instead.
        env["MSWEA_CONFIGURED"] = "true"
        env["MSWEA_SILENT_STARTUP"] = "true"
        env["MSWEA_COST_TRACKING"] = "ignore_errors"
        env["OPENAI_API_BASE"] = env[self.base_url_env]
        return [
            "mini-swe-agent",
            "--model",
            self.model_name_template.format(model_id=model.name),
            "--agent-class",
            "default",
            "--yolo",
            "--cost-limit",
            "0",
            "--exit-immediately",
            "--output",
            str(workdir / "mini-swe-agent-trajectory.jsonl"),
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


register_adapter(_cast(_AgentAdapter, MiniSweAgentAdapter()))
