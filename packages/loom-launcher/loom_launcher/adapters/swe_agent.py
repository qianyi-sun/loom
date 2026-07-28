"""SweAgentAdapter — SWE-agent run_single.py; trajectory.jsonl tail."""

from __future__ import annotations

import shlex
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import tail_log_file
from loom_launcher.registry import register_adapter

# #317 Phase 2: SWE-agent ships as a git-installable editable Python
# package (matches deploy/agent-sandbox/python-requirements.txt). Tag
# pinning is the version-pin equivalent for git+https installs; CI
# lint accepts the `@<tag>` portion via _looks_pinned_pip.
_SWE_AGENT_TAG = "v1.1.0"
_SWE_AGENT_VENV = "/opt/loom-agents/swe-agent"
_SWE_AGENT_PYTHON = f"{_SWE_AGENT_VENV}/bin/python"
_SWE_AGENT_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 py3-pip py3-virtualenv git
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends python3 python3-pip python3-venv git
else
  echo "no supported package manager (apk/apt-get); cannot install swe-agent" >&2
  exit 1
fi
python3 -m venv {_SWE_AGENT_VENV}
{_SWE_AGENT_PYTHON} -m pip install --no-cache-dir \\
  -e git+https://github.com/SWE-agent/SWE-agent@{_SWE_AGENT_TAG}#egg=sweagent
{_SWE_AGENT_PYTHON} -c "import sweagent"
"""


@dataclass(frozen=True)
class SweAgentAdapter:
    name: str = "swe-agent"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_API_BASE"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _SWE_AGENT_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        env["OPENAI_API_BASE"] = env[self.base_url_env]
        model_name = self.model_name_template.format(model_id=model.name)
        script = (
            "set -eu; "
            "repo=$1; shift; "
            'repo_name=$(basename "$repo"); '
            'if [ ! -d "$repo/.git" ]; then '
            'git -C "$repo" init -b main >/dev/null; '
            'git -C "$repo" config user.email loom@example.invalid; '
            'git -C "$repo" config user.name Loom; '
            "fi; "
            'git -C "$repo" add -A; '
            'git -C "$repo" diff --cached --quiet || '
            'git -C "$repo" commit -m loom-baseline >/dev/null; '
            'if git -C "$repo" remote get-url origin >/dev/null 2>&1; then '
            'git -C "$repo" remote set-url origin "$repo"; '
            'else git -C "$repo" remote add origin "$repo"; fi; '
            f"exec {_SWE_AGENT_PYTHON} -m sweagent.run.run_single "
            f"--agent.model.name {shlex.quote(model_name)} "
            "--agent.model.per_instance_cost_limit 0 "
            "--agent.model.total_cost_limit 0 "
            "--agent.model.per_instance_call_limit 2 "
            "--env.deployment.type local "
            "--env.repo.type preexisting "
            '--env.repo.repo_name "$repo_name" '
            '--problem_statement.text "$1"'
        )
        return [
            "sh",
            "-c",
            script,
            "loom-swe-agent",
            str(workdir),
            instruction,
        ]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        path = PurePosixPath("trajectory.jsonl")
        async for event in tail_log_file(exec_handle, path=path):
            yield event


register_adapter(_cast(_AgentAdapter, SweAgentAdapter()))
