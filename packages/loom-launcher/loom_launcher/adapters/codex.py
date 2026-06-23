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

# #317 Phase 2: install codex CLI into the trial sandbox before
# invocation. Version pinned to match deploy/agent-sandbox/npm-packages.txt.
_CODEX_PKG = "@openai/codex"
_CODEX_VERSION = "0.141.0"
_CODEX_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache curl bash nodejs npm
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends curl ca-certificates gnupg
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
npm install -g "{_CODEX_PKG}@{_CODEX_VERSION}"
codex --version
"""


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
    install_script: str | None = _CODEX_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        # Codex 0.141+ refuses to create PATH-alias helper binaries
        # under `/tmp` (it logs "Refusing to create helper binaries
        # under temporary dir" and exits rc=1). Place CODEX_HOME under
        # the trial workdir instead — guaranteed-writable, persists
        # for the trial's lifetime, doesn't leak across trials.
        env["CODEX_HOME"] = f"{workdir}/.codex-home"
        model_name = self.model_name_template.format(model_id=model.name)
        # Codex with `wire_api = "responses"` POSTs to
        # `<base_url>/responses`. OpenAI's actual base URL convention
        # is `https://api.openai.com/v1`, so the gateway must also be
        # addressed as `<gateway>/v1` (the loom gateway hosts the
        # responses route at `/v1/responses`, not the root). The
        # worker injects the gateway URL bare, so append `/v1` here.
        base_url = env[self.base_url_env].rstrip("/")
        if not base_url.endswith("/v1"):
            base_url = base_url + "/v1"
        provider_config = (
            'model_providers.loom={ name = "Loom", '
            f"base_url = {json.dumps(base_url)}, "
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
