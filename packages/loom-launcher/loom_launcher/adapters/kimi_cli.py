"""KimiCliAdapter — kimi TUI CLI; PTY scrape capture."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast as _cast
from uuid import UUID

from loom_launcher.adapter import AgentAdapter as _AgentAdapter
from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import tail_pty
from loom_launcher.registry import register_adapter

# #317 Phase 2: install kimi CLI. Version pinned to
# deploy/agent-sandbox/npm-packages.txt.
_KIMI_PKG = "@moonshot-ai/kimi-code"
_KIMI_VERSION = "0.18.0"
_KIMI_INSTALL_SCRIPT = f"""\
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
npm install -g "{_KIMI_PKG}@{_KIMI_VERSION}"
kimi --version
"""


@dataclass(frozen=True)
class KimiCliAdapter:
    name: str = "kimi-cli"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "openai/{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()
    install_script: str | None = _KIMI_INSTALL_SCRIPT

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        # kimi-code can synthesize an in-memory provider/model from env vars.
        # Use that path so Loom can route the CLI through the per-step Gateway
        # without writing persistent config.toml inside the sandbox.
        env["KIMI_MODEL_NAME"] = self.model_name_template.format(model_id=model.name)
        env["KIMI_MODEL_API_KEY"] = env[self.api_key_env]
        env["KIMI_MODEL_PROVIDER_TYPE"] = "openai"
        env["KIMI_MODEL_BASE_URL"] = env[self.base_url_env]
        env.setdefault("KIMI_MODEL_MAX_CONTEXT_SIZE", "262144")
        return [
            "kimi",
            "--prompt",
            instruction,
            "--output-format",
            "stream-json",
        ]

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in tail_pty(exec_handle):
            yield event


register_adapter(_cast(_AgentAdapter, KimiCliAdapter()))
