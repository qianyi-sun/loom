"""Shared OpenHands SDK runtime contract for SDK-backed adapters."""

from __future__ import annotations

OPENHANDS_SDK_VERSION = "1.34.0"
LOOM_LAUNCHER_REF = "5f3dd23ee9eb15301c774d1b5b3220bf54807bc2"
LOOM_LAUNCHER_REQUIREMENT = "git+https://github.com/qianyi-sun/loom.git@5f3dd23ee9eb15301c774d1b5b3220bf54807bc2#subdirectory=packages/loom-launcher"
UV_VERSION = "0.11.21"
OPENHANDS_SDK_VENV = "/opt/loom-agents/openhands-sdk"
OPENHANDS_SDK_PYTHON = "/opt/loom-agents/openhands-sdk/bin/python"

OPENHANDS_SDK_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache ca-certificates curl git tmux
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends ca-certificates curl git tmux
else
  echo "no supported package manager (apk/apt-get); cannot install openhands-sdk" >&2
  exit 1
fi
export UV_INSTALL_DIR=/opt/loom-agents/bin
export UV_PYTHON_INSTALL_DIR=/opt/loom-agents/python
export UV_CACHE_DIR=/opt/loom-agents/uv-cache
mkdir -p "$UV_INSTALL_DIR" "$UV_PYTHON_INSTALL_DIR" "$UV_CACHE_DIR"
curl -LsSf "https://astral.sh/uv/{UV_VERSION}/install.sh" | sh
ln -sf "$UV_INSTALL_DIR/uv" /usr/local/bin/uv
uv python install 3.12
uv venv --python 3.12 {OPENHANDS_SDK_VENV}
uv pip install --python {OPENHANDS_SDK_PYTHON} --no-cache-dir \\
  "openhands-sdk=={OPENHANDS_SDK_VERSION}" \\
  "openhands-tools=={OPENHANDS_SDK_VERSION}" \\
  "{LOOM_LAUNCHER_REQUIREMENT}"
{OPENHANDS_SDK_PYTHON} -c "import openhands.sdk; import openhands.tools.terminal; import openhands.tools.file_editor; import openhands.tools.task_tracker"
"""
