"""Shared Hermes agent runtime contract for the launcher adapter.

Thin editable ``uv pip install -e`` of pinned ``hermes-agent`` (core deps
only; hermes rejects non-editable wheel builds). Never runs Nous
``install.sh`` / Node / Playwright. Prefer a baked venv at
``/opt/loom-agents/hermes`` when the agent-sandbox image (or a prior trial
layer) already provisioned it.
"""

from __future__ import annotations

# Locked NousResearch/hermes-agent revision (ReAct + terminal/file waist).
# Plain string constants (not concatenation) so install-script pin lint can
# interpolate them from the AST the same way OpenHands does.
HERMES_AGENT_REF = "c8aa5608c24e3636e77c267650c0f1f52e44adb0"
HERMES_AGENT_REQUIREMENT = (
    "hermes-agent@git+https://github.com/NousResearch/hermes-agent.git@"
    "c8aa5608c24e3636e77c267650c0f1f52e44adb0"
)

# loom-launcher pin: prefer baked source under /opt/loom-src when present
# (agent-sandbox image). Cold path falls back to this git subdirectory ref.
LOOM_LAUNCHER_REF = "021a96469199a730070c386f5d25e65ca9b9362e"
LOOM_LAUNCHER_REQUIREMENT = (
    "git+https://github.com/qianyi-sun/loom.git@"
    "021a96469199a730070c386f5d25e65ca9b9362e"
    "#subdirectory=packages/loom-launcher"
)

UV_VERSION = "0.11.21"
HERMES_VENV = "/opt/loom-agents/hermes"
HERMES_PYTHON = "/opt/loom-agents/hermes/bin/python"
# Optional local/vendored tree (Dockerfile COPY or bind) — skips GitHub.
HERMES_SRC_PATH = "/opt/src/hermes-agent"
LOOM_LAUNCHER_SRC_PATH = "/opt/loom-src/loom-launcher"

HERMES_INSTALL_SCRIPT = f"""\
set -euo pipefail
# Prefer baked venv (agent-sandbox image / trial layer cache).
if [ -x {HERMES_PYTHON} ]; then
  if {HERMES_PYTHON} -c "from run_agent import AIAgent" 2>/dev/null \\
    && {HERMES_PYTHON} -c "import loom_launcher.hermes_runner" 2>/dev/null; then
    exit 0
  fi
fi

if command -v apk >/dev/null 2>&1; then
  apk add --no-cache ca-certificates curl git
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends ca-certificates curl git
else
  echo "no supported package manager (apk/apt-get); cannot install hermes" >&2
  exit 1
fi

export UV_INSTALL_DIR=/opt/loom-agents/bin
export UV_PYTHON_INSTALL_DIR=/opt/loom-agents/python
export UV_CACHE_DIR=/opt/loom-agents/uv-cache
mkdir -p "$UV_INSTALL_DIR" "$UV_PYTHON_INSTALL_DIR" "$UV_CACHE_DIR"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf "https://astral.sh/uv/{UV_VERSION}/install.sh" | sh
fi
ln -sf "$UV_INSTALL_DIR/uv" /usr/local/bin/uv 2>/dev/null || true
uv python install 3.12
uv venv --python 3.12 {HERMES_VENV}

# hermes-agent refuses non-editable wheel/sdist builds; always use -e.
# Local/vendored tree first (avoid GitHub 429), else clone the pinned ref.
if [ -f {HERMES_SRC_PATH}/pyproject.toml ]; then
  uv pip install --python {HERMES_PYTHON} --no-cache-dir -e "{HERMES_SRC_PATH}"
elif [ -n "${{HERMES_AGENT_SRC:-}}" ] && [ -f "${{HERMES_AGENT_SRC}}/pyproject.toml" ]; then
  uv pip install --python {HERMES_PYTHON} --no-cache-dir -e "${{HERMES_AGENT_SRC}}"
else
  mkdir -p /opt/src
  if [ ! -f /opt/src/hermes-agent/pyproject.toml ]; then
    git clone --depth 1 \
      https://github.com/NousResearch/hermes-agent.git /opt/src/hermes-agent
    git -C /opt/src/hermes-agent fetch --depth 1 origin {HERMES_AGENT_REF}
    git -C /opt/src/hermes-agent checkout {HERMES_AGENT_REF}
  fi
  uv pip install --python {HERMES_PYTHON} --no-cache-dir -e /opt/src/hermes-agent
fi

# loom-launcher (owns hermes_runner): baked source first, else pinned git.
if [ -f {LOOM_LAUNCHER_SRC_PATH}/pyproject.toml ]; then
  uv pip install --python {HERMES_PYTHON} --no-cache-dir -e "{LOOM_LAUNCHER_SRC_PATH}"
else
  uv pip install --python {HERMES_PYTHON} --no-cache-dir "{LOOM_LAUNCHER_REQUIREMENT}"
fi

{HERMES_PYTHON} -c "from run_agent import AIAgent; import loom_launcher.hermes_runner"
# Never curl Nous install.sh or pull browser extras.
"""
