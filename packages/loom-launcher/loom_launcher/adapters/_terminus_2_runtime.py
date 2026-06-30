"""Shared install-script + venv path for the terminus-2 adapter (#248).

Same shape as `_openhands_runtime.py`: pin upstream + loom-launcher,
provision into a dedicated venv so we don't tangle with the task
image's site-packages, and install the tmux system binary that
upstream `TmuxSession` shells out to.
"""

from __future__ import annotations

# Match the same upstream pin Loom's TB-2 adapter uses
# (`packages/loom-benchmark-terminal-bench-2/loom_benchmark_terminal_bench_2/upstream.py`).
# A future bump there must move in lockstep so the agent and the task
# bundle agree on the prompt template + verifier semantics.
TERMINAL_BENCH_CORE_VERSION = "0.1.1"

# Pin loom-launcher to a current dev SHA the same way openhands does
# (avoids fetching `main` at trial time which would drift). Bump this
# when the Terminus-2 runner module (`loom_launcher.terminus_2_runner`)
# changes meaningfully. Must be a full 40-char SHA on origin/dev so
# `scripts/check_install_scripts_pinned.py` reads it as pinned.
LOOM_LAUNCHER_REF = "b4119828e0bc2f3d9debd7c31b8115c6070aac50"
LOOM_LAUNCHER_REQUIREMENT = "git+https://github.com/qianyi-sun/loom.git@b4119828e0bc2f3d9debd7c31b8115c6070aac50#subdirectory=packages/loom-launcher"

UV_VERSION = "0.11.21"
TERMINUS_2_VENV = "/opt/loom-agents/terminus-2"
TERMINUS_2_PYTHON = "/opt/loom-agents/terminus-2/bin/python"

TERMINUS_2_INSTALL_SCRIPT = f"""\
set -euo pipefail
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache ca-certificates curl git tmux
elif command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends ca-certificates curl git tmux
else
  echo "no supported package manager (apk/apt-get); cannot install terminus-2" >&2
  exit 1
fi
export UV_INSTALL_DIR=/opt/loom-agents/bin
export UV_PYTHON_INSTALL_DIR=/opt/loom-agents/python
export UV_CACHE_DIR=/opt/loom-agents/uv-cache
mkdir -p "$UV_INSTALL_DIR" "$UV_PYTHON_INSTALL_DIR" "$UV_CACHE_DIR"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf "https://astral.sh/uv/{UV_VERSION}/install.sh" | sh
  ln -sf "$UV_INSTALL_DIR/uv" /usr/local/bin/uv
fi
uv python install 3.12
uv venv --python 3.12 {TERMINUS_2_VENV}
uv pip install --python {TERMINUS_2_PYTHON} --no-cache-dir \\
  "terminal-bench-core=={TERMINAL_BENCH_CORE_VERSION}" \\
  "{LOOM_LAUNCHER_REQUIREMENT}"
{TERMINUS_2_PYTHON} -c "from terminal_bench.agents.terminus import Terminus; from loom_launcher.terminus_2_runner import main as _; print('ok')"
"""
