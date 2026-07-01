"""Shared install-script + venv path for the terminus-2 adapter (#248).

Same shape as `_openhands_runtime.py`: pin upstream + loom-launcher,
provision into a dedicated venv so we don't tangle with the task
image's site-packages, and install the tmux system binary that
upstream `TmuxSession` shells out to.
"""

from __future__ import annotations

# Upstream package on PyPI is `terminal-bench` — not `terminal-bench-core`.
# The commit Loom's TB-2 task adapter pins (`91e10457`) was never
# published to PyPI (its pyproject.toml declares version 0.1.0; PyPI
# only carries 0.2.x). Install directly from the git ref so the
# Terminus agent's prompt template + verifier semantics stay in
# lockstep with the task bundle. Bumping the TB-2 task-adapter's pin
# (`packages/loom-benchmark-terminal-bench-2/loom_benchmark_terminal_bench_2/upstream.py`)
# requires bumping this SHA too.
TERMINAL_BENCH_UPSTREAM_SHA = "91e10457b5410f16c44364da1a34cb6de8c488a5"
TERMINAL_BENCH_REQUIREMENT = "terminal-bench@git+https://github.com/laude-institute/terminal-bench.git@91e10457b5410f16c44364da1a34cb6de8c488a5"

# Pin loom-launcher to a current dev SHA the same way openhands does
# (avoids fetching `main` at trial time which would drift). Bump this
# when the Terminus-2 runner module (`loom_launcher.terminus_2_runner`)
# changes meaningfully. Must be a full 40-char SHA on origin/dev so
# `scripts/check_install_scripts_pinned.py` reads it as pinned.
LOOM_LAUNCHER_REF = "9f7a8c56ed3ac3d56b19f3fd3c9a572e15bd4707"
LOOM_LAUNCHER_REQUIREMENT = "git+https://github.com/qianyi-sun/loom.git@9f7a8c56ed3ac3d56b19f3fd3c9a572e15bd4707#subdirectory=packages/loom-launcher"

UV_VERSION = "0.11.21"
TERMINUS_2_VENV = "/opt/loom-agents/terminus-2"
TERMINUS_2_PYTHON = "/opt/loom-agents/terminus-2/bin/python"

# Upstream terminal-bench at the pinned commit declares
# `requires-python = ">=3.13"`, so provision a 3.13 venv (uv fetches
# the CPython build from the astral CPython mirror). The rest of the
# script mirrors the openhands runtime pattern.
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
uv python install 3.13
uv venv --python 3.13 {TERMINUS_2_VENV}
uv pip install --python {TERMINUS_2_PYTHON} --no-cache-dir \\
  "{TERMINAL_BENCH_REQUIREMENT}" \\
  "{LOOM_LAUNCHER_REQUIREMENT}"
{TERMINUS_2_PYTHON} -c "from terminal_bench.agents.terminus import Terminus; from loom_launcher.terminus_2_runner import main as _; print('ok')"
"""
