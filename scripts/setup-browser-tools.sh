#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  PYTHON_BIN="$VIRTUAL_ENV/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  BASE_PYTHON="${PYTHON:-python3}"
  "$BASE_PYTHON" -m venv "$ROOT_DIR/.venv"
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
fi

"$ROOT_DIR/scripts/setup-dev-env.sh"
"$PYTHON_BIN" -m pip install --upgrade pip
"$PYTHON_BIN" -m pip install -c "$ROOT_DIR/constraints/dev-runtime.txt" -e "$ROOT_DIR[browser]"
"$PYTHON_BIN" -m playwright install chromium

printf 'Browser test tools are ready.\n'
printf 'Run: python -m agentic_data_platform.service.frontend_browser_smoke\n'
