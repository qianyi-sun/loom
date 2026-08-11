#!/bin/sh
# Manual compatibility shim only. Loom owns fan-out and process supervision.
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: run_rollout.sbatch STAGE_REQUEST_JSON OUTPUT_DIR" >&2
  exit 20
fi

exec /opt/loom/venv/bin/python -m loom.integrations.behavior.cli run \
  --request "$1" \
  --output-dir "$2"
