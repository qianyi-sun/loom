#!/usr/bin/env bash
# Install the fixed all-node GB10 Slurm acceptance authority on its controller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/../../scripts/ops/gb10_slurm_acceptance_authority.py"
CONTROLLER="gx10-01c7"
CLUSTER="trt-gb10"
INSTALL_PATH="/usr/local/libexec/loom-gb10-slurm-acceptance-authority"
STATE_ROOT="/var/lib/loom-gb10-slurm-authority"

if [ "$#" -ne 0 ]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "error: GB10 acceptance-authority installation requires root" >&2
  exit 1
fi
if [ "$(uname -m)" != "aarch64" ] || [ "$(hostname -s)" != "$CONTROLLER" ]; then
  echo "error: GB10 acceptance authority is controller-only" >&2
  exit 1
fi
if ! scontrol show config | grep -E \
  "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" >/dev/null; then
  echo "error: local Slurm cluster does not match GB10" >&2
  exit 1
fi
if [ ! -f "$SOURCE" ] || [ -L "$SOURCE" ]; then
  echo "error: candidate acceptance authority source is unavailable" >&2
  exit 1
fi

install -d -o root -g root -m 0755 "$(dirname "$INSTALL_PATH")" "$STATE_ROOT"
install -o root -g root -m 0755 "$SOURCE" "$INSTALL_PATH"
/usr/bin/python3 "$INSTALL_PATH" --help >/dev/null
printf 'installed GB10 Slurm acceptance authority: %s\n' "$INSTALL_PATH"
