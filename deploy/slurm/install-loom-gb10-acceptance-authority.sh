#!/usr/bin/env bash
# Install the fixed all-node GB10 Slurm acceptance authority on its controller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$SCRIPT_DIR/../../scripts/ops/gb10_slurm_acceptance_authority.py"
TMPFILES_SOURCE="$SCRIPT_DIR/loom-gb10-slurm-authority.tmpfiles"
CONTROLLER="gx10-01c7"
CLUSTER="trt-gb10"
INSTALL_PATH="/usr/local/libexec/loom-gb10-slurm-acceptance-authority"
STATE_ROOT="/var/lib/loom-gb10-slurm-authority"
RUNTIME_ROOT="/run/loom-gb10-slurm-authority"
TMPFILES_PATH="/etc/tmpfiles.d/loom-gb10-slurm-authority.conf"

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
if [ ! -f "$SOURCE" ] || [ -L "$SOURCE" ] \
  || [ ! -f "$TMPFILES_SOURCE" ] || [ -L "$TMPFILES_SOURCE" ]; then
  echo "error: candidate acceptance authority source is unavailable" >&2
  exit 1
fi

install -d -o root -g root -m 0755 \
  "$(dirname "$INSTALL_PATH")" "$STATE_ROOT" "$(dirname "$TMPFILES_PATH")"
install -o root -g root -m 0755 "$SOURCE" "$INSTALL_PATH"
install -o root -g root -m 0644 "$TMPFILES_SOURCE" "$TMPFILES_PATH"
/usr/bin/systemd-tmpfiles --create "$TMPFILES_PATH"
if [ "$(stat -c '%U:%G:%a:%F' "$RUNTIME_ROOT")" != "root:root:700:directory" ] \
  || [ "$(stat -c '%U:%G:%a:%F' "$RUNTIME_ROOT/jobs")" != "root:root:700:directory" ] \
  || [ "$(stat -c '%U:%G:%a:%F' "$RUNTIME_ROOT/acceptance.lock")" \
    != "root:root:600:regular empty file" ]; then
  echo "error: GB10 acceptance runtime metadata is unsafe" >&2
  exit 1
fi
/usr/bin/python3 "$INSTALL_PATH" --help >/dev/null
printf 'installed GB10 Slurm acceptance authority: %s\n' "$INSTALL_PATH"
