#!/usr/bin/env bash
# Add GB10-11 to the existing shared partition without removing GB10-16.
set -euo pipefail

CONTROLLER="gx10-01c7"
CLUSTER="trt-gb10"
CONFIG="/etc/slurm/slurm.conf"
STATE_ROOT="/var/lib/loom-gb10-slurm-authority"
BACKUP="$STATE_ROOT/slurm.conf.before-node11"
OLD_LINE="PartitionName=gb10 Nodes=trt-gb10-[1-10,12-16] Default=YES MaxTime=1-00:00:00 State=UP PriorityTier=100"
NEW_LINE="PartitionName=gb10 Nodes=trt-gb10-[1-16] Default=YES MaxTime=1-00:00:00 State=UP PriorityTier=100"

if [ "$#" -ne 0 ]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ] \
  || [ "$(uname -m)" != "aarch64" ] \
  || [ "$(hostname -s)" != "$CONTROLLER" ]; then
  echo "error: GB10 partition convergence is controller-root only" >&2
  exit 1
fi
if ! scontrol show config | grep -E \
  "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" >/dev/null; then
  echo "error: local Slurm cluster does not match GB10" >&2
  exit 1
fi
if [ -L "$CONFIG" ] || [ "$(stat -c '%U:%G:%a:%F' "$CONFIG")" != "root:root:644:regular file" ]; then
  echo "error: Slurm configuration metadata is unsafe" >&2
  exit 1
fi

old_count="$(grep -Fxc "$OLD_LINE" "$CONFIG" || true)"
new_count="$(grep -Fxc "$NEW_LINE" "$CONFIG" || true)"
if [ "$old_count" = "1" ] && [ "$new_count" = "0" ]; then
  install -d -o root -g root -m 0755 "$STATE_ROOT"
  if [ ! -e "$BACKUP" ]; then
    install -o root -g root -m 0644 "$CONFIG" "$BACKUP"
  fi
  temporary="$(mktemp /etc/slurm/.slurm.conf.XXXXXX)"
  trap 'if [ -e "$temporary" ]; then unlink "$temporary"; fi' EXIT
  awk -v old="$OLD_LINE" -v new="$NEW_LINE" \
    '{ print ($0 == old ? new : $0) }' "$CONFIG" >"$temporary"
  chown root:root "$temporary"
  chmod 0644 "$temporary"
  mv "$temporary" "$CONFIG"
  if ! scontrol reconfigure; then
    install -o root -g root -m 0644 "$BACKUP" "$CONFIG"
    scontrol reconfigure
    echo "error: Slurm rejected the GB10 partition update; restored backup" >&2
    exit 1
  fi
elif [ "$old_count" != "0" ] || [ "$new_count" != "1" ]; then
  echo "error: GB10 partition line does not match the bounded transition" >&2
  exit 1
fi

if [ "$(grep -Fxc "$NEW_LINE" "$CONFIG" || true)" != "1" ]; then
  echo "error: durable GB10 partition readback failed" >&2
  exit 1
fi
for node in trt-gb10-11 trt-gb10-16; do
  if ! scontrol show node "$node" -o \
    | grep -E '(^| )Partitions=([^ ]*,)?gb10(,| )' >/dev/null; then
    echo "error: live GB10 partition membership did not converge for $node" >&2
    exit 1
  fi
done
printf 'converged GB10 shared partition membership: trt-gb10-[1-16]\n'
