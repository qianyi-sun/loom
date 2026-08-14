#!/usr/bin/env bash
# Add the dedicated OLDLAB staging partition without changing shared jobs.
set -euo pipefail

CONTROLLER="TRT-EAI-OLDLAB-1"
CLUSTER="trt-oldlab"
CONFIG="/etc/slurm/slurm.conf"
STATE_ROOT="/var/lib/loom-oldlab-slurm-authority"
BACKUP="$STATE_ROOT/slurm.conf.before-loom-staging-partition"
ANCHOR_LINE="PartitionName=all Nodes=ALL Default=YES MaxTime=INFINITE State=UP OverSubscribe=NO"
PARTITION="loom-staging"
PARTITION_LINE="PartitionName=$PARTITION Nodes=trt-eai-oldlab-[3-5] Default=NO MaxTime=2-00:00:00 State=UP PriorityTier=100 AllowGroups=loom-rollout OverSubscribe=NO"

if [ "$#" -ne 0 ]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ] \
  || [ "$(uname -m)" != "x86_64" ] \
  || [ "$(hostname -s)" != "$CONTROLLER" ]; then
  echo "error: OLDLAB staging partition convergence is controller-root only" >&2
  exit 1
fi
if ! scontrol show config | grep -E \
  "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" >/dev/null; then
  echo "error: local Slurm cluster does not match OLDLAB" >&2
  exit 1
fi
if [ -L "$CONFIG" ] \
  || [ "$(stat -c '%U:%G:%a:%F' "$CONFIG")" != "trt:sharedwork:664:regular file" ]; then
  echo "error: Slurm configuration metadata is unsafe" >&2
  exit 1
fi

anchor_count="$(grep -Fxc "$ANCHOR_LINE" "$CONFIG" || true)"
partition_count="$(grep -Fxc "$PARTITION_LINE" "$CONFIG" || true)"
named_count="$(grep -Ec "^PartitionName=$PARTITION([[:space:]]|$)" "$CONFIG" || true)"
if [ "$anchor_count" != "1" ]; then
  echo "error: shared OLDLAB partition anchor is not exact" >&2
  exit 1
fi
if [ "$partition_count" = "0" ] && [ "$named_count" = "0" ]; then
  install -d -o root -g root -m 0755 "$STATE_ROOT"
  if [ -e "$BACKUP" ]; then
    if [ -L "$BACKUP" ] \
      || [ "$(stat -c '%U:%G:%a:%F' "$BACKUP")" != "root:root:600:regular file" ] \
      || ! cmp -s "$BACKUP" "$CONFIG"; then
      echo "error: OLDLAB staging partition backup is unsafe or stale" >&2
      exit 1
    fi
  else
    install -o root -g root -m 0600 "$CONFIG" "$BACKUP"
  fi
  temporary="$(mktemp /etc/slurm/.slurm.conf.XXXXXX)"
  trap 'if [ -e "$temporary" ]; then unlink "$temporary"; fi' EXIT
  awk -v anchor="$ANCHOR_LINE" -v partition="$PARTITION_LINE" \
    '{ print; if ($0 == anchor) print partition }' "$CONFIG" >"$temporary"
  chown trt:sharedwork "$temporary"
  chmod 0664 "$temporary"
  mv "$temporary" "$CONFIG"
  if ! scontrol reconfigure; then
    install -o trt -g sharedwork -m 0664 "$BACKUP" "$CONFIG"
    scontrol reconfigure
    echo "error: Slurm rejected the OLDLAB staging partition; restored backup" >&2
    exit 1
  fi
elif [ "$partition_count" != "1" ] || [ "$named_count" != "1" ]; then
  echo "error: OLDLAB staging partition line does not match authority" >&2
  exit 1
fi

if [ "$(grep -Fxc "$PARTITION_LINE" "$CONFIG" || true)" != "1" ]; then
  echo "error: durable OLDLAB staging partition readback failed" >&2
  exit 1
fi
partition_state="$(scontrol show partition "$PARTITION" -o)"
for expected in \
  "PartitionName=$PARTITION" \
  "AllowGroups=loom-rollout" \
  "MaxTime=2-00:00:00" \
  "PriorityTier=100" \
  "OverSubscribe=NO" \
  "State=UP"; do
  if ! grep -F "$expected" <<<"$partition_state" >/dev/null; then
    echo "error: live OLDLAB staging partition readback is incomplete" >&2
    exit 1
  fi
done
for node in trt-eai-oldlab-3 trt-eai-oldlab-4 trt-eai-oldlab-5; do
  if ! scontrol show node "$node" -o \
    | grep -E '(^| )Partitions=([^ ]*,)?loom-staging(,| )' >/dev/null; then
    echo "error: live OLDLAB staging partition membership did not converge for $node" >&2
    exit 1
  fi
done
printf 'converged dedicated OLDLAB staging partition: trt-eai-oldlab-[3-5]\n'
