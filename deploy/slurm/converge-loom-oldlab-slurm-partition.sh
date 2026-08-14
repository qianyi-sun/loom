#!/usr/bin/env bash
# Add the dedicated OLDLAB staging partition without changing shared jobs.
set -euo pipefail

CONTROLLER="TRT-EAI-OLDLAB-1"
CLUSTER="trt-oldlab"
CONFIG="/etc/slurm/slurm.conf"
STATE_ROOT="/var/lib/loom-oldlab-slurm-authority"
BACKUP="$STATE_ROOT/slurm.conf.before-loom-staging-partition"
CONFIG_OWNER="trt"
CONFIG_GROUP="sharedwork"
STATE_OWNER="root"
STATE_GROUP="root"
ANCHOR_LINE="PartitionName=all Nodes=ALL Default=YES MaxTime=INFINITE State=UP OverSubscribe=NO"
PARTITION="loom-staging"
PARTITION_LINE="PartitionName=$PARTITION Nodes=trt-eai-oldlab-[3-5] Default=NO MaxTime=2-00:00:00 State=UP PriorityTier=100 AllowGroups=loom-rollout OverSubscribe=NO"
EXPECTED_NODES=$'trt-eai-oldlab-3\ntrt-eai-oldlab-4\ntrt-eai-oldlab-5'

loom_oldlab_restore_backup_and_fail() {
  local failure="$1"
  install -o "$CONFIG_OWNER" -g "$CONFIG_GROUP" -m 0664 "$BACKUP" "$CONFIG"
  if ! scontrol reconfigure; then
    echo "error: $failure; restored backup on disk, but Slurm rejected the restored backup reconfigure" >&2
    exit 1
  fi
  echo "error: $failure; restored backup" >&2
  exit 1
}

loom_oldlab_fail_readback() {
  local failure="$1"
  local partition_added="$2"
  if [ "$partition_added" = "1" ]; then
    loom_oldlab_restore_backup_and_fail "$failure"
  fi
  echo "error: $failure" >&2
  exit 1
}

loom_oldlab_converge_partition() {
  local anchor_count
  local expected
  local field
  local live_nodes
  local named_count
  local node
  local nodes_expression
  local partition_added=0
  local partition_count
  local partition_state
  local temporary=""
  if ! scontrol show config | grep -E \
    "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" >/dev/null; then
    echo "error: local Slurm cluster does not match OLDLAB" >&2
    exit 1
  fi
  if [ -L "$CONFIG" ] \
    || [ "$(stat -c '%U:%G:%a:%F' "$CONFIG")" \
      != "$CONFIG_OWNER:$CONFIG_GROUP:664:regular file" ]; then
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
    install -d -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0755 "$STATE_ROOT"
    if [ -e "$BACKUP" ]; then
      if [ -L "$BACKUP" ] \
        || [ "$(stat -c '%U:%G:%a:%F' "$BACKUP")" \
          != "$STATE_OWNER:$STATE_GROUP:600:regular file" ] \
        || ! cmp -s "$BACKUP" "$CONFIG"; then
        echo "error: OLDLAB staging partition backup is unsafe or stale" >&2
        exit 1
      fi
    else
      install -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0600 "$CONFIG" "$BACKUP"
    fi
    temporary="$(mktemp "$(dirname "$CONFIG")/.slurm.conf.XXXXXX")"
    trap 'if [ -n "${temporary:-}" ] && [ -e "$temporary" ]; then unlink "$temporary"; fi' EXIT
    awk -v anchor="$ANCHOR_LINE" -v partition="$PARTITION_LINE" \
      '{ print; if ($0 == anchor) print partition }' "$CONFIG" >"$temporary"
    chown "$CONFIG_OWNER:$CONFIG_GROUP" "$temporary"
    chmod 0664 "$temporary"
    mv "$temporary" "$CONFIG"
    temporary=""
    trap - EXIT
    if ! scontrol reconfigure; then
      loom_oldlab_restore_backup_and_fail \
        "Slurm rejected the OLDLAB staging partition"
    fi
    partition_added=1
  elif [ "$partition_count" != "1" ] || [ "$named_count" != "1" ]; then
    echo "error: OLDLAB staging partition line does not match authority" >&2
    exit 1
  fi

  if [ "$(grep -Fxc "$PARTITION_LINE" "$CONFIG" || true)" != "1" ]; then
    loom_oldlab_fail_readback \
      "durable OLDLAB staging partition readback failed" "$partition_added"
  fi
  if ! partition_state="$(scontrol show partition "$PARTITION" -o)"; then
    loom_oldlab_fail_readback \
      "live OLDLAB staging partition readback is unavailable" "$partition_added"
  fi
  for expected in \
    "PartitionName=$PARTITION" \
    "AllowGroups=loom-rollout" \
    "Default=NO" \
    "MaxTime=2-00:00:00" \
    "PriorityTier=100" \
    "OverSubscribe=NO" \
    "State=UP"; do
    if ! grep -E \
      "(^|[[:space:]])$expected([[:space:]]|$)" \
      <<<"$partition_state" >/dev/null; then
      loom_oldlab_fail_readback \
        "live OLDLAB staging partition readback is incomplete" "$partition_added"
    fi
  done
  nodes_expression=""
  for field in $partition_state; do
    case "$field" in
      Nodes=*) nodes_expression="${field#Nodes=}" ;;
    esac
  done
  if [ -z "$nodes_expression" ]; then
    loom_oldlab_fail_readback \
      "live OLDLAB staging partition readback is incomplete" "$partition_added"
  fi
  if ! live_nodes="$(scontrol show hostnames "$nodes_expression")"; then
    loom_oldlab_fail_readback \
      "live OLDLAB staging partition node expansion failed" "$partition_added"
  fi
  if [ "$live_nodes" != "$EXPECTED_NODES" ]; then
    loom_oldlab_fail_readback \
      "live OLDLAB staging partition node set is not exact" "$partition_added"
  fi
  for node in trt-eai-oldlab-3 trt-eai-oldlab-4 trt-eai-oldlab-5; do
    if ! scontrol show node "$node" -o \
      | grep -E '(^| )Partitions=([^ ]*,)?loom-staging(,| )' >/dev/null; then
      loom_oldlab_fail_readback \
        "live OLDLAB staging partition membership did not converge for $node" \
        "$partition_added"
    fi
  done
  printf 'converged dedicated OLDLAB staging partition: trt-eai-oldlab-[3-5]\n'
}

main() {
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
  loom_oldlab_converge_partition
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
