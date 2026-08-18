#!/usr/bin/env bash
# Converge the shared GB10 partition to Loom's canonical nodes 1-15.
set -euo pipefail

CONTROLLER="gx10-01c7"
CLUSTER="trt-gb10"
CONFIG="/etc/slurm/slurm.conf"
STATE_ROOT="/var/lib/loom-gb10-slurm-authority"
BACKUP="$STATE_ROOT/slurm.conf.before-canonical-nodes-1-15"
CONFIG_OWNER="root"
CONFIG_GROUP="root"
STATE_OWNER="root"
STATE_GROUP="root"
OLD_LINE="PartitionName=gb10 Nodes=trt-gb10-[1-9,11-16] Default=YES MaxTime=1-00:00:00 State=UP PriorityTier=100"
NEW_LINE="PartitionName=gb10 Nodes=trt-gb10-[1-15] Default=YES MaxTime=1-00:00:00 State=UP PriorityTier=100"
EXPECTED_NODES="$(printf 'trt-gb10-%s\n' {1..15})"

loom_gb10_restore_backup_and_fail() {
  local failure="$1"
  install -o "$CONFIG_OWNER" -g "$CONFIG_GROUP" -m 0644 "$BACKUP" "$CONFIG"
  if ! scontrol reconfigure; then
    echo "error: $failure; restored backup on disk, but Slurm rejected the restored backup reconfigure" >&2
    exit 1
  fi
  echo "error: $failure; restored backup" >&2
  exit 1
}

loom_gb10_fail_readback() {
  local failure="$1"
  local partition_changed="$2"
  if [ "$partition_changed" = "1" ]; then
    loom_gb10_restore_backup_and_fail "$failure"
  fi
  echo "error: $failure" >&2
  exit 1
}

loom_gb10_converge_partition() {
  local expected
  local field
  local live_nodes
  local named_count
  local new_count
  local nodes_expression
  local old_count
  local partition_changed=0
  local partition_state
  local temporary=""

  if ! scontrol show config | grep -E \
    "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" >/dev/null; then
    echo "error: local Slurm cluster does not match GB10" >&2
    exit 1
  fi
  if [ -L "$CONFIG" ] \
    || [ "$(stat -c '%U:%G:%a:%F' "$CONFIG")" \
      != "$CONFIG_OWNER:$CONFIG_GROUP:644:regular file" ]; then
    echo "error: Slurm configuration metadata is unsafe" >&2
    exit 1
  fi

  old_count="$(grep -Fxc "$OLD_LINE" "$CONFIG" || true)"
  new_count="$(grep -Fxc "$NEW_LINE" "$CONFIG" || true)"
  named_count="$(grep -Ec '^PartitionName=gb10([[:space:]]|$)' "$CONFIG" || true)"
  if [ "$old_count" = "1" ] && [ "$new_count" = "0" ] && [ "$named_count" = "1" ]; then
    install -d -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0755 "$STATE_ROOT"
    if [ -e "$BACKUP" ]; then
      if [ -L "$BACKUP" ] \
        || [ "$(stat -c '%U:%G:%a:%F' "$BACKUP")" \
          != "$STATE_OWNER:$STATE_GROUP:600:regular file" ] \
        || ! cmp -s "$BACKUP" "$CONFIG"; then
        echo "error: GB10 partition backup is unsafe or stale" >&2
        exit 1
      fi
    else
      install -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0600 "$CONFIG" "$BACKUP"
    fi
    temporary="$(mktemp "$(dirname "$CONFIG")/.slurm.conf.XXXXXX")"
    trap 'if [ -n "${temporary:-}" ] && [ -e "$temporary" ]; then unlink "$temporary"; fi' EXIT
    awk -v old="$OLD_LINE" -v new="$NEW_LINE" \
      '{ print ($0 == old ? new : $0) }' "$CONFIG" >"$temporary"
    chown "$CONFIG_OWNER:$CONFIG_GROUP" "$temporary"
    chmod 0644 "$temporary"
    mv "$temporary" "$CONFIG"
    temporary=""
    trap - EXIT
    if ! scontrol reconfigure; then
      loom_gb10_restore_backup_and_fail \
        "Slurm rejected the canonical GB10 partition update"
    fi
    partition_changed=1
  elif [ "$old_count" != "0" ] \
    || [ "$new_count" != "1" ] \
    || [ "$named_count" != "1" ]; then
    echo "error: GB10 partition line does not match the bounded transition" >&2
    exit 1
  fi

  if [ "$(grep -Fxc "$NEW_LINE" "$CONFIG" || true)" != "1" ]; then
    loom_gb10_fail_readback \
      "durable GB10 partition readback failed" "$partition_changed"
  fi
  if ! partition_state="$(scontrol show partition gb10 -o)"; then
    loom_gb10_fail_readback \
      "live GB10 partition readback is unavailable" "$partition_changed"
  fi
  for expected in \
    "PartitionName=gb10" \
    "Default=YES" \
    "MaxTime=1-00:00:00" \
    "PriorityTier=100" \
    "State=UP"; do
    if ! grep -E \
      "(^|[[:space:]])$expected([[:space:]]|$)" \
      <<<"$partition_state" >/dev/null; then
      loom_gb10_fail_readback \
        "live GB10 partition readback is incomplete" "$partition_changed"
    fi
  done
  nodes_expression=""
  for field in $partition_state; do
    case "$field" in
      Nodes=*) nodes_expression="${field#Nodes=}" ;;
    esac
  done
  if [ -z "$nodes_expression" ]; then
    loom_gb10_fail_readback \
      "live GB10 partition readback is incomplete" "$partition_changed"
  fi
  if ! live_nodes="$(scontrol show hostnames "$nodes_expression")"; then
    loom_gb10_fail_readback \
      "live GB10 partition node expansion failed" "$partition_changed"
  fi
  if [ "$live_nodes" != "$EXPECTED_NODES" ]; then
    loom_gb10_fail_readback \
      "live GB10 partition node set is not exact" "$partition_changed"
  fi
  if ! scontrol show node trt-gb10-10 -o \
    | grep -E '(^| )Partitions=([^ ]*,)?gb10(,| )' >/dev/null; then
    loom_gb10_fail_readback \
      "live GB10 partition membership did not converge for trt-gb10-10" \
      "$partition_changed"
  fi
  if scontrol show node trt-gb10-16 -o \
    | grep -E '(^| )Partitions=([^ ]*,)?gb10(,| )' >/dev/null; then
    loom_gb10_fail_readback \
      "live GB10 partition still includes trt-gb10-16" "$partition_changed"
  fi
  printf 'converged canonical GB10 shared partition: trt-gb10-[1-15]\n'
}

main() {
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
  loom_gb10_converge_partition
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
