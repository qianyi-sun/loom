#!/usr/bin/env bash
# Add Loom's dedicated GB10 staging partition without changing shared jobs.
set -euo pipefail

CONTROLLER="gx10-01c7"
CLUSTER="trt-gb10"
CONFIG="/etc/slurm/slurm.conf"
STATE_ROOT="/var/lib/loom-gb10-slurm-authority"
BACKUP="$STATE_ROOT/slurm.conf.before-loom-staging-partition"
CONFIG_OWNER="root"
CONFIG_GROUP="root"
STATE_OWNER="root"
STATE_GROUP="root"
PARTITION="loom-staging"
PARTITION_LINE="PartitionName=$PARTITION Nodes=trt-gb10-[1-15] Default=NO MaxTime=1-00:00:00 State=UP PriorityTier=100 AllowAccounts=loom-staging AllowQos=loom-staging OverSubscribe=NO"
EXPECTED_NODES="$(printf 'trt-gb10-%s\n' {1..15})"

loom_gb10_refresh_stale_backup() {
  local archive
  local backup_digest
  local history
  local replacement=""

  history="$STATE_ROOT/slurm.conf.before-loom-staging-partition.history"
  backup_digest="$(sha256sum "$BACKUP" | awk '{ print $1 }')"
  archive="$history/$backup_digest"
  if [ -e "$history" ] || [ -L "$history" ]; then
    if [ -L "$history" ] \
      || [ "$(stat -c '%U:%G:%a:%F' "$history")" \
        != "$STATE_OWNER:$STATE_GROUP:700:directory" ]; then
      echo "error: GB10 partition backup history is unsafe" >&2
      exit 1
    fi
  else
    install -d -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0700 "$history"
  fi
  if [ -e "$archive" ] || [ -L "$archive" ]; then
    if [ -L "$archive" ] \
      || [ "$(stat -c '%U:%G:%a:%F' "$archive")" \
        != "$STATE_OWNER:$STATE_GROUP:600:regular file" ] \
      || ! cmp -s "$archive" "$BACKUP"; then
      echo "error: GB10 partition backup archive is unsafe or conflicting" >&2
      exit 1
    fi
  else
    install -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0600 "$BACKUP" "$archive"
  fi

  replacement="$(mktemp "$STATE_ROOT/.slurm.conf.before-loom-staging-partition.XXXXXX")"
  if ! install -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0600 \
    "$CONFIG" "$replacement"; then
    unlink "$replacement"
    exit 1
  fi
  mv "$replacement" "$BACKUP"
}

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

loom_gb10_live_partition_is_exact() {
  local expected
  local field
  local live_nodes
  local nodes_expression=""
  local partition_state

  partition_state="$(scontrol show partition "$PARTITION" -o)" || return 1
  for expected in \
    "PartitionName=$PARTITION" \
    "AllowAccounts=loom-staging" \
    "AllowQos=loom-staging" \
    "Default=NO" \
    "MaxTime=1-00:00:00" \
    "PriorityTier=100" \
    "OverSubscribe=NO" \
    "State=UP"; do
    grep -E "(^|[[:space:]])$expected([[:space:]]|$)" \
      <<<"$partition_state" >/dev/null || return 1
  done
  for field in $partition_state; do
    case "$field" in
      Nodes=*) nodes_expression="${field#Nodes=}" ;;
    esac
  done
  [ -n "$nodes_expression" ] || return 1
  live_nodes="$(scontrol show hostnames "$nodes_expression")" || return 1
  [ "$live_nodes" = "$EXPECTED_NODES" ] || return 1
  scontrol show node trt-gb10-10 -o \
    | grep -E '(^| )Partitions=([^ ]*,)?loom-staging(,| )' >/dev/null \
    || return 1
  if scontrol show node trt-gb10-16 -o \
    | grep -E '(^| )Partitions=([^ ]*,)?loom-staging(,| )' >/dev/null; then
    return 1
  fi
}

loom_gb10_converge_partition() {
  local expected
  local field
  local live_nodes
  local named_count
  local partition_count
  local shared_line_number
  local shared_named_count
  local nodes_expression
  local partition_added=0
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

  shared_named_count="$(grep -Ec '^PartitionName=gb10([[:space:]]|$)' "$CONFIG" || true)"
  partition_count="$(grep -Fxc "$PARTITION_LINE" "$CONFIG" || true)"
  named_count="$(grep -Ec "^PartitionName=$PARTITION([[:space:]]|$)" "$CONFIG" || true)"
  if [ "$shared_named_count" != "1" ]; then
    echo "error: shared GB10 partition authority is not singular" >&2
    exit 1
  fi
  shared_line_number="$(
    grep -En '^PartitionName=gb10([[:space:]]|$)' "$CONFIG" | cut -d: -f1
  )"
  if [ "$partition_count" = "0" ] && [ "$named_count" = "0" ]; then
    install -d -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0755 "$STATE_ROOT"
    if [ -e "$BACKUP" ]; then
      if [ -L "$BACKUP" ] \
        || [ "$(stat -c '%U:%G:%a:%F' "$BACKUP")" \
          != "$STATE_OWNER:$STATE_GROUP:600:regular file" ]; then
        echo "error: GB10 partition backup is unsafe" >&2
        exit 1
      fi
      if ! cmp -s "$BACKUP" "$CONFIG"; then
        loom_gb10_refresh_stale_backup
      fi
    else
      install -o "$STATE_OWNER" -g "$STATE_GROUP" -m 0600 "$CONFIG" "$BACKUP"
    fi
    temporary="$(mktemp "$(dirname "$CONFIG")/.slurm.conf.XXXXXX")"
    trap 'if [ -n "${temporary:-}" ] && [ -e "$temporary" ]; then unlink "$temporary"; fi' EXIT
    awk -v anchor_line="$shared_line_number" -v partition="$PARTITION_LINE" \
      '{ print; if (NR == anchor_line) print partition }' "$CONFIG" >"$temporary"
    if [ "$(grep -Fxc "$PARTITION_LINE" "$temporary" || true)" != "1" ] \
      || ! awk -v partition="$PARTITION_LINE" \
        '$0 != partition { print }' "$temporary" | cmp -s - "$BACKUP"; then
      echo "error: candidate GB10 partition config did not preserve foreign state" >&2
      exit 1
    fi
    chown "$CONFIG_OWNER:$CONFIG_GROUP" "$temporary"
    chmod 0644 "$temporary"
    mv "$temporary" "$CONFIG"
    temporary=""
    trap - EXIT
    if ! scontrol reconfigure; then
      loom_gb10_restore_backup_and_fail \
        "Slurm rejected the dedicated GB10 staging partition"
    fi
    partition_added=1
  elif [ "$partition_count" != "1" ] || [ "$named_count" != "1" ]; then
    echo "error: dedicated GB10 staging partition line does not match authority" >&2
    exit 1
  fi

  if [ "$(grep -Fxc "$PARTITION_LINE" "$CONFIG" || true)" != "1" ]; then
    loom_gb10_fail_readback \
      "durable GB10 staging partition readback failed" "$partition_added"
  fi
  if [ "$partition_added" = "0" ] \
    && ! loom_gb10_live_partition_is_exact; then
    if ! scontrol reconfigure; then
      echo "error: Slurm rejected the canonical durable GB10 staging partition reload" >&2
      exit 1
    fi
  fi
  if ! partition_state="$(scontrol show partition "$PARTITION" -o)"; then
    loom_gb10_fail_readback \
      "live GB10 staging partition readback is unavailable" "$partition_added"
  fi
  for expected in \
    "PartitionName=$PARTITION" \
    "AllowAccounts=loom-staging" \
    "AllowQos=loom-staging" \
    "Default=NO" \
    "MaxTime=1-00:00:00" \
    "PriorityTier=100" \
    "OverSubscribe=NO" \
    "State=UP"; do
    if ! grep -E \
      "(^|[[:space:]])$expected([[:space:]]|$)" \
      <<<"$partition_state" >/dev/null; then
      loom_gb10_fail_readback \
        "live GB10 staging partition readback is incomplete" "$partition_added"
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
      "live GB10 staging partition readback is incomplete" "$partition_added"
  fi
  if ! live_nodes="$(scontrol show hostnames "$nodes_expression")"; then
    loom_gb10_fail_readback \
      "live GB10 staging partition node expansion failed" "$partition_added"
  fi
  if [ "$live_nodes" != "$EXPECTED_NODES" ]; then
    loom_gb10_fail_readback \
      "live GB10 partition node set is not exact" "$partition_added"
  fi
  if ! scontrol show node trt-gb10-10 -o \
    | grep -E '(^| )Partitions=([^ ]*,)?loom-staging(,| )' >/dev/null; then
    loom_gb10_fail_readback \
      "dedicated GB10 staging partition membership did not converge for trt-gb10-10" \
      "$partition_added"
  fi
  if scontrol show node trt-gb10-16 -o \
    | grep -E '(^| )Partitions=([^ ]*,)?loom-staging(,| )' >/dev/null; then
    loom_gb10_fail_readback \
      "dedicated GB10 staging partition still includes trt-gb10-16" \
      "$partition_added"
  fi
  printf 'converged dedicated GB10 staging partition: trt-gb10-[1-15]\n'
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
