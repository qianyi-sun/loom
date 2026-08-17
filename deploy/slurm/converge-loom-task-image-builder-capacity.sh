#!/usr/bin/env bash
# Converge one fixed, non-preemptive exclusive builder reservation per cluster.
set -euo pipefail

QOS="loom-task-image-builder"
RESERVATION="loom-task-image-builder"
SERVICE_USER="loom-rollout"
ACCOUNT="loom-staging"
OLDLAB_NODE="trt-eai-oldlab-6"
GB10_NODE="trt-gb10-2"

if [[ "$#" -ne 0 ]]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: task-image builder Slurm convergence requires root" >&2
  exit 1
fi

host="$(hostname -s)"
architecture="$(uname -m)"
case "$host:$architecture" in
  TRT-EAI-OLDLAB-1:x86_64)
    CLUSTER="trt-oldlab"
    CONTROLLER="TRT-EAI-OLDLAB-1"
    PARTITION="all"
    NODE="$OLDLAB_NODE"
    BASE_QOS="normal"
    ;;
  gx10-01c7:aarch64)
    CLUSTER="trt-gb10"
    CONTROLLER="gx10-01c7"
    PARTITION="gb10"
    NODE="$GB10_NODE"
    BASE_QOS="loom-staging"
    ;;
  *)
    echo "error: task-image builder Slurm convergence is controller-only" >&2
    exit 1
    ;;
esac

slurm_config="$(scontrol show config)"
if ! grep -Eq "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" <<<"$slurm_config" \
  || ! grep -Eq "^SlurmctldHost\[0\][[:space:]]*=[[:space:]]*$CONTROLLER(\(|$)" \
    <<<"$slurm_config"; then
  echo "error: local Slurm controller authority does not match the fixed builder contract" >&2
  exit 1
fi
if ! id "$SERVICE_USER" >/dev/null; then
  echo "error: task-image builder Slurm service identity is unavailable" >&2
  exit 1
fi
node_state="$(scontrol show node "$NODE" -o)"
if ! grep -Eq "(^|[[:space:]])NodeName=$NODE([[:space:]]|$)" <<<"$node_state" \
  || ! grep -Eq "(^|[[:space:]])Partitions=([^[:space:]]*,)?$PARTITION(,|[[:space:]])" \
    <<<"$node_state" \
  || grep -Eq "(^|[[:space:]])State=[^[:space:]]*(DOWN|DRAIN|FAIL|INVAL)" \
    <<<"$node_state"; then
  echo "error: fixed task-image builder node is unavailable or unsafe" >&2
  exit 1
fi

account_row="$(timeout 30 sacctmgr --noheader --parsable2 show account where \
  "name=$ACCOUNT" format=Account </dev/null)"
if [[ -z "$account_row" ]]; then
  sacctmgr --immediate add account name="$ACCOUNT" cluster="$CLUSTER" \
    description="loom staging external workers" organization=loom
elif [[ "${account_row%|}" != "$ACCOUNT" ]]; then
  echo "error: existing Loom Slurm account conflicts with the fixed builder contract" >&2
  exit 1
fi

qos_row="$(timeout 30 sacctmgr --noheader --parsable2 show qos where \
  "name=$QOS" format=Name,Flags,MaxJobsPU,MaxSubmitJobsPU,MaxWall </dev/null)"
if [[ -z "$qos_row" ]]; then
  sacctmgr --immediate add qos name="$QOS" flags=DenyOnLimit \
    MaxJobsPU=1 MaxSubmitJobsPU=1 MaxWall=04:00:00
else
  IFS='|' read -r qos_name qos_flags qos_jobs qos_submit qos_wall _ <<<"$qos_row"
  if [[ "$qos_name" != "$QOS" || "$qos_flags" != "DenyOnLimit" \
    || "$qos_jobs" != "1" || "$qos_submit" != "1" \
    || "$qos_wall" != "04:00:00" ]]; then
    echo "error: existing task-image builder QoS conflicts with the fixed contract" >&2
    exit 1
  fi
fi

association_row="$(timeout 30 sacctmgr --noheader --parsable2 show association where \
  "cluster=$CLUSTER" "account=$ACCOUNT" "user=$SERVICE_USER" \
  format=Cluster,Account,User,QOS,DefaultQOS </dev/null)"
if [[ -z "$association_row" ]]; then
  sacctmgr --immediate add user name="$SERVICE_USER" account="$ACCOUNT" \
    cluster="$CLUSTER" defaultaccount="$ACCOUNT"
fi
sacctmgr --immediate modify user where name="$SERVICE_USER" account="$ACCOUNT" \
  cluster="$CLUSTER" set defaultaccount="$ACCOUNT" \
  qos="$BASE_QOS,$QOS" defaultqos="$BASE_QOS"

qos_readback="$(timeout 30 sacctmgr --noheader --parsable2 show qos where \
  "name=$QOS" format=Name,Flags,MaxJobsPU,MaxSubmitJobsPU,MaxWall </dev/null)"
IFS='|' read -r qos_name qos_flags qos_jobs qos_submit qos_wall _ <<<"$qos_readback"
if [[ "$qos_name" != "$QOS" || "$qos_flags" != "DenyOnLimit" \
  || "$qos_jobs" != "1" || "$qos_submit" != "1" \
  || "$qos_wall" != "04:00:00" ]]; then
  echo "error: task-image builder QoS readback did not converge" >&2
  exit 1
fi
association_readback="$(timeout 30 sacctmgr --noheader --parsable2 show association where \
  "cluster=$CLUSTER" "account=$ACCOUNT" "user=$SERVICE_USER" \
  format=Cluster,Account,User,QOS,DefaultQOS </dev/null)"
if [[ "$(wc -l <<<"$association_readback")" != "1" ]]; then
  echo "error: task-image builder association readback did not converge" >&2
  exit 1
fi
IFS='|' read -r association_cluster association_account association_user \
  association_qos association_default _ <<<"$association_readback"
if [[ "$association_cluster" != "$CLUSTER" \
  || "$association_account" != "$ACCOUNT" \
  || "$association_user" != "$SERVICE_USER" \
  || "$association_default" != "$BASE_QOS" \
  || ",$association_qos," != *",$BASE_QOS,"* \
  || ",$association_qos," != *",$QOS,"* ]]; then
  echo "error: task-image builder association readback did not converge" >&2
  exit 1
fi

reservation_state="$(
  scontrol show reservation -o 2>/dev/null \
    | grep -E "^ReservationName=$RESERVATION([[:space:]]|$)" \
    || true
)"
validate_reservation() {
  local state="$1"
  for expected in \
    "ReservationName=$RESERVATION" \
    "Nodes=$NODE" \
    "NodeCnt=1" \
    "PartitionName=$PARTITION" \
    "Users=$SERVICE_USER" \
    "Accounts=$ACCOUNT" \
    "State=ACTIVE"; do
    if ! grep -Eq "(^|[[:space:]])$expected([[:space:]]|$)" <<<"$state"; then
      return 1
    fi
  done
  if ! grep -Eq '(^|[[:space:]])Flags=([^[:space:]]*,)?SPEC_NODES(,|[[:space:]])' \
    <<<"$state"; then
    return 1
  fi
}
if [[ -n "$reservation_state" ]]; then
  if ! validate_reservation "$reservation_state"; then
    echo "error: existing task-image builder reservation conflicts with the fixed contract" >&2
    exit 1
  fi
else
  # IGNORE_JOBS makes creation non-preemptive: a current foreign job may finish,
  # while the active reservation prevents a replacement job from taking the node.
  scontrol create reservation ReservationName="$RESERVATION" StartTime=now \
    Duration=INFINITE Nodes="$NODE" PartitionName="$PARTITION" \
    Users="$SERVICE_USER" Accounts="$ACCOUNT" Flags=IGNORE_JOBS
fi
reservation_readback="$(scontrol show reservation "$RESERVATION" -o)"
if ! validate_reservation "$reservation_readback"; then
  echo "error: task-image builder reservation readback did not converge" >&2
  exit 1
fi

printf 'converged task-image builder cluster=%s node=%s qos=%s reservation=%s\n' \
  "$CLUSTER" "$NODE" "$QOS" "$RESERVATION"
