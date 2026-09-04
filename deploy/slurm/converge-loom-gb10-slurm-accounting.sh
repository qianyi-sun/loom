#!/usr/bin/env bash
# Converge the bounded SlurmDBD identity used by the GB10 Loom autoscaler.
set -euo pipefail

CLUSTER="trt-gb10"
CONTROLLER="gx10-01c7"
LEGACY_SERVICE_USER="loom-rollout"
LEGACY_SERVICE_UID="995"
LEGACY_SERVICE_GID="2007"
EXECUTOR_SERVICE_USER="loom_capacity_executor"
EXECUTOR_SERVICE_HOME="/var/lib/loom-capacity-executor"
EXECUTOR_SERVICE_SHELL="/usr/sbin/nologin"
EXECUTOR_PARTITION="loom-staging"
ACCOUNT="loom-staging"
QOS="loom-staging"

if [ "$#" -ne 0 ]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "error: GB10 Slurm accounting convergence requires root" >&2
  exit 1
fi
if [ "$(uname -m)" != "aarch64" ] || [ "$(hostname -s)" != "$CONTROLLER" ]; then
  echo "error: GB10 Slurm accounting convergence is controller-only" >&2
  exit 1
fi
slurm_config="$(scontrol show config)"
if ! printf '%s\n' "$slurm_config" | grep -Eq \
  "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" \
  || ! printf '%s\n' "$slurm_config" | grep -Eq \
    "^SlurmctldHost\[0\][[:space:]]*=[[:space:]]*$CONTROLLER\(192[.]168[.]20[.]11\)$"; then
  echo "error: local Slurm authority does not match the GB10 controller" >&2
  exit 1
fi
if [ "$(id -u "$LEGACY_SERVICE_USER")" != "$LEGACY_SERVICE_UID" ] \
  || [ "$(id -g "$LEGACY_SERVICE_USER")" != "$LEGACY_SERVICE_GID" ]; then
  echo "error: GB10 legacy Slurm service identity is not installed" >&2
  exit 1
fi

executor_passwd="$(getent -s files passwd "$EXECUTOR_SERVICE_USER")" \
  || executor_passwd=""
executor_group="$(getent -s files group "$EXECUTOR_SERVICE_USER")" \
  || executor_group=""
if [[ ! "$executor_passwd" =~ ^loom_capacity_executor:[^:]*:([0-9]+):([0-9]+):[^:]*:/var/lib/loom-capacity-executor:/usr/sbin/nologin$ ]]; then
  echo "error: GB10 executor service identity is not safely installed" >&2
  exit 1
fi
executor_uid="${BASH_REMATCH[1]}"
executor_passwd_gid="${BASH_REMATCH[2]}"
if [[ ! "$executor_group" =~ ^loom_capacity_executor:[^:]*:([0-9]+):$ ]]; then
  echo "error: GB10 executor service identity is not safely installed" >&2
  exit 1
fi
executor_group_gid="${BASH_REMATCH[1]}"
executor_id_uid="$(id -u "$EXECUTOR_SERVICE_USER")"
executor_id_gid="$(id -g "$EXECUTOR_SERVICE_USER")"
executor_id_groups="$(id -G "$EXECUTOR_SERVICE_USER")"
if [ "$executor_uid" = "0" ] || [ "$executor_group_gid" = "0" ] \
  || [ "$executor_uid" != "$executor_id_uid" ] \
  || [ "$executor_passwd_gid" != "$executor_group_gid" ] \
  || [ "$executor_group_gid" != "$executor_id_gid" ] \
  || [ "$executor_id_groups" != "$executor_group_gid" ]; then
  echo "error: GB10 executor service identity is not safely installed" >&2
  exit 1
fi

account_row() {
  timeout 30 sacctmgr --noheader --parsable2 show account where \
    "name=$ACCOUNT" format=Account,Descr,Org </dev/null
}

qos_row() {
  timeout 30 sacctmgr --noheader --parsable2 show qos where \
    "name=$QOS" format=Name,Flags,MaxJobsPU,MaxSubmitJobsPU </dev/null
}

association_row() {
  local user="$1"
  local partition="$2"
  if [ -n "$partition" ]; then
    timeout 30 sacctmgr --noheader --parsable2 show association where \
      "cluster=$CLUSTER" "account=$ACCOUNT" "user=$user" \
      "partition=$partition" \
      format=Cluster,Account,User,Partition,QOS,DefaultQOS </dev/null
  else
    timeout 30 sacctmgr --noheader --parsable2 show association where \
      "cluster=$CLUSTER" "account=$ACCOUNT" "user=$user" \
      format=Cluster,Account,User,Partition,QOS,DefaultQOS </dev/null
  fi
}

user_row() {
  local user="$1"
  timeout 30 sacctmgr --noheader --parsable2 show user where \
    "name=$user" "cluster=$CLUSTER" format=User,DefaultAccount </dev/null
}

single_row_or_absent() {
  local value="$1"
  [ -z "$value" ] || [ "$(printf '%s\n' "$value" | wc -l)" -eq 1 ]
}

account_before="$(account_row)"
if [ -n "$account_before" ] \
  && [ "${account_before%|}" != "$ACCOUNT|loom staging external workers|loom" ]; then
  echo "error: existing Loom Slurm account conflicts with the fixed contract" >&2
  exit 1
fi
qos_before="$(qos_row)"
if [ -n "$qos_before" ] \
  && [ "${qos_before%|}" != "$QOS|DenyOnLimit|15|15" ]; then
  echo "error: existing Loom Slurm QoS conflicts with the fixed contract" >&2
  exit 1
fi

legacy_user_before="$(user_row "$LEGACY_SERVICE_USER")"
legacy_association_before="$(association_row "$LEGACY_SERVICE_USER" "")"
if ! single_row_or_absent "$legacy_user_before" \
  || ! single_row_or_absent "$legacy_association_before" \
  || [ "${legacy_user_before%|}" != "$LEGACY_SERVICE_USER|$ACCOUNT" ]; then
  echo "error: existing legacy Slurm user conflicts with the fixed contract" >&2
  exit 1
fi
IFS='|' read -r legacy_cluster legacy_account legacy_user legacy_partition \
  legacy_qos legacy_default_qos _ <<<"$legacy_association_before"
if [ "$legacy_cluster" != "$CLUSTER" ] || [ "$legacy_account" != "$ACCOUNT" ] \
  || [ "$legacy_user" != "$LEGACY_SERVICE_USER" ] || [ -n "$legacy_partition" ] \
  || [ "$legacy_default_qos" != "$QOS" ] \
  || [[ ",$legacy_qos," != *",$QOS,"* ]]; then
  echo "error: existing legacy Slurm association conflicts with the fixed contract" >&2
  exit 1
fi

if [ -z "$account_before" ]; then
  sacctmgr --immediate add account name="$ACCOUNT" cluster="$CLUSTER" \
    description="loom staging external workers" organization=loom
fi
if [ -z "$qos_before" ]; then
  sacctmgr --immediate add qos name="$QOS" flags=DenyOnLimit \
    MaxJobsPU=15 MaxSubmitJobsPU=15
fi

user="$EXECUTOR_SERVICE_USER"
executor_user_before="$(user_row "$user")"
executor_association_before="$(association_row "$user" "$EXECUTOR_PARTITION")"
if ! single_row_or_absent "$executor_user_before" \
  || ! single_row_or_absent "$executor_association_before" \
  || { [ -n "$executor_user_before" ] \
    && [ "${executor_user_before%|}" != "$user|$ACCOUNT" ]; }; then
  echo "error: existing executor Slurm user conflicts with the fixed contract" >&2
  exit 1
fi
if [ -z "$executor_association_before" ]; then
  sacctmgr --immediate add user name="$user" account="$ACCOUNT" \
    cluster="$CLUSTER" partition="$EXECUTOR_PARTITION" defaultaccount="$ACCOUNT"
fi
sacctmgr --immediate modify user where name="$user" account="$ACCOUNT" \
  cluster="$CLUSTER" partition="$EXECUTOR_PARTITION" set \
  defaultaccount="$ACCOUNT" qos="$QOS" defaultqos="$QOS"

if [ "$(account_row | sed 's/|$//')" != "$ACCOUNT|loom staging external workers|loom" ]; then
  echo "error: account readback did not converge" >&2
  exit 1
fi
if [ "$(qos_row | sed 's/|$//')" != "$QOS|DenyOnLimit|15|15" ]; then
  echo "error: QoS readback did not converge" >&2
  exit 1
fi
legacy_user_after="$(user_row "$LEGACY_SERVICE_USER")"
legacy_association_after="$(association_row "$LEGACY_SERVICE_USER" "")"
if [ "$legacy_user_after" != "$legacy_user_before" ] \
  || [ "$legacy_association_after" != "$legacy_association_before" ]; then
  echo "error: legacy association readback did not converge" >&2
  exit 1
fi
if [ "$(user_row "$EXECUTOR_SERVICE_USER" | sed 's/|$//')" \
    != "$EXECUTOR_SERVICE_USER|$ACCOUNT" ] \
  || [ "$(association_row "$EXECUTOR_SERVICE_USER" "$EXECUTOR_PARTITION" | sed 's/|$//')" \
    != "$CLUSTER|$ACCOUNT|$EXECUTOR_SERVICE_USER|$EXECUTOR_PARTITION|$QOS|$QOS" ]; then
  echo "error: executor association readback did not converge" >&2
  exit 1
fi

printf 'converged Slurm account=%s qos=%s executor=%s cluster=%s\n' \
  "$ACCOUNT" "$QOS" "$EXECUTOR_SERVICE_USER" "$CLUSTER"
