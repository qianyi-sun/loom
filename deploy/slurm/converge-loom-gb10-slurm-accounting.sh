#!/usr/bin/env bash
# Converge the bounded SlurmDBD identity used by the GB10 Loom autoscaler.
set -euo pipefail

CLUSTER="trt-gb10"
CONTROLLER="gx10-01c7"
SERVICE_USER="loom-rollout"
SERVICE_UID="995"
SERVICE_GID="2007"
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
if ! printf '%s\n' "$slurm_config" | grep -Eq "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" \
  || ! printf '%s\n' "$slurm_config" | grep -Eq \
    "^SlurmctldHost\[0\][[:space:]]*=[[:space:]]*$CONTROLLER\(192[.]168[.]20[.]11\)$"; then
  echo "error: local Slurm authority does not match the GB10 controller" >&2
  exit 1
fi
if [ "$(id -u "$SERVICE_USER")" != "$SERVICE_UID" ] \
  || [ "$(id -g "$SERVICE_USER")" != "$SERVICE_GID" ]; then
  echo "error: GB10 Slurm service identity is not installed" >&2
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
  timeout 30 sacctmgr --noheader --parsable2 show association where \
    "cluster=$CLUSTER" "account=$ACCOUNT" "user=$SERVICE_USER" \
    format=Cluster,Account,User,QOS,DefaultQOS </dev/null
}

user_row() {
  timeout 30 sacctmgr --noheader --parsable2 show user where \
    "name=$SERVICE_USER" format=User,DefaultAccount </dev/null
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
association_before="$(association_row)"
user_before="$(user_row)"
if [ -n "$user_before" ] \
  && [ "${user_before%|}" != "$SERVICE_USER|$ACCOUNT" ]; then
  echo "error: existing Loom Slurm user conflicts with the fixed contract" >&2
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
if [ -z "$user_before" ]; then
  sacctmgr --immediate add user name="$SERVICE_USER" account="$ACCOUNT" \
    cluster="$CLUSTER" defaultaccount="$ACCOUNT"
fi
sacctmgr --immediate modify user where name="$SERVICE_USER" \
  account="$ACCOUNT" cluster="$CLUSTER" set \
  defaultaccount="$ACCOUNT" qos="$QOS" defaultqos="$QOS"

if [ "$(account_row | sed 's/|$//')" != "$ACCOUNT|loom staging external workers|loom" ]; then
  echo "error: account readback did not converge" >&2
  exit 1
fi
if [ "$(qos_row | sed 's/|$//')" != "$QOS|DenyOnLimit|15|15" ]; then
  echo "error: QoS readback did not converge" >&2
  exit 1
fi
if [ "$(association_row | sed 's/|$//')" != "$CLUSTER|$ACCOUNT|$SERVICE_USER|$QOS|$QOS" ] \
  || [ "$(user_row | sed 's/|$//')" != "$SERVICE_USER|$ACCOUNT" ]; then
  echo "error: association readback did not converge" >&2
  exit 1
fi

printf 'converged Slurm account=%s qos=%s user=%s cluster=%s\n' \
  "$ACCOUNT" "$QOS" "$SERVICE_USER" "$CLUSTER"
