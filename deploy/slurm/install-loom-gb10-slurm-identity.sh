#!/usr/bin/env bash
# Converge the fixed service identity used by Loom Slurm jobs on GB10.
#
# Run this exact candidate-owned script as root on each trt-gb10-1..15 node.
# Pass --controller only on trt-gb10-1; that additionally enables the service
# account's persistent systemd user manager for the controller-local autoscaler.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD_INSTALLER="$SCRIPT_DIR/install-loom-slurm-job-cgroup-guard.sh"

SERVICE_USER="loom-rollout"
SERVICE_UID="995"
SHARED_GROUP="sharedwork"
SHARED_GID="2007"
DOCKER_GROUP="docker"
SERVICE_HOME="/var/lib/loom-rollout"
SERVICE_SHELL="/usr/sbin/nologin"
SHARED_ROOT="/shared_work2/loom-staging-rollout"
WORKER_REPO_ROOT="$SHARED_ROOT/worker-repos"
WORKER_ENV_ROOT="$SHARED_ROOT/worker-envs"
JOB_OUTPUT_ROOT="$SHARED_ROOT/job-output"
LINGER_PATH="/var/lib/systemd/linger/$SERVICE_USER"

controller=false
case "${1:-}" in
  --controller)
    controller=true
    shift
    ;;
esac

node="${1:-}"
if [ "$#" -ne 1 ]; then
  echo "usage: sudo $0 [--controller] trt-gb10-N" >&2
  exit 2
fi
case "$node" in
  trt-gb10-[1-9]|trt-gb10-1[0-5]) ;;
  *)
    echo "error: node must be one of trt-gb10-1 through trt-gb10-15" >&2
    exit 2
    ;;
esac
if [ "$controller" = true ] && [ "$node" != "trt-gb10-1" ]; then
  echo "error: --controller is restricted to trt-gb10-1" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "error: GB10 Slurm identity installation requires root" >&2
  exit 1
fi
if [ "$(uname -m)" != "aarch64" ]; then
  echo "error: GB10 Slurm identity installation requires aarch64" >&2
  exit 1
fi

node_record="$(scontrol show node "$node" 2>/dev/null)"
node_address="$(printf '%s\n' "$node_record" | awk '
  {
    for (field = 1; field <= NF; field++) {
      if ($field ~ /^NodeAddr=/) {
        sub(/^NodeAddr=/, "", $field)
        print $field
        exit
      }
    }
  }
')"
if [ -z "$node_address" ] \
  || ! hostname -I | tr ' ' '\n' | grep -qxF "$node_address"; then
  echo "error: Slurm NodeAddr does not belong to this physical host" >&2
  exit 1
fi

shared_record="$(getent group "$SHARED_GROUP" || true)"
shared_gid="$(printf '%s' "$shared_record" | cut -d: -f3)"
if [ "$shared_gid" != "$SHARED_GID" ] || [ "$(getent group "$SHARED_GID")" != "$shared_record" ]; then
  echo "error: shared group identity is invalid" >&2
  exit 1
fi
if ! getent group "$DOCKER_GROUP" >/dev/null; then
  echo "error: Docker group identity is unavailable" >&2
  exit 1
fi

service_record="$(getent passwd "$SERVICE_USER" || true)"
uid_record="$(getent passwd "$SERVICE_UID" || true)"
if [ -z "$service_record" ]; then
  if [ -n "$uid_record" ]; then
    echo "error: service UID is already owned by another identity" >&2
    exit 1
  fi
  useradd \
    --system \
    --uid "$SERVICE_UID" \
    --gid "$SHARED_GROUP" \
    --groups "$DOCKER_GROUP" \
    --home-dir "$SERVICE_HOME" \
    --shell "$SERVICE_SHELL" \
    --no-create-home \
    "$SERVICE_USER"
else
  service_uid="$(printf '%s' "$service_record" | cut -d: -f3)"
  service_gid="$(printf '%s' "$service_record" | cut -d: -f4)"
  service_home="$(printf '%s' "$service_record" | cut -d: -f6)"
  service_shell="$(printf '%s' "$service_record" | cut -d: -f7)"
  if [ "$service_uid" != "$SERVICE_UID" ] \
    || [ "$service_gid" != "$SHARED_GID" ] \
    || [ "$service_home" != "$SERVICE_HOME" ] \
    || [ "$service_shell" != "$SERVICE_SHELL" ] \
    || [ "$uid_record" != "$service_record" ]; then
    echo "error: existing service identity conflicts with the GB10 contract" >&2
    exit 1
  fi
fi

# Reconcile only the one required supplementary privilege. Slurm launches the
# job with the account's current group vector on every compute node.
usermod --gid "$SHARED_GROUP" --append --groups "$DOCKER_GROUP" "$SERVICE_USER"
if [ "$(id -u "$SERVICE_USER")" != "$SERVICE_UID" ] \
  || [ "$(id -g "$SERVICE_USER")" != "$SHARED_GID" ] \
  || ! id -nG "$SERVICE_USER" | tr ' ' '\n' | grep -qxF "$DOCKER_GROUP"; then
  echo "error: service identity did not converge" >&2
  exit 1
fi

install -d -o "$SERVICE_UID" -g "$SHARED_GID" -m 0750 "$SERVICE_HOME"
if [ -L "$SHARED_ROOT" ] || [ -L "$WORKER_REPO_ROOT" ]; then
  echo "error: shared worker authority contains a symlink" >&2
  exit 1
fi
if [ "$(stat -c '%u:%g:%a:%F' "$SHARED_ROOT")" != "$SERVICE_UID:$SHARED_GID:2750:directory" ] \
  || [ "$(stat -c '%u:%g:%a:%F' "$WORKER_REPO_ROOT")" != "$SERVICE_UID:$SHARED_GID:2750:directory" ]; then
  echo "error: shared worker authority metadata is invalid" >&2
  exit 1
fi
install -d -o "$SERVICE_UID" -g "$SHARED_GID" -m 2750 "$WORKER_ENV_ROOT"
install -d -o "$SERVICE_UID" -g "$SHARED_GID" -m 2750 "$JOB_OUTPUT_ROOT"

if [ ! -x "$GUARD_INSTALLER" ]; then
  echo "error: repo-owned Slurm containment guard installer is unavailable" >&2
  exit 1
fi
"$GUARD_INSTALLER" "$node"

if [ "$controller" = true ]; then
  install -d -o root -g root -m 0755 "$(dirname "$LINGER_PATH")"
  if ! loginctl enable-linger "$SERVICE_USER"; then
    # Docker-group recovery runs in a container namespace. Enter the real host
    # namespaces for the logind RPC; a normal root invocation never needs this
    # fallback. The bootstrap container must be explicitly --privileged.
    nsenter --target 1 --mount --uts --ipc --net --pid -- \
      /usr/bin/loginctl enable-linger "$SERVICE_USER"
  fi
  linger_state="$(loginctl show-user "$SERVICE_USER" --property=Linger --value 2>/dev/null \
    || nsenter --target 1 --mount --uts --ipc --net --pid -- \
      /usr/bin/loginctl show-user "$SERVICE_USER" --property=Linger --value)"
  if [ "$linger_state" != "yes" ] \
    || [ ! -f "$LINGER_PATH" ]; then
    echo "error: controller service identity did not acquire linger" >&2
    exit 1
  fi
  systemctl start "user@$SERVICE_UID.service"
  if [ "$(systemctl is-active "user@$SERVICE_UID.service")" != "active" ]; then
    echo "error: controller service user manager is not active" >&2
    exit 1
  fi
fi

printf 'converged %s uid=%s gid=%s node=%s controller=%s\n' \
  "$SERVICE_USER" "$SERVICE_UID" "$SHARED_GID" "$node" "$controller"
