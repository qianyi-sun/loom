#!/usr/bin/env bash
# Install the non-secret runtime root and pinned kubectl on the GB10 controller.
set -euo pipefail

CONTROLLER="gx10-01c7"
CLUSTER="trt-gb10"
SERVICE_USER="loom-rollout"
SERVICE_UID="995"
SERVICE_GID="2007"
RUNTIME_ROOT="/opt/loom-staging-runner"
STATE_ROOT="/var/lib/loom-staging-rollout"
KUBECONFIG_PATH="$STATE_ROOT/kubeconfig"
KUBECTL_VERSION="v1.36.2"
KUBECTL_SHA256="c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e"
KUBECTL_URL="https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/arm64/kubectl"
UV_VERSION="0.11.26"
UV_SHA256="befa1a59c91e96eb601b0fd9a97c03dd666f17baba644b2b4db9c59a767e387e"
UV_ARCHIVE="uv-aarch64-unknown-linux-gnu.tar.gz"
UV_URL="https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$UV_ARCHIVE"

if [ "$#" -ne 0 ]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "error: GB10 autoscaler-controller installation requires root" >&2
  exit 1
fi
if [ "$(uname -m)" != "aarch64" ] || [ "$(hostname -s)" != "$CONTROLLER" ]; then
  echo "error: autoscaler-controller installation is restricted to GB10-1" >&2
  exit 1
fi
slurm_config="$(scontrol show config)"
if ! grep -E \
  "^ClusterName[[:space:]]*=[[:space:]]*$CLUSTER$" \
  <<<"$slurm_config" >/dev/null; then
  echo "error: local Slurm cluster does not match GB10" >&2
  exit 1
fi
if [ "$(id -u "$SERVICE_USER")" != "$SERVICE_UID" ] \
  || [ "$(id -g "$SERVICE_USER")" != "$SERVICE_GID" ]; then
  echo "error: GB10 Slurm service identity is not installed" >&2
  exit 1
fi

temporary_dir="$(mktemp -d)"
trap 'rm -rf -- "$temporary_dir"' EXIT
curl --fail --location --silent --show-error \
  --output "$temporary_dir/kubectl" "$KUBECTL_URL"
printf '%s  %s\n' "$KUBECTL_SHA256" "$temporary_dir/kubectl" \
  | sha256sum --check >/dev/null
install -o root -g root -m 0755 "$temporary_dir/kubectl" /usr/local/bin/kubectl

curl --fail --location --silent --show-error \
  --output "$temporary_dir/$UV_ARCHIVE" "$UV_URL"
printf '%s  %s\n' "$UV_SHA256" "$temporary_dir/$UV_ARCHIVE" \
  | sha256sum --check >/dev/null
tar --extract --gzip --file "$temporary_dir/$UV_ARCHIVE" \
  --directory "$temporary_dir" \
  "uv-aarch64-unknown-linux-gnu/uv"
install -o root -g root -m 0755 \
  "$temporary_dir/uv-aarch64-unknown-linux-gnu/uv" /usr/local/bin/uv

install -d -o root -g root -m 0755 "$RUNTIME_ROOT" "$RUNTIME_ROOT/candidates"
install -d -o "$SERVICE_UID" -g "$SERVICE_GID" -m 0750 \
  "$STATE_ROOT" "$STATE_ROOT/.config" "$STATE_ROOT/.config/systemd" \
  "$STATE_ROOT/.config/systemd/user"
if [ -e "$KUBECONFIG_PATH" ] \
  && [ "$(stat -c '%u:%g:%a:%F' "$KUBECONFIG_PATH")" \
    != "$SERVICE_UID:$SERVICE_GID:600:regular file" ]; then
  echo "error: existing autoscaler kubeconfig metadata is unsafe" >&2
  exit 1
fi

installed_version="$(/usr/local/bin/kubectl version --client -o json \
  | awk -F'"' '/"gitVersion"/ { print $4; exit }')"
if [ "$installed_version" != "$KUBECTL_VERSION" ]; then
  echo "error: kubectl version readback failed" >&2
  exit 1
fi
installed_uv_version="$(/usr/local/bin/uv --version)"
if [ "$installed_uv_version" != "uv $UV_VERSION" ]; then
  echo "error: uv version readback failed" >&2
  exit 1
fi
printf 'installed GB10 autoscaler controller runtime kubectl=%s uv=%s state=%s\n' \
  "$KUBECTL_VERSION" "$UV_VERSION" "$STATE_ROOT"
