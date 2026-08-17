#!/usr/bin/env bash
# Adopt the staging task-image registry into an authenticated, retained lifecycle.
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_UNIT="$SCRIPT_DIR/loom-staging-trial-cache-registry.service"
SOURCE_CONFIG="$SCRIPT_DIR/loom-staging-trial-cache-registry.yml"
SOURCE_PROXY="$SCRIPT_DIR/loom-staging-trial-cache-registry-nginx.conf"
SOURCE_GC_SCRIPT="$SCRIPT_DIR/../../scripts/ops/task_image_registry_gc_once.py"
SOURCE_GC_UNIT="$SCRIPT_DIR/loom-staging-task-image-registry-gc.service"
SOURCE_GC_TIMER="$SCRIPT_DIR/loom-staging-task-image-registry-gc.timer"
SOURCE_STORAGE_GC="$SCRIPT_DIR/loom-staging-trial-cache-storage-gc"
SOURCE_STORAGE_GC_UNIT="$SCRIPT_DIR/loom-staging-trial-cache-storage-gc.service"
SOURCE_STORAGE_GC_TIMER="$SCRIPT_DIR/loom-staging-trial-cache-storage-gc.timer"
SOURCE_CA="$SCRIPT_DIR/../worker-pools/trial-cache/staging-ca.crt"
UNIT_ROOT="/etc/systemd/system"
TLS_ROOT="/etc/loom/staging-trial-cache-registry"
LIBEXEC_ROOT="/usr/local/libexec"
DATA_ROOT="/data/loom-staging/registry"
LISTEN_IP="192.168.50.103"
CA_SHA256="539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3"
REGISTRY_IMAGE="registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
NGINX_IMAGE="nginx@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10"
BUILDER_DOCKER_CONFIG_OLDLAB="/shared_work/loom/staging-rollout/credentials/task-image-builder-docker/config.json"
BUILDER_DOCKER_CONFIG_GB10="/shared_work2/loom-staging-rollout/credentials/task-image-builder-docker/config.json"
GC_DOCKER_CONFIG="$TLS_ROOT/gc-docker/config.json"

if [[ "$#" -ne 0 ]]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: staging trial-cache registry installation requires root" >&2
  exit 1
fi
if [[ "$(hostname -s)" != "TRT-EAI-OLDLAB-1" ]]; then
  echo "error: staging trial-cache registry is OLDLAB-1-only" >&2
  exit 1
fi
if ! /usr/sbin/ip -4 address show | grep -F "${LISTEN_IP}/" >/dev/null; then
  echo "error: OLDLAB-1 private registry address is unavailable" >&2
  exit 1
fi

for path in "$SOURCE_UNIT" "$SOURCE_CONFIG" "$SOURCE_PROXY" "$SOURCE_GC_SCRIPT" \
  "$SOURCE_GC_UNIT" "$SOURCE_GC_TIMER" "$SOURCE_STORAGE_GC" \
  "$SOURCE_STORAGE_GC_UNIT" "$SOURCE_STORAGE_GC_TIMER" "$SOURCE_CA" \
  "$TLS_ROOT/ca.crt" "$TLS_ROOT/server.crt" "$TLS_ROOT/server.key"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "error: staging trial-cache registry input is unavailable" >&2
    exit 1
  fi
done

secure_root_file() {
  local path="$1"
  if [[ ! -f "$path" || -L "$path" || "$(stat -c '%u:%h:%a' "$path")" != "0:1:600" ]]; then
    echo "error: staging trial-cache private credential metadata is unsafe" >&2
    exit 1
  fi
}
for path in "$TLS_ROOT/server.key" "$TLS_ROOT/builder.htpasswd" \
  "$TLS_ROOT/gc.htpasswd" "$TLS_ROOT/gc-control-plane-token" "$GC_DOCKER_CONFIG"; do
  secure_root_file "$path"
done
for path in "$BUILDER_DOCKER_CONFIG_OLDLAB" "$BUILDER_DOCKER_CONFIG_GB10"; do
  if [[ ! -f "$path" || -L "$path" || "$(stat -c '%h:%a' "$path")" != "1:600" ]]; then
    echo "error: task-image builder Docker credential metadata is unsafe" >&2
    exit 1
  fi
done
if [[ "$(stat -c '%u:%a' "$TLS_ROOT/gc-docker")" != "0:700" ]]; then
  echo "error: registry GC Docker credential directory metadata is unsafe" >&2
  exit 1
fi
if [[ "$(sha256sum "$SOURCE_CA" | awk '{print $1}')" != "$CA_SHA256" ]] || \
  ! cmp --silent "$SOURCE_CA" "$TLS_ROOT/ca.crt"; then
  echo "error: installed trial-cache CA does not match the reviewed public CA" >&2
  exit 1
fi
if ! openssl verify -CAfile "$TLS_ROOT/ca.crt" "$TLS_ROOT/server.crt" >/dev/null || \
  ! openssl x509 -in "$TLS_ROOT/server.crt" -noout -checkip "$LISTEN_IP" >/dev/null || \
  ! openssl x509 -in "$TLS_ROOT/server.crt" -noout -checkend 2592000 >/dev/null; then
  echo "error: installed trial-cache server certificate is invalid or near expiry" >&2
  exit 1
fi
certificate_key="$({ openssl x509 -in "$TLS_ROOT/server.crt" -pubkey -noout; } | sha256sum | awk '{print $1}')"
private_key="$({ openssl pkey -in "$TLS_ROOT/server.key" -pubout; } | sha256sum | awk '{print $1}')"
if [[ "$certificate_key" != "$private_key" ]]; then
  echo "error: installed trial-cache certificate and key do not match" >&2
  exit 1
fi
for image in "$REGISTRY_IMAGE" "$NGINX_IMAGE"; do
  if ! docker image inspect "$image" >/dev/null; then
    echo "error: required pinned registry image is not present locally" >&2
    exit 1
  fi
done

install -d -o root -g root -m 0750 "$DATA_ROOT"
install -o root -g root -m 0644 "$SOURCE_CONFIG" "$TLS_ROOT/registry.yml"
install -o root -g root -m 0644 "$SOURCE_PROXY" "$TLS_ROOT/nginx.conf"
install -o root -g root -m 0755 "$SOURCE_GC_SCRIPT" \
  "$LIBEXEC_ROOT/loom-staging-task-image-registry-gc"
install -o root -g root -m 0755 "$SOURCE_STORAGE_GC" \
  "$LIBEXEC_ROOT/loom-staging-trial-cache-storage-gc"
install -o root -g root -m 0644 "$SOURCE_UNIT" \
  "$UNIT_ROOT/loom-staging-trial-cache-registry.service"
install -o root -g root -m 0644 "$SOURCE_GC_UNIT" \
  "$UNIT_ROOT/loom-staging-task-image-registry-gc.service"
install -o root -g root -m 0644 "$SOURCE_GC_TIMER" \
  "$UNIT_ROOT/loom-staging-task-image-registry-gc.timer"
install -o root -g root -m 0644 "$SOURCE_STORAGE_GC_UNIT" \
  "$UNIT_ROOT/loom-staging-trial-cache-storage-gc.service"
install -o root -g root -m 0644 "$SOURCE_STORAGE_GC_TIMER" \
  "$UNIT_ROOT/loom-staging-trial-cache-storage-gc.timer"
systemctl daemon-reload
systemctl enable loom-staging-trial-cache-registry.service
systemctl restart loom-staging-trial-cache-registry.service
systemctl is-active --quiet loom-staging-trial-cache-registry.service

registry_url="https://${LISTEN_IP}:5443"
anonymous_get_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --cacert "$TLS_ROOT/ca.crt" "$registry_url/v2/")"
anonymous_push_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --request POST --cacert "$TLS_ROOT/ca.crt" \
  "$registry_url/v2/loom-trial-cache/blobs/uploads/")"
if [[ "$anonymous_get_status" != "200" || "$anonymous_push_status" != "401" ]]; then
  echo "error: registry anonymous pull/authenticated mutation boundary is invalid" >&2
  exit 1
fi

docker_auth() {
  python3 - "$1" "$LISTEN_IP:5443" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)["auths"][sys.argv[2]]["auth"]
if not isinstance(value, str) or not value:
    raise SystemExit(1)
print(value)
PY
}
builder_auth="$(docker_auth "$BUILDER_DOCKER_CONFIG_OLDLAB")"
gb10_builder_auth="$(docker_auth "$BUILDER_DOCKER_CONFIG_GB10")"
gc_auth="$(docker_auth "$GC_DOCKER_CONFIG")"
if [[ "$builder_auth" != "$gb10_builder_auth" ]]; then
  echo "error: builder registry credentials differ across architectures" >&2
  exit 1
fi
headers="$(mktemp)"
trap 'rm -f "$headers"' EXIT
builder_upload_status="$(curl --silent --show-error --dump-header "$headers" \
  --output /dev/null --write-out '%{http_code}' --request POST \
  --cacert "$TLS_ROOT/ca.crt" --config - \
  "$registry_url/v2/loom-trial-cache/blobs/uploads/" \
  <<<"header = \"Authorization: Basic ${builder_auth}\"")"
upload_location="$(awk 'BEGIN{IGNORECASE=1} /^Location:/ {sub(/\r$/, "", $2); print $2}' "$headers" | tail -1)"
if [[ "$builder_upload_status" != "202" || -z "$upload_location" ]]; then
  echo "error: builder registry credentials cannot create an upload" >&2
  exit 1
fi
if [[ "$upload_location" == /* ]]; then
  upload_location="$registry_url$upload_location"
fi
gc_delete_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --request DELETE --cacert "$TLS_ROOT/ca.crt" --config - "$upload_location" \
  <<<"header = \"Authorization: Basic ${gc_auth}\"")"
if [[ "$gc_delete_status" != "204" ]]; then
  echo "error: GC registry credentials cannot delete an upload" >&2
  exit 1
fi
rm -f "$headers"
trap - EXIT

systemctl enable --now loom-staging-task-image-registry-gc.timer
systemctl enable --now loom-staging-trial-cache-storage-gc.timer
systemctl start loom-staging-task-image-registry-gc.service
systemctl is-active --quiet loom-staging-task-image-registry-gc.timer
systemctl is-active --quiet loom-staging-trial-cache-storage-gc.timer
printf 'installed authenticated staging trial-cache registry and retention services\n'
