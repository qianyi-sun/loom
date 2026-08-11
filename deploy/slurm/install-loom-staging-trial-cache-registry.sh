#!/usr/bin/env bash
# Adopt the staging task-image registry into a root-owned systemd lifecycle.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_UNIT="$SCRIPT_DIR/loom-staging-trial-cache-registry.service"
SOURCE_CA="$SCRIPT_DIR/../worker-pools/trial-cache/staging-ca.crt"
UNIT_DST="/etc/systemd/system/loom-staging-trial-cache-registry.service"
TLS_ROOT="/etc/loom/staging-trial-cache-registry"
DATA_ROOT="/data/loom-staging/registry"
LISTEN_IP="192.168.50.103"
CA_SHA256="539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3"
REGISTRY_IMAGE="registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"

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
for path in "$SOURCE_UNIT" "$SOURCE_CA" "$TLS_ROOT/ca.crt" \
  "$TLS_ROOT/server.crt" "$TLS_ROOT/server.key"; do
  if [[ ! -f "$path" || -L "$path" ]]; then
    echo "error: staging trial-cache registry input is unavailable" >&2
    exit 1
  fi
done
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
if ! docker image inspect "$REGISTRY_IMAGE" >/dev/null; then
  echo "error: required registry:2 image is not present locally" >&2
  exit 1
fi

install -d -o root -g root -m 0750 "$DATA_ROOT"
install -o root -g root -m 0644 "$SOURCE_UNIT" "$UNIT_DST"
systemctl daemon-reload
systemctl enable --now loom-staging-trial-cache-registry.service
systemctl is-active --quiet loom-staging-trial-cache-registry.service
curl --fail --silent --show-error \
  --cacert "$TLS_ROOT/ca.crt" "https://${LISTEN_IP}:5443/v2/" >/dev/null
printf 'installed staging trial-cache registry service\n'
