#!/usr/bin/env bash
# Install exact Docker-daemon trust and prove a staging registry pull.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_CA="$SCRIPT_DIR/../worker-pools/trial-cache/staging-ca.crt"
REGISTRY_AUTHORITY="192.168.50.103:5443"
REGISTRY_REPO="${REGISTRY_AUTHORITY}/loom-trial-cache"
TARGET_DIR="/etc/docker/certs.d/${REGISTRY_AUTHORITY}"
TARGET_CA="${TARGET_DIR}/ca.crt"
CA_SHA256="539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3"
CANARY_DIGEST="sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e"
temporary=""

cleanup() {
  if [[ -n "$temporary" && -e "$temporary" ]]; then
    rm -f -- "$temporary"
  fi
}
trap cleanup EXIT

if [[ "$#" -ne 0 ]]; then
  echo "usage: sudo $0" >&2
  exit 2
fi
if [[ "$(id -u)" -ne 0 ]]; then
  echo "error: staging trial-cache CA installation requires root" >&2
  exit 1
fi
if [[ ! -f "$SOURCE_CA" || -L "$SOURCE_CA" ]] || \
  [[ "$(sha256sum "$SOURCE_CA" | awk '{print $1}')" != "$CA_SHA256" ]] || \
  ! openssl x509 -in "$SOURCE_CA" -noout -checkend 2592000 >/dev/null; then
  echo "error: reviewed staging trial-cache CA is invalid" >&2
  exit 1
fi

install -d -o root -g root -m 0755 "$TARGET_DIR"
temporary="$(mktemp "${TARGET_DIR}/.ca.crt.XXXXXXXX")"
install -o root -g root -m 0644 "$SOURCE_CA" "$temporary"
mv -fT -- "$temporary" "$TARGET_CA"
temporary=""

canary_image="${REGISTRY_REPO}:transport-canary"
docker pull --quiet "$canary_image" >/dev/null
repo_digests="$(docker image inspect --format '{{json .RepoDigests}}' "$canary_image")"
if [[ "$repo_digests" != *"${REGISTRY_REPO}@${CANARY_DIGEST}"* ]]; then
  echo "error: staging trial-cache canary digest does not match" >&2
  exit 1
fi
printf 'installed staging trial-cache Docker trust and verified pull\n'
