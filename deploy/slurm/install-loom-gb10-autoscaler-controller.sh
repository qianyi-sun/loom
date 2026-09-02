#!/usr/bin/env bash
# Install the non-secret runtime root and pinned kubectl on the GB10 controller.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(/usr/bin/dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd -P)"
SOURCE_VERIFIER="$REPO_ROOT/scripts/ops/install_gb10_autoscaler_controller.py"
TRUSTED_SOURCE_ROOT="/opt/loom-gb10-controller-bootstrap"
KUBECTL_VERSION="v1.36.2"
KUBECTL_SHA256="c957eb8c4bea27a3bb35b269edd9082e27f027f7b76b20b5bf4afebc726c6d3e"
KUBECTL_URL="https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/arm64/kubectl"
UV_VERSION="0.11.26"
UV_SHA256="befa1a59c91e96eb601b0fd9a97c03dd666f17baba644b2b4db9c59a767e387e"
UV_ARCHIVE="uv-aarch64-unknown-linux-gnu.tar.gz"
UV_URL="https://github.com/astral-sh/uv/releases/download/$UV_VERSION/$UV_ARCHIVE"
CONTROLLER_PUBLIC_KEY=""
LEGACY_DEPLOY_PUBLIC_KEY=""
SOURCE_SHA=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --controller-public-key)
      if [ "$#" -lt 2 ] || [ -n "$CONTROLLER_PUBLIC_KEY" ]; then
        echo "error: controller public key argument is invalid" >&2
        exit 2
      fi
      CONTROLLER_PUBLIC_KEY="$2"
      shift 2
      ;;
    --legacy-deploy-public-key)
      if [ "$#" -lt 2 ] || [ -n "$LEGACY_DEPLOY_PUBLIC_KEY" ]; then
        echo "error: legacy deploy public key argument is invalid" >&2
        exit 2
      fi
      LEGACY_DEPLOY_PUBLIC_KEY="$2"
      shift 2
      ;;
    --source-sha)
      if [ "$#" -lt 2 ] || [ -n "$SOURCE_SHA" ]; then
        echo "error: source SHA argument is invalid" >&2
        exit 2
      fi
      SOURCE_SHA="$2"
      shift 2
      ;;
    *)
      echo "usage: sudo /usr/local/libexec/loom-gb10-controller-bootstrap --source-sha COMMIT --controller-public-key /absolute/path.pub --legacy-deploy-public-key /absolute/path.pub" >&2
      exit 2
      ;;
  esac
done
if [ -z "$CONTROLLER_PUBLIC_KEY" ] \
  || [[ "$CONTROLLER_PUBLIC_KEY" != /* ]] \
  || [ ! -f "$CONTROLLER_PUBLIC_KEY" ] \
  || [ -z "$LEGACY_DEPLOY_PUBLIC_KEY" ] \
  || [[ "$LEGACY_DEPLOY_PUBLIC_KEY" != /* ]] \
  || [ ! -f "$LEGACY_DEPLOY_PUBLIC_KEY" ] \
  || [ ! -f "$SOURCE_VERIFIER" ] \
  || [ -L "$SOURCE_VERIFIER" ]; then
  echo "error: controller broker installation input is unavailable" >&2
  exit 2
fi
if [[ ! "$SOURCE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: exact source SHA argument is invalid" >&2
  exit 2
fi
if [ "$(/usr/bin/id -u)" -ne 0 ]; then
  echo "error: GB10 autoscaler-controller installation requires root" >&2
  exit 1
fi
if [ "${LOOM_GB10_TRUSTED_BOOTSTRAP:-}" != "1" ]; then
  echo "error: controller installer requires the trusted root bootstrap" >&2
  exit 1
fi
if [ "$REPO_ROOT" != "$TRUSTED_SOURCE_ROOT/$SOURCE_SHA" ]; then
  echo "error: controller installer source is outside the trusted root" >&2
  exit 1
fi
/usr/bin/python3 -I "$SOURCE_VERIFIER" verify-source \
  --trusted-root "$TRUSTED_SOURCE_ROOT" \
  --source-root "$REPO_ROOT" \
  --source-sha "$SOURCE_SHA" >/dev/null
/usr/bin/python3 -I "$SOURCE_VERIFIER" verify-host >/dev/null

temporary_dir="$(/usr/bin/mktemp -d --tmpdir=/var/tmp loom-gb10-controller.XXXXXXXXXX)"
cleanup_temporary_dir() {
  /usr/bin/rm -rf -- "$temporary_dir" || true
}
trap cleanup_temporary_dir EXIT
/usr/bin/curl --fail --location --silent --show-error \
  --output "$temporary_dir/kubectl" "$KUBECTL_URL"
printf '%s  %s\n' "$KUBECTL_SHA256" "$temporary_dir/kubectl" \
  | /usr/bin/sha256sum --check >/dev/null
/usr/bin/chmod 0700 "$temporary_dir/kubectl"

/usr/bin/curl --fail --location --silent --show-error \
  --output "$temporary_dir/$UV_ARCHIVE" "$UV_URL"
printf '%s  %s\n' "$UV_SHA256" "$temporary_dir/$UV_ARCHIVE" \
  | /usr/bin/sha256sum --check >/dev/null
/usr/bin/tar --extract --gzip --to-stdout --file "$temporary_dir/$UV_ARCHIVE" \
  "uv-aarch64-unknown-linux-gnu/uv" >"$temporary_dir/uv"
/usr/bin/chmod 0700 "$temporary_dir/uv"

/usr/bin/python3 -I "$SOURCE_VERIFIER" install \
  --trusted-root "$TRUSTED_SOURCE_ROOT" \
  --source-root "$REPO_ROOT" \
  --source-sha "$SOURCE_SHA" \
  --kubectl-source "$temporary_dir/kubectl" \
  --uv-source "$temporary_dir/uv" \
  --controller-public-key "$CONTROLLER_PUBLIC_KEY" \
  --legacy-public-key "$LEGACY_DEPLOY_PUBLIC_KEY"
