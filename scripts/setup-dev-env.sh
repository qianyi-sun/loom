#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLE_FILE="$ROOT_DIR/.env.example"
LOCAL_FILE="$ROOT_DIR/.env.local"
DEFAULT_SANDBOX_HOST_WORKSPACE_ROOT="$ROOT_DIR/.runtime/sandbox-workspaces"

ensure_sandbox_host_workspace_root() {
  local tmp_file
  tmp_file="$(mktemp)"
  awk -v replacement="$DEFAULT_SANDBOX_HOST_WORKSPACE_ROOT" '
    BEGIN { seen = 0 }
    /^SANDBOX_HOST_WORKSPACE_ROOT=/ {
      value = $0
      sub(/^SANDBOX_HOST_WORKSPACE_ROOT=/, "", value)
      if (value ~ /[^[:space:]]/) {
        print $0
      } else {
        print "SANDBOX_HOST_WORKSPACE_ROOT=" replacement
      }
      seen = 1
      next
    }
    { print }
    END {
      if (!seen) {
        print "SANDBOX_HOST_WORKSPACE_ROOT=" replacement
      }
    }
  ' "$LOCAL_FILE" > "$tmp_file"
  mv "$tmp_file" "$LOCAL_FILE"
  chmod 600 "$LOCAL_FILE"
}

if [[ ! -f "$EXAMPLE_FILE" ]]; then
  printf 'Missing %s; cannot initialize local environment.\n' "$EXAMPLE_FILE" >&2
  exit 1
fi

if [[ -f "$LOCAL_FILE" ]]; then
  ensure_sandbox_host_workspace_root
  printf 'Using existing %s\n' "$LOCAL_FILE"
  exit 0
fi

cp "$EXAMPLE_FILE" "$LOCAL_FILE"
chmod 600 "$LOCAL_FILE"
ensure_sandbox_host_workspace_root
printf 'Created %s from .env.example. Edit it for local provider credentials.\n' "$LOCAL_FILE"
