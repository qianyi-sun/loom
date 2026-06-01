#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLE_FILE="$ROOT_DIR/.env.example"
LOCAL_FILE="$ROOT_DIR/.env.local"

if [[ ! -f "$EXAMPLE_FILE" ]]; then
  printf 'Missing %s; cannot initialize local environment.\n' "$EXAMPLE_FILE" >&2
  exit 1
fi

if [[ -f "$LOCAL_FILE" ]]; then
  printf 'Using existing %s\n' "$LOCAL_FILE"
  exit 0
fi

cp "$EXAMPLE_FILE" "$LOCAL_FILE"
chmod 600 "$LOCAL_FILE"
printf 'Created %s from .env.example. Edit it for local provider credentials.\n' "$LOCAL_FILE"
