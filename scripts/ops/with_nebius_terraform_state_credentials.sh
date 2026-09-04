#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'with_nebius_terraform_state_credentials: %s\n' "$*" >&2
  exit 2
}

if (($# == 0)); then
  die "a command is required"
fi

if [[ -n ${AWS_ACCESS_KEY_ID:-} || -n ${AWS_SECRET_ACCESS_KEY:-} || -n ${AWS_SESSION_TOKEN:-} || -n ${AWS_PROFILE:-} || -n ${AWS_SHARED_CREDENTIALS_FILE:-} ]]; then
  die "refusing ambient AWS credentials"
fi

keychain_service="${LOOM_NB_STATE_KEYCHAIN_SERVICE:-loom-nebius-terraform-state}"
security_bin="${LOOM_NB_SECURITY_BIN:-security}"
command -v "$security_bin" >/dev/null 2>&1 || die "macOS security command is unavailable"

keychain_record="$($security_bin find-generic-password -s "$keychain_service" 2>&1)" ||
  die "state access key is absent from Keychain service $keychain_service"
state_access_id="$(printf '%s\n' "$keychain_record" | awk -F'"' '/"acct"<blob>=/{print $4; exit}')"
state_access_secret="$($security_bin find-generic-password -s "$keychain_service" -w)" ||
  die "state access secret cannot be read from Keychain service $keychain_service"
[[ -n "$state_access_id" ]] || die "state access key ID is empty"
[[ -n "$state_access_secret" ]] || die "state access secret is empty"

export AWS_ACCESS_KEY_ID="$state_access_id"
export AWS_SECRET_ACCESS_KEY="$state_access_secret"
trap 'unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY state_access_secret state_access_id keychain_record' EXIT HUP INT TERM

"$@"
