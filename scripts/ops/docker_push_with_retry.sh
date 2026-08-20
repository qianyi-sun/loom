#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
  echo "usage: docker_push_with_retry.sh IMAGE" >&2
  exit 2
fi

target=$1
max_attempts=3
retry_delays=(5 15)

for (( attempt = 1; attempt <= max_attempts; attempt++ )); do
  set +e
  push_output=$(docker push "$target" 2>&1)
  push_status=$?
  set -e

  if (( push_status == 0 )); then
    printf '%s\n' "$push_output"
    exit 0
  fi

  printf 'docker push attempt %d/%d failed with exit %d for %s\n' \
    "$attempt" "$max_attempts" "$push_status" "$target" >&2
  printf '%s\n' "$push_output" >&2
  if (( attempt == max_attempts )); then
    exit "$push_status"
  fi

  delay=${retry_delays[attempt - 1]}
  printf 'retrying Docker push in %d seconds\n' "$delay" >&2
  sleep "$delay"
done

echo "FAIL: unreachable Docker push retry state" >&2
exit 1
