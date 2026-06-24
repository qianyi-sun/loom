#!/usr/bin/env bash
set -euo pipefail

missing=()
for name in LOOM_CANDIDATE_SHA LOOM_IMAGE_TAG LOOM_RELEASE_GATE_RUN_ID GH_TOKEN; do
  if [[ -z "${!name:-}" ]]; then
    missing+=("${name}")
  fi
done
if (( ${#missing[@]} > 0 )); then
  printf 'error: production deploy requires release gate inputs: %s\n' "${missing[*]}" >&2
  exit 1
fi

if ! [[ "${LOOM_CANDIDATE_SHA}" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'error: LOOM_CANDIDATE_SHA must be a 40-character lowercase git SHA\n' >&2
  exit 1
fi

if ! git merge-base --is-ancestor "${LOOM_CANDIDATE_SHA}" HEAD; then
  printf 'error: candidate SHA %s is not an ancestor of production ref %s\n' \
    "${LOOM_CANDIDATE_SHA}" "$(git rev-parse HEAD)" >&2
  exit 1
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "${tmpdir}"' EXIT

gh run download "${LOOM_RELEASE_GATE_RUN_ID}" \
  --name release-gate-evidence \
  --dir "${tmpdir}"

manifest="${tmpdir}/release-gate-evidence.json"
if [[ ! -f "${manifest}" ]]; then
  manifest=$(find "${tmpdir}" -name release-gate-evidence.json -type f -print -quit)
fi
if [[ -z "${manifest}" || ! -f "${manifest}" ]]; then
  printf 'error: release gate artifact did not contain release-gate-evidence.json\n' >&2
  exit 1
fi

uv run python scripts/ops/release_gate.py verify-production \
  --manifest "${manifest}" \
  --candidate-sha "${LOOM_CANDIDATE_SHA}" \
  --image-tag "${LOOM_IMAGE_TAG}"
