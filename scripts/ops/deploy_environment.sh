#!/usr/bin/env bash
set -euo pipefail

: "${LOOM_DEPLOY_ENVIRONMENT:?set by the deploy-environment workflow}"
: "${LOOM_IMAGE_TAG:?set by the deploy-environment workflow}"
: "${LOOM_KUBECONFIG_B64:?environment-scoped GitHub secret is required}"
: "${LOOM_CLUSTER_CONFIG_B64:?environment-scoped GitHub secret is required}"
: "${LOOM_DEPLOY_TOKEN:?environment-scoped GitHub secret is required}"

case "${LOOM_DEPLOY_ENVIRONMENT}" in
  development|staging|production)
    ;;
  *)
    echo "Unsupported LOOM_DEPLOY_ENVIRONMENT=${LOOM_DEPLOY_ENVIRONMENT}" >&2
    exit 2
    ;;
esac

umask 077
tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

kubeconfig="${tmpdir}/kubeconfig"
cluster_config="${tmpdir}/cluster-config.toml"

printf '%s' "${LOOM_KUBECONFIG_B64}" | base64 --decode > "${kubeconfig}"
printf '%s' "${LOOM_CLUSTER_CONFIG_B64}" | base64 --decode > "${cluster_config}"

export KUBECONFIG="${kubeconfig}"

uv python install 3.11
uv sync --locked --extra cluster --python 3.11
uv pip check --python .venv/bin/python

uv run --no-sync python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml

evidence_dir="${LOOM_ROLLOUT_EVIDENCE_DIR:-rollout-evidence}"
mkdir -p "${evidence_dir}"

uv run --no-sync python - "${cluster_config}" "${LOOM_IMAGE_TAG}" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
image_tag = sys.argv[2]
text = path.read_text(encoding="utf-8")
replacement = f'image_tag = "{image_tag}"'
if re.search(r'(?m)^image_tag\s*=', text):
    text = re.sub(r'(?m)^image_tag\s*=.*$', replacement, text)
else:
    text = replacement + "\n" + text
path.write_text(text, encoding="utf-8")
PY

uv run --no-sync loom cluster audit --config "${cluster_config}"
uv run --no-sync loom cluster render --config "${cluster_config}" > "${tmpdir}/rendered.yaml"
cp "${tmpdir}/rendered.yaml" "${evidence_dir}/rendered.yaml"

manifest_args=(
  cluster
  release-manifest
  --config "${cluster_config}"
  --environment "${LOOM_DEPLOY_ENVIRONMENT}"
  --image-tag "${LOOM_IMAGE_TAG}"
  --git-sha "${GITHUB_SHA:-$(git rev-parse HEAD)}"
  --output "${evidence_dir}/release-manifest-${LOOM_IMAGE_TAG}.json"
)
environment_state_file="deploy/environment-state/${LOOM_DEPLOY_ENVIRONMENT}.toml"
if [[ -f "${environment_state_file}" ]]; then
  manifest_args+=(
    --environment-state-file "${environment_state_file}"
    --env-config-version "${LOOM_ENV_CONFIG_VERSION:-${LOOM_IMAGE_TAG}}"
  )
fi
if [[ -n "${LOOM_EXPECTED_IMAGE_IDENTITIES_JSON:-}" ]]; then
  manifest_args+=(
    --expected-image-identities-json "${LOOM_EXPECTED_IMAGE_IDENTITIES_JSON}"
  )
fi
uv run --no-sync loom "${manifest_args[@]}"

if [[ "${LOOM_DRY_RUN:-false}" == "true" ]]; then
  echo "Dry run complete for ${LOOM_DEPLOY_ENVIRONMENT}; rendered manifests were not applied."
  exit 0
fi

if [[ "${LOOM_DEPLOY_ENVIRONMENT}" != "development" && -z "${LOOM_ROLLOUT_LOCK_DIR:-}" ]]; then
  echo "LOOM_ROLLOUT_LOCK_DIR is required for ${LOOM_DEPLOY_ENVIRONMENT} deploys." >&2
  echo "Set it to the shared protected-environment rollout lock directory." >&2
  exit 2
fi

cluster_up_args=(
  cluster
  up
  --config "${cluster_config}"
  --timeout 900
  --environment "${LOOM_DEPLOY_ENVIRONMENT}"
  --rollout-id "${LOOM_IMAGE_TAG}"
  --rollout-lock-evidence "${evidence_dir}/rollout-mutation-lock-${LOOM_IMAGE_TAG}.json"
)
if [[ -n "${LOOM_ROLLOUT_LOCK_DIR:-}" ]]; then
  cluster_up_args+=(--rollout-lock-dir "${LOOM_ROLLOUT_LOCK_DIR}")
fi
if [[ "${LOOM_FORCE_ROLLOUT_LOCK:-false}" == "true" ]]; then
  cluster_up_args+=(--force-rollout-lock)
fi
uv run --no-sync loom "${cluster_up_args[@]}"
namespace="$(uv run --no-sync python - "${cluster_config}" <<'PY'
import sys
import tomllib
from pathlib import Path

print(tomllib.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["namespace"])
PY
)"

if [[ "${LOOM_RELEASE_GATE_HARD_CHECKS:-false}" == "true" ]]; then
  uv run --no-sync loom admin gb10-workers status \
    --environment "${LOOM_DEPLOY_ENVIRONMENT}" \
    --release-image-tag "${LOOM_IMAGE_TAG}" \
    --release-env-config-version "${LOOM_ENV_CONFIG_VERSION:-${LOOM_IMAGE_TAG}}" \
    --format json \
    > "${evidence_dir}/gb10-workers-status-${LOOM_IMAGE_TAG}.json"

  uv run --no-sync loom cluster minio-storage-preflight \
    --namespace "${namespace}" \
    --output "${evidence_dir}/minio-storage-preflight-${LOOM_IMAGE_TAG}.json" \
    --format json

  release_gate_common_args=(
    cluster
    release-gate
    --manifest "${evidence_dir}/release-manifest-${LOOM_IMAGE_TAG}.json"
    --config "${cluster_config}"
    --rendered-manifest "${evidence_dir}/rendered.yaml"
    --namespace "${namespace}"
    --environment "${LOOM_DEPLOY_ENVIRONMENT}"
    --minio-storage-preflight "${evidence_dir}/minio-storage-preflight-${LOOM_IMAGE_TAG}.json"
    --gb10-workers-status "${evidence_dir}/gb10-workers-status-${LOOM_IMAGE_TAG}.json"
  )
  if [[ -n "${LOOM_ENVIRONMENT_STATE_CHECK_JSON:-}" ]]; then
    release_gate_common_args+=(
      --environment-state-check "${LOOM_ENVIRONMENT_STATE_CHECK_JSON}"
    )
  fi
  uv run --no-sync loom "${release_gate_common_args[@]}" --format json \
    > "${evidence_dir}/release-gate-${LOOM_IMAGE_TAG}.json"
  uv run --no-sync loom "${release_gate_common_args[@]}" --format markdown \
    > "${evidence_dir}/release-gate-${LOOM_IMAGE_TAG}.md"
fi

uv run --no-sync loom cluster status --namespace "${namespace}" --format table
