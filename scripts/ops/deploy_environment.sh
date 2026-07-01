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
uv sync --extra cluster --python 3.11

uv run python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml

evidence_dir="${LOOM_ROLLOUT_EVIDENCE_DIR:-rollout-evidence}"
mkdir -p "${evidence_dir}"

uv run python - "${cluster_config}" "${LOOM_IMAGE_TAG}" <<'PY'
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

uv run loom cluster audit --config "${cluster_config}"
uv run loom cluster render --config "${cluster_config}" > "${tmpdir}/rendered.yaml"
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
uv run loom "${manifest_args[@]}"

if [[ "${LOOM_DRY_RUN:-false}" == "true" ]]; then
  echo "Dry run complete for ${LOOM_DEPLOY_ENVIRONMENT}; rendered manifests were not applied."
  exit 0
fi

uv run loom cluster up --config "${cluster_config}" --timeout 900
uv run loom cluster status --namespace "$(uv run python - "${cluster_config}" <<'PY'
import sys
import tomllib
from pathlib import Path

print(tomllib.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["namespace"])
PY
)" --format table
