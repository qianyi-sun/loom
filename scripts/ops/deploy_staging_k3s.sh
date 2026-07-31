#!/usr/bin/env bash
#
# Sanctioned, repeatable deploy of the loom-staging stack to the multi-node
# OLDLAB k3s cluster. This codifies the hand cutover into one reproducible path
# so staging no longer depends on ad-hoc operator commands (the interim deploy
# engine — option "B" — until the #1097 reconciler takes over).
#
#   Usage:  scripts/ops/deploy_staging_k3s.sh <git-sha> [--skip-build]
#
# Run from a clean repo checkout at (or containing) <git-sha>, as root on the
# k3s control-plane host (bb8-1). See docs/runbooks/deploy-staging-k3s.md for
# the full procedure, one-time prerequisites (secrets bootstrap, :443 DNAT),
# and rollback. This script deploys workloads + migrates the DB; it does NOT
# create Secrets or the external :443 entrypoint — those are one-time bootstrap
# steps documented in the runbook.
#
set -euo pipefail

SHA="${1:?usage: deploy_staging_k3s.sh <git-sha> [--skip-build]}"
SKIP_BUILD="${2:-}"
SHORT="${SHA:0:8}"
TAG="staging-${SHORT}"

# k3s rewrites 192.168.50.13:5000 -> the on-host loom-registry container via
# /etc/rancher/k3s/registries.yaml; we push to it over localhost (in docker's
# insecure 127.0.0.0/8) and reference the .13 form in manifests.
REGISTRY_REF="192.168.50.13:5000"
REGISTRY_PUSH="localhost:5000"
NS="loom-staging"
CONFIG="deploy/environments/staging.multinode.cluster.toml"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
# Locked runtime, then never resolve implicitly (repo uv-lock contract).
UV="uv run --no-sync"

log() { printf '\n=== %s ===\n' "$*"; }

# --- preflight: locked runtime + kubeconfig + required Secrets ---
uv sync --locked --extra cluster
kubectl get ns "${NS}" >/dev/null
for secret in loom-secrets loom-postgres-cnpg-credentials; do
  kubectl -n "${NS}" get secret "${secret}" >/dev/null 2>&1 || {
    echo "FATAL: ${NS}/${secret} missing — run the one-time secrets bootstrap first (see runbook)." >&2
    exit 1
  }
done

# --- 1. build + push the app images at this candidate sha ---
# --network=host: the docker bridge network is MITM'd with a self-signed cert
# (pip/npm fail CERT_VERIFY); the host egress has valid certs.
if [ "${SKIP_BUILD}" != "--skip-build" ]; then
  declare -A DOCKERFILES=(
    [loom-control-plane]=deploy/Dockerfile.control-plane
    [loom-service]=deploy/Dockerfile.service
    [loom-llm-gateway]=deploy/Dockerfile.gateway
    [loom-family-orchestrator]=deploy/Dockerfile.family-orchestrator
    [loom-egress-xds]=deploy/Dockerfile.egress-xds
    [loom-web]=deploy/Dockerfile.web
  )
  for name in loom-control-plane loom-service loom-llm-gateway \
              loom-family-orchestrator loom-egress-xds loom-web; do
    log "build ${name}:${TAG}"
    docker build --network=host \
      --label "org.opencontainers.image.revision=${SHA}" \
      --build-arg "LOOM_BUILD_SHA=${SHA}" \
      -f "${DOCKERFILES[$name]}" -t "${REGISTRY_PUSH}/${name}:${TAG}" .
    docker push "${REGISTRY_PUSH}/${name}:${TAG}"
  done
fi

# --- 2. render the desired state at this tag ---
render_config="$(mktemp)"
sed "s|^image_tag = .*|image_tag = \"${TAG}\"|" "${CONFIG}" > "${render_config}"
render_out="$(mktemp)"
log "render workloads (tag ${TAG})"
${UV} loom cluster render --config "${render_config}" > "${render_out}"

# --- 3. migrate the DB (alembic upgrade head) BEFORE rolling the app, so new
#        pods never boot against an old schema (SchemaNotAtHeadError) ---
migration_out="$(mktemp)"
log "render + run DB migration"
${UV} loom cluster render-migration \
  --image-tag "${TAG}" --namespace "${NS}" --container-registry "${REGISTRY_REF}" \
  > "${migration_out}"
kubectl apply -f "${migration_out}"
migrate_job="$(grep -m1 -oE 'loom-migrate-[a-z0-9-]+' "${migration_out}")"
echo "waiting for job/${migrate_job} ..."
kubectl -n "${NS}" wait --for=condition=complete "job/${migrate_job}" --timeout=600s

# --- 4. apply the workloads (rolling update to the new tag) ---
log "apply workloads"
kubectl apply -f "${render_out}"
${UV} loom cluster status --namespace "${NS}" --format table || true

log "deploy complete: ${NS} @ ${TAG}"
echo "Verify externally: curl -sSk --resolve yylx.world:8443:192.168.50.103 https://yylx.world:8443/staging/"
