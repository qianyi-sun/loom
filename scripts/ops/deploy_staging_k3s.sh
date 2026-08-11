#!/usr/bin/env bash
#
# Host-local deployment helper for the loom-staging stack on the multi-node
# OLDLAB k3s cluster. Normal shared-staging operation uses the installed
# protected rollout authority; this script is its implementation and authorized
# repair surface.
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
kubectl -n "${NS}" rollout status deploy/loom-service --timeout=300s || true
${UV} loom cluster status --namespace "${NS}" --format table || true

# --- 5. provision the deployment-managed headless smoke-user credential ---
#        The release-gate / operator trajectory smoke submits oracle x gb10-smoke
#        via POST /api/v1/trials, which needs a USER-OWNED token. Provision a
#        dedicated non-human loom-smoke user + user-owned submit token (idempotent
#        identity, fresh token each deploy) and store it in loom-secrets so the
#        smoke reads it via smoke_api_token_source. Runs INSIDE loom-service: the
#        host can't reach the CNPG DB directly (NetworkPolicy), but the service
#        pod has the loom CLI + LOOM_SVC_DB_URL + DB reachability. Non-fatal: a
#        smoke-credential hiccup must not fail the app rollout.
log "provision headless smoke-user credential"
svc_pod="$(kubectl -n "${NS}" get pod -l app=loom-service \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [ -n "${svc_pod}" ] && smoke_json="$(kubectl -n "${NS}" exec "${svc_pod}" -- \
    sh -c 'LOOM_DB_URL="$LOOM_SVC_DB_URL" loom admin ensure-smoke-user --format json' \
    2>/dev/null)"; then
  smoke_token="$(printf '%s' "${smoke_json}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' 2>/dev/null || true)"
  if [ -n "${smoke_token}" ]; then
    printf '%s' "${smoke_token}" | kubectl -n "${NS}" patch secret loom-secrets \
      --type merge -p "$(printf '%s' "${smoke_token}" | base64 | tr -d '\n' \
        | sed 's/.*/{"data":{"smoke-api-token":"&"}}/')" >/dev/null \
      && log "smoke-api-token stored in ${NS}/loom-secrets" \
      || echo "WARN: could not store smoke-api-token" >&2
  else
    echo "WARN: smoke-user provisioning returned no token" >&2
  fi
else
  echo "WARN: could not provision smoke-user (loom-service not ready?)" >&2
fi

# --- 6. provision the batch-runner control-plane token ---
#        loom-service fans batches out to the control-plane's POST /trials
#        using LOOM_SVC_BATCH_RUNNER_CP_TOKEN (from loom-secrets/batch-runner-
#        cp-token). A fresh/cutover DB has no valid one -> every batch 401s and
#        never dispatches (single-trial API submits are unaffected). Mint a
#        submit:batch worker token, store it, and restart loom-service to pick
#        it up. Non-fatal.
log "provision batch-runner CP token"
if [ -n "${svc_pod}" ] && br_json="$(kubectl -n "${NS}" exec "${svc_pod}" -- \
    sh -c 'LOOM_DB_URL="$LOOM_SVC_DB_URL" loom admin ensure-batch-runner-token --format json' \
    2>/dev/null)"; then
  br_token="$(printf '%s' "${br_json}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' 2>/dev/null || true)"
  if [ -n "${br_token}" ]; then
    printf '%s' "${br_token}" | kubectl -n "${NS}" patch secret loom-secrets \
      --type merge -p "$(printf '%s' "${br_token}" | base64 | tr -d '\n' \
        | sed 's/.*/{"data":{"batch-runner-cp-token":"&"}}/')" >/dev/null \
      && kubectl -n "${NS}" rollout restart deploy/loom-service >/dev/null 2>&1 \
      && log "batch-runner-cp-token stored; loom-service restarting to pick it up" \
      || echo "WARN: could not store/apply batch-runner-cp-token" >&2
  else
    echo "WARN: batch-runner provisioning returned no token" >&2
  fi
else
  echo "WARN: could not provision batch-runner token (loom-service not ready?)" >&2
fi

# --- 7. bootstrap the data-lifecycle mutation-epoch (idempotent; fresh-DB only) ---
#        Retention/GC needs a staging_mutation_epochs row, which must exist
#        BEFORE any lifecycle-bearing rows (trials/artifacts). On a fresh DB the
#        guarded tool applies it; on an already-bootstrapped or already-dirty DB
#        it reports not-applicable and this is a no-op. Non-serving,
#        non-fatal. Runs inside loom-service (has the loom package + DB + MinIO).
log "bootstrap data-lifecycle mutation-epoch"
if [ -n "${svc_pod}" ] \
   && kubectl -n "${NS}" cp scripts/ops/staging_data_lifecycle_bootstrap.py \
        "${svc_pod}":/tmp/dlb.py 2>/dev/null; then
  dlb_env='LOOM_LIFECYCLE_DB_URL="$LOOM_SVC_DB_URL"'
  dlb_env="${dlb_env}"' LOOM_LIFECYCLE_MINIO_ENDPOINT="$LOOM_SVC_MINIO_ENDPOINT"'
  dlb_env="${dlb_env}"' LOOM_LIFECYCLE_MINIO_ACCESS_KEY="$LOOM_SVC_MINIO_ACCESS_KEY"'
  dlb_env="${dlb_env}"' LOOM_LIFECYCLE_MINIO_SECRET_KEY="$LOOM_SVC_MINIO_SECRET_KEY"'
  dlb_env="${dlb_env}"' LOOM_LIFECYCLE_MINIO_REGION=us-east-1'
  dlb_env="${dlb_env}"' LOOM_LIFECYCLE_STORAGE_AUTH_KIND=static_keys'
  inv="$(kubectl -n "${NS}" exec "${svc_pod}" -- \
    sh -c "cd /app && ${dlb_env} python /tmp/dlb.py inventory --namespace ${NS}" \
    2>/dev/null || true)"
  applicable="$(printf '%s' "${inv}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("applicable"))' 2>/dev/null || true)"
  digest="$(printf '%s' "${inv}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("inventory_digest",""))' 2>/dev/null || true)"
  if [ "${applicable}" = "True" ] && [ -n "${digest}" ]; then
    kubectl -n "${NS}" exec "${svc_pod}" -- sh -c \
      "cd /app && ${dlb_env} python /tmp/dlb.py apply --namespace ${NS} \
         --requested-by deploy_staging_k3s --request-id deploy-${TAG} \
         --approved-inventory-digest ${digest}" >/dev/null 2>&1 \
      && log "data-lifecycle mutation-epoch bootstrapped" \
      || echo "WARN: data-lifecycle bootstrap apply failed" >&2
  else
    log "data-lifecycle bootstrap not applicable (already bootstrapped/dirty) — skipping"
  fi
fi

log "deploy complete: ${NS} @ ${TAG}"
echo "Verify externally: curl -sSk --resolve yylx.world:8443:192.168.50.103 https://yylx.world:8443/staging/"
