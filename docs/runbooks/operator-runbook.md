# Loom Operator Runbook

For operators of a production Loom deployment. Local dev → see the
top-level README + `deploy/docker-compose.dev.yml`.

For the first `main`-based production release, use the focused
[`first-prod-release-runbook.md`](first-prod-release-runbook.md) first. It is
the executable operator path for first-prod bootstrap, temporary staging leases,
frontend route checks, the production release gate, rollback preparation, and
emergency staging drain. The sections below remain the detailed reference.

> **Shared-staging invariant:** every code, image, cluster, host, desired-state,
> catalog, protected-secret, or capacity mutation of `loom-staging` is owned by
> `loom-staging-rollout` and a freshly fetched commit already merged to `dev`.
> Direct low-level commands elsewhere in this reference are implementation
> descriptions or development/custom/authorized-production procedures; they do
> not create a shared-staging operator path. After a successful broker request,
> operators may run disposable application-level staging smoke/tests, but may
> not mutate the protected deployment boundary outside the broker.

> **Cross-repo issue/PR refs:** bare `#N` in this document may point to
> the pre-2026-06-26 `carinrc/loom` archive tracker (numbering was reset
> on the new canonical repo `qianyi-sun/loom`). See
> [`repo-migration.md`](../contributing/repo-migration.md).

## Environment isolation

Loom uses three logical deployment environments. Treat the names below as the
operator contract; do not reuse kubeconfigs, databases, object buckets,
SecretStore keys, worker tokens, provider connections, or deploy credentials
across rows.

| Environment | Git branch / ref | GitHub Environment | Namespace | Public route | API base | DB name | Object buckets |
|---|---|---|---|---|---|---|---|
| `development` | `dev` only | `development` | `loom-dev` | `https://yylx.world/dev` | `https://yylx.world/dev/api` | `loom_dev` | `loom-dev-trajectories`, `loom-dev-artifacts` |
| `staging` | pinned `dev` SHA | `staging` | `loom-staging` | `https://yylx.world/staging` | `https://yylx.world/staging/api` | `loom_staging` | `loom-staging-trajectories`, `loom-staging-artifacts` |
| `production` | `main` only; immutable `vX.Y.Z` tags are records | `production` | `loom-prod` | `https://yylx.world/prod` | `https://yylx.world/prod/api` | `loom_prod` | `loom-prod-trajectories`, `loom-prod-artifacts` |

Staging owns `https://yylx.world/staging`; development owns
`https://yylx.world/dev`. These are distinct public surfaces (the earlier
`/dev` collision, where both claimed `/dev`, is resolved).
They remain separate GitHub/Kubernetes/storage identities for controlled
workflow isolation, but prod-vs-non-prod browser separation is `/prod` vs
`/dev`.

Committed environment profiles live in `deploy/environments/`. Each profile
has a matching `*.cluster.toml` render input. The production profile follows
`main`/release tags only; normal feature, benchmark, provider, worker, and
catalog work stays on `dev` and cannot use the production GitHub Environment.

Set these GitHub Environment secrets independently in `development`,
`staging`, and `production`:

| Secret | Contents |
|---|---|
| `LOOM_KUBECONFIG_B64` | Base64-encoded kubeconfig for only that environment. |
| `LOOM_CLUSTER_CONFIG_B64` | Base64-encoded cluster config for only that environment. |
| `LOOM_DEPLOY_TOKEN` | Environment-scoped deploy marker/credential used to require an environment secret before deploy. |
| `LOOM_SECRET_STORE_MASTER_KEY` | SecretStore master key for only that environment. |
| `LOOM_SERVICE_API_TOKEN` | Environment-scoped service/API automation token reference for release gates and operator-owned submissions. |
| `LOOM_WORKER_TOKEN` | Worker bearer token for only that environment. |
| `LOOM_PROVIDER_SECRET_REF` | Environment-scoped provider bootstrap secret reference. Store only the ref in release evidence. |
| `YIBUAPI_API_KEY` | Environment-scoped YibuAPI provider/rate-card secret when that provider is enabled. |

The workflow `.github/workflows/deploy-environment.yml` binds each job to the
matching GitHub Environment. Because GitHub only exposes environment secrets
after the selected job enters that environment, a development/staging-scoped
job cannot read production credentials. The staging row remains an isolation
contract and CI input, not authority to apply shared staging; the host broker is
its sole mutation path. Configure the `production` GitHub Environment with
required reviewer approval.

Before a production release, run the static boundary validator:

```bash
python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml \
  --dry-run-artifact release-evidence/environment-isolation-dry-run.json
```

It verifies the committed environment profile names, namespaces, canonical
frontend routes/API bases, database names, object buckets, SecretStore key
refs, worker-token refs, service API token refs, provider/YibuAPI secret refs,
provider-connection namespaces, cluster render inputs, and workflow branch
guards. The dry-run artifact records only target identities and safe secret
refs; it must not contain credential values, bearer tokens, signed URLs,
object-store keys, or provider API keys. The same check runs in repository CI
through `tests/ops`.

Before the first production migration or production batch, record a fresh
backup/snapshot pointer for the production database and object buckets in the
release checklist. The pointer belongs in release evidence; raw backup contents
and credentials must stay outside GitHub issues, PRs, Markdown, and workflow
logs.

### Release promotion gate

A production release is a promotion from a pinned `dev` candidate to `main`,
not an automatic deploy of every `dev` merge. The release owner must collect
the heavy staging evidence, encode it as a release gate manifest, and run
`.github/workflows/release-promotion-gate.yml` before opening or merging the
release PR.

`main` accepts only a same-repository production release promotion from `dev`.
The trusted base-branch controller enables squash auto-merge after the release
evidence is attached. The four current-head CI gates are the only merge
authority; author and reviewer identities do not affect eligibility.

Do not conflate separate decisions: manifest
`release_owner_approval` records acceptance of a particular candidate and
evidence package, while Production Environment approval releases deployment
secrets. They are distinct from CI merge authority and are not interchangeable.

Production tags are immutable SemVer Git tags on `main`, for example
`v1.0.0`. Pick the exact `prod_tag` in the release issue before opening the
release PR, record it in the release gate manifest and PR template, and never
move it after publication. If the same code must be re-released, create a new
SemVer tag. A workflow-driven rollback first restores the previous validated
tree on `dev` through its normal CI gate, then promotes that exact `dev`
candidate through a new CI-gated auto-merged `main` release PR and deploys from
`main`; never force-move a tag or open a direct rollback
branch -> `main` PR.

Production deployment dispatches use `main` only. Immutable SemVer tags remain
audit records and cannot enter the protected production workflow directly.

Normal flow:

1. Pick a 40-character candidate SHA from `dev`.
2. Pick the immutable SemVer production tag, for example `v1.0.0`, and record
   it in the release issue. Do not reuse an existing tag.
3. Build or identify the image tag/digests for that candidate.
4. After the candidate has merged into `dev`, deploy that exact merged SHA to
   shared `staging` with `loom-staging-rollout start`. The broker, not the
   staging GitHub Environment workflow, owns that target's mutation path.
5. Run the staging smoke checklist below, including migration dry-run,
   public API/SPA smoke, provider smoke, benchmark reward gate,
   score-positive canary gate before any full production benchmark batch,
   benchmark score-alignment gate, redaction scan, worker-capacity smoke, and
   rollback evidence. For first prod, also include prod/dev frontend route
   evidence, prod/staging state and worker isolation evidence, and the
   raw-delivery/export requirement status from the operator-free user E2E gate.
   The worker-isolation evidence must include the prod-first shared-capacity
   report generated from `deploy/worker-capacity/prod-first.toml`. Validate the
   redacted normal-user evidence package with
   `uv run --no-sync python scripts/ops/operator_free_user_e2e_gate.py validate --evidence
   <operator-free-user-e2e.json> --output-json <operator-free-user-e2e-report.json>`.
   The validator is offline-only; the live #493 journey still requires explicit
   production authority and #493 remains open until that live evidence exists.
6. Write a JSON manifest with `schema_version=1`, `candidate_sha`,
   `image_tag`, `prod_tag`, `staging_url`, image digests for every Loom image,
   and pass records for every required check:
   `repository_ci`, `image_build`, `cluster_render_audit`,
   `migration_dry_run`, `public_api_spa_smoke`, `frontend_route_evidence`,
   `secret_redaction`,
   `provider_smoke`, `benchmark_reward_gate`, `score_positive_canary`,
   `benchmark_score_alignment`, `worker_capacity_smoke`,
   `prod_staging_isolation`, `raw_delivery_export_status`, `rollback_plan`, and
   `release_owner_approval`.
   The `prod_staging_isolation` record is not a link-only checkbox. It must embed
   the dry-run identities the gate compares: production and staging state
   profiles, frontend routes/API bases, worker API URL, worker image and source
   commit, and staging capacity lease status. Safe secret references such as
   `github-environment:production/LOOM_SERVICE_API_TOKEN` are expected in
   `secret_refs`; raw token, provider key, database password, MinIO credential,
   signed URL, or bearer values are forbidden.
7. Run the release gate workflow:
   ```bash
   base64_manifest="$(base64 < release-gate-input.json | tr -d '\n')"
   gh workflow run release-promotion-gate.yml \
     --ref dev \
     -f candidate_sha="$CANDIDATE_SHA" \
     -f image_tag="$IMAGE_TAG" \
     -f evidence_manifest_b64="$base64_manifest"
   ```
8. Attach the workflow run, `release-gate-evidence` artifact, staging URL,
   candidate SHA, prod tag, image digests, frontend route evidence,
   worker-isolation evidence, raw-delivery/export requirement status, and
   rollback notes to the release PR from `dev` to `main`.
9. Confirm squash auto-merge is enabled and the four protected current-head CI
   gates are the only merge authority.
10. Tag the merged `main` commit with the recorded immutable `prod_tag`.
11. Deploy production from `main` with the same candidate SHA, image tag, and
   release gate workflow run id. The production deploy preflight downloads the
   `release-gate-evidence` artifact, verifies the candidate/image match, scans
   for leaked bearer/provider keys, signed URLs, raw secret values, and
   internal service URLs, confirms the candidate is reachable from trusted
   `origin/dev`, and requires its tree to exactly match clean checked-out
   `main` before it can reach `loom cluster up`.
   Artifact workflow/run/actor/digest authenticity binding remains the next
   #789 PR; this local schema/identity slice does not close #789.

Failed gate path: keep the release on `dev`, record the failing check and
evidence link on the release issue or PR, fix the owning subsystem, rerun the
staging gate with a new manifest, and only then retry promotion.

Failed deploy path: do not edit production secrets or reuse staging
credentials. Inspect the failed deployment logs, keep the release gate artifact
attached, and either rerun the production deploy with the same validated gate
artifact after fixing an operator error, or execute the rollback plan recorded
in the manifest.

Hotfix path: branch from `dev`, apply the minimal fix, and land it through the
normal CI-only `dev` auto-merge path. Run the same release gate against the
exact merged `dev` SHA, choose a new SemVer prod tag, then open the only
permitted `dev` -> `main` release-promotion PR. Let the trusted controller
enable squash auto-merge and wait for all protected gates before deploying
production with that candidate SHA and gate run id. Do not open a direct
hotfix branch -> `main` PR.

### Deploy, inspect, and rollback by environment

Development and production deploys run through the GitHub Actions workflow.
Shared staging is the exception: it is changed only by the broker after the
candidate has merged to `dev`.

```bash
gh workflow run deploy-environment.yml \
  --ref dev \
  -f environment=development \
  -f image_tag="$IMAGE_TAG" \
  -f dry_run=false

gh workflow run deploy-environment.yml \
  --ref main \
  -f environment=production \
  -f image_tag="$IMAGE_TAG" \
  -f candidate_sha="$CANDIDATE_SHA" \
  -f release_gate_run_id="$RELEASE_GATE_RUN_ID" \
  -f dry_run=false
```

For development/production, use `dry_run=true` to render and audit with the
environment secret config without applying. Every deploy job writes
`rollout-evidence/rendered.yaml` and
`rollout-evidence/release-manifest-<image-tag>.json` before apply, then uploads
that directory as a workflow artifact for the operator review trail. Production
deploys from every ref except `main` are skipped by the workflow condition and
still require the protected `production` environment approval when they do
run. Production deploys also refuse to run without a successful release gate artifact for the
candidate SHA and image tag being deployed.

Inspect a live environment with its own kubeconfig:

```bash
export KUBECONFIG=/secure/path/to/loom-prod.kubeconfig
loom cluster rollout-evidence cluster-status --namespace loom-prod --status-format table \
  | tee "$ROLLOUT_DIR/cluster-status.txt"
loom cluster audit --config deploy/environments/production.cluster.toml
kubectl -n loom-prod get deploy,sts,svc,ingress
```

For protected rollouts, collect image-tag evidence through the compatibility
helper rather than Docker Go-template snippets. The helper reads Docker
`RepoTags`, never prints container env, and exits non-zero with structured JSON
diagnostics if expected tags are missing or Docker metadata cannot be read:

```bash
jq -r '.rendered_manifest.deployment_images[][]' \
  "$ROLLOUT_DIR/release-manifest-$IMAGE_TAG.json" \
  | sort -u > "$ROLLOUT_DIR/expected-image-tags.txt"

docker image inspect $(cat "$ROLLOUT_DIR/expected-image-tags.txt") \
  > "$ROLLOUT_DIR/docker-image-inspect.json"

loom cluster rollout-evidence docker-images \
  --inspect-json "$ROLLOUT_DIR/docker-image-inspect.json" \
  $(sed 's/^/--expect-repo-tag /' "$ROLLOUT_DIR/expected-image-tags.txt") \
  | tee "$ROLLOUT_DIR/docker-image-evidence.json"
```

`loom cluster rollout-evidence cluster-status --status-format text` remains a
legacy alias for current `table` output. Prefer `--status-format json` for
machine-readable rollout artifacts.

Rollback stays environment-local. Use the matching `*.cluster.toml`,
namespace, kubeconfig, DB backup, and object buckets for the environment being
rolled back:

```bash
# Example: production image rollback only.
PREVIOUS_IMAGE_TAG=staging-known-good
tmp_config="$(mktemp)"
cp deploy/environments/production.cluster.toml "$tmp_config"
python - "$tmp_config" "$PREVIOUS_IMAGE_TAG" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
tag = sys.argv[2]
text = path.read_text()
replacement = f'image_tag = "{tag}"'
if re.search(r'(?m)^image_tag\s*=', text):
    text = re.sub(r'(?m)^image_tag\s*=.*$', replacement, text)
else:
    text = replacement + "\n" + text
path.write_text(text)
PY
loom cluster audit --config "$tmp_config"
loom cluster up --config "$tmp_config" --context "$PROD_CONTEXT"
loom cluster status --namespace loom-prod --format table
```

Do not point a dev or staging kubeconfig at `loom-prod`, do not copy provider
connections between environments, and do not reuse worker tokens across
environments. Remote OLDLAB/Lux capacity attaches behind the environment's own
worker token and service URLs; it does not own production control-plane state.

**OLDLAB x86 execution capacity is Slurm-managed, not in-cluster.**
Staging and production profiles render with
`k8s_worker.enabled=false` (#383). On these clusters, the in-cluster
`loom-worker` Deployment is not present and `POST /api/v1/batches`
rejects submissions whose `required_worker_pools` list contains
`k8s-worker` — use `oldlab` for x86_64 required-pool coverage
instead. Never scale up an in-cluster `loom-worker` on a host that
also participates in the Slurm-managed `oldlab` pool: k8s and Slurm
reservations do not coordinate, and the shared host Docker daemon
will over-subscribe. Local kind clusters (`development` profile)
keep `enabled=true` because there is no external Slurm to share
with.

When a rollout flips or keeps `k8s_worker.enabled=false`, `loom cluster up`
prunes stale in-cluster worker compute/network resources that were rendered by
older profiles:

```bash
kubectl -n "$NAMESPACE" delete deploy loom-worker --ignore-not-found
kubectl -n "$NAMESPACE" delete networkpolicy loom-worker --ignore-not-found
```

The worker trajectory PVC is deliberately retained by default:
`persistentvolumeclaim/loom-worker-trajectories` can contain trajectory or
debug artifacts from earlier in-cluster worker runs and must not be deleted as
part of a profile-toggle cleanup. Delete that PVC only through an explicit
artifact-retention or teardown decision, using the protected-environment backup
and acknowledgement flow required for destructive PVC operations. If a disabled
profile still has live/Ready `app=loom-worker` pods after `cluster up`, the
release gate must fail until the stale Deployment and NetworkPolicy are gone.
For protected `static-host-path` profiles, the renderer still emits the
`loom-worker-trajectories` PVC even when `k8s_worker.enabled=false`, so
preflight can verify that the Retain PV/PVC boundary exists before any apply
continues.

## At-a-glance: deploy a fresh cluster

This bootstrap example is for a new local/custom cluster. It is not the shared
`loom-staging` update path; that target is changed only through
`loom-staging-rollout` after code has merged into `dev`.

The fastest path uses the `loom cluster` CLI (shipped via #76). Install
the optional `cluster` extra and point at your kubeconfig context. For
staging/production rollout runners, use `uv sync --locked --all-packages
--extra cluster --extra rollout --python 3.11`; the `rollout` extra installs the in-repo benchmark sibling packages
(`packages/loom-benchmarks` and `packages/loom-benchmark-terminal-bench-2`)
needed by catalog provisioning:

```bash
pip install "loom[cluster]"   # local/manual cluster tooling only
# rollout runner:
uv sync --locked --all-packages --extra cluster --extra rollout --python 3.11
source .venv/bin/activate
export KUBECONFIG=~/.kube/config   # standard kubectl config

# 1. Configure
cat > cluster-config.toml <<EOF
namespace = "loom"
image_tag = "0.7"
ingress_host = "loom.example.com"
# Optional: bind the www/non-www counterpart to the same certificate and
# redirect it to ingress_host. Do not use it as a separate frontend/API route.
# ingress_redirect_hosts = ["www.loom.example.com"]
ingress_class_name = "nginx"
ingress_tls_secret_name = "loom-tls"
# Optional when cert-manager manages the TLS Secret:
# ingress_cert_manager_cluster_issuer = "letsencrypt-prod"
EOF

# 2. One-time bootstrap (Secrets) — see "Bootstrap Secrets" below
# 3. Verify the cluster is ready to receive Loom. The config supplies the
# namespace and runtime environment.
loom cluster preflight --config cluster-config.toml

# 4. Audit the manifests against the public/internal boundary
loom cluster audit --config cluster-config.toml

# 5. Deploy
loom cluster up \
  --config cluster-config.toml \
  --context $YOUR_CTX \
  --environment development \
  --rollout-id "$IMAGE_TAG" \
  --rollout-lock-evidence "$ROLLOUT_DIR/rollout-mutation-lock-$IMAGE_TAG.json"

# 6. Verify
loom cluster status --config cluster-config.toml --format table
```

Each verb:

| Command | What it does | Exit codes |
|---|---|---|
| `loom cluster preflight` | API-side checks: namespace exists, required Secrets present, IngressClass installed, default StorageClass available, PSS labels OK, and schema-doctor reconciliation for rendered env/Secret drift. With `--config`, preflight validates live `loom-secrets` but checks env vars against the target rendered Deployments so rollouts that add schema-backed env vars are not blocked by old live pods. Protected environments also check the live critical PVC/PV storage boundary and a recent backup manifest; pass `--config cluster-config.toml` so first deploys can prove static host-path Retain PVs before the PVCs exist. Optional runtime-derived worker env such as hostname, idle-exit, fixtures root, benchmark cache, and blocking-I/O executor override may stay unset. | 0 pass / 1 fail / 2 cluster unreachable |
| `loom admin env-diagnostics` | Print scoped runtime environment diagnostics without raw secret values. By default it includes `LOOM_` variables and redacts key names containing `TOKEN`, `SECRET`, `KEY`, password, credential, auth, kubeconfig, or database/DSN markers. Sensitive entries show only `[REDACTED sha256:<12-hex> len=<N>]`, so JSON/Markdown output is safe to attach as release evidence. | 0 written / 2 bad input |
| `loom cluster backup manifest/check` | Write or verify metadata-only backup manifests for staging destructive-operation guards | 0 verified / 1 invalid manifest / 2 bad input |
| `loom cluster render` | Print the rendered YAML to stdout (no cluster contact) | 0 / 2 on bad config |
| `loom cluster release-manifest` | Write a safe pre-apply rollout artifact with the candidate git SHA/image tag, CLI version, cluster-config and rendered-manifest hashes, intended Deployment images, optional expected image digests/IDs from `--expected-image-identities-json`, Alembic heads, and environment-state worker desired-state fingerprints | 0 written / 2 bad input |
| `loom cluster minio-storage-preflight` | Execs into `loom-minio-0` and records `/data` filesystem size/used/free/percent, bucket usage for artifacts/trajectories/benchmark-task data, configured warning/stop thresholds, optional estimated batch headroom, and rapid artifact/trajectory growth when `--previous-evidence` is supplied. Writes JSON with `--output`; exits 1 on stop unless `--allow-storage-stop-override` is supplied. | 0 pass or warning / 1 stop threshold / 2 bad input or unreachable |
| `loom cluster release-gate` | Compare the release manifest against the saved rendered/config hashes, live target-generation image evidence, live DB Alembic heads queried through `deploy/loom-control-plane`, disabled k8s-worker stale-resource evidence when the manifest records `k8s_worker.enabled=false`, the `loom admin environment-state check --format json` artifact when the manifest records external-worker desired state, the `loom admin gb10-workers status --format json` artifact when the manifest records GB10 desired state, and the optional `--minio-storage-preflight` artifact for a `minio-storage-pressure` component row. Running Deployments use exact Ready-pod runtime digest/image-ID comparison when available; kind-loaded `import-YYYY-MM-DD@sha256:...` runtime identities are accepted only with matching target-generation pod spec and Deployment template images; zero-replica managed Deployments use template-image convergence evidence. If pod events show `FailedCreatePodSandBox` / `FailedKillPod` with `context deadline exceeded`, the gate fails the affected image row with `failure_class=node_runtime_sandbox_deadline` instead of reporting a generic application readiness failure. JSON output includes `component_evidence`; `--format markdown` writes the pasteable per-component release evidence table for issue comments. | 0 pass / 1 hard-check fail / 2 bad input or unreachable |
| `loom cluster audit` | Static public/internal boundary check on rendered manifests: TLS ingress, only `/api/v1` → `loom-service` and `/` → `loom-web` or the canonical `/prod`/`/dev` prefixed equivalents, no LoadBalancer/NodePort, no unsafe hostPort, required NetworkPolicies present | 0 clean / 1 violation / 2 bad config |
| `loom cluster up` | Preflight → render → protected-environment rollout mutation lease acquisition → apply non-StatefulSet resources and patch existing StatefulSets only through mutable fields after checking immutable storage intent → prune resources intentionally removed by profile toggles, including stale `deploy/loom-worker` and `networkpolicy/loom-worker` when `k8s_worker.enabled=false` while retaining `persistentvolumeclaim/loom-worker-trajectories` → wait for components ready, Deployment generations observed, updated replicas converged, managed Deployment pods inspectable and free of blocking CrashLoop/image/config/start failures, kube-system rollout controllers healthy, and live Deployment images matching the rendered manifests; prints rendered/live image evidence for managed Deployments. Existing StatefulSet PVC templates may contain Kubernetes default/bound fields such as `volumeName`, empty `storageClassName`, default `volumeMode`, and runtime PVC template `status`; `cluster up` tolerates that drift, but real immutable changes such as claim name, access modes, storage size, selector, service name, or pod management policy still fail closed. With `--recover-sandbox-deadlines`, a not-ready status whose pod events classify as kind/containerd sandbox deadline stalls deletes only the classified pods, capped by `--sandbox-deadline-max-pods`, then retries readiness once. Preflight and backup/storage guards still run before apply/recovery unless the operator explicitly passes `--skip-preflight`. For staging/production, pass `--rollout-id` and `--rollout-lock-evidence` so evidence records acquisition and release/failure state. | 0 ready / 1 lock contention, immutable StatefulSet drift, not-ready, prune failure, recovery failed, or image drift / 2 unreachable, bad input, or kubectl missing |
| `loom cluster status` | Live readiness snapshot with ingress endpoints; marks stale Deployment generations, incomplete updated replicas, failed managed-pod inspection, managed Deployment pod CrashLoop/image/config/start failures, classified `node_runtime_sandbox_deadline` pod sandbox create/kill failures, and visible kube-system controller/scheduler/etcd/API pod failures as not-ready | 0 all-ready / 1 not-ready / 2 unreachable |
| `loom cluster down` | `kubectl delete` of the rendered manifests; opt-in `--with-volumes` (PVCs) and `--delete-namespace` for full teardown. Protected environments require `--backup-manifest` and `--acknowledge-data-loss` before destructive flags. | 0 / 1 on failure, invalid backup guard, or operator-cancelled prompt |

`preflight`, `up`, `status`, and `down` share one target-resolution contract.
When `--config` declares `namespace` and/or `runtime_environment`, those values
are authoritative. Omitted flags are inferred from the config; explicitly
supplied `--namespace` or `--environment` values are safety assertions and
must match. The CLI fails before cluster access on a mismatch. Without a
config, the namespace remains `loom` by default and the environment is inferred
from the namespace unless supplied explicitly.

`loom cluster render` embeds `metadata.namespace` on every namespaced object;
cluster-scoped PersistentVolumes remain unnamespaced. For an explicitly
authorized custom-cluster restore that must use manual `kubectl apply`, keep
both the embedded metadata and an explicit matching `-n` assertion:

```bash
loom cluster render --config cluster-config.toml > /tmp/loom-rendered.yaml
yq -e --arg ns "$NAMESPACE" \
  'select(.kind != "PersistentVolume") | .metadata.namespace == $ns' \
  /tmp/loom-rendered.yaml >/dev/null
kubectl -n "$NAMESPACE" apply -f /tmp/loom-rendered.yaml
```

Never use a bare `kubectl apply -f` for rendered restore manifests. Shared
staging remains broker/rollout-driver owned; this manual form does not grant
authority to bypass that path.

The detailed manual flow (build images → create Secrets → apply
each manifest → mint internal tokens → approve registrations and deliver
invite links) below documents the bootstrap and operator steps the CLI doesn't
yet automate. It's
also the fallback when `cluster-config.toml` doesn't yet expose the
knob you need.

## Initial deployment

1. **Build images.** From repo root:
   ```bash
   docker build -f deploy/Dockerfile.control-plane -t loom-control-plane:0.7 .
   docker build -f deploy/Dockerfile.gateway       -t loom-llm-gateway:0.7   .
   docker build -f deploy/Dockerfile.service       -t loom-service:0.7       .
   docker build -f deploy/Dockerfile.worker        -t loom-worker:0.7        .
   docker build -f deploy/Dockerfile.web           -t loom-web:0.7           .
   ```
   Keep operator-local directories such as `.staging`, `.staging-*`, and
   `.worktrees` out of the Docker context; the repository `.dockerignore`
   excludes them so staging evidence, benchmark caches, and local worktrees
   do not make image builds hang while sending context.
   `Dockerfile.web` is multi-stage (node-slim builds the Vite bundle
   → nginx-alpine serves it) and validates the target-architecture
   Lightning CSS native binding before running the Vite build. Push to your
   registry, then update
   `image:` refs in `deploy/k8s/*.yaml`.

2. **Create the `loom-secrets` Secret.** Required keys are declared in
   `config/loom-schema.toml` — the canonical source of truth. Generate and
   apply the full Secret in one step:

   ```bash
   loom cluster bootstrap-secrets --rotate
   ```

   `--rotate` mints fresh values for entries that carry a `generate:` command,
   including `step-jwt-signing-key`, `postgres-password`, and
   `secret-store-master-key`. For other required secrets (DB URLs, MinIO
   credentials, provider API keys) the placeholder `<EDIT_ME>` appears —
   replace with your values before piping to kubectl.

   To preview and edit before applying:

   ```bash
   loom cluster bootstrap-secrets > /tmp/loom-secret.sh
   $EDITOR /tmp/loom-secret.sh
   bash /tmp/loom-secret.sh
   ```

   **When `pgbouncer.enabled=true` (default per #609):** the command emits a
   shell script rather than a single `kubectl create secret` line, because
   the `*-db-url-pool` secrets are mechanically derived from their direct
   siblings via `loom cluster derive-pool-dsn` (called as a `$(...)` shell
   substitution). Operators only edit the direct-URL slots (`cp-db-url`,
   `gw-db-url`, `svc-db-url`) — the corresponding `*-db-url-pool` values are
   computed at execution time by rewriting host `loom-postgres` → `loom-pgbouncer`
   and port `5432` → `6432`. Do not paste the shell-script output into a
   terminal blindly; the `$(...)` substitutions won't expand until `bash` runs
   them.

   Rollback path for pgbouncer: set `pgbouncer.enabled=false` in the
   profile, re-render manifests, `kubectl apply`. Services fall back to
   `db_url` (direct) and the pool secrets go unused. No secret rotation,
   no data migration. See `docs/architecture/pgbouncer-transaction-mode-design.md`.

   The `worker-token` value is overwritten in step 6.

   Create the singleton admin secret file with the operator CLI and mount it as
   `loom-admin-secret`:

   ```bash
   loom service init-admin --secret-file ./secrets.toml
   ADMIN_TOKEN="$(loom service reveal-admin --secret-file ./secrets.toml --yes)"
   kubectl create secret generic loom-admin-secret \
     --from-file=secrets.toml=./secrets.toml
   ```

3. **Prepare DNS and TLS for the public Web/API ingress.** `loom cluster`
   is the production/public path; dev compose is loopback-only by default and
   must not be used as the Internet-facing deployment.

  - Create a DNS A/AAAA or CNAME record for `ingress_host` pointing at your
     ingress controller's public address.
   - For first production, use the committed route split instead of separate
     user-facing hosts: production renders `https://yylx.world/prod` with API
     calls under `https://yylx.world/prod/api`, and staging
     renders `https://yylx.world/staging` with API calls under
     `https://yylx.world/staging/api`. The web pod writes
     `loom-frontend-config.json` from runtime environment variables on startup
     and nginx serves it with `Cache-Control: no-store`, so a stale Vite build
     or browser-cached config cannot silently keep pointing at the wrong API.
     `www.yylx.world` is redirect-only: profiles list it in
     `ingress_redirect_hosts` so TLS/SNI uses the same public certificate, then
     ingress-nginx's `from-to-www-redirect` handling redirects back to the bare
     `yylx.world` host with the original path. Do not configure a second
     frontend/API base under `www`.
   - Keep the repository-pinned ingress-nginx controller's trusted raw-path
     guard intact. Its ConfigMap leaves `allow-snippet-annotations: "false"`,
     disables slash merging, and returns exact HTTP 404 for path-side `%2F`,
     `%5C`, literal backslash, `//`, or any percent-encoded byte in the first
     path segment before those forms can cross-match a Loom route. Rendered
     `/dev` and `/prod` regexes also disable case folding for the prefix group,
     while the controller keeps an independent raw mixed-case guard. The web
     pod repeats the raw guard, uses case-sensitive valid prefix locations, and
     rejects normalized mixed-case prefixes after percent-decoding for direct
     Service traffic. The raw-path guard stops at the query delimiter, so
     canonical redirects may still preserve
     values such as `?next=%2Fmonitor&x=1`. Staging must observe exactly one
     `Location` at each redirect boundary and exactly one health
     `Content-Type` whose MIME essence is `application/json`; selecting the
     first of duplicate headers is not acceptance. Do not replace this with
     per-Ingress `server-snippet` or `configuration-snippet` annotations. If an
     operator replaces the pinned controller, the trusted controller
     configuration must reproduce this fail-closed behavior and pass the
     labeled cluster and staging smoke jobs before rollout.
     Protected rollout step 03 reapplies the pinned manifest from the rollout's
     fixed-SHA `01-worktree/src` on every run, including existing healthy
     clusters, and fails unless the live controller ConfigMap reads back the
     exact values parsed from the commit-bound evidence bytes. It never reads,
     hashes, or applies the operator checkout's ambient copy. The resolved candidate SHA
     and candidate manifest SHA-256 are part of the
     `node-label-admission-and-commit-bound-controller-config-v4` step fingerprint.
     Evidence
     records `installed` only when the controller Deployment was absent before
     apply; an existing Deployment is `reconciled` even when its IngressClass
     was missing or drifted. `reused` is not a valid controller state. The run
     artifacts expose the candidate SHA, commit-bound evidence path, candidate
     source path, and manifest SHA-256 for direct evidence review.
   - For a lab or invite-only staging host reached directly by IP address, set
     `ingress_host` to that IP and pre-create the TLS Secret with a certificate
     whose Subject Alternative Name includes the IP address. Kubernetes rejects
     IP literals in `Ingress.spec.rules[].host`, so `loom cluster render`
     emits a hostless ingress rule for IP entrypoints while keeping TLS
     enabled through `ingress_tls_secret_name`. With ingress-nginx, also set
     the controller's default certificate to that Secret, for example
     `--default-ssl-certificate=<namespace>/<ingress_tls_secret_name>`,
     because hostless rules do not give the controller a DNS host to use for
     SNI certificate selection.
   - Install an ingress controller matching `ingress_class_name` (default
     `nginx`).
   - Either pre-create the TLS Secret named by `ingress_tls_secret_name`, or
     install cert-manager and set `ingress_cert_manager_cluster_issuer` in
     `cluster-config.toml`.

   Example cert-manager setup:

   ```bash
   helm repo add jetstack https://charts.jetstack.io
   helm upgrade --install cert-manager jetstack/cert-manager \
     --namespace cert-manager --create-namespace --set crds.enabled=true

   kubectl apply -f letsencrypt-prod-cluster-issuer.yaml
   ```

   Rendered Ingress should show exactly one public host and TLS:

   ```bash
   loom cluster render --config cluster-config.toml \
     | yq '. | select(.kind == "Ingress") | .spec'
   ```

4. **Apply stateful dependencies first:**
   ```bash
   kubectl apply -f deploy/k8s/postgres.yaml
   kubectl apply -f deploy/k8s/minio.yaml
   # wait for postgres + minio ready
   ```

5. **Run migrations before DB-facing services start.** Use a one-off Job or
   operator shell with the same image and database Secret as the Control Plane.
   Existing clusters may also exec into an already-running Control Plane pod:
   ```bash
   # Fresh deploy: run this from a one-off migration pod/job before app rollout.
   LOOM_DB_URL="$LOOM_CP_DB_URL" alembic -c migrations/alembic.ini upgrade head

   # Existing deploy, when a compatible Control Plane pod is already running:
   kubectl exec deploy/loom-control-plane -- alembic upgrade head
   ```

   `loom-control-plane`, `loom-llm-gateway`, and `loom-service` validate
   the database Alembic revision at process startup. If the DB is behind the
   image code, they refuse to start with the migration command instead of
   serving requests that later fail with missing-column errors. Production
   images for these services must include `migrations/alembic.ini` and
   `migrations/versions/` so this startup gate can compare DB revisions against
   the image code. Startup validation retries transient DNS, connection, and
   Postgres-starting failures with bounded backoff so a pod sandbox or CoreDNS
   restart does not immediately crash the process; schema mismatch, bad
   credentials, and SecretStore decrypt failures remain hard startup failures.
   Worker pods use the same bounded startup retry for initial Control Plane
   registration and the immediate orphan-trajectory cleanup lookup, while
   deterministic HTTP errors such as bad worker tokens still fail immediately.

6. **Apply DB-facing services and edge components:**
   ```bash
   kubectl apply -f deploy/k8s/llm-gateway.yaml
   kubectl apply -f deploy/k8s/control-plane.yaml
   kubectl apply -f deploy/k8s/loom-service.yaml
   kubectl apply -f deploy/k8s/web.yaml
   kubectl apply -f deploy/k8s/worker.yaml
   kubectl apply -f deploy/k8s/ingress.yaml
   ```

7. **Mint a worker token** via the admin API. The admin credential is the
   singleton `loom-admin-secret` mounted into `loom_service`, the Control Plane,
   and the LLM Gateway. Use the same `ADMIN_TOKEN` revealed in step 2; do not
   create a database-backed admin row for this bootstrap path.

   The Control Plane's `POST /admin/worker-tokens` route is
   intentionally NOT exposed via Ingress (see
   `deploy/k8s/ingress.yaml`). Reach it via port-forward. Pipe the
   token straight into the secret store so it never appears on
   `argv` (visible to `ps`) or in shell history:
   ```bash
   kubectl port-forward deploy/loom-control-plane 8080:8080 &
   loom admin tokens worker mint --format json --expires-in-days 365 \
     | jq -r .token \
     | kubectl create secret generic loom-secrets \
         --from-file=worker-token=/dev/stdin \
         --dry-run=client -o yaml \
     | kubectl apply -f -
   kubectl rollout restart deploy/loom-worker
   ```
   If you need to see the raw token (e.g. to install on a Slurm
   worker host), append `--show-secret` to the mint command instead
   of `--format json`.

7. **(Optional) Provision the batch-runner CP token.** The
   `loom_service` batch-runner needs a `submit:batch` internal token
   to fan out trials from batches. Without it, the runner skips
   its tick with a warning — batches will not advance. The token is
   team-less; Control Plane derives each child trial's `team_id` from
   the parent batch row, so one runner can safely process multiple teams.

   Mint it through Control Plane's admin endpoint, then install it
   into the secret store via stdin — never pass the raw token on
   `kubectl patch -p ...` argv (visible to `ps`, ends up in shell
   history):
   ```bash
   kubectl port-forward deploy/loom-control-plane 8080:8080 &
   curl -sS -X POST http://localhost:8080/admin/batch-runner-tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"expires_in_days": 365}' \
     | jq -r .token \
     | kubectl create secret generic loom-secrets \
         --from-file=batch-runner-cp-token=/dev/stdin \
         --dry-run=client -o yaml \
     | kubectl apply -f -
   kubectl rollout restart deploy/loom-service
   ```

   To inspect the running service environment during deploy/debug checks, use
   the redacted diagnostic path rather than raw shell env dumps:
   ```bash
   kubectl exec deploy/loom-service -- \
     loom admin env-diagnostics --prefix LOOM_SVC_ --format markdown
   ```
   The root cause of the #97 exposure was not a failed token rotation path; it
   was the missing operator-approved boundary for environment inspection, which
   pushed debugging toward raw `printenv` output. Treat
   `loom admin env-diagnostics` as the only evidence-producing environment
   inspection command for staging and production service pods.
   Do not run `printenv | grep TOKEN`, `env | grep SECRET`,
   `kubectl exec ... -- printenv`, or similar raw commands in staging or
   production. They can copy service tokens, SecretStore keys, database URLs,
   provider keys, and password-bearing credentials into terminal scrollback,
   CI logs, issue comments, or rollout evidence.

   After a suspected `batch-runner-cp-token` exposure, rotate it and validate
   only hash-derived evidence. The mint response contains the raw token, so do
   not redirect it to logs or evidence files:
   ```bash
   kubectl port-forward deploy/loom-control-plane 8080:8080 &
   curl -sS -X POST http://localhost:8080/admin/batch-runner-tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"expires_in_days": 365}' \
     | python -c 'import json, sys; body = json.load(sys.stdin); print("new batch-runner token hash prefix:", body["token_hash_prefix"], file=sys.stderr); print(body["token"])' \
     | kubectl create secret generic loom-secrets \
         --from-file=batch-runner-cp-token=/dev/stdin \
         --dry-run=client -o yaml \
     | kubectl apply -f -
   kubectl rollout restart deploy/loom-service
   kubectl rollout status deploy/loom-service --timeout=120s

   kubectl exec deploy/loom-service -- \
     loom admin env-diagnostics --prefix LOOM_SVC_ --format json \
     > "$ROLLOUT_DIR/loom-service-env-diagnostics.json"
   jq -e '.entries[] | select(.name=="LOOM_SVC_BATCH_RUNNER_CP_TOKEN")
          | select(.value=="[REDACTED]")
          | select(.fingerprint | startswith("sha256:"))
          | select(.length > 0)' \
     "$ROLLOUT_DIR/loom-service-env-diagnostics.json"

   # If the suspected old token hash prefix is known, revoke it after the new
   # service pod is live.
   curl -sS -X DELETE "http://localhost:8080/admin/worker-tokens/$OLD_PREFIX" \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Compare the first 8 hex characters after `sha256:` in the env diagnostic
   with the new `token_hash_prefix` printed by the mint step. Record only the
   prefix/fingerprint and rollout status in issues or PRs; never record the
   raw `LOOM_SVC_BATCH_RUNNER_CP_TOKEN` value.

8. **Verify versioned environment state.** Kubernetes manifests do not own
   every rollout-critical runtime row. For shared staging, the candidate-bound
   broker driver applies the repository profile after images and secrets are
   live, then checks drift before Monitor capacity or benchmark evidence can
   pass. Operators inspect the resulting `environment-state-check` artifact
   with `loom-staging-rollout status` or `logs`; they do not run interactive
   `environment-state apply/check` commands against shared staging.

   Staging cluster configs must render backend pods with `LOOM_ENV=staging`,
   and staging environment-state evidence must use the `staging` control-plane
   desired-state key. A `production/gb10` desired-state in staging
   evidence is drift. The broker runs the check from the Slurm submit/shared-
   storage host when the profile contains external Slurm runner pools; the gate
   also verifies runner env files, worker-token fingerprints, shared git
   checkouts, clean git status, and active Slurm job launch env.

   For first prod, also generate the shared physical worker-capacity contract.
   This is a repo-only desired-state/evidence check: it does not mutate worker
   pools, mint tokens, or read live credentials. The default manifest assigns
   every eligible GB10/OLDLAB slot to production and leaves staging/dev at zero
   borrowed slots. The physical v1.0 inventory remains all 15 GB10 hosts at 10
   slots each. The 2026-07-29 owner correction supersedes #822's static
   exclusion: all 15 hosts are infrastructure- and capacity-eligible, for 150
   slots, and acceptance records `excluded_nodes=[]`. A candidate-owned
   drain/quiescence gate defers disruptive convergence on any busy host without
   cancelling or preempting external work. The repo-owned GB10 SSH topology uses
   `trt-gb10-1` as its sole public
   entrypoint on port `2221` and reaches private `trt-gb10-2..15` on port `22`
   through `ProxyJump trt-gb10-1`. When an observed worker
   registration/status artifact is available, pass it with `--observed-json`
   so the report fails on prod/dev
   environment, API URL, image tag, source commit, compose service, Kubernetes
   deployment, slot-count, or worker-identity drift:

   ```bash
   uv run --no-sync python scripts/ops/worker_capacity_manifest.py \
     --manifest deploy/worker-capacity/prod-first.toml \
     --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
     --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
     --var STAGING_IMAGE_TAG="$IMAGE_TAG" \
     --var STAGING_SOURCE_COMMIT="$RELEASE_SHA" \
     --observed-json "$ROLLOUT_DIR/worker-registrations.json" \
     --evidence-out "$ROLLOUT_DIR/worker-capacity-prod-first.json" \
     --format markdown
   ```

   The report is safe to attach to release issues and PRs: secret-bearing input
   fields are omitted or redacted, and the output must not contain service
   tokens, provider keys, signed URLs, MinIO credentials, or raw secret values.
   Secret references are intentionally preserved so the production promotion
   gate can prove prod uses production refs and staging uses
   development refs.

   Shared-staging capacity lease creation, application, drain/release, GB10
   node-agent start/stop, and long-validation orchestration are broker-owned
   mutations. Operators must not invoke `worker_capacity_manifest.py` with
   staging `--apply`, impersonate `loom-rollout`, supply the service admin
   token/SSH identity, or pass an arbitrary validation command. The
   candidate-bound request records the bounded lease, prod-pressure decision,
   worker convergence, validation, and release evidence. Inspect it through
   `loom-staging-rollout status REQUEST_ID` and `logs REQUEST_ID`; repair a
   bounded external failure and resume the original request rather than
   constructing a new capacity operation.

   The status evidence includes `prod_pressure.cause=prod_capacity_pressure`
   when the pause is prod-driven, distinct from drift/errors that indicate a
   staging rollout failure. Nonzero production pressure stops new staging
   claims, immediately returns idle staging slots to production, and reports
   running staging slots as draining. TTL expiry also stops new staging claims.
   `scripts/ops/prod_pressure_worker_control.py` is the runtime bridge: it
   polls the private production CP signal, applies it to staging GB10 desired
   state, and fences all matching worker registrations through
   `Worker.drain_state`. Loss of the production signal fails closed to a
   staging drain. Install the repo-owned
   `deploy/worker-capacity/loom-prod-pressure-worker-control.timer` for the
   supervised 30-second reconcile; a foreground invocation is not automatic
   activation. Pressure clearing restores desired host intents, but claims stay
   fenced until the node-agent reports the active container state.

   A production promotion manifest must record the latest staging lease status in
   `checks.prod_staging_isolation.staging_capacity`. The release gate requires
   `staging_slots=0`, no active lease, and `new_staging_claims_allowed=false` unless
   the same object includes an explicit override with `approved=true`, a
   non-empty reason, and an HTTPS evidence URL.

   The candidate-bound driver invokes the idempotent environment-state APIs for
   worker-pool autoscaler policies, GB10 desired state, and Slurm worker job
   status. A drift failure is actionable, for example desired `gb10`
   actuator `slurm` but live `gb10`, or an active OLDLAB Slurm job still
   pointing at an older `LOOM_REMOTE_WORKER_REPO_DIR`. Fix repository-owned
   drift in a commit merged to `dev`, then start a new broker request. If the
   candidate is unchanged and only a bounded external prerequisite failed,
   resume the same request after fixing that prerequisite. Do not apply an
   interactive profile, patch SQL, substitute an untracked token, or create an
   out-of-envelope retry against shared staging.
   Staging profiles must target the `staging` Control Plane desired-state
   environment. Evidence showing `production/gb10` for a staging rollout
   is drift, not a compatibility exception. Pass the resolved release commit as
   `GIT_SHA`; the GB10 desired state stores it as `source_git_commit`, so
   node-agent status and release-gate checks can reject a clean image/env
   rollout whose host checkout is still stale.
   When a release manifest records this profile, pass the JSON check artifact
   to `loom cluster release-gate --environment-state-check`; a missing artifact,
   `ok=false`, or non-empty `drift` array keeps the protected release gate red
   and blocks workload-validation anchors.
   The broker's candidate-bound rollout driver step 11 first materializes the candidate
   environment-state profile to
   `/data/loom-staging/environment-state/staging.toml` with mode `0600`,
   recording source/target sha256 evidence, then applies the candidate profile
   once and retries the immediate check once. A legacy operator-owned `0600`
   leaf can be unreadable by `loom-rollout` even when the directory's reviewed
   access/default ACL grants atomic replacement. Step 11 treats that leaf as
   stale and replaces it from the candidate; it does not broaden the legacy
   leaf ACL or use it as rollout input. Before comparison it inspects the
   destination entry without following links; symlinks and all other
   non-regular entries fail closed before read, chmod, or replacement. Pure
   `gb10_worker_node_status[...]` drift is recorded but deferred because step
   12 has not started the host-local node-agent yet; mixed drift, such as
   OLDLAB jobs or missing external-runner prerequisites, still fails before
   host prep. Step 14 reruns the
   environment-state check after GB10 prep and before release-gate, retrying
   only pure GB10 node-status drift for a bounded window while asynchronous
   source-checkout updates report back.
   When the release manifest records GB10 desired state, also generate
   `loom admin gb10-workers status --format json` with the release image/env
   target and pass it to `loom cluster release-gate --gb10-workers-status`.
   The gate fails if any declared active GB10 host is missing, unreachable or
   otherwise not `applied`, stale on image/env/source, dirty, or reporting a
   max-concurrency value that differs from desired state.
   For `loom cluster rollout --scope current-gb10`, the cluster config must
   declare `env_state_profile`, `[gb10_pool] hosts = [...]`, and the resolved
   release manifest must contain at least one
   `gb10_worker_pool_desired_states` entry. An empty GB10 desired state is a
   release-contract error, even if a `gb10-workers status` artifact exists,
   because it proves no external worker target was declared. A GB10 desired
   state without `[gb10_pool]` hosts is also a release-contract error because
   rollout step 12 would have no actual hosts to prepare. For the v1.0 staging
   gate, the current profile and repo-owned SSH config both enumerate all 15
   GB10 hosts, and the acceptance artifact records `excluded_nodes=[]`.
   `deploy/environments/staging.cluster.toml` points `[gb10_pool].ssh_config` at that config
   plus `[gb10_pool].ssh_identity_file` at a platform-dev-local deploy
   identity. Step 12 therefore does not depend on `platform-dev` having
   operator-local `trt-gb10-*` aliases or a Mac forwarded-agent session. Step
   12 writes only non-secret release marker keys to each host-local env file
   and starts the host-local GB10 node-agent service; release-gate then
   requires every active host to report 10 slots, the target image/env, and
   the target source commit.
   Protected `environment-state apply/check` uses the same per-environment
   rollout lease as `loom cluster up`, defaulting to
   `$LOOM_ROLLOUT_LOCK_DIR` or `~/.loom/rollout-locks`. Set a shared
   `LOOM_ROLLOUT_LOCK_DIR` on hosts where multiple operators or Codex threads
   can mutate the same protected target. For shared staging, this low-level
   lease is broker-owned. If it reports an active owner, inspect the request
   through `loom-staging-rollout status REQUEST_ID`; operators must not use
   `--force-rollout-lock`. Resume only after the broker reconciles the prior
   attempt as terminal. For GitHub production deploy workflows, configure the
   environment variable `LOOM_ROLLOUT_LOCK_DIR`; the deploy helper fails closed
   for protected environments when it is unset.
   Staging uses the same flow with `--environment staging` and
   `deploy/environment-state/staging.toml`. The broker driver step 11
   keeps the physical `/data/loom-staging/environment-state/staging.toml`
   copy in sync with the candidate profile before mutation, so rerun/resume
   evidence does not depend on a stale one-time manual copy. It then executes
   the profile's required catalog provisioning command after environment-state
   apply and before environment-state check, writing redacted
   `catalog-provisioning.*` evidence. The profile may reference a
   secret-bearing env file, but step 11 always replaces any inherited
   `XDG_CACHE_HOME`, `HF_HOME`, and `HF_HUB_CACHE` values with a private
   mode-`0700` cache beneath the current rollout step. This prevents stale
   breakglass or prior-attempt cache paths from becoming catalog authority.
   A symlink, foreign owner/group, or mode drift in that cache fails before the
   catalog command runs. The operator-only env keys required by that gate are
   `PUBLISHED_SHA`, `HF_TOKEN`,
   `LOOM_SVC_DB_URL`, and `LOOM_SVC_MINIO_*`; secret-bearing values must come
   from the protected `staging-catalog-provisioning.env` file or equivalent
   `env_sources`, not argv or worker pods. The committed staging profile uses
   the HF registration/mirror path for SkillLearnBench; do not add the
   source-copy `loom datasets provision-catalog` step unless the profile also
   declares protected `LOOM_CATALOG_SOURCE_*` inputs. Because the target DB and
   MinIO Secret values use Kubernetes service names such as `loom-postgres` and
   `loom-minio`, step 11 opens short-lived, rollout-owned `kubectl port-forward`
   processes for those services and rewrites only the catalog command's
   effective env to `127.0.0.1:<port>` endpoints. Those forwards are recorded
   in `catalog-provisioning.json` and are stopped when the command exits; they
   are not operator terminal state. The same catalog gate publishes the
   checked-in `deploy/catalog/gb10-smoke` benchmark with
   `loom datasets publish-local` so current-GB10 rollout smoke has a real
   `s3://` task bundle instead of a manual DB row or `fixture://` source. For
   SkillLearnBench, the release
   manifest records the profile's catalog gate and `loom cluster release-gate`
   requires the matching `--hf-mirror-boundary-evidence` artifact before
   staging or production promotion can pass.

9. **Verify service-proxied downloads.** `loom_service` should use the
   cluster-internal MinIO endpoint for object reads, then stream ATIF,
   trajectory, and artifact downloads through authenticated API routes. Browser
   and laptop CLI users should not need direct access to the MinIO S3 port.

   ```bash
   curl -H "Authorization: Bearer $TEAM_TOKEN" \
     "$LOOM_API/api/v1/trials/$TRIAL_ID" \
     | jq -r '.trajectory_url,.atif_url,.artifacts[].download_url'

   loom eval trial show "$TRIAL_ID"
   loom eval trial debug "$TRIAL_ID" --format json
   loom eval trial download "$TRIAL_ID" --kind atif --output atif.json
   loom eval trial download "$TRIAL_ID" --kind trajectory --output events.jsonl
   ```

   Every returned URL should stay on the Loom API host, and a normal authorized
   `curl -L` against those URLs should return the object body without opening a
   separate MinIO tunnel. The CLI should print download commands rather than
   raw MinIO/S3 signed URLs. The debug command should return stable failure
   taxonomy fields, including `failure.reason_code`,
   `failure.failure_class`, `failure.root_cause`,
   `failure.platform_outcome`, `failure.score_outcome`, and
   `failure.rerun_recommendation`, plus lifecycle state, token usage summary,
   scoped evidence links, agent timeout config, last trial event, last LLM call,
   worker heartbeat freshness, stale-running keep/reclaim decision, and
   redacted next actions without bearer tokens, provider keys, internal service
   URLs, or signed object-store URLs. Confirm reward `0` with verifier output
   is a platform-successful `score_failure`; missing verifier output, missing
   trajectory/ATIF, provider no-call/timeout, setup/build/image/preflight
   failures should appear as distinct classes/root causes rather than one
   generic platform failure.

9. **Approve account requests into opted-in teams.** Public registration is
   default-closed per Team (`public_registration_enabled=false`). A researcher
   can only discover and request Teams an admin has explicitly enabled. Private
   remembered UUIDs return `404 team not found`:
   ```bash
   curl -X POST https://loom.example.com/api/v1/auth/registration-requests \
     -H "Content-Type: application/json" \
     -d '{"username":"Mark", "team_id":"00000000-0000-0000-0000-000000000000"}'
   ```
   Internal teams are admin-managed ahead of time. List or create them, then
   enable public registration before approving a request:
   ```bash
   curl https://loom.example.com/api/v1/admin/teams \
     -H "Authorization: Bearer $ADMIN_TOKEN"

   curl -X POST https://loom.example.com/api/v1/admin/teams \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: qianyi" \
     -H "Content-Type: application/json" \
     -d '{"name":"Research Platform"}'
   ```
   An admin lists and approves pending username/password account requests into
   their requested team with an explicit role:
   ```bash
   curl https://loom.example.com/api/v1/admin/registration-requests?status=pending \
     -H "Authorization: Bearer $ADMIN_TOKEN"

   curl -X POST https://loom.example.com/api/v1/admin/registration-requests/$REQUEST_ID/approve \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"role":"member"}'
   ```
   The approval response reveals a one-time browser password setup link exactly
   once. Deliver the link to the requested user out of band; the user sets a
   password and receives the selected team membership without seeing raw API
   credentials. Loom does not email the link in staging.

   The legacy team-registration endpoints are only for old invite-based
   onboarding flows. Use them when a pending row came from
   `/api/v1/teams/register`, not for username/password account requests:
   ```bash
   curl https://loom.example.com/api/v1/admin/team-registrations?status=pending \
     -H "Authorization: Bearer $ADMIN_TOKEN"

   curl -X POST https://loom.example.com/api/v1/admin/team-registrations/$REG_ID/approve \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: qianyi" \
     -H "Content-Type: application/json" \
     -d '{"team_id":"'"$TEAM_ID"'", "role":"member"}'
   ```
   Reject accidental username/password account requests with
   `POST .../registration-requests/$REQUEST_ID/reject`. Reject legacy team
   registrations with `POST .../team-registrations/$REG_ID/reject` and the same
   actor header. Review backend audit evidence with:
   ```bash
   curl https://loom.example.com/api/v1/admin/audit-events?limit=20 \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Audit rows include authenticated or operator-attested actors and safe
   metadata such as token prefixes, never raw bearer, invite, setup, or reset
   tokens. Results use stable `created_at DESC, id DESC` keyset order. When
   `next_cursor` is non-null, pass that event UUID unchanged as the next
   request's `cursor`; the service resolves its timestamp for stable tie-breaker
   traversal. Continue until the cursor is null rather than assuming the first
   page contains the complete audit history.

10. **Staging incident controls.** Use the same admin token plus an
    operator-attested `X-Loom-Admin-Actor` for every emergency mutation:
    ```bash
    # Revoke a leaked or overused API token by hash prefix.
    curl -X DELETE https://loom.example.com/api/v1/tokens/$TOKEN_PREFIX \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "X-Loom-Admin-Actor: incident-commander"

    # Revoke an invite link.
    curl -X POST https://loom.example.com/api/v1/invites/$INVITE_ID/revoke \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "X-Loom-Admin-Actor: incident-commander" \
      -H "Content-Type: application/json" \
      -d '{"reason":"reported outside intended recipient"}'

    # Disable or re-enable a team. Disabled teams cannot call team APIs.
    curl -X POST https://loom.example.com/api/v1/admin/teams/$TEAM_ID/disable \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "X-Loom-Admin-Actor: incident-commander" \
      -H "Content-Type: application/json" \
      -d '{"reason":"suspected token leak"}'
    curl -X POST https://loom.example.com/api/v1/admin/teams/$TEAM_ID/enable \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "X-Loom-Admin-Actor: incident-commander" \
      -H "Content-Type: application/json" \
      -d '{"reason":"leak contained"}'

    # Pause or resume only new submissions for a team.
    curl -X POST https://loom.example.com/api/v1/admin/teams/$TEAM_ID/pause-submissions \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "X-Loom-Admin-Actor: incident-commander" \
      -H "Content-Type: application/json" \
      -d '{"reason":"provider incident hold"}'
    curl -X POST https://loom.example.com/api/v1/admin/teams/$TEAM_ID/resume-submissions \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "X-Loom-Admin-Actor: incident-commander" \
      -H "Content-Type: application/json" \
      -d '{"reason":"provider restored"}'

    # Rotate a provider connection secret.
    loom providers rotate-key "$PROVIDER_CONNECTION" --api-key env:OPENAI_API_KEY
    ```
    Correlate incidents with `GET /api/v1/admin/audit-events?limit=100`,
    `loom eval usage --team-id "$TEAM_ID"`, the Gateway cost dashboard, and
    the Service dashboard's auth-failure / submission-reject panels. Provider
    create/update/test/delete mutations are audit-logged without raw API keys.

11. **Scan logs for leaked secrets before sharing incident bundles.** Pull the
    relevant logs, then fail closed if token, provider-key, or signed-URL
    patterns appear:
    ```bash
    kubectl logs -n loom -l app=loom-service --since=30m > /tmp/loom-service.log
    kubectl logs -n loom -l app=loom-llm-gateway --since=30m > /tmp/loom-gateway.log
    kubectl logs -n loom -l app=loom-worker --since=30m > /tmp/loom-worker.log

    if rg -n --pcre2 \
      '(loom_(team|admin|invite)_[A-Za-z0-9_-]+|X-Amz-Signature=|AWSAccessKeyId=|sk-[A-Za-z0-9_-]{20,}|api[_-]?key=)' \
      /tmp/loom-service.log /tmp/loom-gateway.log /tmp/loom-worker.log; then
      echo "Potential secret leak in logs; do not share bundle"
      exit 1
    fi
    ```
    For completed artifacts, run
    `python scripts/check_no_provider_keys_in_artifacts.py <artifact-path>`
    before publishing or attaching them to issues.

12. **Post generated GitHub issue/comment bodies from files.** Long generated
    Markdown must go through a file-backed path so shell quoting, backticks,
    `$()` fragments, or heredocs cannot corrupt the body or expose secrets.
    Write the body to a temporary file, scan that file before submission, submit
    it with `--body-file`, then remove the temporary file after the API call
    succeeds:
    ```bash
    BODY_FILE=$(mktemp)
    $EDITOR "$BODY_FILE"

    if rg -n --pcre2 \
      '(github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,}|api[_-]?key=)' \
      "$BODY_FILE"; then
      echo "Potential secret leak in GitHub body; do not submit"
      exit 1
    fi

    # Choose the mutation you need.
    gh issue edit "$ISSUE_NUMBER" --repo qianyi-sun/loom --body-file "$BODY_FILE"
    # gh issue comment "$ISSUE_NUMBER" --repo qianyi-sun/loom --body-file "$BODY_FILE"
    rm -f "$BODY_FILE"
    ```
    Do not paste raw bearer tokens, provider keys, signed URLs, or exact secret
    values into issues, PRs, comments, generated Markdown, shell history, or
    shared evidence. If a token-like value reaches a public issue body, replace
    the current body with a redacted file-backed body, revoke or rotate the
    credential, resolve the GitHub secret-scanning alert only after verifying
    the exposed value is inactive, and record the residual risk that GitHub's
    historical edit records or caches may have contained the old body.

13. **Write one-off remote secret files with the checked helper.** Do not pipe a
    token-producing command into `ssh` while also using an SSH heredoc; the
    heredoc consumes stdin and can route the token into shell source or command
    output. Use the helper when a benchmark or incident run needs a short-lived
    remote secret file:
    ```bash
    scripts/ops/write_remote_secret.py \
      --host platform-dev \
      --remote-path /shared_work/qianyi/skilllearnbench-official/.secrets/skilllearnbench-github-token \
      -- gh auth token
    ```
    The helper captures the local secret command, strips only trailing
    newlines, sends the secret as SSH stdin to a quoted remote `bash -lc`
    writer, sets mode `600`, and prints only `mode`, `size`, and `path`.
    Delete short-lived remote secret files as soon as the run has performed its
    own post-run scan.

## Release migrations against protected Postgres (#332)

Staging Postgres is fronted by a NetworkPolicy that only
permits ingress from three service labels (`app=loom-control-plane`,
`app=loom-service`, `app=loom-llm-gateway`) plus — as of #332 — the
sanctioned migration label `app=loom-migration`. Any generic migration
Job you launch without `app=loom-migration` will time out on port 5432,
regardless of what the `cp-db-url` Secret says. This is by design: the
standing NetworkPolicy is the release-critical gate against ad-hoc write
access to the DB.

**Do not label a migration Job with any of the standing service labels
(`app=loom-control-plane`, `app=loom-service`, `app=loom-llm-gateway`).**
Those labels are also the Service selectors, so the transient Job pod
would receive real traffic during rollout.

### Rendering the sanctioned migration Job

For shared staging, only candidate-bound broker step 09 renders, applies, waits
for, and records this Job. The image tag and namespace come from the immutable
request envelope; an operator must not render or apply a Job from an ambient
checkout or chosen tag. Inspect migration results through broker status/logs
and resume the same request after repairing a bounded external failure. Manual
render/apply is limited to development or custom clusters that do not target
`loom-staging`.

The generated Job name gets a UTC-timestamp suffix so broker resume against the
same image tag does not collide with a still-lingering previous Job. The prior
Job remains for its `ttlSecondsAfterFinished` evidence window.

The Job carries three self-cleaning settings:

| Setting | Value | Why |
|---|---|---|
| `activeDeadlineSeconds` | 600 | An Alembic upgrade shouldn't need more than 10 min. If it does, the operator sees the failure instead of a stuck pod. |
| `ttlSecondsAfterFinished` | 600 | Automatic cleanup so the next rollout doesn't need `kubectl delete job` first. |
| `backoffLimit` | 1 | A single retry, then the failure surfaces cleanly. |
| `restartPolicy` | `Never` | Alembic isn't reentrant on partial state. |

### If it fails

The most common failure modes:

- **Connection timeout on port 5432** — check that the Job carries the
  `app=loom-migration` label. If someone renders this by hand without the
  subcommand, they may drop the label. `kubectl get job -o yaml` shows both
  the Job's `metadata.labels` and the pod template's `spec.template.metadata.labels`;
  both must be present.
- **`alembic upgrade head` fails** — check `kubectl logs`. Common causes:
  missing migration in the release image (compare `alembic current` vs
  `alembic history`), or a schema state the migration doesn't expect
  (typically a partial prior migration). This is not a NetworkPolicy
  issue; see the release plan for the migration.
- **Job already exists** — the Job's `spec.template` is immutable. Either
  wait for `ttlSecondsAfterFinished`. On shared staging, keep the evidence and
  resume the original broker request; do not delete or re-apply the Job from an
  operator shell. Development/custom-cluster operators may remove their own
  stale Job before rerunning their separately scoped manual flow.

## Rollout build resilience (#199)

Staging rollout builds run `pip install` inside each service
image (`Dockerfile.control-plane`, `Dockerfile.gateway`, `Dockerfile.service`,
`Dockerfile.worker`, `Dockerfile.egress-xds`). A single transient PyPI
`ReadTimeoutError` used to fail the whole rollout — observed during
`staging-92f0090` where `loom-service` aborted mid-build even though CI
and the preceding `loom-control-plane` image had built cleanly.

Each rollout-critical Dockerfile now sets:

```dockerfile
ENV PIP_RETRIES=10 PIP_DEFAULT_TIMEOUT=60
```

pip honors these env vars natively, so the individual `pip install` lines
don't need per-invocation flag surgery. Effect vs. defaults:

| Setting | Default | Rollout | Notes |
|---|---:|---:|---|
| retries per package | 5 | **10** | doubles tolerance for transient DNS / TLS / mid-transfer resets |
| connect + read timeout | 15 s | **60 s** | tolerates a slow single-wheel transfer without giving up |

Worst case per package: 10 × 60 s = 10 min. In practice pip's exponential
backoff caps sooner and the retries are only exercised when PyPI is actually
struggling. Fast-path installs are unchanged.

**Sandbox images** (`Dockerfile.agent-sandbox`, `Dockerfile.gateway-sandbox`)
are not release-gating; they can be rebuilt out-of-band without blocking a
rollout, and they intentionally do not set these env vars so the operator
can override PyPI mirror settings at build time when constructing bespoke
agent variants.

**When the retry is exhausted anyway** — the rollout evidence directory
still contains the `docker-build.log` for each service. Search for
`Retrying (Retry(total=` or `ReadTimeoutError` lines to distinguish a
network-timeout failure from an application-code error. #340 (rollout
driver) will surface this classification in the evidence summary.

Test coverage: `tests/loom_cli/test_dockerfile_pip_resilience.py`
parametrizes over every rollout-critical Dockerfile and asserts
`PIP_RETRIES >= 5` and `PIP_DEFAULT_TIMEOUT >= 30`, so a future change
that drops the env vars breaks CI before the rollout does.

## One-command rollout driver (#340)

`loom cluster rollout` orchestrates the full staging rollout: resolve
target ref → worktree → build → kind-cluster → kind-load → backup → audit →
render → preflight → migrate → cluster up → env-state → GB10 prep →
production defaults → release-gate → smoke → summary. Every step writes evidence
into a per-rollout directory tree; a re-run of the same command safely
resumes from the interrupted step. The driver also records its process owner in
`state.json`; a second invocation refuses while the previous driver is still
alive, and takes over a stale `running` state after the previous driver process
has exited.

### TaskSet materialization recovery

TaskSet materialization publishes a generation only when its lease-fenced
transaction replaces the current `Task` rows. For a stalled rebuild, inspect the
materializer job/lease state and heartbeat together with the current
`Task.source` generation; those sources remain the authoritative publication
pointer while another generation stages.

Do not manually purge `tasksets/user/<team>/<slug>/` or delete an object-store
prefix to clear a stalled TaskSet. The TaskSet GC poll loop runs a bounded live
generation reconciler alongside retained soft-delete root cleanup. It retries
only stale, unreferenced `materializations/<job-id>/<epoch>/` prefixes and
leaves durable inputs, legacy paths, active lease epochs, and current Task
sources intact. A durable database cursor advances only after each completed
bounded sweep, so clean older TaskSets or jobs do not indefinitely delay a
later abandoned generation and a restart repeats rather than skips a page. A
complete root is removed only after the configured soft-delete retention delay;
investigate materializer/job state instead of shortening that retention or
performing object/DB surgery.

### Disposable TaskSet lease-fencing canary (#756)

Task 6 tests are not staging proof: they remain deterministic fixture support
for the fenced materializer. Task 7 supplies the deployment-side,
authorization-restricted cooperative runner. It runs only through
`loom cluster taskset-fence-canary`; it adds no HTTP route, normal-user CLI
command, worker mode, generic pause, or failure-injection control. It is not a
release-gate schema change and does not persist candidate identity in a TaskSet
row.

Do not collect this canary until the runner is merged, its PR CI is green, the
candidate staging rollout is complete, the candidate SHA/image tag is fixed,
and Task 7 integration has explicitly reached staging collection. The command
accepts no candidate SHA/image tag, owner, storage prefix, output path, or
authorization-token argument. It derives candidate identity only from the
completed rollout's `inputs.json`, rejects any non-staging/prod/mismatched
candidate, and writes exactly one immutable JSON record at the rollout-owned
`canaries/taskset-lease-fencing/evidence.json` path.

If a failed candidate rollout needs a separate retry evidence directory, use a
staging tag such as `staging-rerun-<sha7>`. The deployment canary accepts that
form only when the final seven hexadecimal characters match the fixed candidate
SHA; a retry label never permits a different candidate or image identity.

The launcher pins the fixed staging Kubernetes context and selects a Ready
`loom-service` Pod only when its service-container digest, template image, and
converged Deployment generation match the completed candidate release
manifest. It rechecks that exact Pod identity around both internal operations;
mixed revisions, stale rollout evidence, a changed pod, or a non-ready target
fail closed. Evidence is written through the validated rollout directory as a
private fsynced temporary file, atomically published without replacement, and
directory-fsynced; a partial private temporary file is discarded on retry.

The root-owned installation and merged staging profile provision the same
high-entropy capability in both service-managed locations: the protected
staging `loom-secrets` key `taskset-fence-canary-token` (mounted only into
`loom-service` as `LOOM_SVC_TASKSET_FENCE_CANARY_TOKEN`) and the fixed
platform-dev source used by the candidate-bound command. Operators do not
create, rotate, copy, or reconcile this secret. If either side is absent or
mismatched, stop collection, repair it through the merged profile/root-owned
maintenance path, and resume the original broker request. Never pass, print,
or attach the value.

Inside the selected service Pod, the deployment runner creates a fresh
one-task disposable bundle under the fixed `loom-system-taskset-fence-canary`
system Team through normal TaskSet intake. Migration `0065` reserves this Team
only when neither its fixed UUID nor name exists; any pre-existing identity
fails the upgrade rather than being adopted. The runner matches both the UUID
and name, takes the Team row lock, and fails closed if it is absent, disabled,
or altered. Neither registration, an administrator invite, nor administrator
Team mutation controls can alter or add a human to this identity. A database
downgrade restores the `0064` image-tag constraint and
removes this Team only while the identity is pristine and has no dependent
rows; any existing canary/reference/alteration makes the downgrade fail closed
rather than cascade or reuse deployment data. If legacy cleanup has already
removed the deployment-owned identity, the downgrade only restores the `0064`
constraint and a later upgrade recreates it. The runner calculates
the normal materializer checksum, creates the TaskSet/job, and writes its
durable one-use authorization bound to the candidate, exact initial job,
checksum, and a service-generated high-entropy nonce retained only as a digest
in one database transaction. Only then does it return safe TaskSet/checksum
metadata to the launcher for the later runner exec. The external command never
accepts a TaskSet id or checksum, so a fresh ordinary user TaskSet cannot be
selected, authorized, or consumed by this flow. The later runner only locks and
consumes that pre-existing record. A pre-existing, rebuilt, claimed, published,
deleted, mismatched, or replayed canary TaskSet fails closed. If a post-stage
check fails, the current lease is relinquished through the normal fenced
transition rather than remaining running. If the runner exits before consuming
its authorization, it retires that exact TaskSet; an authenticated later
prepare also retires any earlier unconsumed system canary before allocating its
successor, so repeated killed-driver handoffs cannot consume the Team quota.
Stream API responses through a whitelist if retaining submission/status
context; never save a full Task response because it includes a storage
`source` field. The public catalog returns private TaskSet source locations
only to the owning Team; foreign catalog/detail reads retain non-sensitive
metadata but redact `source`.

```bash
loom cluster taskset-fence-canary \
  --rollout-dir "$ROLLOUT_DIR"
```

The runner cooperatively stages A, relinquishes only A's current lease through
the normal fenced transition, claims/stages/publishes B, then resumes A's
normal publication CAS and requires `LeaseLost`. It writes the candidate-bound
JSON evidence only after the winning generation, expected checksum, and stale
CAS result all match. The artifact contains candidate SHA/image tag, job/epoch
pairs, opaque owner fingerprints, published generation, task count/checksum,
stale-CAS outcome, loser-GC eligibility (not GC execution), and timestamps.
Validate the final JSON before attachment:

- It contains no raw `claimed_by`, token, cookie, authorization header, host or
  PID, source URI, object key/prefix, signed URL, manifest, credential, or raw
  error payload.
- The winner's published generation equals its lease epoch, the selected task
  checksum is from the normal task API, and the loser was fenced before
  publication.
- The canary never performs object deletion or runs GC. On a runner exit it
  soft-deletes only its own authorization-bound system TaskSet, and the next
  authenticated prepare retires any earlier unconsumed system canary. This
  frees the active TaskSet quota while retaining the root for the normal
  delayed GC policy. Eligibility is evidence for the existing bounded
  reconciler, not permission to run it during collection.

Do **not** manufacture the handoff by killing a driver or pod, using `SIGSTOP`,
editing rows with manual SQL, mutating the object store, injecting a failure,
or deleting a prefix. If the normal cooperative path cannot be observed without
one of those actions, stop the collection and record it as unavailable rather
than claiming the canary passed.

### Protected workload-trust contract (#755)

For a staging or production rollout, the profile must declare exactly
`internal_trusted` with `taskset_transforms_enabled=false`,
`taskset_transform_network_isolated=false`, and
`untrusted_workload_isolation=false`. Protected preflight records the
`workload-trust-contract` check. `--skip-preflight` does not bypass the
contract: `loom cluster up` revalidates it before it can obtain the protected
rollout lease or apply resources.

The rollout release manifest carries only the structural four-field contract.
The release gate then checks that it matches the live `loom-service`
environment. Evidence may report structural failures but must not emit raw
invalid profile, manifest, or live env values; use the normal redacted evidence
forms instead. A transform canary is not a valid v1 smoke: transform manifests
must fail with `transform_unavailable_in_internal_trusted` before fetch or run.
A protected namespace is authoritative: `--environment` must match it or the
command fails before it can obtain a rollout lease or apply resources, including
with `--skip-preflight`. Explicit environments remain valid for non-protected
custom namespaces.
Manual rollout validates protected cluster and namespace identity before
evidence or Kind work, so neither a dry-run plan nor the driver can downgrade
or swap a protected physical target.
For the decision and post-v1 boundary, see
[`v1-workload-trust-contract.md`](../architecture/adr/v1-workload-trust-contract.md).

### Independent staging operator interface (#803)

The supported `platform-dev` interface is the root-installed broker, not a
personal checkout or user-owned systemd unit:

```bash
# Verify the installed exact candidate without allocating a request.
loom-staging-rollout preflight

# Preview caller authorization and the freshly fetched merged dev candidate.
loom-staging-rollout start --dry-run

# Create a verified backup and launch one detached rollout.
loom-staging-rollout start

# Inspect the active request, or one known request.
loom-staging-rollout status
loom-staging-rollout status REQUEST_ID
loom-staging-rollout logs REQUEST_ID
loom-staging-rollout logs REQUEST_ID --follow

# Continue the same immutable candidate after a real failed/cancelled attempt.
loom-staging-rollout resume REQUEST_ID

# Stop an abandoned or unsafe attempt; the reason is mandatory audit evidence.
loom-staging-rollout cancel REQUEST_ID --reason "bounded operational reason"

# Remove only this failed pre-launch request's incomplete, no-manifest backup root.
loom-staging-rollout cleanup-incomplete-backup REQUEST_ID
```

For attempts that entered protected apply, `status` may include
`protected_component` and `protected_component_status`. An `incomplete` value
identifies the exact component whose intent exists without an immutable
terminal record; it is safe diagnostic metadata, not permission to edit the
request store or bypass the broker. For `gb10-candidate`, a
`protected_failed_hosts` list contains only validated names from the fixed
release inventory; it deliberately excludes SSH or remote-command output.
If normalized final-gate evidence exists before driver state or `driver.log`,
`status` may also include `final_gate_check`, `final_gate_outcome`, and the
contract-defined `final_gate_failure_code`. It never returns evidence payloads,
remediation text, subprocess output, or credential material.

`qianyi`, `hongjian`, and `devansh` are members of
`loom-staging-operators`. Invoke these broker commands directly as the
authenticated operator, without a leading `sudo`; the broker derives the OS
caller rather than accepting an actor argument. Exact-source `install` and
`check` remain the separate root-owned path documented below. Every new
`start` fresh-fetches exactly `refs/heads/dev` from
`https://github.com/qianyi-sun/loom.git`, pins
that merged 40-character SHA, and derives `staging-<sha7>`. Unmerged pull
requests, feature branches, tags, historical SHAs, local commits, custom image
tags, alternate remotes, and target/config/secret overrides are forbidden.
Rollback is a merged revert on `dev` followed by another normal `start`.

`preflight` is requestless: it binds the exact candidate and current mutation
epoch, runs Tier 0-2, and creates no request, lifecycle pointer, backup, systemd
unit, rollout directory, or staging mutation. The host installer uses only this
command for its post-install readiness check. The minimal readonly epoch probe
does not require the later runtime capacity row; the complete Tier 2 database
check still does, so one requestless run reports that row as a blocker together
with all independent failures. A missing row is retained as explicit database
evidence rather than masking health, authentication, catalog or network
results; only `staging.storage-db` reports
`dependency-capacity-unready`. Tier 1 feeds the rendered bytes through the same
`loom-staging-rollout` server-side apply field manager, strict validation, and
request timeout used by protected convergence. API/schema validation uses a
mutation-free server dry-run whose conflict forcing is confined to that one
request. The independent field-ownership check uses the exact no-force
fixed-manager contract later consumed by protected apply. Schema rejection and
legacy managed-field ownership conflicts are therefore both reported before a
request or backup, while only ownership blocks final protected convergence.
Never use the schema-only force flag to adopt live fields. `start --dry-run` writes a
non-active preview request but creates no backup, systemd unit, rollout
directory, or staging mutation. A real `start` first writes and
verifies a new immutable backup manifest, then publishes the private request
envelope and launches a detached `loom-rollout` service unit. It never consumes
a mutable `backups/latest` pointer. `resume` uses the original request's exact
SHA, image tag, backup manifest and digest, rollout ID, and config binding even
if `dev` has advanced.

Every private backup file is created with no-follow/exclusive semantics and is
kept empty until its owner, link count, `0600` mode, and access ACL converge.
The writer removes an inherited POSIX access ACL and proves that no ACL remains
before writing PostgreSQL, object, secret, inventory, or manifest bytes. A
missing removal primitive, metadata drift, or residual named reader fails the
backup before publication; the later trusted reader remains equally strict.
The isolated Secret clone requires PostgreSQL credentials plus each service's
direct DSN and rewrites them to the rehearsal database. PgBouncer DSNs remain
optional exactly as in the workloads: present pool keys are also rewritten,
while an absent optional pool key stays absent and the service uses its direct
DSN fallback.
The checkpoint also freezes worker heartbeat and staging-capacity timestamps.
Before the candidate admission smoke, the executor publishes a deterministic
worker row and refreshes only the cloned capacity row after proving its exact
policy/evidence digests and counters remain admission-safe. The capacity
counters are not invented or recollected from protected staging: they remain
the checkpoint snapshot and are rebound only to the isolated namespace. A
missing, corrupt, policy-drifted, or high-water row fails rehearsal before the
candidate submission. HTTP failure evidence retains only a fixed request ID,
allowlisted reason code, and response digest; raw response bodies are never
persisted.
Before apply, the isolated release artifact also uses the API-server canonical
shape: mapping-valued null fields are omitted and container resource quantities
are strings. Empty `EnvVar.value` fields are also omitted because the API server
uses the same empty value while dropping that JSON field. Live readback remains
an exact subset check; arbitrary numeric and string values are not treated as
interchangeable.

When `manifests.field-ownership` reports only the recognized legacy lifecycle
resources, keep rollout admission disabled with the root-owned maintenance
marker and keep the lifecycle CronJob suspended. From the exact installed
sealed candidate, first produce a requestless review document:

```bash
sudo /usr/bin/python3 \
  /opt/loom-staging-runner/source/scripts/ops/staging_rollout_host.py \
  maintenance-enable
loom-staging-rollout manifest-ownership inventory
```

The root helper validates the exact ready install record/source, publishes the
marker under the broker launch lock, and proves there is no active pointer or
rollout unit. It does not remove broker sudo authority: the marker itself makes
ordinary `start` fail closed while the bounded maintenance subcommand remains
available. A normal host `check` reports `maintenance-marker` until the window
is explicitly closed.

Review its candidate/tree, epoch, complete existing rendered-resource set,
UID/resourceVersion and optional generation records, live/managed-fields/
desired/overlay digests, and `inventory_sha256`. Cluster-scoped resources use
an empty namespace identity; not-yet-created candidate resources are excluded
from adoption and remain owned by the normal protected apply. Only then use a
new bounded maintenance request ID and repeat that exact digest:

```bash
loom-staging-rollout manifest-ownership apply \
  --request-id req-manifest-ownership-EXACTSUFFIX \
  --approved-inventory-sha256 EXACT_64_HEX_DIGEST
```

The broker recomputes the inventory before mutation, publishes it once under
`/var/lib/loom-staging-rollout/maintenance/manifest-ownership/`, claims the
protected mutation epoch, adopts every existing rendered field set without
semantic change, and then retires only recognized legacy managed-field entries.
That cleanup preserves `loom-staging-rollout` and controller ownership, uses an
exact JSON Patch for each affected resource, and must pass a server-side dry-run
and semantic comparison before the actual patch. It never clears the complete
`managedFields` array. Only the three exact NetworkPolicies are then applied
through the normal no-force manager. Any inventory, UID/resourceVersion, epoch,
journal, cleanup, apply or post-readback drift stops the request. Never invoke
`kubectl --force-conflicts` or patch `managedFields` directly, reuse a
maintenance request ID, or unsuspend the CronJob as part of ownership adoption.

The ownership path and the final rollout share one checked-in mutation-epoch
CAS renderer. It accepts only bounded request IDs, hexadecimal evidence
digests, non-negative integer epochs, and an explicit bootstrap boolean; the
validated literals are rendered into one newline-free SQL argument. Do not
replace this contract with `psql -v` substitution for `-c` statements or with
operator-provided SQL.

After server-side apply returns, the operator permits only three read-only
live-state observations over a bounded one-second window (immediate, +250 ms,
+750 ms). This absorbs a stale API-server read without repeating any mutation;
the successful observation count is journaled. These observations consume the
same checked-in semantic comparator as the inventory dry-run, including the
exact empty NetworkPolicy rule-list and legacy apply-bookkeeping normalization;
there is no second post-apply predicate. Persistent drift keeps the same
fail-closed failure code and requires a new request after diagnosis.

Kubernetes may omit an explicitly empty NetworkPolicy `ingress` or `egress`
array from its live JSON while retaining legacy server-side-apply ownership for
that field. The adoption overlay includes such an empty field only when the
exact desired value is `[]` and a recognized legacy manager still owns the
matching managed-field path. Non-empty or unowned fields remain excluded, and
the server dry-run must still prove the overlay is a semantic no-op.
For that proof only, a missing NetworkPolicy rule list is equivalent to `[]`
when the matching `policyTypes` entry is explicit; a non-empty rule change is
never normalized away.
The exact legacy `kubectl.kubernetes.io/last-applied-configuration` annotation
is also excluded from that semantic comparison because it is client-side
bookkeeping and server dry-run may omit it. Every other annotation remains
protected and must match.

If a request claims the epoch and adopts only part of the rendered resource set
before failing, preserve that request and use a new request only after a new
inventory. The inventory accepts each existing rendered target when it is
still held by a recognized legacy manager or is already held by
`loom-staging-rollout`, which allows exact partial-state convergence. The same
plan covers spec-bearing resources and top-level authorities such as ConfigMap
`data`; its force dry-run must prove a semantic no-op for every target before
mutation. Unknown managers and controller-only ownership remain fail-closed.
If legacy entries remain after adoption, the next exact request repeats the
selective cleanup; already-clean resources generate no patch. Cleanup journals
its exact resource count and aggregate digest before any later convergence
stage.

After the exact post-readback and final no-force dry-run succeed, close the
window through the same installed source; the command refuses if any rollout
pointer or unit is active:

```bash
sudo /usr/bin/python3 \
  /opt/loom-staging-runner/source/scripts/ops/staging_rollout_host.py \
  maintenance-disable
sudo /usr/bin/python3 \
  /opt/loom-staging-runner/source/scripts/ops/staging_rollout_host.py \
  check
```

The staging sizing policy is based on an aggregate-only live inventory taken
on 2026-07-15; no keys, credential values, or payloads were emitted. It measured
579,714 objects and 12,517,813,079 bytes across
the two protected buckets (13,460 trajectory objects / 807,799,196 bytes and
566,254 artifact objects / 11,710,013,883 bytes). The installer-validated,
root-owned policy allows at most 1,000,000 MinIO objects and 1,000,004 regular
files across the complete bundle; the four-file allowance is the PostgreSQL
dump plus three protected Secret exports. This replaces the accidental 99,996
object ceiling while retaining finite headroom over the measured store.

The current safe-key shape charges 6,420,179 conservative MinIO entries, has
maximum mirror depth 18, maximum direct-directory fanout 6,530, and no unsafe
keys. The independent fail-closed bounds are therefore 16,000,000 entries and
16 TiB for the whole bundle (15 TiB for MinIO and 1 TiB for PostgreSQL), plus
20,000 listing pages,
depth 64, 22 hours elapsed time, safe relative object paths, 256 MiB free-space
reserve, 1,024 free-inode reserve, and immutable manifest publication. The
installed config must contain `backup_max_objects = 1000000` and
`backup_max_entries = 16000000`; arbitrary operator overrides are rejected.
At 800,000 objects or 12,800,000 conservative entries (80% of either hard cap),
the platform owner must rerun the aggregate inventory and review objects,
entries, pages, bytes, depth, and fanout in a merged PR before changing the
root-owned literals. Runtime auto-growth is forbidden.

These exact policy keys extend the operator config within schema v1 and are
installed atomically with the matching broker package. A stale v1 config
missing either key is rejected before backup or mutation, and downgrading an
installed runner to a binary that does not know the keys is unsupported; use a
merged revert and the normal atomic installer path instead.

Object exhaustion is recorded as the secret-safe public reason
`backup_object_limit_exceeded`, distinct from generic `backup_failed`, and
still publishes no envelope or unit. `cleanup-incomplete-backup` accepts only a
failed pre-launch request while staging is idle. It rejects previews, any
request with an envelope or attempt, ambiguous roots, unsafe ownership/modes,
symlinks, hard links, special files, traversal-limit drift, and every root containing
`backup-manifest.json` or targeted by `backups/latest`. Cleanup is
request-scoped and idempotent; it never deletes a valid manifest-backed restore
point and leaves the request status failed for audit.

Before starting the expensive PostgreSQL dump, the broker opens its own
localhost-only MinIO transport with `kubectl port-forward --address 127.0.0.1
service/loom-minio :9000` and derives the ephemeral local port only from that
child process's exact readiness line. It never claims, probes, reuses, or stops
the historical local port `19000`, so an operator-owned or concurrent tunnel
on that port is outside the broker lifecycle. The broker keeps its child alive
through the bounded MinIO mirror and confirms that exact child has exited
before any manifest can be published. Startup or cleanup failure is reported
as the non-secret public reason `backup_transport_failed`; it publishes no
envelope or rollout unit and remains eligible for the same request-scoped
incomplete-backup cleanup path.

Only one full staging request may be pending or running. A second `start` or
`resume` fails instead of queueing or preempting and reports only safe active
request metadata. `status` and `logs` are available to every operator; a
terminal disconnect does not stop the service-owned unit. Do not edit
`active.json`, `state.json`, the request envelope, locks, or evidence to recover
a run.

#### Root installation and update

Installation is a root maintenance action, not part of each rollout. Run it
only from a clean root-owned merged checkout beneath a root-owned,
non-group/world-writable parent chain. A developer-owned checkout remains
replaceable after `sudo`, triggers Git's dubious-ownership protection, and is
not supported; never add it to global `safe.directory`. This is an operator
bootstrap prerequisite: the script cannot establish checkout trust after
Python has already loaded it. Its in-process ownership check catches accidental
drift before installer-managed mutation, not a malicious checkout controlled by
an already root-equivalent operator. The installer takes its assets from the
exact freshly fetched `dev` head by default. The only exception is the
coordinator-only sealed cumulative repair mode below; it still rejects
working-tree content and binds immutable Git objects rather than a branch or
arbitrary ref.

The host must provide Ubuntu with systemd, a root-owned `/usr/bin/python3`
whose resolved executable stays under `/usr` and reports Python 3.11 or newer,
and a root-owned, non-group/world-writable `/usr/local/bin/uv`. Do not satisfy
these checks with a developer-managed interpreter, a user-local `uv`, a PPA, or
a hand-written compatibility symlink. The current reviewed x86_64 Linux
bootstrap uses upstream `uv` 0.11.26 and its published checksum:

```bash
uv_version=0.11.26
uv_asset=uv-x86_64-unknown-linux-gnu.tar.gz
uv_tmp=$(mktemp -d)
(
  cd "$uv_tmp"
  curl -fsSLO "https://github.com/astral-sh/uv/releases/download/${uv_version}/${uv_asset}"
  curl -fsSLO "https://github.com/astral-sh/uv/releases/download/${uv_version}/${uv_asset}.sha256"
  sha256sum -c "${uv_asset}.sha256"
  tar -xzf "$uv_asset"
  sudo install -o root -g root -m 0755 \
    "uv-x86_64-unknown-linux-gnu/uv" /usr/local/bin/uv
)
rm -rf "$uv_tmp"
/usr/local/bin/uv --version
```

The `loom-rollout` service identity must expose the same nonzero UID through
its passwd entry and `id -u loom-rollout`. It also has one exact primary-group
authority: the passwd primary GID, the GID of the named `loom-rollout` group,
and `id -g loom-rollout` must be the same nonzero value. Installation and
`check` fail closed on any disagreement; file ownership must not silently
target a different group from the service's actual primary group.

The repository deliberately does not track a cross-environment `uv.lock`;
`pyproject.toml` and the selected merged candidate are its dependency
authority. The installer therefore resolves that candidate's declared
constraints into the root-owned venv without editable installs. It does not
use `--frozen`, which would always fail in a fresh checkout with no lockfile.
A repeated install at the same source is a no-op; a newly merged source causes
the service venv to be synchronized again. Any prior install record whose
`installation_state` is not `ready` forces both candidate-checkout and venv
resynchronization even when its recorded source SHA already matches the fresh
`dev` head. This closes the crash window after publishing the provisional
record but before replacing the installed package. A failed package-resource
probe also forces synchronization at the same SHA, and the exact
`--reinstall-package loom` option restores the project package even when uv
would otherwise consider it satisfied. Immediately after `uv sync`, the
installer opens uv's generated
`/opt/loom-staging-runner/candidates/<source-sha>/venv/.lock` without
following symlinks, requires a regular root-owned file, converges it to mode
`0600`, and only then validates the complete venv authority tree. A symlink,
non-root owner, special file, or any other writable venv entry fails closed.
An interrupted install is retried through the same boundary: the installer
detects this exact root-owned regular lock-mode drift before full-tree readiness,
enters its fail-closed install transaction, hardens the lock, and then resumes
authority validation. It does not repair any other ownership or file-type drift.
The wheel carries exact tested copies of `config/loom-schema.toml`, the five
Grafana dashboards, the Envoy egress bootstrap, and the imported `_env.j2`
template partial as package data. The non-editable broker therefore never
resolves runtime resources relative to `site-packages`. CI builds and replaces
the editable install with the wheel, imports it from a temporary working
directory, compares the copied assets byte-for-byte, and renders the complete
default manifest set. Immediately after package synchronization, the installer
repeats that import/render probe as `loom-rollout` under `/usr/bin/env -i` with
Python `-I -B`; this package-only probe does not require a pre-existing host
configuration on a first install. The installer then writes
`/etc/loom/staging-rollout.toml` as `root:loom-rollout` mode `0640` and runs the
full broker probe as the service user, including loading that fixed operator
configuration, before it can restore admission. The final file must also be a
single-link regular file (`nlink=1`): the installed loader and host `check`
enforce the same exact owner, group, mode, type, and link-count authority. The
file is service-group readable but never group- or world-writable, and contains
path bindings, fingerprints, and the smoke-on-behalf team ID rather than raw
secret values. A hard-linked config is never accepted as ready or edited in
place. Only after the protected-pointer plus systemd-unit inactivity proof
described below succeeds may the installer atomically republish the canonical
path with a fresh single-link inode, detaching it from every pre-existing alias.
The installed broker wrapper also uses `-I -B -m`, preventing caller
working-directory module shadowing and bytecode writes.

The complete install transaction and trust/ACL ledger remains root-only at
`/etc/loom/staging-rollout.install.json` (`root:root`, `0600`). A successful
ready install additionally publishes
`/etc/loom/staging-rollout.install-attestation.json` as a single-link
`root:loom-rollout` `0640` file. This second file contains no credential value,
team identifier, ACL ledger, or key fingerprint: it contains only the exact
source identity, a digest of the private record, and SHA-256 digests of the
fixed client, broker, config, trust tool, known-hosts, mount unit, and tmpfiles
assets. The sudoers policy remains root-only and is verified directly
by host `check`; it is not exposed to the service reader. Host `check`
regenerates and compares the statement from the private authority; Tier 0
independently re-hashes every service-readable live asset. Do not copy,
edit, chmod, or synthesize this attestation by hand—rerun the exact sealed host
installer and `check` when it is absent or drifted.

The service user-manager probe also constructs its clean
`XDG_RUNTIME_DIR`/D-Bus/`PATH` environment *after* `sudo -u loom-rollout` via
fixed `/usr/bin/env -i` and `/usr/bin/systemctl` paths. Ubuntu `sudo` may reset
an environment attached to the outer `sudo` process, so moving those values
outside the sudo boundary is unsupported and makes the probe fail closed. The
probe reads the user manager's `Version` property and validates its restricted
version shape. It deliberately does not require `is-system-running=running`:
Ubuntu can report the whole user manager as `degraded` because an unrelated
globally enabled desktop or snap unit failed, even while the D-Bus connection
and Loom transient-unit boundary are healthy. Loom's own runtime units remain
subject to the installer check and broker status gates below.

Tier 0 checks Docker with the same three fixed read-only probes used by the
compatibility preflight: `docker info`, `docker buildx version`, and the numeric
`fs.inotify.max_user_instances` value through fixed `/usr/sbin/sysctl`. All probes run so the report exposes
daemon, plugin, and host-capacity blockers together. Their raw output is
discarded. The host installer owns
`/etc/sysctl.d/90-loom-staging-rollout.conf` and requires at least 1024 inotify
instances; rerun the exact sealed installer and its `check` instead of retrying
a rollout or raising the image-check timeout.

The fixed Kubernetes client probe reads the current context and gets only the
target namespace using the explicit installed kubeconfig. It does not apply or
patch resources. Tier 0 binds the result to the kubeconfig metadata digest and
reports context and namespace failures together; replace the exact installed
kubeconfig rather than editing context state in place.

The source kubeconfig is the fixed root-only `/root/.kube/config`. The
installer validates the complete root-owned, non-writable parent chain, opens
the source once with `O_NOFOLLOW`, and accepts only a root-owned, single-link,
mode-0600 regular file. It copies the bytes read from that verified descriptor
into a process-private snapshot and passes the snapshot to `kubectl` with an
explicit `--kubeconfig` flag. This removes the check/use path race and remains
deterministic when the installer's clean subprocess environment intentionally
omits `HOME`; relying on kubectl's implicit home-directory lookup is
unsupported and fails closed.

For declared input and data ACLs, the installer reads the full raw and effective
ACL, computes the smallest explicit mask expansion needed by `loom-rollout`,
and writes the service entry and mask together with `setfacl -n`. A mask change
is allowed only when no undeclared user or group gains an effective permission;
raw permissions for the declared `qianyi`, `hongjian`, and `devansh` operators
may become effective, while the OLDLAB-2 numeric UID 2012 exception is preserved
without gaining any new bit. If a data directory has no default ACL, the
installer creates the required default base from its effective access base and
records that the whole default ACL was absent. Because the ACL mask is represented in
the numeric group mode bits, inspect `getfacl` raw/effective entries rather than
using mode alone as the permission proof.

Protected credential leaves have a stricter convergence rule than data
directories. Their access ACL may retain the owning group object, including its
existing read permission, but may contain no named user except `loom-rollout`
and no named group. The installer removes such stale named readers in the same
planned `setfacl -n` transition that converges the service entry. Before any ACL
write it persists the complete access-ACL preimage and expected postimage in a
root-owned snapshot ledger; plan/apply drift fails closed, retry is idempotent,
`check` requires the postimage, and uninstall restores the exact preimage. Do
not repair this condition with an ad-hoc `setfacl` command because that bypasses
the install ledger and rollback authority.

Protected-input parents intentionally grant only traverse (`--x`) permission;
the leaf grants read (`r--`). Linux preflight opens those parent descriptors
with `O_PATH|O_DIRECTORY|O_NOFOLLOW`, preserving no-listing access while still
rejecting symlinked path components. The Tier 0 `credentials.metadata` gate and
the final browser/operator consumers use this same reader implementation. It
reads the POSIX ACL from the already-open descriptor before and after the
bounded file read, rejects undeclared named users/groups and write/execute
grants outside the owner, verifies that only the rollout service obtains the
declared read grant, and fails if inode metadata or the ACL changes during the
read. Preflight evidence contains only owner/group/mode plus metadata, ACL, and
bounded content fingerprints; paths and credential values are never emitted.
The admin-token fingerprint is computed from the same stripped token bytes as
the installer and final browser/smoke consumers, so a conventional trailing
newline does not create a false authority drift while any actual token-byte
change still fails closed.
The data contract includes
the declared rollout, Postgres, MinIO, backup, and pre-existing
`environment-state` subdirectories. Each receives access/default `rwx`; the
staging root remains read/traverse-only. This explicit environment-state grant
is required because a new parent default ACL does not retrofit an existing
directory. It also does not retrofit a pre-existing operator-owned leaf, so
step 11 must atomically replace `environment-state/staging.toml` without
requiring that stale leaf to be readable by the service account. It accepts
only a real regular destination entry; symlinks and other non-regular entries
are never followed or replaced.

The service-private `generated/` directory must contain at least one validated
GB10 worker env template before rollout admission is restored. On first install
or repair of an empty directory, the installer performs the one-way bootstrap
from the newest fixed-name legacy file under
`/shared_work/qianyi/loom-worker-capacity/`. The source must be a bounded,
single-link regular UTF-8 dotenv file that is not group- or world-writable and contains the complete
control-plane, Gateway, worker-token, and MinIO key set. The copied bootstrap is
owned by `loom-rollout`, mode `0600`, and its values are never printed. Existing
private templates are not refreshed from legacy state. `check` reports
`generated-gb10-worker-env-template` and installation remains admission-closed
when no safe template can be proven; do not create an ad-hoc empty env file.

Create the fixed invocation checkout as root with a deterministic umask. On a
later update, require a clean checkout, fetch `dev`, and detach at the fetched
remote head. Do not force over local drift:

During `install`, the credential installer atomically creates or reuses the
dedicated database-backed application probe at the fixed root-only path
`/etc/loom/staging-rollout-readonly-probe-token`. It converges exactly one
deployment-managed `readonly_probe` row for the exact smoke team, with only
`read:own`, no creating user, and no admin/submit scope; authority drift or a
second active probe fails closed. The file is root:root `0600`, single-link,
and non-empty. This is a bootstrap authority, not an operator token: never
reuse the admin, service, worker, browser, or smoke credential and never pass
the value through argv, environment variables, install records, or rollout
evidence.

During installation, the exact candidate applies the checked-in readonly and
rehearsal RBAC/admission manifests with the root kubeconfig, then obtains
six-hour TokenRequest credentials for the exact service accounts and the fixed
K3s service-account audience
`https://kubernetes.default.svc.cluster.local`. The installer rejects and
rotates a still-fresh token carrying any other audience; metadata freshness
alone cannot admit a token the API server will reject. It publishes
minified service-owned `0600` kubeconfigs under
`/var/lib/loom-staging-rollout/credentials`; inherited root client keys are
discarded. `check` fails once either token has less than two hours remaining,
while `install` requires at least four hours remaining and rotates both
kubeconfigs otherwise. This headroom prevents a token that is still
runtime-valid at install admission from expiring during the complete
post-install preflight. The installed root-owned
`loom-staging-rollout-credential-refresh.timer` checks them hourly and mints
both before publishing either when refresh is needed. Each service-owned
kubeconfig is replaced atomically as `loom-rollout:loom-rollout` at mode
`0600`. The refresh then verifies the exact service-account subject, its
required capability, and denial of TokenRequest minting authority. Any mint,
publication, or read-back failure is
fail-closed and emits no token material. The credential check reports only
authority metadata and expiry; the deep-preflight attestation retains only
metadata fingerprints and evidence hashes, never a token value. An
incident-only host refresh timer may be retired only after installing the exact
merged SHA and `staging_rollout_host.py check` confirms this managed timer
and both credential authorities.

The same install transaction creates or reuses a separate root-owned MinIO
credential and converges one fixed non-mutating policy for
`loom-staging-trajectories` and `loom-staging-artifacts`. The service copy is a
single-link mode-0600 file under the credential directory. Its policy permits
only bucket location/versioning, object/version enumeration, and exact-version
reads; writes and deletes are absent. The readonly TokenRequest may port-forward only
to `loom-minio-0` and `loom-postgres-0`. `check` independently compares the
server-side policy document and user binding, and Tier 0 then proves it by
enumerating the exact buckets and measuring `/data/loom-staging/minio` as
`loom-rollout`. Tier 2 proves MinIO reachability with a bounded versioning read
through the same fixed localhost transport; it does not require Kubernetes
`services/proxy` authority. Do not replace either probe with the application
MinIO secret, a broader proxy grant, or a privileged `kubectl exec` inventory.

```bash
INSTALLER_CHECKOUT=/root/loom-staging-installer
sudo /bin/sh -c 'umask 077; exec /usr/bin/git "$@"' loom-staging-git \
  clone --origin origin --branch dev --single-branch \
  https://github.com/qianyi-sun/loom.git "$INSTALLER_CHECKOUT"

# Subsequent merged-dev refreshes:
sudo /bin/sh -c 'umask 077; exec /usr/bin/git "$@"' loom-staging-git \
  -C "$INSTALLER_CHECKOUT" status --porcelain=v1 --untracked-files=all
sudo /bin/sh -c 'umask 077; exec /usr/bin/git "$@"' loom-staging-git \
  -C "$INSTALLER_CHECKOUT" fetch --prune origin \
  refs/heads/dev:refs/remotes/origin/dev
sudo /bin/sh -c 'umask 077; exec /usr/bin/git "$@"' loom-staging-git \
  -C "$INSTALLER_CHECKOUT" checkout --detach refs/remotes/origin/dev

sudo "$INSTALLER_CHECKOUT/scripts/ops/staging_rollout_host.py" plan
sudo "$INSTALLER_CHECKOUT/scripts/ops/staging_rollout_host.py" install \
  --smoke-on-behalf-team-id "<agentic-rl-team-uuid>"
```

During one explicitly approved replacement-attempt incident, Qianyi or
Hongjian may accumulate reviewed fixes locally and install the cumulative
result before one final durable PR. This is not a general unmerged deployment path. Create a new
standalone root-owned checkout (a linked Git worktree is not accepted), detach
it at the independently reviewed exact commit, retain only the approved GitHub
origin URL, and record the exact commit tree and approved merged base. The
sealed checkout and every parent must be root-owned and non-group/world-
writable; it must be clean, detached, free of alternates, grafts, shallow or
replacement objects, and contain a bounded linear chain of at most 512 commits
from the approved base. Do not fetch or resolve `origin/dev` in this mode.

```bash
SEALED_CHECKOUT=/root/loom-staging-sealed-cumulative
SEALED_SHA=<reviewed-40-character-commit>
SEALED_TREE=<reviewed-40-character-tree>
SEALED_BASE=<approved-40-character-merged-base>

sudo "$SEALED_CHECKOUT/scripts/ops/staging_rollout_host.py" install \
  --source-mode sealed-cumulative \
  --sealed-source-sha "$SEALED_SHA" \
  --sealed-source-tree "$SEALED_TREE" \
  --sealed-approved-base-sha "$SEALED_BASE" \
  --smoke-on-behalf-team-id "<agentic-rl-team-uuid>"
sudo "$SEALED_CHECKOUT/scripts/ops/staging_rollout_host.py" check
```

The installer validates the sealed checkout before any staging mutation,
copies only the exact commit into its root source without resolving a remote
ref, imports the same commit into the service candidate, and records source
mode, commit, tree, and approved base in the install ledger, protected config,
request, and attempt envelope. In sealed mode only `qianyi` and `hongjian` may
start or resume; the other operators retain status/log access. A coordinator
handoff always creates a new request under the new initiator; never resume or
reuse another initiator's request. A missing or mismatched exact
argument fails closed. Never bypass this path by copying files into the install
directory, adding a global/system `safe.directory`, injecting Git objects, or selecting a
caller-provided source path.

Broker and worker systemd management operations use a bounded two-minute
budget, separate from the shorter general command budget, so transient manager
I/O cannot strand a launch. A management timeout still fails closed as an
explicit launch or query error and is never retried implicitly.

For first installation, run the clone command only when the destination is
absent. For updates, skip clone and use the three refresh commands. An occupied,
dirty, wrongly owned, writable, or wrong-origin checkout must be inspected and
recreated as a new root-owned path rather than repaired in place.

Once per newly generated service-key lifecycle, use one explicitly approved
Ed25519 admin identity as the bootstrap channel to the exact 15-host active
set. The fixed 15-host topology remains validated, and a migrated legacy
revocation ledger must cover all 15 hosts. The
identity must be an absolute, single-link, mode-0600 regular file under its
same-owner mode-0700 parent. The tool derives its public key with the fixed
system `ssh-keygen`; it never copies or prints either private key.

Choose exactly one transition operation. Use `bootstrap` when the canonical
`loom-staging-rollout` marker is absent, or when the target service key itself
is the marked entry that needs idempotent repair. If the explicitly approved
bootstrap key currently occupies that one canonical marker, use
`rotate-bootstrap`; ordinary `bootstrap` deliberately rejects that ambiguous
state. Rotation replaces only that exact canonical bootstrap entry with the
service key. A missing, duplicate, option-prefixed, unrelated, or tombstoned
marker fails without changing `authorized_keys`, and a repeated successful
rotation is a no-write `already-present` result.

Both operations create a process-private mode-0600 SSH config containing the
bootstrap identity first and the service identity as retry fallback. This same
config reaches every ProxyJump child, so a partial retry works after some hosts
already accept only the service identity. Private hosts converge before the
jump host; any private-host failure leaves the jump host unchanged:

```bash
CANDIDATE_SHA=<exact-40-character-candidate-sha>

# Fresh or same-service-key repair:
sudo "/opt/loom-staging-runner/candidates/${CANDIDATE_SHA}/venv/bin/python" \
  /usr/local/libexec/loom-staging-rollout-gb10-trust bootstrap \
  --bootstrap-identity /secure/path/<approved-admin-ed25519-key>

# Only when that approved bootstrap key occupies the canonical marker:
sudo "/opt/loom-staging-runner/candidates/${CANDIDATE_SHA}/venv/bin/python" \
  /usr/local/libexec/loom-staging-rollout-gb10-trust rotate-bootstrap \
  --bootstrap-identity /secure/path/<approved-admin-ed25519-key>

sudo "$INSTALLER_CHECKOUT/scripts/ops/staging_rollout_host.py" check --format json
loom-staging-rollout start --dry-run
```

Repeated `install` is the supported update path and is a no-op when source,
policy, files, ownership, ACLs, service state, and merged `dev` SHA already
match. An update first disables broker admission under the shared launch lock
and refuses while a rollout is active. A failed update keeps admission disabled
and records an uninstall-safe recovery ledger rather than exposing a partially
updated runner. Installer-managed and broker candidate Git commands run as
`loom-rollout` through a fixed clean environment and `0077` umask; the broker
also disables system/global Git configuration and terminal prompts. Existing
group/world write bits are removed only
inside that maintenance transaction and only after full service ownership,
ordinary entry type, and contained-symlink validation; foreign ownership,
special files, or escaping symlinks fail closed. Candidate readiness repeats
the complete non-writable-tree validation before any Git command. Before any
ACL write, the provisional root-owned ledger stores
the complete before and expected after ACL for every mask change. A retry accepts
only those exact states and never resamples drift as a new baseline. The write
also upgrades legacy schema v1 or trust-ledger schema v2 install records to v3;
the current installer can migrate either predecessor, while old v1/v2 installer
versions reject v3 and therefore cannot perform an incomplete downgrade
uninstall. While a v1 trust migration is pending, v3 separately records the
legacy source SHA and keeps using it across retries; the field disappears only
after the trust ledger is durable. An interrupted v2 migration without that
binding is ambiguous and must be repaired rather than retried with a guessed
source.

The inactivity proof used during update and uninstall does not import the old
broker runtime being replaced. After publishing the root-owned maintenance
marker under the broker launch lock, it requires the service-owned state root,
an absent `active.json`, and only loaded terminal (`inactive` or `failed`)
`loom-staging-rollout-*` user units. Any nonterminal unit state, unsafe
metadata, unreadable state, systemd stderr, malformed output, present active
pointer, or user-manager query failure blocks the operation. A valid but stale
pointer must be cleared through supported broker reconciliation, never manual
file deletion. This permits a merged installer to repair a broken packaged
runtime without weakening the no-active-rollout gate. Config hardlink
detachment and every other installed-file mutation remain strictly after this
pointer-and-systemd proof.

The rollout-local cluster config is synthesized from the resolved candidate
worktree's repo-local profile, rather than from the long-lived runner checkout,
then pins that candidate's image tag. This keeps new protected-profile fields
in preflight/render/release-gate even while the fixed runner itself has not yet
checked out the candidate.

`prod` is also an explicit lower-level selector, but it fails closed until
first-prod values are configured; it never falls back to staging values. The
lower-level full-argv driver is an implementation surface, not a supported
staging operator or diagnostic interface. For staging, only the broker may
construct that argv from a validated private envelope.

The broker intentionally does not use `ssh -A`. Step 12 authenticates with the
service-owned `/var/lib/loom-staging-rollout/gb10-deploy-ed25519` declared by
the candidate-bound cluster config. The private file is mode 0600, is never
shared with operators, and is not committed or printed. Every rollout keeps
the merged all-15-host fail-closed gate. A busy host remains capacity-eligible,
but candidate-owned drain/quiescence must complete before disruptive
convergence and must never cancel or preempt external jobs. Host checkout, env
update, legacy worker
retirement, and node-agent start remain ordered per host while the fixed broker
policy bounds concurrency across independent hosts.

#### Root break-glass and revocation

Break-glass is for repairing the installed service, not selecting a different
candidate. Disable new admission, preserve the request/rollout ledger, and
rerun the root installer from a clean freshly fetched merged `dev` checkout.
Then use `loom-staging-rollout resume REQUEST_ID`, which revalidates the
original service-owned envelope and pinned SHA. Do not fall back to a Qianyi
user unit, forwarded agent, personal key, lower-level arbitrary argv, or manual
evidence edits.

For full revocation, wait for a safe terminal state and run:

```bash
sudo ./scripts/ops/staging_rollout_host.py uninstall --retain-ledger
```

Uninstall removes admission, takes the maintenance/launch lock, refuses an
active request, and revokes only the recorded service public key from every
host in the root-owned revocation ledger. Current installs and migrated legacy
ledgers both record all 15 hosts. Only installer-recorded
ACLs/memberships/linger and
generated key/runtime state are removed, while each recorded ACL preimage is
restored, including removal of a wholly installer-created default ACL. A
pre-existing service ACL is never removed merely because its mask required
convergence. If any revocation step fails, or the live ACL matches neither the
recorded before nor after state, stop and repair the drift; do not run a manual
`setfacl` workaround or delete the local key or ledger first.

For `--environment staging`, the driver intentionally refuses legacy physical
targets. Use `--cluster-name loom-staging`, `--namespace loom-staging`, and
`--rollout-root /data/loom-staging` after creating those resources; do not run
a logical staging rollout against any older pre-production cluster, namespace,
or data root.

The lower-level driver still requires `--backup-manifest`, but normal staging
operators never supply it. The broker creates the Postgres dump, MinIO
snapshot, and protected Secret backup, verifies the manifest and freshness
window, and binds its immutable path and digest into the request envelope
before unit launch. Step 05 rechecks that same manifest and refuses to advance
without sufficient remaining freshness. A backup failure launches no driver
and performs no staging mutation. Inspect the safe request status; if it is a
failed pre-launch request with only an incomplete no-manifest root, use
`loom-staging-rollout cleanup-incomplete-backup REQUEST_ID`, then fix the backup subsystem
before starting a new request. Never remove a request root manually or delete a
manifest-backed backup.

`ADMIN_TOKEN_SOURCE` is a secret source reference, not a raw token; use
`env:VAR` or `file:PATH` so shell history, process listings, logs, JSON, and
Markdown evidence only record the source reference. The one-command rollout
driver rejects stdin `-` because step 11 must replay the same token source for
both apply and check, and the rollout inputs are persisted for resume evidence.
Step 11 forwards that same source plus `ADMIN_TOKEN_FINGERPRINT` into
`loom admin environment-state apply/check`; the fingerprint guard fails before
control-plane mutation if the local token source drifted. The token must carry
the protected admin scopes used by environment-state reconciliation, including
worker-pool administration, because step 11 configures autoscaler policy and
GB10 desired state after the cluster is up and before GB10 host prep starts.

`WORKER_TOKEN_SOURCE` is also a source reference, not a raw token. Use
`env:VAR` or `file:PATH`; the rollout driver rejects stdin `-` because the
environment-state check is replayed during resume. Step 11 passes this source
only to `loom admin environment-state check --worker-token` so external Slurm
runner `env_file` fingerprints can be compared against the active protected
worker token without writing token values to logs or evidence. It does not
pass the worker token to environment-state apply. When the environment profile
declares `external_slurm_runner_prerequisites` with `materialize = true`, step
11 first syncs the current profile into the rollout root, then reconciles the
declared `env_file` and `repo_dir` for the rollout image tag before
apply/check. The env file is copied from the profile's template glob when
missing, only release keys and the active worker token are updated, mode is
forced to `0600`, and evidence records paths, mode, git HEAD, clean status,
and redacted worker-token fingerprint only. Profiles that do not opt in to
`materialize = true` remain operator-owned prerequisites and must be created
before rerunning step 11.

For staging GB10, `repo_dir` is an exact image-tagged direct child of
`/shared_work2/qianyi/.loom-staging-rollout/worker-repos`. First establish the
checked-in exporter authority boundary on `trt-gb10-2`. The exporter has no
existing noninteractive root path, so this requires one explicit external
administrator bootstrap. Provision a standalone root-owned mode-`0700`, clean,
detached checkout at the fixed path below from the independently reviewed
sealed bundle. Then the external administrator runs exactly:

```bash
EXPORTER_SOURCE=/opt/loom-staging-exporter-authority/source
SEALED_SHA=<reviewed-40-character-cumulative-commit>
SEALED_TREE=<reviewed-40-character-tree>

/usr/bin/python3 \
  "$EXPORTER_SOURCE/scripts/ops/staging_rollout_shared_work2_export_authority.py" \
  bootstrap --source-sha "$SEALED_SHA" --source-tree-sha "$SEALED_TREE"
```

Do not run that command through the coordinator's sudo identity: bootstrap
intentionally requires a direct external root administrator and is absent from
the installed sudoers rule. The fixed approved merged base is embedded in the
reviewed boundary. Bootstrap validates the checkout and sudoers before
publication, converges its three root-owned mode-`0755` directory roots inside
the same transaction, installs sudoers last, and rolls back all directories
and files created by the failed attempt in reverse order. Pre-existing exact
directories are retained, while a wrong type, owner, or mode fails closed. It
does not create a general root command channel.

If `qianyi` already has access to the exporter's rootful Docker daemon, the
reviewed Docker bootstrap handoff is an equivalent one-time administrator
channel. It uses the exact ARM64 content-addressed image built from
`deploy/worker-pools/gb10/Containerfile.shared-work2-export-bootstrap`; its only
entrypoint is the reviewed Python bootstrap launcher. The container must run
with `--network none`, `--read-only`, `no-new-privileges`, an exact capability
mask of `CHOWN`, `DAC_OVERRIDE`, and `FOWNER`, and no command override. Bind
only the sealed bundle read-only plus `/opt`, `/usr/local`, `/etc`, and
`/var/lib` at their identical paths with recursive bind propagation disabled.
These four parent binds are the narrowest atomic route while all four exact
authority children are absent; a recursive `/var/lib` bind would expose Docker
state and is forbidden. The launcher refuses wrong mount roots/options,
usable non-loopback devices or routes, a writable container root, capability drift,
bundle drift, source drift, or non-atomic source publication. Do not substitute
an interactive shell, `--privileged`, or a writable `/` bind.
Do not substitute an AMD64 image or rely on optional binfmt emulation; the
build and runtime reject any non-ARM64 target.

A reviewed cumulative replacement is permitted only when the installed
authority is provably unused: its exclusive lock is held, its exact old sealed
identity and assets validate, and its install journal is empty. Its export
fragment must either be absent or exactly match the old sealed asset and the
single canonical `/var/lib/nfs/etab` client/options record. This second state
captures only a failed post-refresh verification before any install journal
was written. The new commit must be a descendant of that old commit on the same
approved base. The Docker launcher removes sudo authority first,
moves only fixed assets to same-directory no-replace backups, and either
atomically bootstraps the new identity or restores the old identity with
sudoers last. Do not use this path after any successful `install`; such an
authority requires a separately reviewed lifecycle change.

The fixed exporter `install` verb owns convergence of a missing
`/etc/exports.d` as root-owned mode `0755` before publishing its one fragment.
It retains a pre-existing exact directory, rejects any type/owner/mode drift,
and removes a directory created by the failed attempt after fragment and
`exportfs` rollback.

The sealed checkout may contain a reviewed Git symlink such as
`deploy/.env -> ../.env`, but validation never follows it. An allowed link must
be mode `120000` in both the exact tree and stage-zero index, retain the
expected no-follow owner and single link count, stay lexically within the
checkout, avoid tracked directories or other symlinks, and hash from its
literal `readlink` payload to the exact tree blob. After lexical normalization,
the resolved target must not have the literal, case-sensitive `.git` name as
its first path component. Absolute, escaping, untracked,
Git-administration-targeting, retargeted, type-changed, or dirty links fail
before bootstrap or install.

After bootstrap, the coordinator may run only:

```bash
sudo /usr/local/libexec/loom-staging-rollout-shared-work2-export-authority install
sudo /usr/local/libexec/loom-staging-rollout-shared-work2-export-authority check
```

Neither verb accepts additional arguments, environment overrides, source
paths, refs, hosts, clients, networks, or fragment content. Root reloads the
mode-`0600` policy and revalidates the fixed checkout, wrapper, validator, and
sudoers identities before running the exact helper. The `check` verb uses a
shared read-only lock and does not append to the journal. The locked `install`
verb appends only sanitized SHA/tree/base evidence after success.

In sealed cumulative mode the underlying helper requires the same
`--sealed-source-sha`, `--sealed-source-tree`, and
`--sealed-approved-base-sha` binding as the host installer and validates that
fixed script checkout before changing the export. It installs only the exact
`192.168.50.103/32` allowance, requires the exact effective export options, and
fails rather than widening or overwriting a drifted fragment. Human-oriented
`exportfs -v` output proves the exact client is visible but is not an option
authority because it omits default options; the root-owned, bounded canonical
`/var/lib/nfs/etab` entry is the exact effective-options authority. A newly created
fragment is removed and export state refreshed if the first `exportfs -ra`
fails; pre-existing exact state is never removed. The platform-dev root
installer then installs and starts the
fixed `shared_work2.mount` unit and rejects any source other than
`192.168.20.12:/shared_work2` over NFSv4.2 with the declared hard/TCP and
`nosuid,nodev,noexec` options. A directory with no matching mountinfo entry is
not accepted. The root installer
converges that authority only with admission closed and the service inactive,
records the resolved `loom-rollout` and `qianyi` UID plus service/sharedwork
GID values, and verifies effective access: the service owns and writes only
the dedicated root; the Slurm submitter reads/searches but cannot write it.
Do not add `loom-rollout` to `sharedwork`, widen the host-only export, or move
the private mode-`0600` token/env authority from platform-dev into shared
storage. Do not request or store the exporter sudo password, enable root SSH,
add wildcard sudo arguments, or revive the abandoned 14-host authority design.

Repository materialization always claims the final candidate directory with
one atomic no-replace `mkdir`, initially mode `2700`, with the sharedwork group
and setgid bit inherited from the dedicated root. It clones with
`--no-hardlinks` while the consumer cannot search or read that private
directory. Before publication it rejects authority symlinks, foreign
ownership, group/other write, hard-linked or special files, extra directories,
and non-exact candidate HEADs while
allowing tracked git symlinks. Publication is the inode-bound final-directory
mode transition from private `2700` to immutable consumer-readable `0750` after
a complete tree validation. An existing exact target is reused only after a
complete immutable-tree validation, while any private, drifted, or different
HEAD fails closed without replacement, cleanup, or takeover. The installer
runs the same private-claim, access-gate, publish, and collision sequence in a
bounded, randomized, self-cleaning probe under the service identity before
declaring the NFS authority ready. This avoids assuming optional
`RENAME_NOREPLACE` support from the NFS server while retaining atomic name
reservation and fail-closed consumer visibility.
The platform installer refreshes its service-owned local candidate from the
root-owned sealed install source through a fixed local `git upload-pack`
command bound to that source's exact `.git` safe-directory exception. It never
adds a persistent or wildcard Git safety exception.
Protected GB10 convergence uses the same fixed-command pattern when each
`qianyi` checkout fetches from the service-owned shared candidate: the
upload-pack is bound only to that candidate's exact `.git` directory derived
from the attested SHA. This avoids Git's cross-owner rejection without changing
shared ownership or any global, system, or user Git configuration.

Before request creation the broker also checks the exact 15 GB10 SSH targets
as `qianyi`: the 14 clients must expose the exact NFSv4 source and
mount identity, `trt-gb10-2` must expose the ext4 backend, and the shared root
must have the fixed owner/group/mode and
be readable/searchable but not writable. Immediately after the one-time
checkout publish, and before environment-state apply, step 11 reads the
verifier as a commit-bound blob from the exact resolved SHA and streams those
captured bytes over the protected SSH stdin path to `/usr/bin/python3 -`.
Verifier code is never loaded from the mutable rollout worktree or the target
under test.
All 14 nodes must independently observe the exact HEAD, a zero status including
ignored and untracked entries, the complete index-derived file/directory modes,
a readable deterministically selected tracked file, and non-writable
root/target. Content digests and tracked-entry counts must agree across nodes.
Mount/device/inode values are bound into sanitized per-node evidence. A
non-zero SSH or remote verifier exit receives at most thirteen exact-command
observations over a bounded 390-second incremental-backoff window. Structured
evidence that is valid but content-divergent fails immediately; an exhausted
transient records only host, attempt count, and a non-sensitive failure class
before the rollout fails closed.

After that 14/14 consumer proof, sealed-cumulative GB10 prep fetches the exact
commit from the fixed shared checkout with the system upload-pack and object
fsck enabled. It does not resolve or fetch `origin/dev`; merged-dev prep keeps
the existing GitHub-origin fetch path. The shared source path is derived only
from the candidate-bound image tag beneath the fixed worker-repository root;
the upload-pack receives an exact per-repository `safe.directory` binding
without changing global or system Git configuration.

The
preflight derives the mount major/minor pair from each repository directory's
`st_dev`; the separately recorded inode is never interpreted as a device
minor number. It selects the most specific mountinfo entry containing
`/shared_work2`: clients therefore bind the exact NFSv4 mount while the
exporter binds the ext4 filesystem that contains its export directory even
when `/shared_work2` is not a separate mountpoint.

`SERVICE_TOKEN_SOURCE` is a Service API token source reference for
rollout-owned CLI commands that mutate or verify DB-backed service defaults.
It is separate from the Control Plane admin token. Step 13 resolves this source
inside a temporary, private `$XDG_CONFIG_HOME` and writes only the token value
there for the duration of `loom admin rate-cards sync-yibuapi` and
`loom providers update/show`; rollout logs and inputs retain only the source
reference. Provider default mutations set `X-Loom-Admin-Actor:
rollout-production-defaults` for the service audit trail. The server URL is
derived from the rollout cluster config, for example `https://yylx.world/staging`
for staging and `https://yylx.world/prod` for first prod. Do not use stdin
`-`; the source must be replayable as `env:VAR` or `file:PATH`.

Deep preflight also normalizes these secret-free desired values into a
candidate SHA/tree-bound `production-defaults.json` artifact. Detached
rehearsal and protected final apply consume that exact digest-addressed file;
neither stage may reopen the environment profile or discover provider defaults
after backup.

The protected component reads the service token only from the exact `file:`
source already bound into the request envelope and verifies its preflight
metadata fingerprint immediately before use. Provider/rate-card classification
is read-only; only the fixed Service API sync/update operations may mutate, and
the component journal is not terminal until an identical post-apply classifier
returns exact. Do not substitute an ambient CLI login, direct SQL update or a
newly resolved environment profile.

Step 15 defaults to `user-token` mode. In that mode, pass a replayable
`--smoke-api-token env:VAR` or `--smoke-api-token file:PATH` source; the
legacy `LOOM_SMOKE_API_TOKEN` environment variable remains an interactive
fallback, but it is not a complete detached rollout input unless the systemd
unit also sets it. The credential must be a user-owned API token whose
`/api/v1/auth/whoami` reports `credential_type=user_owned_api_token` and
includes `submit` scope. Admin secrets, internal service credentials, and
legacy team tokens are refused before trial submission because they cannot
create user-facing work under the account-auth model. For
`--scope=current-gb10`, `--smoke-task-id` defaults to
`loom-smoke/gb10-oracle-hello-world` with
`required_worker_pool=gb10`, because that task is oracle-compatible and
declares `cpu_arch=any`. For `--scope=full-cluster`, the default is
not defined: the rollout fails closed unless `--smoke-task-id` names a short,
audited physical task from the current profile, for example
`terminal-bench-2@tb2.1-r6/<task>`. The task must exist in the live
`/api/v1/tasks/{id}` catalog and be compatible with the rollout scope and
selected worker pool; Loom never guesses a TB2 task or falls back to TB2.0. If
the selection must target a specific pool, set
`--smoke-required-worker-pool` explicitly; the driver only injects the GB10
pool for its built-in current-gb10 default.

For a release canary where the rollout must represent an active user/team and a
user-owned smoke token is unavailable, the installed broker supplies the fixed
admin-on-behalf smoke identity from root-owned configuration. Operators cannot
override that team, username, task, required worker pool, or audit actor.

The rollout driver resolves the admin credential only from `--admin-token`'s
secret-source reference, validates `--expect-admin-token-fingerprint` before
any service call when that guard is provided, reuses an existing deterministic
rollout-smoke batch on resume, otherwise submits one audited batch through
`POST /api/v1/admin/batches/on-behalf`, then polls
`GET /api/v1/batches/{batch_id}` until `state=finished`,
`result_status=succeeded`, and `trial_summary.succeeded` covers the expected
trial count. For `--scope=current-gb10`, this mode defaults to
`loom-smoke/gb10-oracle-hello-world` with
`required_worker_pool=gb10`. That task is a checked-in release-smoke
fixture published by the catalog provisioning gate through
`loom datasets publish-local deploy/catalog/gb10-smoke`; it is
oracle-compatible and explicitly `cpu_arch=any`, so the batch API preflight and
GB10 claimability gate agree. Evidence must contain only the admin source
reference, fingerprint, represented username/team id, batch id, and redacted
response JSON. Do not record raw bearer values in shell history, argv evidence,
issue comments, PR bodies, Markdown, or logs.

Lower-level driver flags are broker-owned implementation details, not staging
operator options:

- `--exclude-oldlab` is derived from the installed scope policy. Operators
  cannot toggle it or claim full-cluster acceptance while excluding a
  release-managed pool.
- `--resume` is emitted only after `loom-staging-rollout resume REQUEST_ID`
  validates the original envelope. Operators do not select an image tag or
  remove `state.json`.
- Driver `--dry-run` is internal. The public preview is
  `loom-staging-rollout start --dry-run`, which performs broker authorization,
  fresh-fetch, binding, singleton, and redaction checks without launching the
  driver.
- The post-install readiness command is `loom-staging-rollout preflight`. It
  performs the exact candidate/epoch Tier 0-2 assessment without publishing a
  preview request and therefore cannot consume a request identifier merely by
  installing or checking the runner.

### Candidate-source tooling contract

For protected staging, the broker resolves the fixed `dev` ref once and the
driver validates the private envelope before creating `01-worktree/src` at
that exact SHA. It uses that candidate checkout for rollout-owned Loom
subcommands whose output becomes release evidence.
Steps that delegate to `loom ...` run `python -m loom_cli` from the rollout
runner venv with `01-worktree/src/src` first on `PYTHONPATH`, so child
subprocesses do not depend on a globally installed `loom` executable or an
operator wrapper's ambient `PATH`. The runner venv must be synced with
`--extra rollout`; otherwise catalog provisioning can import candidate
`loom_cli` while still missing benchmark sibling packages such as
`loom_benchmarks` and their dependencies.

If the candidate checkout does not contain importable Loom CLI source at
`01-worktree/src/src/loom_cli/__main__.py`, these steps fail instead of
falling back to ambient tooling. Fix the defect on a branch, merge it into
`dev`, and start a new broker request; do not select a different ref. For an
existing request whose candidate is intact, `loom-staging-rollout resume
REQUEST_ID` revalidates the same candidate and envelope. The broker supplies
the candidate-bound cluster-config path; operators do not pass one.

Rollout step 03 applies this rule to its non-CLI ingress dependency too. It
resolves `deploy/k8s/ingress-nginx-kind.yaml` under the fixed candidate
worktree, derives the expected controller ConfigMap and SHA-256 from that exact
file, and passes its absolute path to `kubectl apply`. A missing rollout id,
candidate worktree, or candidate manifest fails closed; the ambient checkout is
never a fallback.

For cluster render/apply/gate subcommands, the driver also writes a
rollout-owned `rollout-cluster-config.toml` artifact under the rollout
evidence directory. This synthesized config copies the operator's source
config and pins `image_tag` to the invocation's `--image-tag`, so stale
long-lived config files cannot silently render or gate an older image. The
operator's original `cluster-config.toml` is not modified. Repo-relative GB10
SSH path fields in the source config, including `[gb10_pool].ssh_config`,
`ssh_identity_file`, and `ssh_certificate_file`, are rewritten to rollout-stable
paths before the artifact is written. Repo-owned files resolve into the pinned
candidate worktree; operator-owned absolute paths remain absolute. This keeps
step 14 HF boundary evidence from resolving source-config-relative SSH paths
against the rollout evidence directory. On `--resume`, the driver also migrates
an existing rollout-local config if it still contains pre-fix relative GB10 SSH
paths, so recovery does not depend on manually deleting stale artifacts.

### Evidence layout

Each invocation gets a directory under `<rollout-root>/rollouts/`:

```
20260702t235959z-staging-abc1234/
├── state.json               # driver's state machine snapshot
├── inputs.json              # resolved CLI args + config sha
├── logs/driver.log
├── 00-resolve-target/       # per-step: result.json, stdout.log, stderr.log,
├── 01-worktree/             # step-specific artifacts (rendered.yaml,
├── ...                      # loaded-images.json, migration.yaml, etc.)
├── 15-smoke/
└── 99-summary/summary.md
```

Every step transitions through the tiny FSM
`not_started → running → verifying → done | failed` and the driver
persists after every transition, so `Ctrl-C`, terminal timeout, or SSH
disconnect followed by `--resume` picks up where it left off. `state.json`
version is `1`; it includes a best-effort driver owner record with pid,
hostname, boot id, and last update timestamp so stale `running` state is
distinguishable from an active writer. `inputs.json` pins the rollout to the
SHA resolved at launch. On `--resume`, the driver keeps that pinned SHA when
the operator supplies the same target ref and image tag, even if the branch now
points at a newer commit.

### Resume semantics

- For each step, the driver reads `result.json`. If `state=done` and
  the current `inputs_hash` matches the persisted one → skip.
- If the persisted state is `running` or `verifying`, the driver calls
  the step's `verify()` (read-only). MATCH → mark done and continue.
  MISMATCH → drop back to `running` and retry. UNKNOWN → refuse to
  advance and print a diagnostic (operator inspects and re-runs).
- If `state.json` names a previous driver that is still alive on the same
  host/boot, a second invocation refuses rather than double-writing evidence.
  If the previous driver process is gone, the new invocation records itself as
  the owner and resumes from the persisted step.
- After a successful `run()`, the driver calls `verify()` again and
  requires MATCH or UNKNOWN before marking done. A MISMATCH here means
  the world doesn't match what `run()` reported — a real problem, not
  a transient hiccup, so we refuse rather than retry silently.

### Refusal rules

- `--scope=full-cluster` + `--exclude-oldlab` → refused before any
  mutation. (#340 acceptance criterion.)
- Persisted `inputs.json` differs from current invocation → refused
  with a per-key diff. Prevents accidentally continuing a rollout
  against a different target ref, image tag, cluster, namespace,
  environment, config hash, credential source reference, scope, or OLDLAB
  inclusion policy.

### Individual step reference

Each step is a small Python module under
`src/loom_cli/rollout/steps/`. Every step defines its own
`inputs_hash`, `verify` and `run` — see the source for the exact
observability and mutation contract:

Step number 04 is intentionally unused. GB10 prep used to run there, but
#593 moved it after env-state so node-agent startup cannot apply a stale
desired state.

| # | Step | Delegates to |
|---|---|---|
| 00 | resolve-target | git rev-parse; validates image-tag ↔ sha7 |
| 01 | worktree | `git worktree add` at target sha |
| 02 | build-images | `docker build` × every rollout-critical image (#365) |
| 03 | kind-cluster | Ensure the staging kind cluster, kubeconfig, fixed-candidate pinned ingress-nginx IngressClass/controller (`deploy/k8s/ingress-nginx-kind.yaml` at the resolved candidate SHA), namespace, static worker trajectory Retain PV/PVC, and backup-manifest Kubernetes secrets exist before any image load or migration. Every run labels the control-plane node `ingress-ready=true`, validates the candidate worktree identity and cleanliness, materializes the manifest's commit-bound Git blob into the step evidence path, applies that evidence copy, reads back the live controller ConfigMap against values parsed from the same bytes, waits for the controller pod plus admission endpoint, and records `installed` only for an absent pre-apply Deployment or `reconciled` for an existing Deployment; the candidate SHA plus manifest SHA-256 invalidate stale completed-step evidence. Artifacts record the commit-bound evidence path, candidate source path, and hash. Missing candidate inputs fail closed without an ambient-checkout fallback. Recreated clusters still bind the protected `/data/...` root. Secret restore sanitizes runtime metadata/client-side apply annotations and uses server-side apply so reruns converge after partial restores. This restores the cluster substrate needed for step 08 preflight, not only the kube API (#206). |
| 04 | kind-load-images | candidate-source `loom cluster load-images` (#96) |
| 05 | backup | candidate-source `loom cluster backup check --manifest <path> --min-remaining-hours <N>` with the broker-bound reviewed traversal ceilings (#363, #619 freshness buffer) |
| 06 | audit | candidate-source `loom cluster audit` |
| 07 | render | candidate-source `loom cluster render` → `rendered.yaml` |
| 08 | preflight | candidate-source `loom cluster preflight --backup-manifest <path>` using the same manifest and broker-bound traversal ceilings verified by step 05 |
| 09 | migrate | candidate-source `loom cluster render-migration`, apply the rendered Postgres/MinIO stateful substrate plus static worker trajectory PV/PVC from step 07 and wait for those StatefulSets, then apply/wait for the migration Job (#332, #206). This keeps missing-kind recovery restartable without starting application Deployments before Alembic and without leaving protected preflight with a partial critical PVC set. |
| 10 | cluster-up | candidate-source `loom cluster up --backup-manifest <path> --recover-sandbox-deadlines --sandbox-deadline-max-pods 4` using the same manifest and broker-bound traversal ceilings verified by step 05 and preflighted by step 08. Running this immediately after migration recreates the Control Plane before env-state uses the CP API during missing-kind recovery (#203 fix for updated replicas, #206 bounded kind/containerd sandbox-deadline retry). |
| 11 | env-state | candidate-source `loom admin environment-state apply`, then required profile `catalog_provisioning.command` via protected env/env-source inputs and a rollout-owned private cache namespace, then `loom admin environment-state check --admin-token <source> --expect-admin-token-fingerprint <fingerprint>` (#331 fix for stop-on-disable, #533 guard for scoped admin token drift, #543 catalog-owned GB10 smoke task provisioning). Before the apply, rollout waits for the private CP URL's `/healthz` and records `control-plane-readiness.json`, so cluster-up pod recreation and managed tunnel restarts cannot race the env-state mutation. Pure GB10 node-status convergence drift is recorded and deferred because step 12 has not started node-agent apply yet; mixed drift still fails immediately. |
| 12 | gb10-prep | SSH ×N hosts after desired-state apply: require `Linger=yes` before any unit mutation; fetch, checkout, write env file, retire the legacy `loom-gb10-worker.service`; install the candidate service+timer; remove only the exact known legacy `deploy-window.conf` timer override (unknown or extra drop-ins fail closed); daemon-reload; start the node-agent once; enable/restart its timer; then verify installed bytes, absence of effective drop-ins, and live systemd state. The node-agent service is `Type=oneshot`, so success is `Result=success` / `ExecMainStatus=0` even when it is `inactive/dead`; the timer must finish loaded, enabled, active/waiting, point at that service, and need no daemon reload. Tier 0 may admit the exact loaded/enabled `active/elapsed` restart state as protected-repairable only when every other manager, linger, service, and timer predicate matches; prep must then converge it to waiting. This step intentionally runs after step 11 so a host-local node-agent cannot apply a stale Control Plane desired-state row (#593). The 2026-07-29 owner correction requires the exact all-15-host set with `excluded_nodes=[]`; candidate-owned drain/quiescence defers a disruptive host step while external work is active and never cancels or preempts that work. After rollout, disconnect SSH, wait more than one timer period, stop one acceptance-owned canary worker, and require periodic recovery plus a fresh heartbeat across all 15 hosts. |
| 13 | production-defaults | candidate-source `loom admin rate-cards sync-yibuapi --format json`, then `loom providers update/show` for hosted provider pricing defaults declared in the environment-state profile, using `--service-token <source>` in an isolated CLI config derived from the rollout route. This keeps DB-backed cost-attribution defaults from disappearing after a fresh rollout without depending on ambient operator login state. |
| 14 | release-gate | record `image-identities-<image-tag>.json` for rollout-managed rendered images, candidate-source `loom cluster release-manifest --expected-image-identities-json ...` → `release-manifest-<image-tag>.json`, and—only for the fixed staging root `/data/loom-staging`—ask Docker to prune unused images and build cache older than 24 hours before measuring storage headroom. The cleanup writes `staging-host-cache-retention.json`, fails closed on either Docker command, and revalidates the exact candidate image identities before continuing. Then run `loom cluster minio-storage-preflight --output minio-storage-preflight-<image-tag>.json`, require non-empty GB10 desired state for `current-gb10` rollouts, collect GB10 status from the manifest's `control_plane_environment`, rerun `loom admin environment-state check --format json`, and finally `loom cluster release-gate --manifest <that file> --minio-storage-preflight <that storage artifact>` (#339 fix for stale kind-import, #536 guard for GB10 status token drift, #593 post-prep env-state recheck). GB10 convergence mismatches are retried for up to 15 minutes so a just-triggered node-agent apply can report the new image/env/source state before the gate fails. This retry window includes release-target mismatches returned directly by `loom admin gb10-workers status`, such as a worker registration that exists before its first fresh heartbeat lands. Active GB10 hosts must also show a linked fresh active docker worker registration (`worker_id`, `worker_status=active`, `worker_fresh=true`, `worker_backend_names` contains `docker`), because smoke/admin submission uses `/api/v1/backends`, not node-agent metadata alone. |
| 15 | smoke | HTTP health + smoke identity + benchmarks + smoke task lookup. Default `user-token` mode submits a user-owned trial and checks trajectory/usage; `admin-on-behalf` mode submits an audited represented-user batch through the admin API, uses a batch-compatible current-GB10 default task, and polls batch success. |
| 16 | staging-admin-browser-acceptance | Runs the exact candidate-built Playwright image under the broker attempt, exchanges the singleton admin bearer for the fixed `qianyi` validation principal, verifies authenticated admin surfaces and correlated audit identity, logs out, and stores one sanitized report bound to request, attempt, envelope digest, candidate SHA, and runtime build SHA. |
| 99 | summary | write `summary.md` from every prior step's result.json |

Step 14 deliberately writes a narrow image-identity artifact instead of full
`docker image inspect` output. The artifact is keyed by Deployment/container and
contains the rendered image tag plus immutable `image_id` and optional
`repo_digest` for images built by this rollout. External images, such as Envoy,
are not included in that artifact.

## Kind cluster: loading local images before rollout (#96)

When a cluster runs on top of `kind`, images built with
host docker are **not automatically visible** to the kind node's containerd. A
plain `kubectl apply` against a Deployment that references a local tag
(`loom-worker:staging-<sha7>`) will hit `ErrImagePull` / `ImagePullBackOff`
because containerd tries the default registry (`docker.io/library/...`) and
finds nothing.

The `loom cluster load-images` subcommand wraps `kind load docker-image` for
each requested tag and gives a preflight-friendly `--check-only` mode.

For shared staging, candidate-bound broker step 04 derives the complete image
set from the rendered manifest and loads only the pinned merged candidate.
Operators do not run this command against `kind-loom-staging` or select an
image tag. The following examples are for a development/custom kind cluster:

```bash
loom cluster load-images \
  --cluster-name loom-dev-local \
  --image loom-control-plane:dev-local \
  --image loom-service:dev-local \
  --image loom-llm-gateway:dev-local \
  --image loom-worker:dev-local
```

Or extract the image set directly from a rendered manifest so nothing drifts
between what `loom cluster render` emits and what gets loaded:

```bash
loom cluster render > /tmp/rendered.yaml
loom cluster load-images \
  --cluster-name loom-dev-local \
  --from-manifest /tmp/rendered.yaml
```

Registry-qualified images (`docker.io/library/postgres:16`,
`gcr.io/foo/bar:baz`, `registry.k8s.io/...`) are skipped — those can be
pulled by the cluster itself.

Preflight smoke: verify without mutating anything. This is what belongs in the
rollout driver before `kubectl apply`:

```bash
loom cluster load-images \
  --cluster-name loom-dev-local \
  --from-manifest /tmp/rendered.yaml \
  --check-only
```

Exit `0` when every requested tag is present in the kind node's containerd,
`1` with a diagnostic listing missing images and the exact
`loom cluster load-images ...` command to fix.

`kind load docker-image` is idempotent — rerunning after a partial load
converges without any special handling.

## Kind/containerd pod sandbox deadline recovery (#206)

During a kind-backed staging rollout, kubelet and containerd can
stall while creating or killing pod sandboxes. The observable signatures are
`FailedCreatePodSandBox` or `FailedKillPod` events with `DeadlineExceeded` /
`context deadline exceeded`; affected new pods often sit in
`ContainerCreating` or `RunContainerError`, while old rollout pods remain
`Terminating`. This is a node-runtime cleanup/create failure, not an
application readiness failure.

`loom cluster status --format json` classifies this as
`failure_class=node_runtime_sandbox_deadline` on the affected Deployment and
records the exact pod/reason/operation diagnostics. `loom cluster release-gate`
uses the same classifier and keeps the release red even if a target-generation
pod is Ready but an old pod is still stuck in sandbox teardown.

The one-command rollout driver runs step 10 with
`--backup-manifest "$BACKUP_MANIFEST" --recover-sandbox-deadlines
--sandbox-deadline-max-pods 4`. `cluster up` runs its own protected preflight,
so the rollout carries the same manifest that step 05 verified and step 08
preflighted instead of bypassing the backup freshness guard. That path only runs
after the normal protected preflight, backup/storage guards, render, and apply
path. If readiness times out and the classifier finds sandbox-deadline pods, it
deletes at most four classified pods and retries readiness once. It does not
delete PVCs, namespaces, kind clusters, Docker volumes, or arbitrary unready
pods.

Brokered staging rollouts bind the reviewed operator backup ceilings into the
immutable rollout inputs and pass them explicitly to the candidate CLI in steps
05, 08, and 10: `--backup-max-files`, `--backup-max-entries`, and
`--backup-max-total-bytes`. The file ceiling is the reviewed MinIO object limit
plus the fixed non-MinIO allowance. Omitting these flags from a manual or
standalone CLI check retains the conservative generic defaults; the protected
runner does not read ambient environment overrides.

For a staging retry, use `loom-staging-rollout resume REQUEST_ID`. The broker
revalidates the original pinned candidate and backup envelope, and the driver
replays the same bounded recovery flags. Do not invoke `loom cluster up`
directly or delete pods ad hoc.

If the bounded retry still fails, preserve the rollout evidence and inspect the
kind node runtime directly (`kubelet`, `containerd`, node disk/I/O pressure).
Do not bypass protected preflight or storage/backup guards to continue the
rollout.

## Upgrade

Shared `loom-staging` upgrades follow the invariant at the top of this runbook:
merge the config/schema/image change to `dev`, start the broker, and inspect or
resume its request. The direct secret/image commands in this reference section
are development/custom or separately authorized production procedures only.

### Breaking changes by release

**v0.3 (PR #150, 2026-06-17) — config consolidation.** Two operator-visible breakages when upgrading from a pre-#150 cluster:

1. **`worker_max_concurrent` removed from `cluster-config.toml`.** The old top-level field no longer exists. Modern cluster render uses `[worker_capacity].max_concurrent` and `[replicas].worker` instead. If your `cluster-config.toml` set the old field, delete it — otherwise `loom cluster render` exits with `unknown keys in cluster config: ['worker_max_concurrent']`.

2. **Secret keys renamed in `loom-secrets`.** Two keys changed:
   - `cp-db-url` (used by loom-service) → `svc-db-url` (loom-service now has its own DB credential slot; control-plane keeps `cp-db-url`)
   - `svc-campaign-runner-cp-token` → `batch-runner-cp-token`

   To migrate an existing cluster:
   ```bash
   # Copy the existing values into the new keys, then drop the old ones.
   OLD_SVC_DB=$(kubectl get secret loom-secrets -o jsonpath='{.data.cp-db-url}' | base64 -d)
   OLD_CP_TOKEN=$(kubectl get secret loom-secrets -o jsonpath='{.data.svc-campaign-runner-cp-token}' | base64 -d)
   kubectl patch secret loom-secrets \
     -p "{\"stringData\":{\"svc-db-url\":\"$OLD_SVC_DB\",\"batch-runner-cp-token\":\"$OLD_CP_TOKEN\"}}"
   # Optional: drop the old keys after the rollout settles.
   ```

   Or run `loom cluster doctor` after the upgrade — it reports any orphan keys (no schema entry references them) and any missing keys (declared in schema, absent from Secret).

3. **New Secret keys required.** `postgres-user`, `postgres-password`, and `secret-store-master-key` are now declared in `[infra_secrets]` and must exist in `loom-secrets` (previously templates assumed some of these values existed but nothing checked). If they're already populated, no action. If not, add them — `loom cluster bootstrap-secrets --rotate` mints fresh `postgres-password` and `secret-store-master-key`, and emits `<EDIT_ME>` for `postgres-user`.

### Image upgrade

```bash
# Build + push new images
NEW_TAG=0.8
docker build -t loom-control-plane:${NEW_TAG} -f deploy/Dockerfile.control-plane .
# ... push, then bump image refs:
kubectl set image deploy/loom-control-plane control-plane=loom-control-plane:${NEW_TAG}
kubectl set image deploy/loom-llm-gateway   gateway=loom-llm-gateway:${NEW_TAG}
kubectl set image deploy/loom-service       loom-service=loom-service:${NEW_TAG}
kubectl set image deploy/loom-web           loom-web=loom-web:${NEW_TAG}
kubectl set image deploy/loom-worker        worker=loom-worker:${NEW_TAG}
```

Workers drain on SIGTERM (default 600 s); k8s sends SIGTERM during rollout.
`loom-service`, Control Plane, and `loom-web` are stateless — restart-safe.

## Rollback

For shared `loom-staging`, do not use the direct Kubernetes/image procedures
below. Merge a revert into `dev` and launch a new `loom-staging-rollout`
request, or resume the existing immutable request when no candidate change is
needed. The direct procedures in this section are for separately authorized
production/custom-cluster recovery and do not override the staging broker.

For a single-component rollback (the new image is bad but everything
else is fine):

```bash
kubectl rollout undo deploy/loom-control-plane
kubectl rollout undo deploy/loom-llm-gateway
kubectl rollout undo deploy/loom-service
kubectl rollout undo deploy/loom-web
kubectl rollout undo deploy/loom-worker
```

For a full-cluster rollback (manifest schema change went wrong, or
the new release misbehaves across components), point
`cluster-config.toml` back at the previous `image_tag` and re-deploy:

```bash
# 1. Edit cluster-config.toml: set image_tag back to the prior known-good
# 2. Audit + apply the previous shape:
loom cluster audit --config cluster-config.toml
loom cluster up --config cluster-config.toml --skip-preflight
loom cluster status
```

Use `--skip-preflight` only for same-storage-boundary image rollbacks where
Secrets, IngressClass, and critical PVC/PV bindings are already known-good. Do
not skip preflight during staging storage migration or restore
work; pass `--config`, `--environment`, and `--backup-manifest` instead.

Migration rollbacks: `alembic downgrade -1` from a Control Plane pod or
one-off migration Job, then restart DB-facing services so their startup schema
gate re-checks the downgraded revision.
DB-level downgrades that drop columns are NOT reversible without
restore from snapshot — gate destructive migrations behind a flag.

## Rate-card management

Costs are computed from versioned rate cards. To bump prices:

```bash
curl -X POST https://loom.example.com/api/v1/rate-cards \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d @new-rate-card.json
```

The public request is authenticated by `loom_service` and forwarded to the
internal Gateway admin route. The Gateway's in-memory cache invalidates
immediately; in-flight requests use whatever card was active when they started.

For hosted YibuAPI pricing, prefer syncing the official catalog:

```bash
loom admin rate-cards sync-yibuapi
```

This stores the official `source_url`, `pricing_version`, check time, group
ratio, and model count metadata in the service rate-card payload. Hosted
YibuAPI provider connections should use `pricing_source=rate-card` with
`rate_card_provider=yibuapi`; user self-deployed/private API connections should
normally stay `tokens-only`, which reports token totals and
`cost_status=not_applicable` without assigning a fabricated dollar amount.
Protected rollout profiles can declare `[rate_card_sync.yibuapi]` plus
`[[hosted_provider_pricing_defaults]]`; step 13 applies and verifies those
defaults on every rollout before release-gate or smoke can proceed.

## Trial-cache (per-trial agent install)

Workers install each adapter's CLI into the trial sandbox at spawn
time via a content-addressed layered image (`task_image` +
`install_script` → `loom-trial-cache:<sha256>`). First trial of a
new `(task_image, agent)` pair takes a few minutes (network +
package installs); subsequent trials reuse the layered image.

The benchmark task image is not the place to bake in a user's selected
agent, provider, or model. Task images carry benchmark/task dependencies,
assets, harnesses, and verifiers. The selected agent/model/provider come from
the submitted run or trial payload and are injected when the worker launches
the sandbox. To change a model or provider, submit a new run with different
payload fields; do not republish the benchmark image.

Subprocess agents call the LLM Gateway from inside the trial sandbox,
not from the worker process. Keep `LOOM_WORKER_GATEWAY_URL` pointed at
the worker-reachable gateway URL, and set
`LOOM_WORKER_SUBPROCESS_GATEWAY_URL` when the sandbox needs a different
sandbox-facing URL. Kubernetes workers get it from
`worker_subprocess_gateway_url` in `cluster-config.toml`; the default is
`http://host.docker.internal:30443/openai/v1`, matching the rendered
gateway-router hostPort and preserving the OpenAI-compatible subprocess
path. At launch time the worker normalizes that URL by adapter dialect:
OpenAI adapters use `/openai/v1`, Anthropic adapters use the sibling
`/anthropic` facade, and Gemini adapters use `/google`. Kind-on-host
deployments where another host service already owns `30443`, such as
platform-dev with the shared dev compose stack, should set:

Do not point OpenAI-compatible adapters such as Codex at an explicit
`/anthropic` or `/google` subprocess facade. The worker now rejects
incompatible explicit facade paths during agent startup so a bad remote-worker
env file fails fast instead of producing a zero-call benchmark result.

```toml
worker_subprocess_gateway_url = "http://host.docker.internal:30444/openai/v1"
```

For shared staging, commit that config to `dev`; the broker rollout validates
the already installed durable subprocess Gateway tunnel from `30444` to
`svc/loom-llm-gateway:9100`. Missing tunnel installation is root-owned broker
maintenance: disable admission, prove the active request terminal, repair the
installed unit from clean merged `dev`, restore admission, and resume the
original request. Do not invoke `worker_service_tunnels.py install-systemd`
from an operator checkout.

Bind only on an address/firewall boundary reachable by the local Docker
sandbox containers. `DockerDriver` injects the Linux host-gateway alias for
`host.docker.internal`.

See `docs/architecture/agent-adapter.md` for the architecture.

### Config knobs (`config/loom-schema.toml`, `[service_config]`)

DB-backed token `last_seen_at` / `last_used_at` updates are debounced to one
write per token per 60 seconds. If high-concurrency worker sweeps show
`QueuePool limit` errors or many sessions waiting on `UPDATE tokens`, verify
that debounce path first; raising `db_pool_size` and `db_max_overflow` alone can
increase queued connections without removing the token-row write hotspot.

| Key | Default | What it does |
|---|---|---|
| `db_pool_size` | `20` | Control Plane SQLAlchemy DB connection pool size. Size with concurrent worker heartbeats, claims, state patches, and trajectory index writes. |
| `db_max_overflow` | `40` | Control Plane SQLAlchemy overflow connections above `db_pool_size` for short writeback bursts. |
| `db_pool_timeout_sec` | `30.0` | Timeout while a Control Plane request waits for a DB connection from the pool. Pool timeouts show up as HTTP 500s on claim/state/writeback paths and can leave worker trials stuck before `running`. |
| `claimed_without_start_expiry_sec` | `3600` | Control Plane crash-detector threshold for reclaiming a trial stuck in `claimed` with `started_at IS NULL`, even when the owning worker heartbeat is still fresh. The crash detector ages from `pre_start_heartbeat_at` when upgraded workers are still reporting setup/cache progress, falling back to `claimed_at` for older workers. Keep above normal claim-to-start latency, including task materialization and image/cache setup. |
| `pre_start_heartbeat_interval_sec` | `60.0` | Worker cadence for trial-specific pre-start progress heartbeats while a claimed trial is still in task bundle lookup, materialization, task-image build, or layered trial-cache build. Keep this comfortably below `claimed_without_start_expiry_sec`. |
| `trial_cache_registry_repo` | `""` (unset) | Registry path to share layered images across workers (Docker Hub / GHCR / ECR / self-hosted). When unset, each worker caches locally. |
| `trial_cache_registry_pull_timeout_sec` | `15.0` | Per-attempt timeout for the registry pull. |
| `trial_cache_base_image_pull_timeout_sec` | `1800.0` | Per-attempt timeout for the underlying task-image pull (SWE-Bench instance images are 1–2 GB). |
| `trial_cache_ttl_hours` | `168` (7d) | Layered images older than this are pruned on the next eviction sweep. |
| `trial_cache_min_free_gb` | `20` | Capacity backstop — when free disk drops below this, oldest-by-creation entries are evicted first. |
| `trial_cache_build_lock_timeout_sec` | `1800.0` | Cluster-wide builder-slot TTL. The slot's owner refreshes every 60 s while building. |
| `trial_cache_build_max_concurrent` | `1` | Daemon-wide cap for concurrent layered trial-cache Docker builds across different cache keys. Keep `1` on shared OLDLAB/k8s Docker daemons; raise only for isolated daemon hosts after load testing. |
| `setup_health_guard_enabled` | `true` | Enables node-health admission before claiming work and before Docker setup/build work. |
| `setup_health_io_full_avg10_max` | `50.0` | Pauses claims and setup/build work when Linux `/proc/pressure/io` full avg10 exceeds this value. |
| `setup_health_min_mem_available_mb` | `2048` | Pauses claims and setup/build work when Linux `MemAvailable` falls below this MiB threshold, including no-swap hosts. |
| `setup_health_min_swap_free_mb` | `1024` | Blocks new setup/build work when swap is configured and free swap falls below this MiB threshold. |
| `setup_health_dstate_max` | `32` | Blocks new setup/build work when D-state process count exceeds this value. |
| `setup_health_wait_timeout_sec` | `300.0` | Claimed-trial wait budget for setup-health recovery before requeueing as `node_setup_health`; this pre-start placement attempt is refunded. |
| `setup_health_poll_interval_sec` | `5.0` | Poll interval while waiting for setup-health recovery. |
| `subprocess_gateway_url` | unset; k8s render injects `worker_subprocess_gateway_url` | Sandbox-facing gateway URL for subprocess agents. It may be the OpenAI facade default or a bare gateway-router URL; the worker normalizes it to the adapter's facade dialect before injecting SDK env vars. |
| `docker_api_timeout_sec` | `1800` | Docker SDK API timeout for worker-created clients. Keep this near the largest expected pull/build budget so docker-py does not fail long pulls, Dockerfile builds, or sidecar startup at its shorter default. |
| `minio_max_pool_connections` | `256` | Worker-side boto3 S3 connection pool size. Size with `max_concurrent` because task materialization, artifacts, and trajectory upload overlap during sweeps. |
| `minio_connect_timeout_sec` | `5.0` | S3 connect timeout for worker object-store calls. |
| `minio_read_timeout_sec` | `120.0` | S3 socket read timeout for worker object-store calls. |
| `minio_operation_timeout_sec` | `300.0` | Wall-clock timeout for each worker S3 wrapper call. `download_prefix` uses this separately for prefix listing and each individual object download; `put_object`, multipart part upload, and presign also use it per call. |
| `minio_operation_attempts` | `3` | Number of worker S3 operation attempts after timeouts, socket/client disconnects, or retryable S3 5xx/throttle responses. Each attempt gets the full operation timeout and reconnects the boto3 client before retrying. |
| `task_materialize_timeout_sec` | `300.0` | Wall-clock timeout for pre-start task bundle materialization. If an `hf://`, `fixture://`, or `s3://` materializer hangs before `started_at`, the worker fails the claimed trial through setup failure writeback instead of leaving it stuck `claimed`. |

### Setting up the optional shared registry (Docker Hub example)

1. Create a Docker Hub team (or pick an existing organization) and a
   private repository — e.g. `loomops/trial-cache`.
2. Create a robot account / access token with `read+write` to that
   repo.
3. Create a `docker-config` Secret for every worker namespace. Loom
   mounts the Docker-registry Secret's `.dockerconfigjson` key as
   `/root/.docker/config.json` so docker-py can send registry auth
   during task-image builds and sidecar/base-image pulls:
   ```bash
   kubectl -n loom create secret docker-registry docker-config \
     --docker-server=https://index.docker.io/v1/ \
     --docker-username=<robot-user> \
     --docker-password=<robot-token>
   ```
   `loom cluster render` wires this Secret into the worker Deployment
   as an optional read-only mount. Workers still start if the Secret
   is missing, but Docker Hub pulls remain anonymous until the Secret
   exists. At startup, each worker logs only secret-free metadata:
   config path, whether the file is present, configured registry
   domains, and whether a credential store is configured.
4. Set the config:
   ```bash
   loom admin config set trial_cache_registry_repo loomops/trial-cache
   ```
5. Roll workers. The first trial of each `(task_image, agent)` pair
   pushes; every subsequent trial pulls.

Any registry that `docker pull` / `docker push` understands works —
GHCR, ECR, Harbor, self-hosted Distribution. Push failures degrade
silently to local-only caching.

### Disabling the shared cache

`loom admin config set trial_cache_registry_repo ""` → workers fall
back to per-worker local cache (still hot for the worker's own retry,
but each worker pays the build cost once).

### Troubleshooting

- **Trial fails with `TrialCacheError: timed out pulling task image`**
  → bump `trial_cache_base_image_pull_timeout_sec`; the benchmark's
  task image is bigger than the default 30 min budget can handle on
  the worker's link.
- **Trial setup fails with Docker SDK read timeout during image pull,
  Dockerfile build, or sidecar startup** → bump `docker_api_timeout_sec`
  and verify the worker has Docker Hub/registry auth mounted. The
  task-level `build_timeout_sec` still bounds Dockerfile builds; this
  knob prevents docker-py itself from timing out first.
- **Concurrent setup/build work fails with apt/dpkg/containerd errors, killed
  setup containers, high `/proc/pressure/io`, full swap, or SSH/login symptoms
  on a shared worker host** → root cause is setup/build admission, not user
  auth. Keep `trial_cache_build_max_concurrent=1` for that daemon group and keep
  the setup-health guard enabled. The same daemon setup slot now covers task
  Dockerfile builds, layered `(task_image, agent)` cache builds, and sidecar
  image pulls/builds; it does not reduce already-warm trial concurrency. Run
  `loom worker setup status` on the worker host to see the guard decision and
  Loom-labeled setup/trial containers. If the trial fails with
  `failure_reason=node_setup_health`, treat it as platform setup pressure and
  drain/pause the worker pool before targeted cleanup. Do not remove retained
  `loom.trial-cache=true` images unless the cache policy or disk-pressure
  remediation explicitly calls for it.
- **Trial fails with `failure_reason=task_image_build_timeout` or a message
  like `building Docker image ... exceeded 1800s`** → treat it as a platform
  setup failure, not benchmark/model evidence. For GB10/ARM64 or mixed-arch
  full100 gates, warm the task image on each required architecture first:
  keep the shared trial-cache registry enabled, run a small architecture-targeted
  canary for the representative task image, and confirm subsequent trials pull
  or reuse the cache before launching the high-concurrency batch. Only raise
  task `build_timeout_sec` after confirming the Docker daemon, registry auth,
  disk, and CPU pressure are healthy enough that the longer build is expected.
  Terminus 2 bundles based on `mictern2/terminus2-full:latest` create a
  worker-local ARM64 compatibility base on first use of an ARM64 Docker daemon;
  if that prewarm fails, keep the run blocked as platform setup evidence
  rather than treating the trial as model-quality evidence.
- **Trial setup fails with `S3 download_prefix` list/download timeout,
  retryable S3 5xx/throttle responses, socket disconnects, or
  trajectory/artifact upload timeouts under high concurrency** → first confirm
  MinIO health and network reachability, then raise
  `minio_operation_timeout_sec` or `minio_operation_attempts`.
  `download_prefix` retries prefix listing and each object download as separate
  units, so one object disconnect should not restart the whole prefix. If
  errors coincide with high worker concurrency and connection starvation, raise
  `minio_max_pool_connections`; if Python setup threads are saturated, tune
  `LOOM_WORKER_BLOCKING_IO_MAX_WORKERS` separately. Exhausted retries leave the
  final exception type/message in the trial failure message for diagnosis.
- **Trial setup fails with `task materialization timed out` before
  `started_at`** → inspect the source scheme in the failure message and worker
  logs. For `hf://`, verify whether the benchmark should already have been
  mirrored into internal object storage; direct worker HF access is only a
  compatibility path for private/gated source repos and should not replace the
  internal mirror/provision workflow. Raise `task_materialize_timeout_sec` only
  when the source is reachable and legitimately slow, and keep it coordinated
  with the claimed-without-start reclaim policy tracked by #193; otherwise a
  healthy worker may still lose a long setup claim before it can mark the trial
  running. Upgraded workers report `pre_start_heartbeat_at` while setup/cache
  work is active, and the crash detector measures the pre-start reclaim window
  from that timestamp instead of the original claim time. When the crash
  detector reclaims a pre-start claim, it records
  `failure_reason=worker_lost_claim` and a `claimed_without_started_reclaimed`
  message with the prior worker id, claim time, pre-start heartbeat time,
  expiry window, and `started_at=NULL`; if the retry budget is already
  exhausted, the terminal `retry_exhausted` row preserves that message for
  attribution.
- **Trial fails with `TrialCacheError: failed to acquire build slot`**
  → check the `active_trial_cache_builds` table for a stuck row past
  its `expires_at`; the next claimant will steal it on its own, but
  if the table grows unboundedly the heartbeat thread on workers is
  not refreshing. Inspect worker logs for the cache_key.
- **Cache filling disk** → lower `trial_cache_ttl_hours`, raise
  `trial_cache_min_free_gb`, or run a manual sweep on the host:
  ```bash
  docker images --filter "label=loom.trial-cache.created-at" --format json
  docker image prune --filter "label=loom.trial-cache.created-at"
  ```
- **Adapter unchanged but cache misses every trial** →
  `install_script` text differs across releases (e.g. you bumped a
  pinned version). Expected — that's the whole point of content
  addressing.

## Token rotation

For shared staging, do not execute the direct Kubernetes or admin mutation
examples in this section. Update the canonical protected token source through
the approved configuration path, merge the corresponding change to `dev`, and
start a broker rollout so every in-cluster, OLDLAB, and GB10 consumer changes
inside one attributed envelope. The examples below are for development/custom
clusters or separately authorized production maintenance.

### Worker tokens — `loom admin tokens worker`

The CP admin surface (`/admin/worker-tokens`) is NOT exposed via
Ingress; reach it through a port-forward:

```bash
kubectl port-forward deploy/loom-control-plane 8080:8080 &
export LOOM_ADMIN_TOKEN=$(yq '.[admin].token' loom-admin-secret.toml)
```

Then use the CLI. The mint/rotate commands default to printing only
the token's hash prefix in text mode. When no remote workers are attached,
pipe the JSON output straight into the Secret store so the raw value never
lands on `argv` (visible to `ps`) or in shell history. When GB10/OLDLAB
remote pools need the same token, write it once to a root/operator-readable
`0600` file, install Kubernetes from that file, distribute the same value to
remote env files, and use that file as the `--worker-token file:` proof source:

```bash
# One-shot rotate: mint, install, restart, revoke old.
install -m 600 /dev/null /secure/path/worker-token
loom admin tokens worker rotate --format json --expires-in-days 365 \
  | jq -r .token > /secure/path/worker-token
kubectl create secret generic loom-secrets \
  --from-file=worker-token=/secure/path/worker-token \
  --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl rollout restart deploy/loom-worker
# Reconcile attached remote-worker consumers through the target environment's
# separately authorized maintenance path, then prove token parity there.
# After in-cluster and remote workers re-register cleanly (no 401s),
# revoke the old prefix:
loom admin tokens worker revoke <OLD_PREFIX>
```

If you need to read the raw token (e.g., to drop it into a Slurm
worker host's `~/.loom/worker.env`), pass `--show-secret` instead
of `--format json`:

```bash
loom admin tokens worker mint --show-secret --expires-in-days 365
```

Or one-off without the rollout reminder:

```bash
loom admin tokens worker mint --expires-in-days 365   # prefix only
loom admin tokens worker revoke ab12cd34
```

All three subcommands accept `--cp-url URL` (default
`http://localhost:8080`) and `--admin-token SRC` (default
`env:LOOM_ADMIN_TOKEN`; same `env:VAR | file:PATH | -` indirection
form as the rest of the CLI). The prefix is the 4-64 hex chars from
`token_hash_prefix` returned at mint.

Raw curl is still supported for emergency operations:
```bash
curl -X POST http://localhost:8080/admin/worker-tokens \
  -H "Authorization: Bearer $LOOM_ADMIN_TOKEN" \
  -d '{"expires_in_days": 365}'
curl -X DELETE http://localhost:8080/admin/worker-tokens/$OLD_PREFIX \
  -H "Authorization: Bearer $LOOM_ADMIN_TOKEN"
```

### Worker-token staleness

The Control Plane's metrics refresher publishes
`loom_worker_tokens_stale_count{reason}` every 30 seconds, and
`LoomWorkerTokenStaleness` fires after either label has been non-zero
for 1 hour:

- `reason="unused_30d"` — a live (non-revoked, non-expired) worker
  token whose `last_seen_at` (or `issued_at`, if never used) is
  older than 30 days. Most common cause is a worker pool that was
  decommissioned without revoking its token.
- `reason="aged_90d"` — a live worker token whose `issued_at` is
  older than 90 days. Rotation is overdue per the SOC2-equivalent
  service-credential rotation cadence.

The audit never auto-revokes. Pulling a live pool's token 401s
in-flight claims and pages on-call — humans decide.

List the candidate tokens without exposing any raw value:

```sql
SELECT encode(token_hash, 'hex') AS hash_prefix,
       name,
       issued_at,
       last_seen_at,
       expires_at
  FROM tokens
 WHERE type = 'worker'
   AND revoked_at IS NULL
   AND (expires_at IS NULL OR expires_at > NOW())
   AND (COALESCE(last_seen_at, issued_at)
          < NOW() - INTERVAL '30 days'
        OR issued_at < NOW() - INTERVAL '90 days')
 ORDER BY COALESCE(last_seen_at, issued_at) ASC;
```

Response per case:

1. **Pool is genuinely retired** → revoke it.
   ```bash
   loom admin tokens worker revoke <PREFIX>
   ```
2. **Pool is alive but long-cycle (e.g., a Slurm pool that batches
   monthly)** → add an alertmanager silence keyed by the token's
   hash prefix until the next scheduled batch. The 30d threshold
   is tight for SOC2-equivalent hygiene; long-cycle pools are
   the known false-positive case.
3. **Aged 90d, pool is alive** → rotate per the procedure above
   (mint new, install via stdin pipe, restart workers, revoke
   old).

The query is also useful as a quarterly hygiene check independent
of the alert.

### Storage retention policy

Loom's object store accumulates trajectories and artifacts. Without
a server-side lifecycle policy, the disk fills silently and trial
uploads start failing. The `loom cluster bootstrap-storage-lifecycle`
subcommand applies operator-configurable retention rules to every
configured bucket via the storage backend's native lifecycle API
(`put_bucket_lifecycle_configuration` for S3-compatible backends).

The config lives in `config/storage-lifecycle.toml` (operator-owned;
copy the bundled `storage-lifecycle.example.toml` as the starting
point). It's provider-neutral — the same config renders into MinIO,
AWS S3, Cloudflare R2, Backblaze B2, and Wasabi.

Strategies shipped:

| Strategy | Use case | Notes |
|---|---|---|
| `expire_after_days` | Trajectories, raw artifacts | Deletes objects N days after creation. The 95% case. |
| `keep_forever` | ATIF, evidence bundles | Explicit no-op; documents intent so future config changes don't accidentally apply expiry. |
| `cleanup_incomplete_uploads_after_hours` | Every bucket | Aborts stuck multipart uploads. Doesn't touch completed objects. |

Recommended defaults in the example file:

- `trajectories`: expire after 30 days. Highest churn; least information density past a successful trial finalize.
- `artifacts`: expire after 180 days. Larger objects, generous review window.
- `atif`: keep forever. Small, queryable, the permanent record.

Apply the policy:

```bash
# Dry-run: print the rendered lifecycle rules without contacting
# the store. Useful for review before a real apply.
loom cluster bootstrap-storage-lifecycle \
  --config config/storage-lifecycle.toml --dry-run

# Live apply. Idempotent — re-running produces no change at the
# storage layer if the config is unchanged.
loom cluster bootstrap-storage-lifecycle \
  --config config/storage-lifecycle.toml
```

Credentials default to `LOOM_SVC_MINIO_ACCESS_KEY` +
`LOOM_SVC_MINIO_SECRET_KEY` (falling back to `MINIO_ROOT_USER` +
`MINIO_ROOT_PASSWORD`). Endpoint defaults to
`$LOOM_SVC_MINIO_ENDPOINT` or `http://loom-minio:9000`; override
with `--endpoint URL` for out-of-cluster runs (port-forward
required).

Tuning:

- If `LoomMinioPVCUsageHigh` fires sooner than expected, shorten the
  trajectory window. Edit `storage-lifecycle.toml`, re-run the
  subcommand. The new rules take effect on the next MinIO lifecycle
  sweep.
- If batches frequently get aborted mid-upload, lengthen the
  `cleanup_incomplete_uploads_after_hours` window for the affected
  bucket — `trajectories` defaults to 14 days to cover SWE-Bench-class
  long trials.

#### Verify retention is in effect

`loom cluster doctor` accepts an optional storage-lifecycle config
and compares live MinIO state to what the renderer would produce:

```bash
kubectl port-forward -n loom service/loom-minio 9000:9000 &
loom cluster doctor \
  --storage-lifecycle-config config/storage-lifecycle.toml \
  --storage-lifecycle-endpoint http://localhost:9000
```

Exit 0 plus `[ok] storage lifecycle rules match config` means the
operator's expected rules are live on every bucket. Exit 1 with a
`Storage lifecycle drift detected:` block surfaces:

- **`missing on storage`** — the rule isn't applied. Most common cause
  is "operator never ran `bootstrap-storage-lifecycle`" (e.g. fresh
  cluster) or "operator edited the config and forgot to re-apply."
- **`present on storage but not in config`** — somebody added a rule
  out-of-band via `mc ilm rule add`. Doctor doesn't manage it; the
  detail line tells you the rule ID so you can decide whether to
  fold it into `storage-lifecycle.toml` or remove it.
- **`content drift on rule(s)`** — same rule ID, different parameters.
  Re-running the bootstrap fixes it.

Re-applying the bootstrap is always safe — the rendered XML is
byte-stable, so re-runs are no-ops when nothing changed.

#### Migrating an existing deployment to enable retention

Deployments running before PR #226 landed have no retention rules
applied. The mechanism is in the new release; the rules are not yet
live until somebody runs the bootstrap. Procedure for picking it up
on a running cluster:

1. **Audit current bucket usage** so you know what you're working
   with:
   ```bash
   mc du loom-minio/trajectories loom-minio/artifacts loom-minio/atif
   ```
2. **Decide whether to preserve historical data.** **Important:**
   `Expiration.Days` in S3 lifecycle is computed against object age
   (i.e., the object's creation time), not against when the rule was
   set. So if your trajectory bucket already holds 60-day-old objects
   and you set a 30-day expiry, **MinIO will delete those objects on
   the next lifecycle sweep**. If anything in the bucket is worth
   keeping permanently, mirror it to off-cluster cold storage first:
   ```bash
   mc mirror loom-minio/trajectories /backup/loom-trajectories-snapshot
   ```
3. **Copy the example config** to a stable location:
   ```bash
   cp config/storage-lifecycle.example.toml \
      config/storage-lifecycle.toml
   $EDITOR config/storage-lifecycle.toml   # tune days per workload
   ```
4. **Dry-run** to audit what will be applied before mutating live
   state:
   ```bash
   kubectl port-forward -n loom service/loom-minio 9000:9000 &
   loom cluster bootstrap-storage-lifecycle \
     --config config/storage-lifecycle.toml \
     --endpoint http://localhost:9000 --dry-run
   ```
5. **Apply** for real:
   ```bash
   loom cluster bootstrap-storage-lifecycle \
     --config config/storage-lifecycle.toml \
     --endpoint http://localhost:9000
   ```
6. **Verify** with `mc` and the doctor sub-check:
   ```bash
   mc ilm rule ls loom-minio/trajectories
   mc ilm rule ls loom-minio/artifacts
   mc ilm rule ls loom-minio/atif   # should be empty (keep_forever)
   loom cluster doctor \
     --storage-lifecycle-config config/storage-lifecycle.toml \
     --storage-lifecycle-endpoint http://localhost:9000
   ```
7. **Watch the PVC trend for a week.** Expectation: trajectories
   volume plateaus or shrinks; artifacts trend depends on workload
   age distribution. If usage keeps climbing, the retention window
   is longer than the disk can sustain at the current trial volume
   — tune `days` down or expand the PVC (`kubectl patch pvc
   data-loom-minio-0 -p '{"spec":{"resources":{"requests":
   {"storage":"1Ti"}}}}'`).

#### Re-applying after config changes

The bootstrap is idempotent; the rendered XML is byte-stable. Edit
`storage-lifecycle.toml`, re-run the same command, and the new rules
take effect on the next MinIO lifecycle sweep. Use `--dry-run` to
audit the diff before applying. `loom cluster doctor
--storage-lifecycle-config <path>` is the cheap reverse check —
"are my expected rules actually live?"

#### Deployment shapes

The retention engine works across S3-compatible backends and (when
the GCS renderer ships) GCS. Each shape has different operator
prerequisites, monitoring story, and backup posture. Pick the
section below that matches your deployment.

##### Shape 1: MinIO single-node (default)

The bundled `deploy/k8s/minio.yaml`. Cluster-internal scratch
space; **not** durable archival storage. Right for developer and
single-research-cluster deployments.

- **Bucket creation:** Loom does it at startup (loom_service ensures
  `trajectories`, `artifacts`, `atif` exist).
- **Credentials:** `minio-access-key` / `minio-secret-key` in
  `loom-secrets` (minted by `bootstrap-secrets`).
- **Bootstrap retention:**
  ```bash
  kubectl port-forward -n loom service/loom-minio 9000:9000 &
  loom cluster bootstrap-storage-lifecycle \
    --config config/storage-lifecycle.toml \
    --endpoint http://localhost:9000
  ```
- **Monitoring:** `LoomMinioPVCUsageHigh` / `LoomMinioPVCUsageCritical`
  on `kubelet_volume_stats_*` for `data-loom-minio-0`. See [MinIO PVC
  usage](#minio-pvc-usage) below.
- **Backup:** operator-driven `mc mirror` to off-cluster (no
  Loom-side automation).
- **Limitations:** no replication, no off-cluster durability, single
  point of failure.

##### Shape 2: AWS S3 (managed, durable)

Recommended for production research clusters that need durable
archival. Loom does not deploy or manage the bucket; the operator
pre-creates it.

- **Bucket pre-creation (operator's IaC, e.g. Terraform):**
  - Region matching the Loom service deployment.
  - **Versioning enabled** (recommended for accidental-delete
    recovery — pairs with `keep_forever` ATIF retention).
  - Cross-region replication optional.
  - Public access blocked.
  - **Do NOT pre-create lifecycle rules** — Loom applies them via
    `bootstrap-storage-lifecycle` and `doctor` would otherwise flag
    drift.
- **IAM policy for IRSA** (preferred over static keys on EKS):
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLifecycleConfiguration",
        "s3:PutBucketLifecycleConfiguration",
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::loom-prod-trajectories",
        "arn:aws:s3:::loom-prod-trajectories/*"
      ]
    }]
  }
  ```
  Annotate the Loom service account:
  `eks.amazonaws.com/role-arn: arn:aws:iam::123:role/loom-storage`.
- **Bootstrap retention:**
  ```bash
  loom cluster bootstrap-storage-lifecycle \
    --config config/storage-lifecycle.toml \
    --endpoint https://s3.us-east-1.amazonaws.com
  ```
  No port-forward required — S3 is reachable from the cluster.
- **Monitoring:** CloudWatch `BucketSizeBytes` metric per bucket;
  alarm at provisioned capacity threshold or growth rate. No
  Prometheus rule ships from Loom for managed backends; wire CloudWatch
  → alertmanager via your incident tooling.
- **Backup:** provider-side. Object Versioning + cross-region
  replication; no Loom-side automation.

> IRSA is wired end-to-end via `loom.storage_credentials.build_s3_client`.
> Set `LOOM_SVC_STORAGE_AUTH_KIND=irsa` (default is `static_keys`) so
> boto3 walks its standard provider chain — the ServiceAccount
> annotation above is what makes the STS `AssumeRoleWithWebIdentity`
> call resolve on EKS. Static access keys in `loom-secrets` remain
> supported for deployments not on EKS.

##### Shape 3: GCS (managed, durable)

Recommended for production deployments on GKE. Same shape as
AWS S3 with provider-specific differences.

- **Bucket pre-creation:**
  - Location matching the Loom service deployment.
  - Object Versioning enabled.
  - **Do NOT pre-set lifecycle rules** — Loom applies them.
- **Workload Identity** (preferred over service-account JSON):
  - Roles on the bucket: `roles/storage.objectAdmin`,
    `roles/storage.legacyBucketReader`.
  - Annotate the Loom service account:
    `iam.gke.io/gcp-service-account: loom@<project>.iam.gserviceaccount.com`.
- **Bootstrap retention:**
  ```bash
  loom cluster bootstrap-storage-lifecycle \
    --config config/storage-lifecycle.toml \
    --endpoint https://storage.googleapis.com
  ```
- **Monitoring:** Stackdriver / Cloud Monitoring
  `storage.googleapis.com/storage/total_bytes` per bucket; alert at
  threshold.
- **Backup:** Object Versioning + scheduled `gsutil rsync` if
  off-platform copy is required.

> The GCS lifecycle renderer ships. `bootstrap-storage-lifecycle
> --dry-run --config … --endpoint https://storage.googleapis.com`
> emits the GCS-native lifecycle JSON dialect (`{"rule": [...]}`).
> The SDK-based native apply is deferred until the first GCS
> deployment lands (`google-cloud-storage` integration in the
> factory); until then, operators pipe the `--dry-run` output into
> `gcloud storage buckets update --lifecycle-file` or `gsutil
> lifecycle set`. `loom cluster doctor --storage-lifecycle-config`
> against GCS is deferred on the same schedule.

##### Shape 4: On-prem distributed MinIO (erasure coding)

For air-gapped or hybrid deployments where AWS/GCS isn't available
but operators want durable storage.

- **Pre-deploy:** distributed MinIO (4-node minimum for EC4+2;
  6-node for EC4+3). Operator's call on hardware, drive layout,
  erasure-set configuration. See MinIO upstream docs.
- **Endpoint:** load-balanced front door, e.g.
  `http://minio-lb.internal:9000`. Loom config:
  ```toml
  [service_config.minio_endpoint]
  value = "http://minio-lb.internal:9000"
  ```
- **Credentials:** static root key in `loom-secrets`, same shape
  as Shape 1.
- **Bootstrap retention:** identical to Shape 1, just with the
  operator's endpoint.
- **Monitoring:** MinIO Prometheus exporter (operator's
  responsibility — distributed MinIO exposes its own metrics
  endpoint per node).
- **Backup:** distributed MinIO has site-level replication built
  in (`mc admin replicate`); use it. Don't rely on per-node disk
  redundancy alone.

### MinIO PVC usage

MinIO ships as a single-replica `StatefulSet`. The default static manifest
requests a 500Gi PVC (`deploy/k8s/minio.yaml`), while rendered
environment profiles may request a different PVC/PV size. In the
staging hostPath/local-PV contract, that Kubernetes capacity is allocation
metadata, not the effective quota. The effective storage limit is the
filesystem mounted at `/data` inside `loom-minio-0`; operators must treat
`df /data` as the source of truth before large staging batches.

`LoomMinioPVCUsageHigh` (warning) fires when utilization is above
80% for 15 minutes; `LoomMinioPVCUsageCritical` (critical) at >95%
for 5 minutes. Both link back to this section.

Triage:

```bash
# Per-mount disk usage from inside the MinIO pod.
kubectl exec -n loom loom-minio-0 -- df -h /data

# Per-bucket breakdown (run from any host with `mc` configured).
mc du loom-minio/trajectories loom-minio/artifacts loom-minio/atif
```

Before any full-batch, full-max-slot, or large staging run, write a
storage preflight artifact into the run evidence directory:

```bash
loom cluster minio-storage-preflight \
  --namespace loom-staging \
  --output "$RUN_EVIDENCE_DIR/minio-storage-preflight.json" \
  --format json
```

The artifact records `/data` size/used/free/percent, bucket usage for
`artifacts`, `trajectories`, `loom-benchmarks`, and `loom-tasks`, configured
warning/stop thresholds, optional estimated batch headroom, and an
artifact/trajectory growth row when a previous preflight is supplied with
`--previous-evidence`. The default thresholds are warning below 25% free and
stop below 15% free.

If the artifact outcome is `stop`, do not submit the batch. Reclaim MinIO
space, shorten retention, or provision backing storage first. `loom eval batch
create --storage-preflight-evidence ...` refuses a stopped artifact unless the
operator also passes `--override-storage-preflight-stop`; using that override
must be recorded in the evidence summary with the accepted risk. `loom cluster
release-gate --minio-storage-preflight ...` also renders a
`minio-storage-pressure` component row and fails when the artifact outcome is
`stop`.

On fixed platform-dev staging, `/data` and Docker share the same ext4 backing
filesystem. The brokered rollout therefore performs one bounded host-cache
retention pass immediately before the storage preflight: Docker may remove only
images and build-cache records it considers unused and older than 24 hours.
Each Docker prune command has a two-hour fail-closed ceiling so an initial
convergence with thousands of stale images can finish without becoming an
unbounded maintenance operation.
The current candidate is freshly built, then its exact image identities are
re-read after cleanup. The resulting `staging-host-cache-retention.json` records
the policy, candidate SHA/tag, command exit codes, and measured free bytes
before/after without turning the rollout into a generic path or prune surface.
Do not substitute raw filesystem deletion, prune active images, or apply this
staging-only policy to production.

Remediation paths, in priority order:

1. **Retention rules missing or stale.** Most common cause for
   first-time fills. Apply or re-apply the policy:
   ```bash
   loom cluster bootstrap-storage-lifecycle \
     --config config/storage-lifecycle.toml
   ```
   MinIO's background lifecycle sweep applies new rules within
   minutes to hours; usage should trend down on the next dashboard
   refresh.

2. **Disk grew faster than expected even with retention.** The
   retention window may be longer than the disk can support at the
   current trial volume. Either shorten the window in
   `storage-lifecycle.toml` (then re-apply), or expand the PVC if the
   StorageClass supports volume resize:
   ```bash
   kubectl patch pvc data-loom-minio-0 -n loom \
     --type='merge' -p '{"spec":{"resources":{"requests":{"storage":"1Ti"}}}}'
   ```

3. **Emergency — disk is full RIGHT NOW.** Manual expire of old
   objects buys time:
   ```bash
   mc rm --recursive --older-than 14d loom-minio/trajectories
   ```
   Then apply the retention policy permanently. If the StorageClass
   doesn't support resize, the durable answer is migration to
   external object storage via `loom cluster --storage external`.

On the fixed `platform-dev` host, `/data` and `/shared_work` currently share
the large ext4 root filesystem.  Do not delete `/shared_work`: it is active
multi-user data, not rollout cache.  A default five-percent ext4 root reserve
on this multi-terabyte filesystem can also make MinIO's non-root `df /data`
view cross the stop threshold even when physical free blocks remain.  The
sealed-candidate helper below provides the only supported bounded convergence
for that exact host and device; it keeps a three-percent root reserve, validates
the exact mount/device/type/ownership and large-filesystem identity before
mutation, locks concurrent calls, reads back the result, and rolls back to the
exact prior reserved-block count on a bad readback:

```bash
sudo /usr/bin/python3 \
  "$SEALED_CHECKOUT/scripts/ops/staging_rollout_data_reserve.py" check
sudo /usr/bin/python3 \
  "$SEALED_CHECKOUT/scripts/ops/staging_rollout_data_reserve.py" install
sudo /usr/bin/python3 \
  "$SEALED_CHECKOUT/scripts/ops/staging_rollout_data_reserve.py" check
```

Run it only with no active rollout attempt and only from an independently
validated exact sealed checkout.  It is not a storage-stop override: the
15-percent MinIO threshold remains unchanged, and a new preflight must still
pass before a replacement rollout.  Device, filesystem, or reserve drift fails
closed and requires a reviewed repo update rather than ad hoc `tune2fs` flags.

4. **Long-term durability.** Single-node MinIO + a fixed PVC is fine
   for cluster-internal scratch space but is not durable archival
   storage (no replication, no off-cluster backup beyond manual
   `mc mirror`). For runs you want to keep forever, migrate to
   managed object storage. The design discussion is tracked in #221.

### Legacy team-token compatibility — `loom admin tokens team`

Normal automation should use a user-owned API token created from a browser or
username/password user session. `loom admin tokens team` exists for legacy
unowned team-token rotation, revocation, and migration support. Tokens minted
with an admin bearer have no `created_by_user_id`; they can authenticate for
non-submitting compatibility, but the service rejects them for batch creation,
direct trial creation, failed-case reruns, Run Library clone, and artifact
reuse. The route is public (`/api/v1/tokens`), so it uses the bearer + server
URL from `loom auth login` (no port-forward). Raw `loom_api_...` values are
shown only on mint/rotate. Admin callers must supply `--admin-actor NAME`,
which the server records in `admin_audit_events`.

```bash
loom auth login --server https://loom.example.com --token env:LOOM_ADMIN_TOKEN

# Mint a fresh legacy team token + print the rollout checklist.
loom admin tokens team rotate \
  --name nightly-cli \
  --team-id <UUID> \
  --scopes read:own \
  --expires-in-days 90 \
  --admin-actor qianyi

# After clients have moved over, revoke the old token by its 8-hex prefix:
loom admin tokens team revoke <OLD_PREFIX> --admin-actor qianyi
```

One-off mint / revoke without the rollout reminder:

```bash
loom admin tokens team mint \
  --name support-cli \
  --team-id <UUID> \
  --scopes read:own \
  --admin-actor qianyi
loom admin tokens team revoke 01234567 --admin-actor qianyi
```

Legacy token migration checklist:

1. On every host that may submit work, run `loom auth whoami`. If it prints
   `Principal: legacy team token` or no `User:` line, do not submit from that
   config.
2. Run `loom auth logout`, then log in with the approved username/password:
   `loom auth login --server https://loom.example.com --username USER --password env:LOOM_PASSWORD`.
3. For unattended jobs, create a named API token from Team access or a
   user-owned session and store it as `LOOM_API_TOKEN`; verify `loom auth whoami`
   prints `Principal: user-owned API token` and the intended user/team.
4. The known oldlab1 stale-config failure mode is a lingering
   `$XDG_CONFIG_HOME/loom/config.toml` or repo-local shell env that still points
   at an old EAI team token. Check oldlab1 before validation runs, especially
   under `/home/qianyi/dev/loom` and any Slurm submit shell, because stale
   configs can otherwise submit into the wrong owner team with no
   `submitted_by_user`.

`--scopes` accepts a comma-separated list. Known team scopes: `read:own`,
`submit`, `providers:manage`, and `tokens:manage`; anything else is rejected
client-side before the round-trip. `--type` defaults to `team`, and admin
credentials are managed only by
`loom service init-admin`, `loom service reveal-admin`, and
`loom service rotate-admin`. Default lifetime is 90 days.

Raw curl is still supported for scripted automation:
```bash
curl -X POST https://loom.example.com/api/v1/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "X-Loom-Admin-Actor: qianyi" \
  -d '{"name": "support-cli", "type": "team", "team_id": "...",
       "scopes": ["submit"], "expires_in_days": 90}'
```
`type` is required and must be `team`. The service rejects DB-backed admin
tokens and any requested `admin:*` scope.

### Audited admin on-behalf canary batch submission

Use this path only when a staging or release canary must represent a named
active user/team and the user's browser session or user-owned API token is not
available. It does not mint a user-owned token and it does not relax normal
submission auth: legacy team tokens remain rejected by `POST /api/v1/batches`.

Requirements before submission:

- The real operator authenticates with an admin-capable bearer and passes
  `--admin-actor NAME`, which becomes `X-Loom-Admin-Actor`.
- The represented user is active, not disabled, and belongs to the represented
  team.
- The represented team is not disabled or paused for submissions.
- The evidence bundle records the command, the returned batch id, the
  represented username/team id, and the matching `batch.submit_on_behalf`
  admin audit event. Do not record raw bearer values.

No-model canary example:

```bash
loom auth login --server https://loom.example.com --token env:LOOM_ADMIN_TOKEN

loom admin batches submit-on-behalf \
  --represented-username qianyi \
  --team-id <agentic-rl-team-id> \
  --admin-actor <operator-name> \
  --name-suffix oracle-smoke \
  --task-filter '{"task_ids":["loom-smoke/gb10-oracle-hello-world"]}' \
  --agent oracle \
  --n-per-task 1 \
  --required-worker-pool gb10
```

Model-backed provider canary example:

```bash
loom admin batches submit-on-behalf \
  --represented-username qianyi \
  --team-id <agentic-rl-team-id> \
  --admin-actor <operator-name> \
  --name-suffix opencode-yibuapi-smoke \
  --task-filter '{"task_ids":["source-useful-frontier-5003/shard003__software_development__buildsqliteissuetrackercli"]}' \
  --provider mz_tn_canada_qianyi \
  --model glm-5.1-thinking \
  --agent opencode \
  --n-per-task 1 \
  --backend docker \
  --required-worker-pool gb10
```

Audit evidence:

```bash
tmp_headers="$(mktemp)"
chmod 600 "$tmp_headers"
printf 'Authorization: Bearer %s\n' "$LOOM_ADMIN_TOKEN" > "$tmp_headers"
curl -s https://loom.example.com/api/v1/admin/audit-events?limit=20 \
  -H "@$tmp_headers" \
  | jq '.items[] | select(.action=="batch.submit_on_behalf")'
rm -f "$tmp_headers"
```

The event metadata records the represented user id, represented username,
represented team id, expected trial count, and backend. It intentionally omits
user-controlled free-text such as the batch name. Do not capture the temporary
header file or shell trace output in evidence; the evidence artifact should
contain only the redacted command pattern and the selected audit JSON.

### Provider API key rotation — `loom providers rotate-key`

When a provider key needs to be rotated (compromise, scheduled rotation,
key wrap-around), one command swaps the encrypted ciphertext + verifies
the new key in a single round-trip:

```bash
loom providers rotate-key openai-prod --api-key env:NEW_OPENAI_KEY
```

What this does end-to-end:

1. `PATCH /api/v1/provider-connections/<id>` with `api_key=<new value>`.
2. Server's route encrypts the new value via `SecretStore.put`
   (fresh ref, fresh nonce, ref bound as AAD), swaps
   `provider_connections.encrypted_api_key_ref`, and bumps the row's
   `updated_at`.
3. CLI immediately follows with `POST .../test` to probe the upstream
   provider with the new key.
4. Exit `0` if rotation + test both succeed; `1` if rotation succeeds
   but the upstream probe is invalid (rare — usually means the new
   key hasn't propagated yet, or the operator pasted the wrong value).

`--skip-test` bypasses the post-rotation probe — useful when the
upstream provider takes minutes to propagate a freshly-minted key
(some hosted-OpenAI-compatibles have noticeable lag). The rotation
itself still completes; the operator re-runs
`loom providers test <name>` after the propagation delay.

The OLD encrypted ciphertext is NOT deleted by this command. It stays
in the `secrets` table — decryptable but no longer pointed at by any
connection — until Phase 5's cleanup job sweeps revoked refs older
than the cache TTL. This is intentional: in-flight gateway requests
that loaded the old ref before rotation can still complete.

Gateway-side: there is no in-memory cache for provider-connection rows
(the gateway looks up by id per-request — see
`src/loom_llm_gateway/routes/_facade_common.py:_lookup_provider_connection`).
The new key takes effect on the very next gateway call. No cache
invalidation step is needed.

`loom providers update --api-key SOURCE` does the same rotation
without the post-test step; `rotate-key` is the runbook-friendly verb
for the "swap + verify" flow.

#### Repairing legacy/malformed stored API-key refs

A provider connection's `encrypted_api_key_ref` must be a runtime-
supported encrypted reference of the form `loom://<namespace>/<uuid>`
(or `k8s://<namespace>/<name>` for the k8s secret store). Rows
created by the current `POST /api/v1/provider-connections` path
always meet this shape. Legacy/imported rows may carry an argv-style
string like `env:STAGING_SMOKE_OPENAI` instead; the gateway
cannot decrypt these and every call fails.

Symptoms:

- Gateway returns HTTP 502 with detail containing
  `kind=malformed_ref` (instead of an unhandled 500 traceback). The
  message tells the operator to run `loom providers rotate-key`.
- `POST /api/v1/provider-connections/<id>/test` returns HTTP 503 and
  side-effects the row: `status='invalid'`, `last_validation_error`
  starts with `malformed_ref:`, `last_validated_at` is set. After
  this, the SPA / `loom providers list` / `loom providers test` show
  the row as `invalid` instead of silently `valid`.

Fix:

```bash
loom providers rotate-key <connection-name> --api-key env:NEW_KEY
```

The rotate path writes a fresh `loom://...` ref via
`SecretStore.put`, then re-probes the upstream. After it succeeds
the row's status returns to `valid`.

### Secret-store master-key rotation — `loom admin secret-store rewrap`

All provider-connection API keys are AES-GCM encrypted at rest using
`LOOM_SECRET_STORE_MASTER_KEY` (or `LOOM_SECRET_STORE_MASTER_KEYS` plural
during rotation). When you need to rotate the master key (compromise,
scheduled rotation, compliance requirement), use the online 3-step
protocol — no downtime:

**Step 1 — generate + deploy new key as FALLBACK**

```bash
# Generate a new key and see the kubectl commands:
loom admin secret-store rewrap --generate-new-key
```

This prints a fresh base64-encoded 32-byte key and the exact `kubectl
patch` command to deploy it. Follow the printed instructions:

```bash
# Read the current primary key:
OLD_KEY=$(kubectl get secret loom-secrets \
  -o jsonpath='{.data.secret-store-master-key}' | base64 -d)

# OR if already using plural form:
OLD_KEY=$(kubectl get secret loom-secrets \
  -o jsonpath='{.data.secret-store-master-keys}' | base64 -d | cut -d, -f1)

# Deploy new key as PRIMARY, old as FALLBACK:
kubectl patch secret loom-secrets \
  -p "{\"stringData\":{\"secret-store-master-keys\":\"$NEW_KEY,$OLD_KEY\"},
       \"data\":{\"secret-store-master-key\":null}}"
kubectl rollout restart deploy/loom-service
kubectl rollout status deploy/loom-service
```

After this deploy, new secrets encrypt with the NEW key; existing secrets
are still readable via the fallback. `loom-service` and
`loom-llm-gateway` validate existing `secrets` rows during startup; if
the deployed key set cannot decrypt a stored row, the process fails fast
with a SecretStore startup validation error instead of serving provider
requests that later fail with HTTP 500.

**Step 2 — run the rewrap walk**

```bash
loom auth login --server https://loom.example.com --token env:LOOM_ADMIN_TOKEN
loom admin secret-store rewrap --admin-actor <your-name>
```

This calls `POST /api/v1/admin/secret-store/rewrap` which walks every row
in the `secrets` table and re-encrypts each one with the PRIMARY key. The
walk never short-circuits — all rows are attempted. A summary of
successes and failures is printed.

On success:
```
Rewrapped N secret(s).

All secrets now use the primary key.
Next step: drop the fallback key from loom-secrets and restart loom-service:
  ...
```

On partial failure: the endpoint returns HTTP 207 with a `failed` list.
Investigate the per-ref errors (typically tampered ciphertext or corrupt
rows) and re-run after fixing them.

**Step 3 — drop the old key**

Once the walk completes with zero failures, no rows use the old key.
Remove the fallback and switch back to the singular env var:

```bash
kubectl patch secret loom-secrets \
  -p "{\"stringData\":{\"secret-store-master-keys\":null,
       \"secret-store-master-key\":\"$NEW_KEY\"}}"
kubectl rollout restart deploy/loom-service
kubectl rollout status deploy/loom-service
```

Rotation is complete. Verify with `loom providers test <name>` to
confirm provider connections are still readable.

If provider validation or model discovery returns HTTP 503 with
`stored provider secret cannot be decrypted or read`, treat it as a
SecretStore key/configuration problem rather than an upstream provider
outage. Restore the correct `LOOM_SECRET_STORE_MASTER_KEY`, deploy the
old key as a fallback in `LOOM_SECRET_STORE_MASTER_KEYS` during rotation,
or rotate/re-enter the provider API key. Do not debug the provider's
`/models` endpoint until `loom providers test <name>` can decrypt the
stored key.

If `loom-service` or `loom-llm-gateway` refuses to start with
`SecretStore startup validation failed`, use the same recovery path:
restore the key that encrypted the row, configure that key as a fallback,
or re-enter/rotate the affected provider key after loading the correct
old key. Empty databases skip this validation and do not require a
secret-store master key until a provider secret is created.

**Audit trail**

Each call to the rewrap endpoint writes one `secret_store.rewrap` event
to `admin_audit_events`. View with:
```bash
curl -s https://loom.example.com/api/v1/admin/audit-events \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq '.items[] | select(.action=="secret_store.rewrap")'
```

**Emergency / scripted use**

For disaster recovery (can't redeploy first), supply the new key directly:
```bash
loom admin secret-store rewrap \
  --new-key "$(base64 -w0 /path/to/new-key.bin)" \
  --admin-actor your-name
```

This tells the server to re-encrypt to THAT key even if it isn't the
deployed primary. Use with caution — after this, only that key can
decrypt the rewrapped rows.

## Provider connection cost attribution

BYO provider connections default conservatively:

- `anthropic` and `google` default to `pricing_source='rate-card'` with
  matching rate-card provider namespaces.
- `openai-compatible` and `custom` default to token-only accounting.

For hosted OpenAI-compatible services such as Together or Fireworks, set
the rate-card namespace explicitly when registering or updating the
connection:

```bash
loom providers create \
  --name together-prod \
  --type openai-compatible \
  --base-url https://api.together.xyz/v1 \
  --api-key env:TOGETHER_API_KEY \
  --rate-card-provider together

loom providers update together-prod \
  --pricing-source rate-card \
  --rate-card-provider together
```

### Sharing a provider connection across teams

Use provider sharing when one existing connection should serve another team
without cloning the encrypted secret or asking an operator to read/paste the
raw API key:

```bash
loom providers share yibuapi-prod --target-team-id "$TARGET_TEAM_ID"
loom providers unshare yibuapi-prod --target-team-id "$TARGET_TEAM_ID"
```

Owner-team tokens with `providers:manage` can share their own provider. A
singleton admin credential must include `--admin-actor` so the service can
write an audit event. The target team can list, select, and submit with the
shared provider, but cannot rotate, update, test, refresh/hide/unhide models,
or delete the owner connection. Audit metadata records the provider id/name,
owner team, target team, actor, and action only; do not include provider API
keys or secret refs in issue comments or evidence.

Runtime LLM calls use the same share boundary. The gateway facade accepts a
provider connection when the step token's team owns it or has an explicit
`provider_connection_shares` row. The gateway still decrypts only the
owner-side secret; `llm_calls`, usage, and cost views stay attributed to the
consuming team/user represented by the trial rather than to the provider owner.

For shared-provider spend review, filter usage by the single
`provider_connection_id` and break down by consuming team or user:

```bash
loom eval usage \
  --start YYYY-MM-DD \
  --end YYYY-MM-DD \
  --provider-connection-id "$PROVIDER_CONNECTION_ID" \
  --breakdown-by team

loom eval usage \
  --start YYYY-MM-DD \
  --end YYYY-MM-DD \
  --provider-connection-id "$PROVIDER_CONNECTION_ID" \
  --team-id "$TARGET_TEAM_ID" \
  --breakdown-by user
```

Admin-on-behalf submissions keep the represented team/user on the batch for
product ownership, but usage and billing attribution follows the real acting
admin/user when that identity is available. Singleton break-glass admin calls
fall back to the required audit actor string. The represented team/user keeps
normal owner permissions for monitor/detail/debug/rerun/cancel and artifact
access; the admin actor is not the product owner merely because it submitted
the run.

Run the `--pricing-source rate-card` update only after the Gateway's
`rate_cards` table has matching `(provider, model)` entries. Facade calls
with a missing entry still record tokens and use
`rate_card_hash='facade:rate-card:missing'` with `cost_usd=0`, but the
service projection reports `estimated_cost_usd=null`,
`cost_status=price_unknown`, `cost_estimate_source=unpriced`, and
`cost_estimate_confidence=unavailable`. Do not treat this as free usage:
either sync/add the rate-card row or switch the connection to
operator-supplied pricing with `input_usd_per_1m` and
`output_usd_per_1m` in `pricing_data`.

Batch submissions may include `budget_usd` with `budget_policy=hard` or
`soft`. Hard budgets reject pre-run estimates above the cap and cancel
running batches once recorded provider usage exceeds the cap; the
cancel diagnostic is stored in `batches.budget_diagnostics` with reason
`budget_hard_limit_exceeded`. Soft budgets return a confirmation error
when the pre-run estimate is above the cap or unknown/unpriced unless
the client resubmits with `budget_confirmed=true`. After supplemental reruns,
audit the combined spend with
`loom eval usage --batch-id <main-batch-id> --include-batch-family
--include-batches` so the original and linked rerun child batches are treated
as one production budget family.

## BYO provider model selection

Provider discovery is cached per connection:

```bash
loom providers models lab-vllm --refresh
loom providers models lab-vllm --preflight MODEL_ID
loom providers models lab-vllm
```

Discovery and preflight are separate:

- Refresh reads the upstream `/models` catalog and updates which ids Loom can
  offer in the picker.
- Preflight sends one minimal generation request for one model id and stores
  `last_preflight_status`, HTTP status, and a redacted error code/message on
  that cached row.
- Connection test, refresh, and preflight re-resolve the stored provider
  `base_url` before sending a service-side request. If the current DNS/IP result
  violates the team egress policy, the operation is blocked before upstream
  contact; model preflight records `egress-policy-rejected`.
- Batch creation rejects provider model ids that are absent from the connection's
  cached model catalog. Run refresh, add the model manually, or choose a cached
  model before submitting.
- Untested rows remain selectable. Rows with a known failed preflight show a
  warning in New Batch, and `POST /api/v1/batches` rejects that
  provider/model pair until it passes preflight or the user chooses another
  model.

The service launch catalog at `GET /api/v1/models` defaults to
agent-capable models only. Raw provider entries are still available with
`GET /api/v1/models?view=raw`; suppressed entries include
`hidden_reason` values such as `classifier-non-llm` so operators can
debug noisy OpenAI-compatible catalogs.

Before submitting an agent/provider smoke matrix, generate the repo-side
compatibility plan:

```bash
loom qa matrix \
  --compatibility-plan \
  --output provider-harness-compatibility.md \
  --json-output provider-harness-compatibility.json
```

This is a no-login, no-provider-call planning command. It emits per-agent cells
for every default displayed service-mode-ready harness and every tracked
provider endpoint type, including generic `supported_providers=["*"]` agents
such as `litellm` and `opencode`. `blocked` and `skipped` cells include the
pre-submit reason; `supported` cells keep live-smoke fields as
`pending_live_smoke` until authorized low-cost provider validation is merged
from a sanitized `--compatibility-evidence` JSON file. Evidence files must use
safe references such as `env:PROVIDER_API_KEY`; the CLI rejects raw-looking
bearer tokens, provider keys, and signed URLs before writing Markdown or JSON.

Before spending provider calls on the full #35 agent x ready-benchmark matrix,
turn the catalog snapshot plus the compatibility-plan JSON into a deterministic
pre-submit plan:

```bash
loom qa matrix \
  --preflight-plan \
  --catalog-snapshot qa-catalog-snapshot.json \
  --provider-compatibility-plan provider-harness-compatibility.json \
  --output agent-benchmark-preflight-plan.md \
  --json-output agent-benchmark-preflight-plan.json
```

This command is also login-free and does not call `/api/v1/*`, contact model
providers, submit batches, read artifact storage, or require live secrets. The
catalog snapshot must contain `agents.items[]` shaped like `GET /api/v1/agents`
and `benchmarks.items[]` shaped like `GET /api/v1/benchmarks`, plus offline
evidence needed to pick one representative task per benchmark:
`representative_task_id` or `tasks[]`, license evidence such as `license_spdx`,
capability evidence such as `capability_evidence`, and architecture evidence
such as `architecture_evidence` or `supported_architectures`.

The output marks each agent x benchmark x provider-endpoint row as
`planned_submit`, `blocked`, or `skipped`. Provider-family mismatches are
consumed from the #114 compatibility plan; no-model agents are represented once
per benchmark with provider endpoint `no-model` and `agent_model=null`. Cells
with supported metadata but missing live #114 smoke evidence remain `blocked`
with `pending_live_evidence`.

The pre-submit plan is an operator planning artifact only. It does not satisfy
live #35 acceptance; #35 still requires terminal live trial evidence or a
pre-submit skipped/blocked reason for every displayed ready agent x ready
benchmark cell. Both the catalog snapshot and compatibility plan are scanned
before rendering, and raw-looking bearer tokens, provider keys, and signed URLs
are rejected rather than redacted into Markdown or JSON.

For self-hosted endpoints that do not implement useful discovery, add
the model id manually through the provider model API:

```bash
curl -X POST \
  "https://loom.example.com/api/v1/provider-connections/$CONNECTION_ID/models" \
  -H "Authorization: Bearer $TEAM_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"model_id":"my-vllm-checkpoint"}'
```

Manual entries are tied to the provider connection and remain visible
after refreshes even when upstream `/models` omits them. The SPA's
normal launch flow stores the selected provider connection and concrete
model id on each agent/model combination. Batch-level provider fields
remain a compatibility default, but new multi-combination submissions can
mix provider connections and provider models in one batch; the runner
persists each trial's effective provider route.

## GPU-cluster checkpoint provider onboarding

For Lux-like clusters, prefer the user-facing bundle generator documented in
[`provider-onboarding.md`](../integrations/provider-onboarding.md#gpu-cluster-checkpoint):

```bash
loom inference deploy slurm \
  --model Qwen/Qwen2.5-Coder-7B-Instruct \
  --served-model-name qwen2.5-coder-7b-instruct \
  --partition compute \
  --gres gpu:h100:1 \
  --venv /pm/qy/uv_envs/vcbm \
  --expose user-provided \
  --endpoint-url http://bastion.example.com:18001/v1 \
  --output-dir ~/loom-inference/lux-qwen25 \
  --no-submit
```

The generated bundle stores the provider API key in an owner-only file, starts
vLLM through a launcher that avoids putting the key in long-lived process argv,
and emits non-secret Loom registration fields. Operator validation should cover
`./submit.sh`, the network exposure path, `./healthcheck.sh`,
`./register-provider.sh`, `loom providers test NAME`,
`loom providers models NAME --refresh`, and one small model-backed batch.

Loom does not need SSH access to the GPU cluster for normal calls; it only needs
HTTP reachability to the registered `/v1` endpoint. SSH, Slurm credentials, or
cluster-specific submit rights are only needed when a user or future Loom
automation is launching or restarting the inference service.

## BYO provider egress allowlist

Cluster NetworkPolicies keep `loom-service`, `loom-llm-gateway`, and the
optional `loom-egress-proxy` on a default-deny boundary. Hosted providers on
public `443` or `80` work with the default service and gateway policies: the
service policy covers provider validation, model discovery, and preflight, and
the gateway policy covers runtime model calls. GPU-cluster bastion forwards
often use ports such as `18001`. Approve those endpoints in
`cluster-config.toml`; do not patch live NetworkPolicies by hand.

Use IP/CIDR entries, not DNS names. Kubernetes NetworkPolicy enforces CIDRs,
so the operator must resolve and approve the exact address or CIDR before
rendering:

```toml
# cluster-config.toml
provider_egress_allowlist = [
  "202.78.161.51:18001",
]
```

Then render, audit, and redeploy from the same config. For shared staging,
commit the allowlist, merge it to `dev`, and use `loom-staging-rollout start`;
do not run the direct apply command below. The example is for a custom cluster:

```bash
loom cluster render --config cluster-config.toml > /tmp/loom-rendered.yaml
grep -n "202.78.161.51/32" /tmp/loom-rendered.yaml
loom cluster audit --config cluster-config.toml
loom cluster up --config cluster-config.toml
```

The render adds exact `ipBlock + TCP port` rules to:

- `loom-service`, so `loom providers test NAME` and model discovery can probe
  the connection and model preflight can run.
- `loom-llm-gateway`, so runtime model calls can reach the approved endpoint.
- `loom-egress-proxy`, so the same endpoint works when Envoy egress proxy mode
  is enabled.

Do not allow `0.0.0.0/0`, loopback, link-local, metadata, multicast, or empty
entries. The renderer rejects those ranges and rejects hostnames with a clear
error. Private RFC1918 ranges are allowed only when an operator deliberately
lists them, which keeps the approval decision visible in config review.

Smoke after deploy with the team that owns the provider connection:

```bash
loom providers test lux-qwen25-coder-7b
loom providers models lux-qwen25-coder-7b --refresh
loom providers models lux-qwen25-coder-7b
```

Then submit one small model-backed batch through the SPA or CLI. If provider
test still times out, check both sides of the path: the bastion/forwarding
process and the rendered NetworkPolicy in the live cluster:

```bash
kubectl get networkpolicy -n loom loom-service loom-llm-gateway loom-egress-proxy -o yaml
kubectl logs -n loom -l app=loom-service --since=10m
kubectl logs -n loom -l app=loom-llm-gateway --since=10m
```

## Observability: dashboards

Five Grafana dashboards ship in `deploy/grafana/dashboards/`. When you run
`loom cluster render` (and then `loom cluster up`), they are bundled into a
`ConfigMap` (`loom-grafana-dashboards`) with the `grafana_dashboard: "1"` label
so the **kube-prometheus-stack** Grafana sidecar auto-discovers and imports them.

If you're not using kube-prometheus-stack, import each JSON file manually:
**Grafana UI → Dashboards → Import → upload JSON file**.

| Dashboard | File | What it answers |
|---|---|---|
| Operator Overview | `deploy/grafana/dashboards/operator-overview.json` | Is anything broken? Service-up, queue, workers, latency, error rates, top-5 cost |
| Control Plane | `deploy/grafana/dashboards/control-plane.json` | Scheduling deep dive: transitions, queue depth, claim latency, state-PATCH timeouts |
| LLM Gateway | `deploy/grafana/dashboards/llm-gateway.json` | Provider call rates, latency, error rates, cost by team |
| Loom Service | `deploy/grafana/dashboards/loom-service.json` | HTTP request rate/latency, batch runner, token issuance |
| Worker Fleet | `deploy/grafana/dashboards/worker.json` | Trial throughput, duration, failure rates, heartbeats |

For the on-call alert → dashboard mapping, see
[`docs/architecture/observability.md`](../architecture/observability.md).

## Production alerts

`deploy/k8s/prometheus-rules.yaml` ships a `PrometheusRule` resource
covering the Control Plane, LLM Gateway, loom-service, and worker
metrics emitted by `src/loom_control_plane/metrics.py`,
`src/loom_llm_gateway/metrics.py`, `src/loom_service/metrics.py`, and
`src/loom_worker/metrics.py`.

Apply with:
```bash
kubectl apply -f deploy/k8s/prometheus-rules.yaml
```

Requires `prometheus-operator` (the `PrometheusRule` CRD) or
`kube-prometheus-stack`. If you ship Prometheus differently, copy
the `groups` block into your `prometheus.yml`.

| Alert | Severity | Threshold | What it means | First triage |
|---|---|---|---|---|
| `LoomControlPlaneDown` | **critical** | `up{job=~".*loom-control-plane.*"} == 0` for 5m | Prometheus can't scrape CP. Every other alert below is silently inert. | `kubectl get pods -n loom -l app=loom-control-plane`; verify ServiceMonitor selectors. |
| `LoomNoWorkersActive` | **critical** | `loom_workers_active == 0` for 2m | Every queued trial is blocked. | `kubectl logs -n loom -l app=loom-worker --tail=200 --previous`. CrashLoopBackOff → check `worker-token` in `loom-secrets` + CP / Postgres reachability. |
| `LoomQueueBacklog` | warning | `sum(loom_queue_depth) > 100` for 10m | Trials arriving faster than workers claim them. | Inspect per-team breakdown `sum by (team_id) (loom_queue_depth)`; scale workers or DRF-tune Postgres. |
| `LoomTrialsStuckClaimed` | warning | `loom_trials_inflight{state="claimed"} > 5` per team for 15m | Trials entered `claimed` but never `running` — worker crashed mid-spawn or agent runtime image is broken. | `kubectl logs -n loom -l app=loom-worker --since=20m \| grep -i "claim\|spawn\|runtime"`; check for recent image push. |
| `LoomWorkerReclaimsSpiking` | warning | `rate(loom_worker_reclaim_total[5m]) > 0.5` for 10m | Crash detector is reclaiming > 30 trials/min. Workers are dying mid-trial. | `kubectl describe pod -n loom -l app=loom-worker \| grep -A2 "Last State\|OOMKilled\|Reason"`. |
| `LoomStatePatchTimeouts` | warning | `rate(loom_state_patch_total{result="timeout"}[5m]) > 0` for 5m | Fenced state-PATCH is timing out; workers retry, eventually the crash detector reclaims. | `kubectl exec deploy/loom-control-plane -- pg_isready -h loom-postgres`; check Postgres connections + active queries; consider rolling back recent CP image. |
| `LoomClaimLatencyP95High` | warning | `histogram_quantile(0.95, sum by (le) (rate(loom_claim_latency_sec_bucket[5m]))) > 1.0` for 15m | DRF claim scan p95 > 1 second. Workers spinning instead of running. | Verify `trials(state, submitted_at)` index is hot; check `pg_stat_statements` for the claim query. |
| Slurm worker capacity low | warning | No default alert; inspect `loom_slurm_worker_desired_slots`, `loom_slurm_worker_active_slots`, `loom_slurm_worker_pending_slots`, and `loom_slurm_worker_stale_slots` by environment/pool. | Elastic Slurm jobs were requested but are pending, stale, failed, cancelled, or exited idle. | `loom admin slurm-workers status --cp-url <private-cp-url>` and `loom resources status`; inspect pending reasons, stale records, failed submissions, and `squeue`/`sacct` for the recorded job ids. |
| `LoomLLMGatewayDown` | **critical** | `up{job=~".*loom-llm-gateway.*"} == 0` for 5m | Prometheus cannot scrape Gateway, so provider-call metrics are blind. | `kubectl get pods -n loom -l app=loom-llm-gateway`; `kubectl logs -n loom -l app=loom-llm-gateway --tail=200`. |
| `LoomGatewayProviderErrorRate` | warning | provider-level `loom_gateway_llm_calls_total{result!="ok"}` ratio > 5% for 10m | A provider is failing calls; common causes are expired keys, provider outage, SSRF/egress policy, or dialect drift. | `kubectl logs -n loom -l app=loom-llm-gateway --since=15m`; run `loom providers test <connection-name>`; check provider status. |
| `LoomGatewayCostSpike` | warning | `increase(loom_gateway_cost_usd_total[30m]) > 10` per team for 10m | A team accumulated more than $10 provider-attributed Gateway cost in 30 minutes. This is an alert only, not quota enforcement. | Inspect Gateway cost dashboard by `team_id`; check recent batches and provider configuration; disable the team or rotate provider secrets if spend is unintended. |
| `LoomServiceDown` | **critical** | `up{job=~".*loom-service.*"} == 0` for 5m | Prometheus cannot scrape the public API service. | `kubectl get pods -n loom -l app=loom-service`; `kubectl describe svc -n loom loom-service`; verify ServiceMonitor selectors. |
| `LoomServiceHighErrorRate` | warning | `loom_svc_http_requests_total{status_class="5xx"}` ratio > 2% for 10m | The public API is returning elevated 5xx responses. | `kubectl logs -n loom -l app=loom-service --since=15m \| grep -i "500\|exception\|traceback"`; check Control Plane and Gateway dependencies. |
| `LoomServiceAuthFailureSpike` | warning | `sum(rate(loom_svc_auth_failures_total[5m])) > 1` for 10m | Browser/session or bearer-token failures exceed 60/min. | Inspect Service dashboard by `auth_kind` and `reason`; check ingress source concentration; revoke affected API tokens or disable the team if abusive. |
| `LoomServiceSubmissionRejectSpike` | warning | `sum(rate(loom_svc_submission_rejects_total[5m])) > 0.2` for 10m | Batch submissions are being rejected before fan-out more than 12/min. | Inspect Service dashboard rejection reasons; `no_workers` maps to worker capacity, `team_paused` maps to an operator hold, and `invalid_input`/`provider_connection` map to user or provider config. |
| `LoomWorkerProcessDown` | warning | `up{job=~".*loom-worker.*"} == 0` for 5m | A worker scrape target is silent. `LoomNoWorkersActive` remains the page for full capacity loss. | `kubectl get pod -n loom -l app=loom-worker`; `kubectl logs -n loom -l app=loom-worker --previous`. |
| `LoomWorkerHeartbeatFailing` | warning | `rate(loom_worker_heartbeat_failures_total[5m]) > 0` for 10m | Worker heartbeats to CP are failing; CP will eventually reclaim that worker's trials. | Verify worker-to-CP reachability; check `loom_worker_claim_loop_iterations_total{result="error"}` for related connectivity failures. |
| `LoomWorkerTrialFailureRateHigh` | warning | worker `loom_worker_trials_completed_total{result!="succeeded"}` ratio > 20% for 15m | Many worker-run trials are failing, cancelling, or crashing. | Inspect `sum by (result) (rate(loom_worker_trials_completed_total[5m]))`; compare recent trajectories for common failure reasons. |
| `LoomRetryExhaustedSpiking` | warning | `rate(loom_retry_exhausted_total[5m]) > 0.1` for 10m | CP's retry-exhausted sweeper is transitioning > 6 trials/min to `failed/retry_exhausted`. Indicates workloads are exhausting their retry budget or a flaky upstream is causing real failures across many trials. | Inspect `sum by (team_id, task_id) (rate(loom_trials_state_total{to_state="failed"}[15m]))`; correlate with `LoomWorkerTrialFailureRateHigh` + recent provider/sandbox failures; inspect `max_attempts` only if the workload is genuinely retry-heavy. |
| `LoomWorkerTokenStaleness` | warning | `loom_worker_tokens_stale_count > 0` for 1h | Live worker tokens are flagged as either unused for 30+ days (`reason="unused_30d"` — usually a decommissioned pool whose token was never revoked) or older than 90 days since mint (`reason="aged_90d"` — rotation overdue). Soft signal; never auto-revokes (would 401 in-flight claims). | See [Worker-token staleness](#worker-token-staleness) below. |
| `LoomMinioPVCUsageHigh` | warning | PVC `data-loom-minio-0` > 80% used for 15m | The disk backing MinIO is filling. Without intervention trial uploads will start failing with "no space left on device." | See [MinIO PVC usage](#minio-pvc-usage) below. |
| `LoomMinioPVCUsageCritical` | **critical** | PVC `data-loom-minio-0` > 95% used for 5m | Trial uploads will fail within minutes to hours. Page on-call. | See [MinIO PVC usage](#minio-pvc-usage) below. |
| `LoomPgbouncerClientWaiting` | warning | `sum(pgbouncer_pool_client_waiting_connections) > 0` for 1m | pgbouncer's backend pool is saturated — clients queueing for a free Postgres backend. Every waiting client is a request stalling in SQLAlchemy pool_pre_ping. | `kubectl exec deploy/loom-pgbouncer -c pgbouncer -- psql -h localhost -p 6432 -U loom pgbouncer -c 'SHOW POOLS'`. Bump `pgbouncer.default_pool_size` in the profile (currently 25) and re-render + apply. Verify Postgres `max_connections` (currently 150) has headroom. |
| `LoomPgbouncerScrapeDown` | **critical** | `up{job="pgbouncer-exporter"} == 0` for 3m | pgbouncer-exporter is unreachable — the whole pgbouncer path is likely down. Every service SQLAlchemy engine routes through pgbouncer. | `kubectl get pods -n loom -l app=loom-pgbouncer`; `kubectl logs -n loom -l app=loom-pgbouncer -c pgbouncer --tail=200`. Rollback path if pgbouncer is broken: set `pgbouncer.enabled=false` in the profile, re-render, apply — services fall back to `db_url` (direct-to-Postgres). |
| `LoomListenWatcherPollFallback` | warning | `loom_listen_watcher_push_mode == 0` for 2m | A LISTEN watcher failed its boot-time NOTIFY round-trip probe and degraded to poll-only mode. Push-mode latency (~100ms) has degraded to poll-interval latency (5-10s). | The watcher's DSN routes through pgbouncer transaction mode. Watcher connections MUST be direct-to-Postgres. Check that `LOOM_EGRESS_XDS_DB_URL` and `svc-db-url` in `loom-secrets` point at `loom-postgres:5432` (direct), not `loom-pgbouncer:6432`. Verify with `loom cluster doctor`. |
| `LoomMinioWriteLatencyHigh` | warning | MinIO `PutObject`/`CompleteMultipartUpload` p95 > 2s for 10m | Trial finalize is inheriting slow MinIO writes. Under single-node MinIO this is disk saturation or CPU/network throttling; under distributed MinIO it can be erasure recovery from a degraded drive/node. | `kubectl top pod -n loom -l app=loom-minio`; `mc admin info local`. If single-node and sustained, plan cutover to distributed mode (#610). If distributed, check `mc admin heal --scan` for in-progress heal ops. |
| `LoomMinioRequestErrorRateHigh` | **critical** | MinIO server-side error rate > 1% on `PutObject`/`GetObject`/`CompleteMultipartUpload` for 5m | Trial finalize is lossy or broken — every 5xx is a lost audit trail. | `kubectl logs -n loom -l app=loom-minio --tail=200 \| grep -Ei 'error\|WARN'`; `mc admin info local`. Rollback path when a distributed cutover just happened: flip `MINIO_ENDPOINT` back to the pre-cutover single-node service (kept online 24h post-cutover). |
| `LoomMinioNodeOffline` | warning | `minio_cluster_nodes_offline_total > 0` for 5m | A MinIO pod in the distributed pool is unreachable from its peers. Erasure quorum still met (pool tolerates N-parity failures) but margin is narrowed. | `kubectl get pods -n loom -l app=loom-minio`; `kubectl describe pod` for the offline peer. If persistent, inspect logs for peer-discovery failures (headless service DNS, NetworkPolicy). |

Thresholds are starting points — tune per team's trial volume +
workload shape. Halve the `for:` durations for staging.

## Alarm response (troubleshooting matrix)

| Symptom | Likely cause | First check |
|---|---|---|
| `loom_trials_inflight{state="claimed"}` rising, no `running` | Workers can't reach gateway/MinIO, task setup is hanging before `started_at`, or stale claimed rows are waiting for crash-detector reclaim | `kubectl logs deploy/loom-worker --tail=200` for connect/setup errors; inspect batch debug evidence for `claimed_without_started`; confirm `claimed_without_start_expiry_sec` is above normal setup latency |
| `loom_worker_reclaim_total` spiking | Workers crashing or heartbeat thread blocked | Check worker memory pressure + `state_patch_error` log lines |
| 502 from Control Plane | Postgres unreachable | `kubectl exec deploy/loom-control-plane -- pg_isready -h loom-postgres` |
| Trials stuck queued | No worker matches `requires_caps` | Inspect `trials.requires_caps` vs registered `workers.capabilities`, including `cpu_arch`; legacy missing `cpu_arch` means x86_64-only |
| Batch finishes `all_failed` with zero child trials | Batch fan-out was rejected by deterministic submission policy/config checks after preview-time checks were bypassed or state changed | Open Batch Detail or `loom eval batch show <id>` and inspect `failure_reason`, `failure_message`, and `fanout_errors`; update the task config, provider/backend selection, permissions, or catalog state before retrying |
| `POST /api/v1/batches` returns 400 `agent×task capability mismatch` | Selected agent's `requires_capabilities` (from `/api/v1/agents`) isn't satisfied by every task in the filter — e.g. `oracle` against a task that doesn't ship `solution/solve.sh`; mixed adapters such as TB2.1 must publish an explicit `oracle_eligible=true` tag, and no task-id prefix grants capability | Resubmit with a compatible tagged task slate (drop the listed incompatible tasks), or drop the incompatible agent from `combinations` |
| Worker logs `state_patch_error`, CP returns 400 requiring `result`, or DB rejects with `trials_succeeded_has_result` | A writeback path patched `state='succeeded'` before persisting/providing `result` (#416 Slice 4). CP should reject this as a clear 400 before the database constraint is reached; the constraint still blocks direct inconsistent writes | Inspect `select id, state, result, finished_at from trials where state='succeeded' and result is null` — should be empty post-#416. If non-empty, audit recent worker code for an out-of-order writeback |
| 429 from Gateway | Provider rate limit | Check `loom_llm_calls_total{provider,result}` panel |
| Trajectory or artifact uploads failing | MinIO credentials wrong or runtime bucket bootstrap failed | `kubectl logs deploy/loom-worker --tail=200` for `ensure_bucket`, `trajectory_flush_failed`, or `artifact_upload_failed`; verify `mc ls loom-minio/trajectories` and `mc ls loom-minio/artifacts` |

## Trial state/result consistency (#416 Slice 4)

Migration `0039_trials_succeeded_has_result.py` ships a `CHECK
(state != 'succeeded' OR result IS NOT NULL)` constraint as
`NOT VALID` — new writes are blocked, but pre-existing legacy rows
(present in staging DBs from before #416 ships) are not
re-checked at apply time. Apply the migration and inspect the
NOTICE for the violation count; the count is also surfaced by:

```sql
SELECT count(*) FROM trials
 WHERE state = 'succeeded' AND result IS NULL;
```

Cleanup options for legacy violations:

- **Reclaim and re-run** (preferred when the batch is still useful):
  ```sql
  UPDATE trials SET state = 'queued', worker_id = NULL
   WHERE state = 'succeeded' AND result IS NULL;
  ```
  The fan-out idempotency key protects against duplicate child trials;
  the next sweep picks the row up and re-attempts on a healthy worker.

- **Mark as failed** (when the trial is unrecoverable, e.g. its
  batch is already terminal or the trajectory artifacts are gone):
  ```sql
  UPDATE trials
     SET state = 'failed',
         failure_reason = 'missing_result',
         failure_message = '#416 backfill: state=succeeded with NULL result'
   WHERE state = 'succeeded' AND result IS NULL;
  ```

After legacy rows are cleaned up, a follow-up `VALIDATE` migration
(filed separately when an operator confirms the cleanup) runs the
full back-check and locks the invariant for the entire table.

## Worker/gateway rolling restart (#416 Slices 1 + 3)

The platform tolerates a rolling worker or gateway restart without
producing platform-failed trials for in-flight work, subject to the
following bounds:

- **Worker drain window:** `drain_timeout_sec` (default 600s). The
  `SIGTERM` handler stops the claim loop, waits up to this for
  in-flight trials to finish, then cancels remaining trials. Set the
  k8s deployment's `terminationGracePeriodSeconds >= drain_timeout_sec`
  or trials will be `SIGKILL`'d mid-finalize.
- **Gateway rolling restart:** the per-request retry budget is
  `llm_retry_budget_sec` (default 60s post-#416 Slice 3), with up to
  `llm_retry_max_attempts=5` attempts and an `llm_retry_max_backoff_sec=8`
  ceiling on each. This covers a typical k8s `maxUnavailable=1,
  maxSurge=1` rollout on a 2-replica gateway (~10-30s of transient
  502s). Subprocess agents also retry text-only transport disconnects such as
  "server disconnected without sending a response"; longer outages exhaust
  the budget and the trial fails with `gateway_error` or
  `provider_transport_disconnect`.
- **Orphan trajectory cleanup** (post-#416 Slice 1) no longer deletes
  JSONLs for trials whose CP state is still `running`/`claimed`/`queued`,
  regardless of `worker_id`. The reclaim sweep gets the trial back to
  a healthy worker and the JSONL serves as the forensic record for
  the prior attempt.
- **Claimed without start:** the crash detector also requeues stale
  `claimed` trials with `started_at IS NULL` after
  `claimed_without_start_expiry_sec` (default 3600s), even if the worker
  heartbeat is fresh. Upgraded workers refresh `pre_start_heartbeat_at` during
  setup/materialization/task-image/layered-cache work, so legitimate local
  pre-start queues age from recent progress instead of the original
  `claimed_at`. Rows with no pre-start heartbeat still age from `claimed_at`
  for abandoned-claim recovery and compatibility with older workers. Reclaim
  records `worker_lost_claim` plus a `claimed_without_started_reclaimed`
  diagnostic before clearing `worker_id`; a subsequent claim clears stale
  failure fields, while terminal retry exhaustion preserves the last reclaim
  message. During large cold-start batches, inspect `loom resources status
  --json` for `pre_start_heartbeat_fresh_tasks` and
  `oldest_starting_task_age_sec` before raising the TTL further.

**Operator smoke for the in-flight-trial-across-restart path:**

1. Submit a long-running batch (e.g. an aime sweep, takes ~30s per trial).
2. While trials are running, restart the worker pod:
   `kubectl rollout restart deploy/loom-worker -n loom-prod`.
3. Watch the CP `crash_detector_reclaimed` log line — should report a
   count matching trials in-flight at SIGTERM time, within
   `worker_heartbeat_expiry_sec + worker_reclaim_sweep_interval_sec`
   (default 120+30=150s).
4. Confirm no trials enter terminal state with `failure_reason IN
   ('trajectory_flush_failed', 'gateway_error',
   'provider_transport_disconnect')`. Reclaimed trials
   should complete normally on the new worker.
5. Inspect the new worker's startup logs for
   `orphan_trajectory_preserved` (the slice-1 happy path) rather
   than `orphan_trajectory_deleted` for the reclaimed trial ids.

## Rollout evidence path setup (#174)

Staging environments back host-mounted service data (Postgres,
MinIO, backups, trajectories) under `/data/<environment-name>/`. That root
directory is `root:root 755` so the operator user cannot create new siblings
without sudo. Operator rollout/evidence workflows expect a stable set of
writable directories (`rollouts/`, `evidence/`, `logs/`) at that same level.

**Rule of thumb:** service data directories under `/data/<environment>/` stay
locked down to their service accounts; operator evidence directories are
owned by the human operator user with mode `755`. The
`bootstrap-evidence-paths` command emits the exact `sudo install -d` sequence
that establishes the operator-writable set idempotently:

```bash
loom cluster bootstrap-evidence-paths \
  --rollout-root /data/loom-staging \
  --operator-user qianyi
```

By default this emits `install -d` for `rollouts`, `evidence`, and `logs`.
Override with `--evidence-paths rollouts,extra` when a workflow needs a
different set. The command refuses to bootstrap directories that collide
with reserved service names (`backups`, `migrations`, `minio`, `postgres`,
`trajectories`); those already have specific ownership defined by the
storage-migration runbook and must not be widened by an evidence bootstrap.

Review the emitted script before running it:

```bash
loom cluster bootstrap-evidence-paths \
  --rollout-root /data/loom-staging \
  --operator-user qianyi \
  > /tmp/bootstrap-evidence.sh
$EDITOR /tmp/bootstrap-evidence.sh
sudo bash /tmp/bootstrap-evidence.sh
```

`install -d` is idempotent — rerunning after a partial setup converges without
deleting anything. Once the directories exist with operator ownership, all
subsequent rollout evidence dirs (per-SHA subdirectories under
`rollouts/`) can be created without sudo.

## Backup + restore

For normal shared-staging rollout, the broker performs the component backup,
publishes the immutable manifest, and binds it into the private request
envelope before mutation. Operators do not run the manual backup/apply commands
below to start or resume shared staging. Broker unavailability does not grant
authority to use those commands: repair or reinstall the root-owned service
from clean, merged `dev`, then resume the original request and private envelope.
The procedures below remain only for initial storage bootstrap or separately
authorized production/custom recovery; they are not a shared-staging rollout or
resume path.

Staging and production are protected environments. Before any operation that
can destroy or orphan cluster state, create a fresh backup bundle and metadata
manifest. The first-phase guard is intentionally conservative: `loom cluster
down --with-volumes` or `--delete-namespace` refuses to run against
`staging` or `production` unless a recent verified manifest is
provided and the operator passes an explicit acknowledgement.

Backup bundle layout:

```bash
export ENVIRONMENT=staging
export NAMESPACE=loom-staging
export BACKUP_ROOT=/data/loom-staging/backups/$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$BACKUP_ROOT"/{postgres,minio,secrets}
```

Protected embedded deployments should also use host-managed static PVs instead
of the kind/local-path default. Keep the data root outside the kind node's
Docker volume boundary:

```toml
namespace = "loom-staging"
persistent_storage_backend = "static-host-path"
persistent_storage_host_path_root = "/data/loom-staging"
```

The repo-owned staging and production profiles declare this boundary directly:
`deploy/environments/staging.cluster.toml` uses `/data/loom-staging`, and
`deploy/environments/production.cluster.toml` uses `/data/loom-prod`.

For kind-backed protected environments, the kind control-plane node must also
bind the host data root into the node. A static hostPath PV under `/data/...`
without this mount still lives inside the node container's filesystem and is
not a durable host boundary. `loom cluster preflight --context kind-...`
fails `kind-host-storage-mount` when the Docker inspect mount list does not
show a bind mount covering `/data` or the exact environment root:

```yaml
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: /data/loom-staging
        containerPath: /data/loom-staging
```

Create the host directories before first apply:

```bash
install -d -m 700 \
  /data/loom-staging/postgres \
  /data/loom-staging/minio \
  /data/loom-staging/trajectories \
  /data/loom-staging/backups
```

For an existing staging namespace that already has local-path PVCs,
do not assume changing `cluster-config.toml` is enough: StatefulSet
`volumeClaimTemplates` are effectively immutable. `loom cluster up` ignores
Kubernetes-populated binding/default fields on existing templates
(`volumeName`, empty `storageClassName`, and default `volumeMode`) so routine
rollouts do not try to patch protected Postgres/MinIO storage. It still fails
closed if the rendered storage intent changes, for example claim name, access
modes, storage size, selector, service name, or pod management policy. For
those real changes, take a fresh backup, pause writers, and treat the move to
static PVs as a restore or data-copy operation before deleting old PVCs/PVs.

- **Postgres:** run a standard `pg_dump` of the `loom` DB into
  `$BACKUP_ROOT/postgres/loom.dump`. The dump includes DB-backed worker-pool
  desired state, environment state, catalog rows, users, teams, provider
  connections, batches, trials, cost records, and token metadata.
- **MinIO:** mirror the `trajectories` and `artifacts` buckets into
  `$BACKUP_ROOT/minio/` with `mc mirror` or the environment's equivalent
  object-store copy command. Workers recreate buckets idempotently, but object
  contents are release evidence and must be restorable.
- **Secrets:** back up Kubernetes/runtime secrets needed for restore into
  `$BACKUP_ROOT/secrets/`, especially `loom-secrets`, `loom-admin-secret`,
  TLS secrets, and the secret-store master key. Keep this directory owner-only
  and do not paste its contents into issues, logs, or PRs.

After creating the component backups, write the metadata-only manifest:

```bash
loom cluster backup manifest \
  --environment "$ENVIRONMENT" \
  --namespace "$NAMESPACE" \
  --postgres-dump "$BACKUP_ROOT/postgres/loom.dump" \
  --minio-snapshot "$BACKUP_ROOT/minio" \
  --k8s-secrets "$BACKUP_ROOT/secrets" \
  --output "$BACKUP_ROOT/backup-manifest.json"

loom cluster backup check \
  --environment "$ENVIRONMENT" \
  --namespace "$NAMESPACE" \
  --manifest "$BACKUP_ROOT/backup-manifest.json" \
  --min-remaining-hours 2
```

The manifest records paths, sizes, and hashes only; it must not contain raw
secret values. For long protected rollouts, require a remaining freshness
window when checking the manifest. A manifest that is merely under the 24-hour
max age at launch can still expire before GB10 prep reaches `cluster up`; the
2-hour rollout default catches that case before any rollout mutation work.
Use the manifest for protected preflight:

```bash
loom cluster preflight \
  --environment "$ENVIRONMENT" \
  --namespace "$NAMESPACE" \
  --config cluster-config.toml \
  --backup-manifest "$BACKUP_ROOT/backup-manifest.json"
```

Use the same `--environment`, `--config`, and `--backup-manifest` flags on
`loom cluster up` when rolling a protected environment so its preflight
evaluates the same backup, storage, and target-render schema checks before
apply.

```bash
loom cluster up \
  --environment "$ENVIRONMENT" \
  --namespace "$NAMESPACE" \
  --config cluster-config.toml \
  --backup-manifest "$BACKUP_ROOT/backup-manifest.json"
```

For a new protected namespace, this `--config` path lets preflight accept the
static Retain PV plan before the PVCs exist. For an existing namespace, live
critical PVC/PV bindings are audited first and must already be on the durable
boundary. If a restartable recovery has only created part of the static critical
PVC set, preflight may continue only when every present critical PVC is already
bound to an audited Retain PV and the target config declares the missing static
host-path PVCs that `cluster up` or rollout substrate bootstrap will recreate.

`loom cluster doctor` remains the live-cluster reconciliation command. During a
rollout, use `loom cluster preflight --config ...` or `loom cluster up
--config ...` for the apply gate: those commands validate existing Secrets but
compare env vars against the target rendered Deployments, not the old pods that
are about to be replaced.

For protected destructive operations, pass both the manifest and an exact
environment acknowledgement:

```bash
loom cluster down \
  --environment "$ENVIRONMENT" \
  --namespace "$NAMESPACE" \
  --with-volumes \
  --backup-manifest "$BACKUP_ROOT/backup-manifest.json" \
  --acknowledge-data-loss "$ENVIRONMENT"
```

Do not use unbacked `kind delete cluster`, namespace deletion, PVC deletion,
Docker volume cleanup, or `loom cluster down --with-volumes` for staging or
production. On shared staging, operators also do not run the otherwise
non-volume-destructive `loom cluster down --yes`; cluster recovery remains a
broker resume or admission-disabled root-maintenance operation. For separately
authorized production/custom recovery, ordinary pod/service restarts and
non-volume teardown preserve PVCs and do not require the destructive-operation
acknowledgement.

Restore drill checklist:

1. Recreate the cluster or isolated namespace.
2. Restore Kubernetes/runtime secrets first.
3. Ensure `cluster-config.toml` uses the intended durable storage boundary.
   For protected embedded deployments, this is `static-host-path` under
   `/data/<environment>`.
4. Restore Postgres with `pg_restore`.
5. Restore MinIO buckets by mirroring objects back.
6. Deploy the matching image tag and run `loom cluster preflight` with the
   config and backup manifest.
7. Verify API health, login/setup path, provider-secret decryption,
   benchmark/agent catalog rows, batches/trials, artifacts, trajectories, cost
   records, and Monitor worker pools.

## Staging smoke gate

Before promoting a release from `dev` to `main`, exercise the staging
account flow on a staging cluster and attach the evidence to the release issue or
PR. The gate has two parts:

- **Operator/browser evidence** for DNS/TLS, account request approval, password
  setup/reset, SPA submission, and visual checks that require a real browser
  session.
- **Repeatable API evidence** from `scripts/staging_smoke_gate.py`, which
  verifies public API auth, provider discovery, service-proxied downloads, Run
  Library sharing, cross-team denials, provenance, claimed-without-started
  batch diagnostics, and leak scanning.

Quota and rate-limit enforcement are intentionally not part of this staging gate.
Team remains the execution, cost, provider credential, member, and API-token
boundary; spend response is handled through alerts and operator controls until a
separate product policy exists.

### Prereqs

- A successful `loom-staging-rollout` request for the merged candidate SHA;
  direct `loom cluster up` is not a shared-staging prerequisite.
- A public host with TLS enabled, for example `https://loom.example.com`.
- Access to an operator/admin browser session that can create two teams and
  invite users.
- Two disposable staging teams:
  - **Team A** owns the source provider connection and completed smoke run.
  - **Team B** validates org-wide Run Library read/reuse without gaining Team A
    execution control.
- Disposable non-admin Team A and Team B smoke users, each with a user-owned
  API token minted through the normal `/api/v1/tokens` flow. Do not use a
  platform-admin browser/session principal, an admin-minted legacy team token,
  or manual SQL token insertion as release evidence.
- A real or mock OpenAI-compatible provider key for Team A. Use an environment
  reference such as `env:OPENAI_API_KEY`; do not paste raw provider keys into
  issue comments, shell history, or committed files.
- One canonical task fixture registered. `hello-world` is enough for the gate;
  another tiny task is fine if it produces ATIF, trajectory, and at least one
  safe artifact.
- A ready benchmark catalog provisioned into the staging database
  and object store. This is release data, not test fixture data, and must not
  be created through `scripts/seed_test_data.py`.
- If the staging deployment has a remote-worker pool outside the
  Kubernetes cluster, durable private tunnels are installed for Control Plane,
  Gateway, and MinIO. See [remote-worker-pool.md](remote-worker-pool.md).
- One seeded blocked artifact on the source trial, marked
  `share_status=blocked`, whose raw object body contains a fake secret such as
  `seeded-staging-secret`. The release evidence should prove Team B cannot
  download it and that the fake secret does not appear in API responses.
- One private source trial or batch with a safe artifact that Team A can read
  and Team B cannot read through Run Library.

### Benchmark catalog provisioning

Before inviting staging users or starting manual New Batch testing, require the
successful broker step 11 `catalog-provisioning` artifact. It proves the merged
profile published/registered/mirrored the ready catalog with protected service
sources. Shared-staging operators do not export DB, MinIO, or HF credentials or
run the catalog commands below. Missing credentials or source artifacts are
release blockers fixed through the merged profile and resumed original request.
Do not insert benchmark/task rows manually, patch JSON in SQL, or seed staging
with `scripts/seed_test_data.py`.

The Path A/B commands below are development/custom-cluster examples only. They
must not target the shared staging database or object store.

**Path A: copy from a known-good source catalog and object store.** Use this
when the source environment already has runnable task rows and bundle objects:

```bash
export LOOM_CATALOG_SOURCE_DB_URL="$SOURCE_LOOM_DB_URL"
export LOOM_CATALOG_SOURCE_MINIO_ENDPOINT="$SOURCE_LOOM_MINIO_ENDPOINT"
export LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY="$SOURCE_LOOM_MINIO_ACCESS_KEY"
export LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY="$SOURCE_LOOM_MINIO_SECRET_KEY"

# Development/custom target values only:
export LOOM_DB_URL="$CUSTOM_DB_URL"
export LOOM_MINIO_ENDPOINT="$CUSTOM_MINIO_ENDPOINT"
export LOOM_MINIO_ACCESS_KEY="$CUSTOM_MINIO_ACCESS_KEY"
export LOOM_MINIO_SECRET_KEY="$CUSTOM_MINIO_SECRET_KEY"

# Inside a deployed loom-service pod, the command also accepts the service
# Secret names LOOM_SVC_DB_URL and LOOM_SVC_MINIO_* for target values.
loom datasets provision-catalog \
  --target-bucket loom-benchmarks \
  --imported-by "development:${IMAGE_TAG:-manual}"
```

The command is idempotent. It upserts only benchmarks whose stored task rows are
fully runnable, materializes the service-mode agent catalog into the target
`agents` table, creates the target bucket when needed, copies missing
`s3://...` bundle objects, skips matching target objects, and exits non-zero if
any source task bundle prefix has no objects. The service code catalog remains
the agent source of truth; the DB rows are an auditable restore/provision
snapshot with runtime contracts and compatibility metadata. A healthy run
reports non-zero `ready_agents`, non-zero `ready_benchmarks`, non-zero
`ready_tasks`, and `missing=0`.

**Path B: rebuild rows from the published Hugging Face manifest and mirror
runtime bundles internally.** Use this when the published dataset repo is the
source of truth for the benchmark, or when the source catalog/object-store pair
is unavailable. For private or gated repos such as the current SkillLearnBench
release repo, provision `HF_TOKEN` into the operator pod as a Kubernetes Secret
or environment reference before running the command. Do not pass raw HF tokens
in issue comments, command-line history, or committed files.

Hugging Face is the publication and provenance boundary only. Runtime worker
pods should materialize benchmark bundles from internal object storage, not from
HF, so workers do not need HF tokens or broad outbound 443 egress for benchmark
sources.

```bash
export LOOM_HF_ORG="${LOOM_HF_ORG:-PRHW}"

# Development/custom target DB and object-store values only:
export LOOM_DB_URL="$CUSTOM_DB_URL"
export LOOM_MINIO_ENDPOINT="$CUSTOM_MINIO_ENDPOINT"
export LOOM_MINIO_ACCESS_KEY="$CUSTOM_MINIO_ACCESS_KEY"
export LOOM_MINIO_SECRET_KEY="$CUSTOM_MINIO_SECRET_KEY"

# Inside loom-service pods, the command also accepts LOOM_SVC_DB_URL from the
# service Secret and LOOM_SVC_MINIO_* for target object storage. For gated
# repos, HF_TOKEN must already be present in the operator context.
loom datasets register skilllearnbench \
  --revision "$PUBLISHED_SHA" \
  --mirror-to-object-store \
  --bucket loom-benchmarks \
  --registered-by "development:${IMAGE_TAG:-manual}"
```

If the HF repo is private/gated and the pod lacks `HF_TOKEN`, the 401/403 is a
real rollout blocker. Fix it by updating the Secret/profile and restarting the
operator context; do not replace it with hand-written DB rows.

For protected current-GB10 rollout smoke, step 11 publishes the checked-in
release smoke fixture through the same official local-benchmark path before
step 15. This creates a real DB task row and internal `s3://` bundle source for
`loom-smoke/gb10-oracle-hello-world`; it is idempotent and uses the same target
DB/MinIO environment variables as the catalog commands above. For shared
staging, fix catalog inputs in merged source and launch or resume the broker;
do not run the command manually. The example below is only for a custom-cluster
diagnosis or separately authorized production repair.

```bash
loom datasets publish-local deploy/catalog/gb10-smoke \
  --bucket loom-benchmarks \
  --imported-by "release:${IMAGE_TAG:-manual}"
```

For adapter-backed benchmark publishing, use the protected `Publish benchmarks
to HF Hub` workflow when possible. The workflow fails hard when the selected
benchmark publish command fails, including missing `HF_TOKEN` and HF 403
authorization failures. The per-benchmark summary records the benchmark, target
repo, success/failure, exit code, and whether the token was present without
printing any token value. For PRHW namespace failures, provide a
PRHW-authorized write token in the `huggingface-publish` environment or have a
PRHW admin pre-create the dataset repo with write access for the publishing
token; do not switch release evidence to a personal namespace.

The mirror path is idempotent. It downloads the exact HF revision with the
operator token, writes bundle objects under deterministic internal keys, stores
`s3://...` runtime sources in `tasks.source`, and preserves HF repo/revision/
path/checksum provenance in task tags without storing tokens. Keep the two
provenance layers separate: `benchmarks.upstream_*` records the adapter/source
origin and may legitimately be `git` for SkillLearnBench, while the runtime HF
mirror provenance used by the release gate comes from task tags such as
`hf_repo_id`, `hf_revision`, `hf_path`, and `hf_checksum` plus the internal
`s3://` source prefix.
The manifest checksum covers every real file in the bundle, including
dotfiles from upstream repository copies and generated Loom helper scripts.
Do not bypass checksum failures by excluding broad dotfile patterns; if a
client cache artifact appears inside a snapshot, fix that specific materializer
boundary or republish the dataset with an explicit manifest contract.

For staging or production promotion, generate a secret-safe HF
mirror/token-boundary artifact with the first-class CLI and keep it with the
rollout evidence. Protected rollout step 14 runs this before
`loom cluster release-gate`; manual release investigations can run the same
command shape:

```bash
loom datasets hf-boundary-evidence skilllearnbench \
  --environment "${ENVIRONMENT:-staging}" \
  --namespace "$K8S_NAMESPACE" \
  --cluster-config "$CLUSTER_CONFIG" \
  --gb10-workers-status "$ROLLOUT_DIR/gb10-workers-status-$IMAGE_TAG.json" \
  --canary-batch-id "$CURRENT_CANDIDATE_SLB_CANARY_BATCH_ID" \
  --output "$ROLLOUT_DIR/hf-mirror-boundary-evidence-$IMAGE_TAG.json"
```

The command collects catalog audit data through the `loom-service` pod, reads
task-level runtime mirror provenance from the target DB, finds a succeeded
SkillLearnBench GB10 canary unless `--canary-batch-id` is supplied, and checks
GB10 worker `.env` files plus worker container env keys for `HF_TOKEN`
presence. It records only counts, paths, prefixes, batch ids, worker
registration ids, booleans, and redacted/secret-safe references. It also binds the evidence to the candidate
environment, image tag, full Git SHA, and canonical SHA-256 of the exact GB10
status artifact consumed by the release gate; reusing evidence from an older
candidate or a different status snapshot fails closed. Every canary trial's
persisted `worker_id` must belong to an active, fresh manifest-selected worker
registration in that same GB10 status snapshot. A worker restart therefore
invalidates an older canary even when the batch otherwise succeeded, and an
explicit `--canary-batch-id` does not bypass this check. Protected rollout
step 14 now submits and waits for a deterministic one-trial
`skilllearnbench/fix-security-bug/fix-security-bug-1` oracle canary after the
GB10 status artifact proves all manifest-active hosts have fresh current
registrations. The canary name binds the image tag and a stable digest of the
exact host-to-worker registration set, so heartbeat timestamp changes reuse the
same batch while a worker restart creates a new canary. Evidence generation is
then pinned to the returned batch id instead of discovering an older succeeded
batch. Manual investigations must preserve the same order and pass their exact
current-candidate batch id explicitly. The status and environment-state
subcommands each retain a bounded 180-second timeout because a protected status
snapshot can include the complete retained unlinked-worker audit ledger; a
timeout remains a release-gate failure rather than permission to skip or reuse
older status. The GB10 check uses the same
`[gb10_pool].ssh_config`, `ssh_identity_file`, and optional
`ssh_certificate_file` as rollout GB10 prep; failed SSH, failed container
listing or inspection, or no running inspected worker container on any active
host are release-gate failures. The remote probe uses `docker ps` without
`-a`, so stopped or exited historical containers do not count as coverage.
The 2026-07-29 owner correction supersedes the #822 quarantine. The exact
active staging set is `trt-gb10-1` through `trt-gb10-15` with
`excluded_nodes=[]`; all 15 hosts must appear in the active HF boundary probe.
A candidate-owned drain/quiescence gate defers disruptive convergence while
external work is active and must never cancel or preempt that work. The
evidence records the sorted, actual SSH targets in `checked_host_names`, plus
`docker_ps_failed_hosts` and `hosts_without_containers`, so a matching count
cannot hide a wrong or partially inspected host set. The remote worker probe is
sent to `python3 -` over SSH stdin rather than embedded as a multiline
`python3 -c` argument, so the check does not depend on remote shell quoting
preserving Python source code.

The generated artifact has this shape:

```json
{
  "schema_version": 1,
  "environment": "staging",
  "benchmark_id": "skilllearnbench",
  "candidate_binding": {
    "environment": "staging",
    "release_image_tag": "staging-$CANDIDATE_SHA",
    "release_git_sha": "$CANDIDATE_FULL_SHA",
    "gb10_workers_status_sha256": "$CANONICAL_STATUS_SHA256"
  },
  "catalog": {
    "runnable_tasks": 100,
    "artifact_contract_classified_tasks": 100,
    "apd5_required_artifact_contract_tasks": 1,
    "requires_caps": {"cpu_arch": "any"}
  },
  "runtime_sources": {
    "total_task_sources": 100,
    "internal_s3_sources": 100,
    "non_internal_sources": [],
    "sample_s3_source": "s3://loom-benchmarks/skilllearnbench/task-000/"
  },
  "hf_provenance": {
    "upstream_kind": "huggingface",
    "upstream_locator": "PRHW/loom-benchmark-skilllearnbench",
    "upstream_revision": "$PUBLISHED_SHA"
  },
  "worker_boundary": {
    "canary_started": true,
    "terminal_state": "succeeded",
    "canary_task_filter": {
      "benchmark_id": "skilllearnbench"
    },
    "canary_worker_pools": {
      "active": {},
      "terminal": {"gb10": 2}
    },
    "expected_trial_count": 2,
    "succeeded_trials": 2,
    "canary_task_provenance": {
      "trial_count": 2,
      "target_benchmark_trial_count": 2,
      "non_target_trial_count": 0,
      "task_set_trial_count": 0,
      "benchmark_ids": ["skilllearnbench"],
      "worker_ids": ["$CURRENT_WORKER_UUID_1", "$CURRENT_WORKER_UUID_2"]
    },
    "hf_token_present": false,
    "hf_token_isolated": true,
    "direct_hf_egress_required": false,
    "materialized_from_internal_source": true,
    "gb10_hf_token_check_summary": {
      "checked_hosts": 15,
      "checked_host_names": [
        "trt-gb10-1", "trt-gb10-10", "trt-gb10-11", "trt-gb10-12",
        "trt-gb10-13", "trt-gb10-14", "trt-gb10-15", "trt-gb10-2",
        "trt-gb10-3", "trt-gb10-4", "trt-gb10-5", "trt-gb10-6",
        "trt-gb10-7", "trt-gb10-8", "trt-gb10-9"
      ],
      "ssh_failed_hosts": [],
      "docker_ps_failed_hosts": [],
      "hosts_without_containers": [],
      "env_file_missing_hosts": [],
      "env_file_hf_token_present_hosts": [],
      "hosts_with_container_hf_token_present": [],
      "containers_checked": 15,
      "inspect_failed": []
    }
  },
  "secret_scan": {"raw_secret_values_present": false}
}
```

`artifact_contract_classified_tasks` must equal `runnable_tasks`, and
`apd5_required_artifact_contract_tasks` must be exactly `1`. A zero or missing
value means `PUBLISHED_SHA` points to a SkillLearnBench manifest published
before the required-artifact contract. Republish from current `dev`, update the
merged environment-state pin, and rerun protected catalog provisioning; do not
patch task JSON directly in the database.

Pass the artifact to `loom cluster release-gate` with
`--hf-mirror-boundary-evidence "$ROLLOUT_DIR/hf-mirror-boundary-evidence-$IMAGE_TAG.json"`.
The release gate fails staging/production when the release manifest records the
SkillLearnBench HF catalog gate but this artifact is missing, has non-`s3://`
runtime sources, lacks HF provenance, uses a task filter that can select any
non-SkillLearnBench source, cannot prove every actual canary trial joined to a
SkillLearnBench benchmark task, cannot bind every canary trial to the current
candidate's active GB10 worker registrations, reports worker `HF_TOKEN` presence, has
missing/failed GB10 token inspection, requires direct worker HF egress, or
contains raw secret-looking values. This repo-side check does not replace live
staging validation; it makes the required evidence acceptance-testable and
production-promotion-blocking.

`loom cluster render` injects the optional `loom-secrets` key
`huggingface-api-key` into `loom-service` as `HF_TOKEN` for gated catalog mirror
provisioning. It must not inject that token into worker Deployments or remote
worker env files. To rotate the read token, update only
`loom-secrets/huggingface-api-key`, then roll the pods that are allowed to read
it:

```bash
HF_READ_TOKEN="$(security find-generic-password -w -s loom-hf-read-token)"
kubectl -n loom patch secret loom-secrets --type merge \
  --patch "{\"stringData\":{\"huggingface-api-key\":\"${HF_READ_TOKEN}\"}}"
unset HF_READ_TOKEN
kubectl -n loom rollout restart deploy/loom-service deploy/loom-llm-gateway
```

Keep raw HF tokens out of issue comments, command-line transcripts, committed
profiles, and evidence artifacts. The durable target for private benchmark
sources remains the HF publish -> internal object-store mirror -> worker
materialize-from-internal-source flow; worker `HF_TOKEN` is not a substitute
for that mirror when the benchmark has already been provisioned internally.

Verify the target before continuing:

```bash
loom datasets audit --all --verify-bundles
curl -sf -H "Authorization: Bearer $TEAM_A_TOKEN" \
  "$PUBLIC_SERVER_URL/api/v1/benchmarks?limit=200&include_empty=true"
python scripts/benchmark_reward_gate.py readiness \
  --server-url "$PUBLIC_SERVER_URL" \
  --token env:TEAM_A_TOKEN
```

When validating SkillLearnBench specifically, confirm its rows carry runnable
task configs instead of empty placeholders:

```sql
SELECT count(*) AS skilllearnbench_tasks
FROM tasks
WHERE benchmark_id = 'skilllearnbench'
  AND jsonb_typeof(config) = 'object'
  AND config ? 'environment';
```

The public API response must include at least one benchmark with
`task_count > 0`, must not include required user-facing benchmarks stuck in
`Needs publish` / `Needs republish`, and the New Batch page must show
selectable benchmark choices after sign-in. Required pending benchmarks should
be published/republished and proven, not hidden from the user path. Benchmarks
with `readiness_label="Not supported yet"` and
`blocker_reason="unsupported_runtime"` are explicit product exclusions: they
remain visible with guidance but are skipped by the supported-benchmark
readiness gate and cannot be selected until their runtime follow-up lands.
Rows with `readiness_label="Deferred"` and
`blocker_reason="deferred_support"` follow the same gate semantics for
benchmarks that need an explicit product/data-access follow-up before they can
enter the supported catalog.
Rows with `readiness_label="Not in v1.0"` and
`blocker_reason="not_v1_supported"` are built-in catalog benchmarks outside the
v1.0 allowlist; they remain visible for transparency, are not selectable, and
are skipped by the supported-benchmark readiness gate until a support issue
adds them to scope.

For release acceptance, submit supported-benchmark batch work with the intended
production runner/provider mix and wait for every batch to finish. One batch may
cover the whole v1.0 allowlist, or operators may run one batch per benchmark.
Then verify that every v1.0-supported benchmark has numeric-reward coverage for
every currently runnable task:

```bash
python scripts/benchmark_reward_gate.py sweep \
  --server-url "$PUBLIC_SERVER_URL" \
  --token env:TEAM_A_TOKEN \
  --batch-id "$SUPPORTED_BENCHMARK_ACCEPTANCE_BATCH_ID"
```

Repeat `--batch-id` when the acceptance sweep uses separate batches. The sweep
gate queries `/api/v1/tasks/count` for each v1.0-supported benchmark, groups
trials by benchmark/task id, and requires distinct numeric-reward task coverage
to match the runnable task count. Numeric rewards, including `0`, count as
successful benchmark evaluation. If a provider, agent, or platform transient
failure is rerun in a later batch and the same task gets a numeric reward, the
later result supplies the task coverage. Missing rewards without rerun coverage,
benchmark-side verifier errors, task-image/environment failures, incomplete
fan-out, or missing allowlist benchmark coverage fail the gate.

Before submitting any full production benchmark batch, run a small canary with
the exact production agent/provider/model/runtime/worker-pool mix and a
representative task-family subset. A canary that reaches the provider and
records platform-succeeded trials still does not prove score viability if every
scored trial has reward `0`, and it is also suspicious if every scored trial has
full reward `1.0` on a realistic multi-task slice. The score-positive gate is
mandatory and blocks full production submission unless at least one scored
canary trial has reward greater than `0` and at least one scored trial has
reward below `1.0`:

```bash
python scripts/benchmark_reward_gate.py score-positive-canary \
  --server-url "$PUBLIC_SERVER_URL" \
  --token env:TEAM_A_TOKEN \
  --batch-id "$CANARY_BATCH_ID" \
  --json-output "$EVIDENCE_DIR/score-positive-canary.json" \
  --markdown-output "$EVIDENCE_DIR/score-positive-canary.md"
```

The JSON/Markdown evidence records the canary batch id, baseline agent/model
and provider fields, scored task ids, reward distribution, unscored-trial
taxonomy, and override metadata when present. If the command exits nonzero, fix
the runner/provider/task configuration or choose another accepted path before
submitting production scale work. An override is only valid when the operator
records both an issue or PR reference and rationale:

```bash
python scripts/benchmark_reward_gate.py score-positive-canary \
  --server-url "$PUBLIC_SERVER_URL" \
  --token env:TEAM_A_TOKEN \
  --batch-id "$CANARY_BATCH_ID" \
  --json-output "$EVIDENCE_DIR/score-positive-canary.json" \
  --markdown-output "$EVIDENCE_DIR/score-positive-canary.md" \
  --override-issue "#445" \
  --override-rationale "Coordinator accepted an alternate score-positive path; keep the failed agent/provider issue open."
```

Score credibility has a separate Layer 1 gate that is independent of live model
quality. Before using benchmark scores as release evidence, verify that every
v1.0-supported benchmark has a canonical scoring reference, score-semantics
contract, Harbor/upstream parity decision, and same-output replay case:

```bash
python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/score-alignment/manifest.json
```

Layer 1 answers whether Loom scores a fixed output correctly. Layer 2 matched
Loom-vs-Harbor or Loom-vs-upstream runs remain separate run evidence.

### Checklist

1. **Cluster healthy.** `loom cluster status --namespace loom` reports
   `all_ready=True`. `kubectl get pods -n loom` shows no `CrashLoopBackOff`.
2. **Public surface reachable.** For root-host deployments,
   `curl -sf https://<ingress_host>/api/v1/health` returns `200` and
   `curl -sf https://<ingress_host>/` returns the SPA index when `loom-web`
   replicas > 0. For first prod route-split deployments, check
   `https://yylx.world/prod/api/v1/health`,
   `https://yylx.world/staging/api/v1/health`, `https://yylx.world/prod`, and
   `https://yylx.world/staging`.
3. **Boundary holds.** `loom cluster audit` exits 0. `kubectl get svc -n loom`
   shows no `LoadBalancer` / `NodePort` services. `kubectl get ingress -n loom`
   shows TLS enabled and backends only for `loom-service` at `/api/v1` and
   `loom-web` at `/`, or the canonical `/prod`/`/dev` prefixed equivalents.
   `loom-llm-gateway`, Control Plane, Postgres, MinIO, workers,
   worker-token admin routes, and batch-runner bootstrap routes stay
   internal-only.
4. **Frontend route metadata holds.** Before traffic, run the no-secret route
   smoke against the public runtime config:
   ```bash
   python scripts/ops/frontend_route_smoke.py \
     --route production=https://yylx.world/prod=https://yylx.world/prod/api \
     --route staging=https://yylx.world/staging=https://yylx.world/staging/api \
     --json

   python scripts/ops/frontend_security_headers.py \
     --route production=https://yylx.world/prod \
     --route staging=https://yylx.world/staging \
     --json
   ```
   Both route documents must report the expected `environment`,
   `environmentLabel`, `routePath`, `apiBase`, and `apiRouteBase`; the response
   must be `no-store`. Production labels must not contain beta wording. The
   route smoke also requires a nested pseudo-asset probe such as
   `/batches/assets/loom-route-smoke-nonexistent.js` to return `404`; a
   `200 text/html` SPA fallback there fails the gate. The security report must
   pass exact singleton CSP, `nosniff`, referrer, permissions, and HSTS checks
   on 200, 308, and wrong-asset 404 responses; it records no headers or
   response bodies. Staging CI separately proves the web-origin policy on a
   restored, disposable-pod 500.
   Then prove that the built bundle actually mounts in a fresh logged-out
   Chromium context on the exact, slash-canonical, and supported deep routes,
   both on direct entry and after refresh:
   ```bash
   (
     cd web
     npm ci
     npx --no-install playwright install chromium
     npm run smoke:routes -- \
       --route https://yylx.world/prod \
       --route https://yylx.world/staging \
       --trace ../frontend-route-browser-trace.zip
   )
   ```
   The command emits a redacted route/status report. It waits for React commit,
   explicit settled anonymous/authenticated state, and a bounded quiet window
   with no active same-origin requests or cross-origin scripts. Cross-origin
   non-script resources do not block that window; a pending cross-origin script
   remains blocking until its success or failure is observed. Each final client
   URL must equal the requested route; only an explicitly anonymous root with
   exactly one exact `${routePath}/api/v1/auth/me` fetch/XHR `401` in that phase
   may use the canonical `${routePath}/auth/login` login fallback. Chromium's
   post-response `ERR_ABORTED` is tolerated only when paired by request identity
   to that one delivered `401`; an unpaired network failure, a `204`,
   malformed `200`, `5xx`, network failure, or mixed response set fails rather
   than being treated as signed out. Before refresh the smoke restores the
   original direct URL, preserving bookmark coverage even after that fallback.
   Failed same-origin requests, cross-origin scripts, application console/page
   errors, non-2xx same-origin scripts/styles, and assets outside the selected
   prefix fail. Browser-generated errors are ignored only when they match an
   observed failed request or `4xx`/`5xx` response for a cross-origin non-script
   resource, or the exact anonymous
   `${routePath}/api/v1/auth/me` fetch/XHR when that phase's complete response
   set is one exact `401`; no query variant, competing status, or other
   same-origin error is exempt. The trace comes only from the
   command's fresh logged-out context; retain it as rollout evidence and do not
   substitute a signed-in profile.

   A Loom recovery panel during this check is failure evidence, not a blank
   page and not anonymous success. `Loom could not start` identifies runtime
   config loading, `Loom could not verify your session` identifies auth-session
   loading, `Loom could not display this page` identifies the root render
   boundary, and `Loom could not display this section` identifies a contained
   routed-page failure. Record the fixed title, `WEB-*` support reference,
   redacted route pathname, candidate SHA, timestamp, and reproduction steps.
   Do not copy the complete browser URL, query string, response body, token,
   raw error, or stack into rollout evidence.

   The support reference is emitted through a bounded in-browser reporter with
   `kind`, redacted `pathname`, and optional same-origin source position. That
   reporter is currently only a local hook: Loom does not provide a default
   server, metrics, tracing, or third-party telemetry transport for looking up
   a `WEB-*` value. Do not claim remote correlation unless the deployment has
   separately installed and validated such an adapter. Use Retry for a fresh
   config/session, root-render, or transient route attempt; use Reload Loom to
   rebuild document and module state; use Go to Loom home to return to `/dev/`,
   `/prod/`, or `/`. A cached `React.lazy` rejection intentionally omits Retry
   because only reload can replace the cached rejected module promise.

   Session checks have four states: `loading`, `authenticated`, `signed-out`,
   and `unavailable`. Only an exact `/api/v1/auth/me` `401` proves
   `signed-out`. Network failures, non-401 HTTP failures, `204`, malformed JSON,
   and invalid session shapes are `unavailable` (`network`, `http`, or
   `invalid`) and must keep the smoke in the `error` state until Retry or
   Reload succeeds. See
   [`frontend-error-recovery.md`](../architecture/frontend-error-recovery.md)
   for the complete boundary and redaction contract.

   The repository `web-checks` job is a prerequisite for this candidate-bound
   evidence, not a substitute for it. The frontend gate must already be green
   on the exact merged `dev` SHA before a future candidate is fixed and sent to
   the broker. A local branch or Draft PR cannot be added to, re-resolve, or
   replace an in-progress fixed candidate. See
   [`frontend-quality-gate.md`](../architecture/frontend-quality-gate.md) for
   the reusable Playwright/axe/failure-ledger contract and its recovery-test
   extension boundary.

   **Staging-only authenticated admin UI acceptance.** After the logged-out
   route smoke passes in the candidate-bound brokered protected-staging
   rollout, validate the protected Admin Access and Rate cards surfaces with
   the short-lived, audited #692 exchange. The ephemeral kind workflow renders
   `runtime_environment = "development"` and performs only a credential-free
   `404` deny probe; it cannot substitute for or impersonate protected staging.
   Keep the broker-created request envelope, backup guard, and rollout mutation
   lease intact. Read the singleton admin bearer from an owner-only (`0600`)
   mounted file or redirected, non-interactive stdin; never place the raw
   bearer on the command line or in the process environment. Bind the report to
   the build identity exposed by the running service, not an ambient checkout
   or PR ref:

   Broker-owned step 16 executes this check from the candidate-built,
   revision-labelled browser image and stores its report with request/attempt
   evidence. Do not run it manually against shared staging from an ambient
   checkout. Operators inspect the evidence with
   `loom-staging-rollout status REQUEST_ID` and
   `loom-staging-rollout logs REQUEST_ID`.

   The following illustrates the required broker-owned step, not an authorized
   standalone operator command:

   ```bash
   DEPLOYED_SHA="$(kubectl --context kind-loom-staging -n loom \
     exec deploy/loom-service -- cat /opt/loom/build-sha | tr -d '\r\n')"
   npm --prefix web run smoke:staging-admin -- \
     --route https://yylx.world/staging \
     --expected-deployed-sha "$DEPLOYED_SHA" \
     --admin-token-source file:/absolute/path/to/mounted/staging-admin-token \
     --username qianyi \
     --report /tmp/loom-staging-admin-browser-smoke.json
   ```

   Use only an existing enabled `active` or `pending_setup` platform admin who
   is already an owner of the enabled `admin` team. The bootstrap must not
   create, enable, promote, or repair any authority; normal grant/revoke remains
   the #802 workflow. The image build bakes that SHA into
   `/opt/loom/build-sha` and the OCI `org.opencontainers.image.revision` label;
   runtime environment overrides do not establish identity. The check verifies
   the actual deployed build identity, exact correlated request ID and safe
   audit event, every tab's product API, all six Admin Access states, keyboard
   roving focus with Arrow/Home/End, exact ARIA tab-to-panel relationships, the
   Audit log, and Rate cards.
   It emits only the sanitized JSON report: do not retain a trace, screenshot,
   storage state, cookie, or bearer. Its `finally` cleanup logs out, revokes the
   session, and proves `/api/v1/auth/me` returns `401`; a cleanup failure fails
   the gate. The bootstrap route is deliberately `404` outside staging and its
   fixed 900-second session cannot be refreshed or mutate any endpoint except
   exact logout. This admin-only report does
   not replace the later normal-user onboarding and submission evidence.

5. **Remote-worker private tunnels hold.** If remote workers are attached, the
   shared-staging broker collects watchdog evidence and verifies the exact
   worker-facing URLs from the control node and declared worker hosts. Inspect
   the redacted watchdog, local, remote, and optional subprocess-gateway probe
   results through `loom-staging-rollout status REQUEST_ID` and
   `loom-staging-rollout logs REQUEST_ID`; do not rerun the helper from an
   interactive checkout or substitute env/kubeconfig/host inputs. This gate is
   required after every rollout because public ingress can pass while private
   worker tunnels are down.
5. **Invite-only onboarding.** From the operator/admin browser session, confirm
   Teams that should accept public requests have public registration enabled,
   then submit username requests for each team from `/auth/login`. Approve each
   request in Admin access -> Accounts, open the
   setup link in a fresh browser profile, set a password, and confirm the user
   lands in the selected team without seeing raw API credentials. Generated
   setup/reset links must already use the public HTTPS route base from
   `LOOM_SVC_PUBLIC_BASE_URL` or ingress forwarded headers, for example
   `https://yylx.world/staging` in staging or `https://yylx.world/prod` in
   production; fix that configuration before sharing any one-time link. Capture
   only safe prefixes and redacted links in shared evidence.
6. **CLI login.** In a fresh shell, sign in with the approved account:
   ```bash
   export LOOM_PASSWORD=...
   loom auth login --server https://loom.example.com --username TeamAUser --password env:LOOM_PASSWORD
   loom auth whoami
   ```
   Repeat with Team B's user. Evidence should show usernames, teams, roles, and
   scopes, never raw passwords. Then mint short-lived smoke API tokens from the
   logged-in non-admin user session, not from an admin bearer or manual DB write:
   ```bash
   export SMOKE_TOKEN_NAME="staging-team-a-$(date -u +%Y%m%dT%H%M%SZ)"
   python - <<'PY'
   import json
   import os

   from loom_cli.config import load_config
   from loom_cli.server_client import authed_client

   payload = {
       "name": os.environ["SMOKE_TOKEN_NAME"],
       "type": "team",
       "scopes": ["read:own", "submit", "providers:manage", "tokens:manage"],
       "expires_in_days": 7,
   }
   with authed_client(load_config()) as client:
       response = client.post("/api/v1/tokens", json=payload)
       response.raise_for_status()
       print(json.dumps(response.json(), indent=2))
   PY
   ```
   Save the returned raw `token` only in the local operator shell as
   `TEAM_A_TOKEN` or `TEAM_B_TOKEN`; shared evidence should keep only the hash
   prefix. Verify each token in an isolated config or shell:
   `loom auth login --server https://loom.example.com --token env:TEAM_A_TOKEN`
   followed by `loom auth whoami` must print `Principal: user-owned API token`,
   a `User:` line, the intended team, and no `platform_admin` role. After the
   smoke, revoke by prefix through the same user session:
   ```bash
   export SMOKE_TOKEN_PREFIX=<8-hex-prefix>
   python - <<'PY'
   import os

   from loom_cli.config import load_config
   from loom_cli.server_client import authed_client

   with authed_client(load_config()) as client:
       response = client.delete(f"/api/v1/tokens/{os.environ['SMOKE_TOKEN_PREFIX']}")
       response.raise_for_status()
   PY
   ```
7. **Provider connection create + test.** As Team A, create the staging
   smoke provider through the CLI (or `POST /api/v1/provider-connections`),
   then probe. The release fixture uses YibuAPI through Loom's
   OpenAI-compatible provider path so the same connection can run the
   GB10-backed Source Useful smoke:
   ```bash
   export YIBUAPI_API_KEY=...
   loom providers create \
     --name mz_tn_canada_qianyi \
     --type openai-compatible \
     --base-url https://yibuapi.com/v1 \
     --api-key env:YIBUAPI_API_KEY \
     --rate-card-provider yibuapi
   loom providers test mz_tn_canada_qianyi
   ```
   `test` must return `status=valid`; `http_status` shows the upstream
   HTTP response code. Exit code is 0 for valid, 1 for invalid.
8. **Model discovery.** `loom providers models mz_tn_canada_qianyi --refresh`
   followed by `loom providers models mz_tn_canada_qianyi` returns a
   non-empty catalog, including `glm-5.1-thinking`. `curl /api/v1/models` from a
   user-owned API token shows the agent-capable view with provider namespace
   `yibuapi`.
9. **Model preflight.** Run
   `loom providers models mz_tn_canada_qianyi --preflight glm-5.1-thinking`.
   The model row
   should show `preflight=valid`. A 401/403 should show `access-denied` without
   raw provider keys.
10. **Submit small batches from SPA and CLI.** Pick `hello-world` (or another
    canonical fixture). Submit once from the SPA New Batch page and once from the
    CLI. Before submitting either canary, run the object-store write gate by
    itself so MinIO free-space failures are caught before trial execution:
    ```bash
    python scripts/staging_smoke_gate.py \
      --server-url https://loom.example.com \
      --team-a-token env:TEAM_A_TOKEN \
      --team-b-token env:TEAM_B_TOKEN \
      --catalog-minio-endpoint "$STAGING_MINIO_ENDPOINT" \
      --catalog-minio-access-key env:STAGING_MINIO_ACCESS_KEY \
      --catalog-minio-secret-key env:STAGING_MINIO_SECRET_KEY \
      --object-store-write-check-only \
      --object-store-write-check-bucket trajectories \
      --object-store-write-check-count 64 \
      --object-store-write-check-concurrency 16 \
      --fail-on-skip \
      --markdown-output staging-object-store-preflight.md \
      --json-output staging-object-store-preflight.json
    ```
    The `object_store.minio_write_probe` row must be `PASS`. Keep the probe at
    a nontrivial count/concurrency for full100 or remote-worker acceptance so
    object-store connection pooling is exercised before worker artifact and
    trajectory uploads start. The CLI commands below keep the no-model canary
    separate from the provider-backed path:
   ```bash
   # No-model oracle canary; no provider/model flags are needed.
   loom eval batch create \
     --name-suffix oracle-smoke \
     --task-filter '{"task_ids":["loom-smoke/gb10-oracle-hello-world"]}' \
     --agent oracle \
     --n-per-task 1 \
     --required-worker-pool gb10

   # Model-backed path through the provider gateway. This is the release
   # provider smoke because it exercises codex + YibuAPI OpenAI-compatible.
   # Platform admins must pass the provider owner's team id explicitly;
   # provider-name lookup is scoped to that team.
   loom eval batch create \
     --team-id <agentic-rl-team-id> \
     --name-suffix opencode-yibuapi-smoke \
     --task-filter '{"task_ids":["source-useful-frontier-5003/shard003__software_development__buildsqliteissuetrackercli"]}' \
     --provider mz_tn_canada_qianyi \
     --model glm-5.1-thinking \
     --agent opencode \
     --n-per-task 1 \
     --backend docker \
     --required-worker-pool gb10
   # then tail it:
   loom eval batch show <batch-id>
   ```
   For mixed-pool release evidence, repeat `--required-worker-pool` for every
   pool that must produce terminal evidence, for example `oldlab`,
   `k8s-worker`, and `gb10`. The service adds one extra pool-pinned
   coverage trial for each requested pool while leaving the normal batch trials
   portable. When a target pool's CPU architecture is known from active workers
   or autoscaler policy, the coverage trial is selected from tasks compatible
   with that architecture; if the selected task slate cannot satisfy the pool,
   fanout records `required_worker_pool_incompatible` instead of submitting an
   unclaimable queued trial. Do not depend on theoretical max-slot saturation
   alone to force OLDLAB/k8s/GB10 participation; the smoke gate below checks the
   resulting terminal pool coverage explicitly.
   `loom eval batch create` validates local built-in agents first, but falls
   back to the deployed `/api/v1/agents` catalog when local `loom-launcher`
   adapters are absent. A fresh rollout/operator venv can therefore submit a
   service-mode `codex` batch as long as the target staging service catalog
   lists `codex` as ready.
   If the canary must represent a named active user/team but the user's
   browser session or user-owned API token is unavailable, use the audited
   `loom admin batches submit-on-behalf` flow above instead of minting a
   legacy team token or writing directly to the database. Preserve the returned
   batch id and the `batch.submit_on_behalf` audit event in the evidence
   bundle.
   Re-run `batch show` until `state` reaches a terminal value.
11. **Live progress visibility.** While the batch runs, the SPA Monitor page
   shows planned trials and current state transitions, and
   `GET /api/v1/trials/{id}` echoes the same state.
12. **Final evaluator output.** Trial reaches `succeeded` (or `failed`
   with a sensible reason). `GET /api/v1/trials/{id}` carries
   `aggregate_reward`, `failure_reason` (when applicable),
   `total_prompt_tokens`, `total_completion_tokens`,
   `llm_calls_count`, `diagnosis`, `debug_evidence`, plus `atif_url`,
   `trajectory_url`, `atif_ready`, `trajectory_ready`, and `artifacts` for
   download.
   Artifact rows include `share_status` and a safe `blocked_reason`
   when org-wide sharing is blocked. Trial and batch detail responses
   carry local token and cost projections (`estimated_cost_usd`,
   `cost_status`, `cost_estimate_source`,
   `cost_estimate_confidence`, `pricing_modes`,
   `usage_estimate_confidence`); use
   `/api/v1/usage` with optional `include_batches=true` for admin totals
   and per-batch drilldown. For a main batch plus linked supplemental reruns,
   query `/api/v1/usage?batch_id=<main>&include_batch_family=true` or
   `loom eval usage --batch-id <main> --include-batch-family`; add
   `include_batches=true` / `--include-batches` to list each child batch in
   the family. Failed upstream audit rows surface as
   `pricing_mode=failed-upstream` and `cost_status=failed_upstream`, not as
   priced provider usage. If confidence is `partial` or `missing`, inspect
   `partial_usage_llm_calls_count` and `missing_usage_llm_calls_count`
   before treating token or dollar totals as complete.
   For model-backed provider runs, terminal trials and batches also carry
   `llm_evidence_status`. Treat `no_calls_invalid` and `partial_no_calls` as
   invalid benchmark evidence: the model path did not persist the expected
   gateway call records. `loom eval batch show <id>` prints
   `no_call_trials` and an invalid-evidence warning for this case, and
   diagnosis uses `batch.no_llm_calls` when a finished batch has zero calls.
   Trial detail also exposes `no_call_reason`, `no_call_message`, and
   `no_call_retryable`; batch detail exposes `no_call_reason_counts` and
   `effective_no_call_reason_counts`. A Codex subprocess exit such as
   `codex_high_demand_no_call` means the agent failed before any Loom Gateway
   request was recorded. Exclude those trials from clean #6 score-alignment
   and #85 request-parameter baselines unless a retry succeeds with
   `llm_evidence_status=calls_observed`; do not count the original reward-0 row
   as clean model/provider parity evidence.
   For `terminus-2`, inspect the Harbor-embedded runtime path documented in
   [`terminus2-runtime.md`](../architecture/terminus2-runtime.md). The agent
   runs pinned Harbor `Terminus2` in-process in the worker image
   (`deploy/Dockerfile.worker`, Harbor `@527d50d`); it is not a
   `loom-launcher` subprocess adapter.
   A healthy trajectory should include `terminus2_runtime_provenance` (Harbor
   pin + bridge revision), then model-driven `terminus2_turn`,
   `terminus2_command`, and `terminus2_terminal_observation` events with LLM
   rows joined to real Control Plane `llm_calls.id` values via the gateway
   ledger. Setup failures before the first model call often surface as
   `no_calls_invalid` or a trial `agent` phase error from
   `CheckpointBridgeError` (missing CP client, ambiguous token match, or
   command without a matching observation).
   Harbor artifacts should appear under `.loom/agent/trajectory.json` and
   `.loom/agent/recording.cast`. If import or Harbor version drift is
   suspected, verify the deployed worker image tag matches the rollout SHA and
   that `deploy/worker-image.lock` was regenerated for the candidate build.
   On ARM64 GB10 hosts, the first Terminus-2 task image may also spend time
   in `_ensure_terminus_2_arm64_base_if_needed` before `started_at`; treat
   long pre-start claims with fresh `pre_start_heartbeat_at` as normal cold
   cache work, not provider failure.
   For opencode/subprocess timeout investigation, confirm a worker watchdog
   hard deadline or control-plane stale-running reclaim produces
   `state=failed`, `failure_reason=agent_timeout`, and a failure message that
   includes runtime, effective agent timeout, hard deadline, last event/LLM
   silence, and worker heartbeat freshness. Operator or Control Plane
   cancellation should remain `state=cancelled` and must not be conflated with
   timeout reclaim.
   Default `GET /api/v1/batches/{id}` is intentionally lightweight for large
   runs: it uses aggregate LLM-call counts and omits heavyweight
   `debug_evidence` and `diagnosis`. Use
   `GET /api/v1/batches/{id}?include_debug=true` or the dedicated
   `/debug` and `/diagnosis` endpoints when investigating one batch. Those
   batch diagnostic paths still use bounded trial projections and must not
   fetch full trial `trajectory_index` rows; full trajectory and artifact
   listings stay on per-trial detail/download routes.
   For multi-benchmark batches, `GET /api/v1/batches/{id}` also
   returns `benchmark_summary`; verify the SPA Batch Detail page shows
   each benchmark's score, completed/expected trial count, and platform
   failure count instead of only one overall average.
   For multi-agent/model batches, `GET /api/v1/batches/{id}` and
   `GET /api/v1/run-library/batches/{id}` also return
   `combination_summary`; verify the Run Library Batch Detail page shows
   each requested combination's reward, actual/expected trial count,
   scored-trial count, success/failure counts, LLM calls, and token totals.
   `GET /api/v1/batches/{id}` also returns
   `effective_combination_summary` when supplemental reruns replace failed
   originals. Combinations with no materialized trials and combinations with
   trials but no scored rewards must be visually distinguishable.
   For failed or partially failed work, verify
   `GET /api/v1/trials/{id}/diagnosis`,
   `GET /api/v1/batches/{id}/diagnosis`,
   `loom eval diagnose trial <id>`, and
   `loom eval diagnose batch <id>` return the same primary cause,
   reason clusters, score reliability text, and next actions without
   printing provider secrets, bearer tokens, internal service URLs, or
   signed object-store URLs. The SPA Trial Detail page should show the
   diagnosis before the raw debug evidence disclosure. The SPA Batch Detail and
   Run Library Batch Detail pages keep the default read lightweight; Run
   Library detail must use a capped typed-artifact preview and bounded trial
   projections instead of fetching full trial `trajectory_index` rows or
   enumerating complete artifact inventories. Click
   **Load diagnostics** before checking the same diagnosis/debug order.
   Also verify `GET /api/v1/batches/{id}/rerun-plan` and
   `loom eval batch rerun-plan <id>` before launching supplemental work. The
   plan must keep auto-safe platform/transient failures separate from
   operator-approval rows and not-rerunnable rows, support explicit repeated
   `task_id` filters, expose `supplemental_coordinates` for repeated samples or
   combinations of the same task, and exclude task compatibility failures and
   reward `0` score failures from automatic reruns. Monitor and Run Library
   should label reward `0` verifier-output rows as platform-successful score
   failures, not platform failures.
13. **Trajectory + artifact download.** `GET /api/v1/trials/{id}/trajectory`
    streams event pages; `GET /api/v1/trials/{id}/trajectory/download`
    returns raw JSONL, using `trial_events` as the fallback source when the
    legacy object-store copy is absent; fallback reconstruction must also fold
    in gateway `llm_calls` rows and terminal `trials.state/result` so users do
    not receive a sparse event table that omits usage or final state. The
    service renumbers the reconstructed JSONL to a clean trial-wide sequence.
    `GET /api/v1/trials/{id}/atif` returns the ATIF JSON from the object copy
    first, then reprojects it from the same reconstructed event stream plus
    `trials.result.agent` metadata when the object copy is absent. If ATIF
    cannot be safely reprojected, the service returns HTTP 409 and the raw
    trajectory remains downloadable for debugging. Artifact `download_url`
    entries from trial detail return object bodies. The URLs must stay on
    `/api/v1/trials/...`, not raw MinIO/S3 signed URLs, and cross-team callers
    must not be able to use owner-team artifact proxy URLs.
    Verify the public CLI path with `loom eval trial download ...`; it should
    write the object body locally without printing internal object-store URLs.
14. **Batch-family delivery bundle.** For a release or customer handoff, create
    the one-command delivery archive from the finished source batch:
    ```bash
    loom eval batch delivery-bundle "$TEAM_A_BATCH_ID" \
      --output "$ROLLOUT_DIR/team-a-delivery-bundle.tar.gz"
    ```
    The default mode is the lightweight #390 delivery bundle: final ledger,
    selected trajectories, ATIF, manifest, payload checksums, and archive
    checksum sidecar. For raw provider/Harbor handoff, request the expanded
    layout explicitly:
    ```bash
    loom eval batch delivery-bundle "$TEAM_A_BATCH_ID" \
      --mode raw-harbor \
      --output "$ROLLOUT_DIR/team-a-raw-harbor-delivery.tar.gz"
    ```
    For the TB2/Phase 1 training handoff profile, request the versioned
    adapter explicitly:
    ```bash
    loom eval batch delivery-bundle "$TEAM_A_BATCH_ID" \
      --mode raw-harbor-tb2-v1 \
      --output "$ROLLOUT_DIR/team-a-raw-harbor-tb2-v1-delivery.tar.gz"
    ```
    If transient failures were repaired in linked rerun batches, pass each
    supplemental batch explicitly with repeated
    `--supplemental-batch-id "$RERUN_BATCH_ID"` flags. The service also follows
    linked rerun descendants when no explicit list is provided, but explicit ids
    make the rollout evidence auditable. Confirm the CLI prints the SHA-256,
    writes a `.sha256` sidecar, and exits only after verifying the downloaded
    archive against the service checksum. The CLI streams the archive to disk
    while hashing chunks; it must not hold the full bundle in memory.

    The corresponding API path is
    `POST /api/v1/batches/{id}/delivery-export`, followed by
    `GET /api/v1/batches/{id}/delivery-export` and the returned
    route-aware `/api/v1/batches/{id}/delivery-export/{artifact_id}/download`
    URL. On hosted staging/prod the returned URL must include the environment
    route prefix, for example `https://yylx.world/staging/api/v1/...` or
    `https://yylx.world/prod/api/v1/...`; users must not need to manually
    rewrite `/api` links. The SPA Batch Detail page should show the same
    Delivery bundle status, selected trial count, object counts, checksum, and
    download action.

    Inspect `manifest.json`, `summary.json`, `ledger/trials.jsonl`,
    `ledger/trials.csv`, `checksums/SHA256SUMS`, `atif/`, and
    `trajectories/` inside the archive. The
    manifest must list the main batch, supplemental batch lineage, deterministic
    selection rule, final selected trial per task/sample/combination, object
    storage evidence, and the payload checksum file. The archive checksum is
    exposed outside the tarball by the API/CLI, artifact metadata, and `.sha256`
    sidecar rather than self-referentially in `manifest.json`. Preparing the
    bundle requires submit/admin scope and must fail with a structured message
    before upload if any selected trajectory or ATIF object is unreadable, or if
    a task/sample/combination has no successful final trial after reruns.
    Service-side archive creation uses a bounded spool and streams object-store
    bodies into the tar writer while computing payload and archive SHA-256
    values; operators should treat any regression to full in-memory archive
    assembly as a production blocker for 100+ and 5000-trial raw exports.

    In raw Harbor modes, also inspect `provider_logs/manifest.json`,
    `task_bundles/<task_id>/...`, `agent_runs/<task_id>/<trial_id>/`, and
    `derived/sft_messages.jsonl`. Each selected trial gets
    `execution_result.json`, `metrics.json`, `artifact_manifest.json`,
    `verifier_output.json`, `provider_logs_manifest.json`, and `atif.json`.
    Base `raw-harbor` preserves the Loom-native event stream as
    `trajectory.jsonl` and leaves assistant payload schemas unchanged.
    `raw-harbor-tb2-v1` writes a TB2-facing `trajectory.json`, keeps the
    original Loom stream as `loom_trajectory.jsonl` timing/audit evidence, and
    reconstructs `derived/sft_messages.jsonl` from provider log payloads rather
    than the Loom event stream alone. Raw provider request/response logs are
    sourced from Gateway-routed `llm_calls` rows, preserve prompt/assistant
    payloads for training/audit handoff, and redact bearer values, provider API
    keys, secret-looking fields, and known secret text before persistence or
    export.

### Task compatibility and verifier-detail production gate (#387/#379/#369/#361)

Run this gate before a formal Source Useful or user-brought TaskSet production
roll that depends on Dockerfile task images and script verifier diagnostics.
The goal is to prove Loom reports incompatible bundles before expensive worker
fanout and preserves verifier diagnostics once a trial reaches verifier output.

Use a fresh evidence directory under the candidate rollout:

```bash
export COMPAT_GATE_DIR="$ROLLOUT_DIR/task-compat-verifier-gate-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$COMPAT_GATE_DIR"/{fixtures,publish,canaries,api}
loom auth status | tee "$COMPAT_GATE_DIR/api/auth-status.txt"
loom auth whoami | tee "$COMPAT_GATE_DIR/api/whoami.txt"
```

**1. Local code-path sanity.** On the exact candidate source checkout, run the
targeted tests that pin the gate's non-network behavior:

```bash
uv run --no-sync pytest \
  tests/unit/test_task_bundle_compat.py \
  tests/unit/test_loom_benchmark_tool_safety.py \
  tests/integration/test_taskset_materialization.py \
  tests/integration/test_local_benchmark_publish.py \
  tests/unit/test_verifier_result.py \
  tests/unit/test_script_verifier.py \
  tests/unit/test_main_loop_cleanup.py \
  -q | tee "$COMPAT_GATE_DIR/local-targeted-tests.txt"
```

**2. Catalog publish preflight must fail by default.** Create a deliberately
incompatible Source Useful-style local benchmark whose Dockerfile copies the
bundle to `/app/` but references a file that only exists under
`environment/`:

```bash
BAD_BENCH="$COMPAT_GATE_DIR/fixtures/source-useful-bad-layout"
mkdir -p "$BAD_BENCH/tasks/app-path-missing/environment"
cat > "$BAD_BENCH/benchmark.toml" <<'TOML'
schema_version = 1
id = "source-useful-bad-layout"
display_name = "Source Useful bad layout"
series = "operator-validation"
license_spdx = "MIT"
TOML
cat > "$BAD_BENCH/tasks/app-path-missing/task.toml" <<'TOML'
schema_version = "1"

[task]
id = "app-path-missing"
name = "App path missing"

[environment]
os = "linux"
dockerfile = "environment/Dockerfile"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
TOML
printf 'do task\n' > "$BAD_BENCH/tasks/app-path-missing/instruction.md"
printf '#!/bin/sh\necho setup\n' > "$BAD_BENCH/tasks/app-path-missing/environment/setup_repo.sh"
cat > "$BAD_BENCH/tasks/app-path-missing/environment/Dockerfile" <<'DOCKER'
FROM debian:bookworm
COPY . /app/
RUN chmod +x /app/setup_repo.sh && /app/setup_repo.sh
DOCKER
```

Then run `publish-local` without overrides. The command assumes `LOOM_DB_URL`
and `LOOM_MINIO_*` are already exported; do not add credential flags because
those values can appear in process argv. It must exit non-zero before upload or
task-row registration and print `TASK_COMPAT_APP_PATH_MISSING`:

```bash
set +e
loom datasets publish-local "$BAD_BENCH" \
  2>&1 | tee "$COMPAT_GATE_DIR/publish/bad-layout-default.txt"
bad_rc=${PIPESTATUS[0]}
set -e
test "$bad_rc" -ne 0
grep -q 'TASK_COMPAT_APP_PATH_MISSING' \
  "$COMPAT_GATE_DIR/publish/bad-layout-default.txt"
```

If a legacy operator bridge is explicitly approved, rerun with retained
evidence. The command must show `compat_flattened_files=<N>`; absence of that
line means the bridge is hidden and the gate fails:

```bash
loom datasets publish-local "$BAD_BENCH" \
  --compat-flatten-environment \
  2>&1 | tee "$COMPAT_GATE_DIR/publish/bad-layout-explicit-flatten.txt"
grep -q 'compat_flattened_files=' \
  "$COMPAT_GATE_DIR/publish/bad-layout-explicit-flatten.txt"
```

**3. DNS/NSS mutation must be a compatibility failure.** Validate either a
TaskSet materialization row or a one-task canary whose Dockerfile mutates
`/etc/resolv.conf`, `/etc/nsswitch.conf`, or `/etc/hosts` before Loom installs
the service-mode agent layer. The accepted evidence is one of:

- materialization error JSON containing `code=TASK_COMPAT_DNS_MUTATION`,
  `phase=agent_layer_build`, and a remediation hint; or
- a submitted canary trial with `state=failed`,
  `failure_reason=task_compatibility`, and a failure message containing
  `TASK_COMPAT_DNS_MUTATION`.

Capture the API/CLI evidence:

```bash
loom tasksets status "$DNS_TASKSET_SLUG_OR_ID" --format json \
  | tee "$COMPAT_GATE_DIR/canaries/dns-taskset-status.json"

loom eval trial show "$DNS_COMPAT_TRIAL_ID" --format json \
  | tee "$COMPAT_GATE_DIR/canaries/dns-trial.json"
loom eval diagnose trial "$DNS_COMPAT_TRIAL_ID" --format json \
  | tee "$COMPAT_GATE_DIR/canaries/dns-trial-diagnosis.json"
```

**4. Corrected clean bundle must run.** Publish or select the corrected bundle
that will be used for production, run a small batch on the intended worker
pool, and save the batch/trial evidence:

```bash
loom eval batch create \
  --agent "$AGENT" \
  --provider "$PROVIDER" \
  --model "$MODEL" \
  --benchmark "$CORRECTED_BENCHMARK_ID" \
  --task-filter "@$COMPAT_GATE_DIR/corrected-task-filter.json" \
  --n-per-task 1 \
  --name "compat-verifier-gate-${CORRECTED_BENCHMARK_ID}" \
  --required-worker-pool gb10 \
  | tee "$COMPAT_GATE_DIR/canaries/corrected-batch-create.txt"

loom eval batch show "$CORRECTED_BATCH_ID" --format json \
  | tee "$COMPAT_GATE_DIR/canaries/corrected-batch.json"
loom eval batch debug "$CORRECTED_BATCH_ID" --format json \
  | tee "$COMPAT_GATE_DIR/canaries/corrected-batch-debug.json"
```

Acceptance: no trial in the corrected canary has
`failure_reason=task_compatibility`, `task_image_build`, `agent_layer_build`,
or `verifier_error`. Reward `0` is acceptable only when verifier output is
present and the failure is model/task score, not platform execution.

**5. Script verifier detail must be preserved.** Include at least one canary
task whose script verifier emits `checks[].detail`, including a legacy string
detail such as `"exit_code=1"` or a structured object. The trial must reach a
scored terminal outcome instead of `failure_reason=verifier_error`.

Save the detail-bearing trial and verify the field survives API projection:

```bash
loom eval trial show "$VERIFIER_DETAIL_TRIAL_ID" --format json \
  | tee "$COMPAT_GATE_DIR/canaries/verifier-detail-trial.json"
jq -e '
  .result.verifier.checks
  | any(.detail != null and .detail != {})
' "$COMPAT_GATE_DIR/canaries/verifier-detail-trial.json"
jq -e '.failure_reason == null or .failure_reason != "verifier_error"' \
  "$COMPAT_GATE_DIR/canaries/verifier-detail-trial.json"
```

**6. Issue evidence comment.** Link the evidence directory and summarize:

- candidate image/source SHA and staging rollout directory;
- commands above with exit status;
- incompatible bundle diagnostics for `TASK_COMPAT_APP_PATH_MISSING` and
  `TASK_COMPAT_DNS_MUTATION`;
- corrected canary batch id, trial counts, and worker pool;
- verifier-detail trial id and the observed `checks[].detail` shape;
- any explicit `--compat-flatten-environment` use with
  `compat_flattened_files=<N>`.

Keep #387/#379/#369/#361 open as `[Needs validation]` if any accepted evidence
is missing. Do not close them from local tests alone.

15. **Run Library sharing.** Confirm the completed source run appears in Run
    Library -> My team for Team A and Run Library -> All teams for Team B.
    Evidence must include the owner-team label, completed state, score/cost
    summary, task/agent/model summary, bounded artifact badges, diagnosis,
    debug evidence, and artifact groups. Team B must be able to download a safe
    artifact only through the Run Library service URL.
16. **Clone and reuse.** From Team B, clone config from Team A's completed run.
    If the source run used a provider connection, select a Team B-owned
    provider connection before cloning. Then reuse a safe artifact from the
    source trial. Both created records must belong to Team B and show
    `source_provenance` with the source batch/trial/artifact key.
16. **Blocked and private access denied.** Team B must be denied when trying to:
    download the seeded blocked artifact through Run Library; download Team A's
    artifact through the normal owner-team trial route; mutate Team A's original
    batch, such as cancelling it; or inspect/download private or blocked source
    artifacts. Denials should include safe reasons only.
17. **Provider error surfaces.** Create a disposable smoke-team provider
    connection backed by a dedicated invalid test secret (or use the approved
    mock provider), re-run a trial, and confirm the SPA + API
    surface a clear `provider_error` reason rather than a generic 500. Confirm
    diagnostic text does not contain raw provider keys, bearer tokens, signed
    URL query parameters, or internal service hostnames.
    Never rotate or overwrite a protected canonical shared-staging provider
    secret for this negative test; remove only the disposable connection and
    test secret when its evidence is complete.
    For transient provider/gateway transport drops, confirm diagnostics use
    `provider_transport_disconnect` rather than `internal_error` and that the
    trial retry budget is consumed before a terminal failure is recorded.
18. **Automated evidence script.** After steps 4-16, run the repeatable API
    gate. Use disposable staging data because clone/reuse checks create Team B
    records. Pass smoke-gate tokens, MinIO credentials, and explicit secret
    needles as `env:VAR`, `file:PATH`, or `-` sources; do not expand raw secret
    values into argv:
    ```bash
    python scripts/staging_smoke_gate.py \
      --server-url https://loom.example.com \
      --team-a-token env:TEAM_A_TOKEN \
      --team-b-token env:TEAM_B_TOKEN \
      --provider-connection-name mz_tn_canada_qianyi \
      --provider-model-provider yibuapi \
      --provider-model-name glm-5.1-thinking \
      --batch-id "$TEAM_A_BATCH_ID" \
      --trial-id "$TEAM_A_TRIAL_ID" \
      --safe-artifact-key "$SAFE_ARTIFACT_KEY" \
      --blocked-artifact-key "$BLOCKED_ARTIFACT_KEY" \
      --private-trial-id "$PRIVATE_TRIAL_ID" \
      --private-artifact-key "$PRIVATE_ARTIFACT_KEY" \
      --clone-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
      --reuse-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
      --catalog-minio-endpoint "$STAGING_MINIO_ENDPOINT" \
      --catalog-minio-access-key env:STAGING_MINIO_ACCESS_KEY \
      --catalog-minio-secret-key env:STAGING_MINIO_SECRET_KEY \
      --object-store-write-check \
      --object-store-write-check-bucket trajectories \
      --object-store-write-check-count 64 \
      --object-store-write-check-concurrency 16 \
      --k8s-namespace loom-staging \
      --required-worker-pool gb10 \
      --secret-needle env:STAGING_SECRET_NEEDLE \
      --internal-url-needle loom-minio.loom.svc.cluster.local \
      --allow-mutating-checks \
      --fail-on-skip \
      --markdown-output staging-smoke.md \
      --json-output staging-smoke.json
    ```
    Attach `staging-smoke.md` or paste its table into the release comment.
    Store `staging-smoke.json` with release artifacts if the environment has
    a private artifact store. The script redacts raw API tokens, seeded fake
    secrets, provider-key-like values, signed object-store URLs, and internal
    service URLs before writing evidence. The `auth.team_a_whoami` and
    `auth.team_b_whoami` rows must prove non-admin user-owned API tokens; a
    `legacy_team_token` or `platform_admin` principal fails because it cannot
    exercise cross-team negative checks. If an API request times out or raises a
    transport exception, the script records a failed row with method, endpoint,
    timeout, and response detail, then still writes Markdown/JSON evidence. The
    `object_store.minio_write_probe` row must pass before submitting canary or
    release-trial work; if it reports `XMinioStorageFull`, connection failures,
    or timeouts, reclaim/provision MinIO-backed storage or reduce worker
    concurrency first instead of discovering the failure during worker
    trajectory or artifact upload. The final `service.no_oom_restarts` row
    must pass for full100/release evidence after all HTTP/API route probes; if
    it reports a restart-count increase during the smoke, an `OOMKilled` last
    state, or an unexpected current restart count, inspect `loom-service`
    memory, previous pod logs, and large batch detail/cancel traffic before
    accepting the gate. For v1.0's GB10-only gate, the
    `runs.worker_pool_coverage` row must pass with
    `--required-worker-pool gb10`; a missing pool means the batch did not
    produce deterministic terminal evidence on the GB10 worker pool. For
    v1.1/full-cluster OLDLAB-required evidence, repeat the same pattern with an
    additional `--required-worker-pool oldlab` constraint so the gate is
    deterministic rather than a post-hoc DB distribution check.
19. **Teardown clean.** On shared staging, delete only the disposable smoke
    teams, users, provider connections, runs, and test objects created by this
    checklist through their supported application APIs. Never run `loom cluster
    down`, delete the namespace/PVCs, or use destructive flags as smoke
    teardown. If the shared cluster itself needs recovery, preserve the request
    evidence and use broker resume or admission-disabled root maintenance. A
    development/custom cluster may use its separately scoped cluster teardown.

A staging release that fails any check is NOT eligible for `main`.
Capture artifact links + a brief note for each pass in the
`docs/release-history.md` entry (or the equivalent for your fork).

### Automation status

Two CI workflows plus the staging smoke script automate parts of this
checklist:

- **`cluster-smoke`** (kind, label-gated `cluster-smoke`) — covers
  steps 1, 3, 17. Uses placeholder images, `--no-wait` apply, schema
  + boundary + apply + status + down round-trip. Fast (~1 min).
- **`staging-smoke`** (kind, label-gated `staging-smoke`) — builds
  REAL images, applies them, waits for every pod to reach Ready,
  probes `/healthz` + `/metrics` on every component. In parallel on a separate
  runner, its manifest-owned `system-smoke` job executes the Compose full-stack
  lane selected by `config/component-ownership.toml`; the aggregate gate
  requires both jobs when the planner selects staging validation. This closes the
  cold-start and dormant-system-fixture regression gaps (~15-20 min). The
  system lane applies Alembic migrations explicitly, builds current service and
  worker images, mounts the manifest-owned test fixtures read-only into the worker,
  uses only test-local credentials, preserves redacted Compose diagnostics before
  teardown on failure, and always removes its containers and volumes. This required PR gate is
  credential-free and renders `runtime_environment=development`; it proves the
  staging-only admin exchange stays hidden with a `404`, but does not perform
  authenticated staging acceptance. It never enters `ci-aws` and does not claim
  real AWS S3 coverage. Record authenticated staging and real-AWS validation only
  from separately protected, brokered post-merge/release runs; a missing or
  skipped protected run is not evidence.
- **`scripts/staging_smoke_gate.py`** — covers public health, logged-out SPA
  reachability, two-team non-admin user-owned API-token auth, provider/model
  discovery, runnable benchmark catalog presence, sampled ready benchmark bundle objects,
  concurrent object-store write/delete probing, final service pod restart/OOM status,
  batch/trial detail, `claimed_without_started=0` from batch debug evidence,
  required terminal worker-pool coverage from batch debug evidence,
  service-proxied ATIF/trajectory downloads, My team and All teams Run Library
  visibility, owner-team label, cross-team safe artifact download,
  direct-route denial,
  clone config, reuse artifact, provenance, blocked artifact denial, private
  artifact denial, cross-team mutation denial, structured API timeout/transport
  failures, and response leak scanning.
- **`scripts/ops/worker_service_tunnels.py`** — covers the private
  remote-worker tunnel gate when out-of-cluster workers are attached. It renders
  durable systemd user units, installs the watchdog timer that restarts stale
  active-looking tunnel units after repeated failed probes, emits secret-free
  watchdog evidence with the resolved env-file path, checks the exact
  worker-facing URLs locally, includes the optional subprocess Gateway facade
  probe, and verifies those URLs from SSH worker hosts.

Browser-only invite acceptance, SPA visual submission, and provider-error UI
screenshots remain manual release evidence unless the staging environment adds a
mock provider and browser automation job.

For the final staging #49/#129 full/max-slot three-cluster canary, use
[`docs/runbooks/full-max-slot-canary-runbook.md`](full-max-slot-canary-runbook.md).
That runbook is GO-gated: prepare the commands, preflight checklist, stop
conditions, and evidence directory up front, but do not submit the canary until
the coordinator confirms the clean anchor and #190 targeted durability evidence.

## Terminal-Bench 2.1 revision-6 staging readiness

The `terminal-bench-2` public selector may execute only the immutable
`terminal-bench-2@tb2.1-r6` profile. Harbor Hub
`terminal-bench/terminal-bench-2-1@6`, its metadata version, and all 89 locked
package digests are the execution authority. The old 86-task TB2.0 catalog is
historical/read-only and is never an execution, rollback, or smoke fallback.
The exercises in this section are cluster-side #749 acceptance gates that unit
CI cannot supply.

After a successful broker rollout of the merged candidate, disposable G3/G4/
G6/G9 staging trials and smoke checks may run independently. Only G5 catalog
publish/register/mirror is restricted: shared staging gets it from broker step
11 and operators inspect the redacted `catalog-provisioning` artifact. The G5
manual credentials and commands below are development/custom examples;
production requires its separately authorized release path.

Prerequisites:

- `kubectl` configured against the target cluster (use the env-scoped
  kubeconfig from `LOOM_KUBECONFIG_B64`, not your personal one).
- `loom` CLI on PATH (from the repo root, run
  `uv sync --locked --all-packages --extra cluster --extra rollout --python 3.11`
  and then invoke `uv run --no-sync loom`, or use the release tarball).
- A team API token with permission to launch batches.
- The MinIO endpoint, access key, and secret key for the target environment.
- Object-store credentials authorized to publish the immutable bundle prefix.

### Direct publish, register, fresh-audit, and activate

The bundle is large enough that pulling it from Hugging Face at trial-time
saturates the worker setup budget. Publish it directly to the deployment-owned
object store, register that exact content-addressed revision, and audit:

```bash
# Development/custom example only; never substitute shared-staging secrets.
export LOOM_DB_URL="postgresql+psycopg://loom:$LOOM_DB_PASS@dev-db.yylx.world:5432/loom_dev"
export LOOM_MINIO_ENDPOINT=https://minio.dev.yylx.world
export LOOM_MINIO_ACCESS_KEY=...
export LOOM_MINIO_SECRET_KEY=...

loom datasets publish terminal-bench-2 --target object-store
# Copy the rev=<16-hex> value from the successful publish output.
export TB21_OBJECT_REVISION=<published-revision>
loom datasets register terminal-bench-2 --source object-store \
  --revision "$TB21_OBJECT_REVISION"
loom datasets audit terminal-bench-2@tb2.1-r6 \
  --tb21-audit-json "$PWD/tb21-audit.json" \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT"
loom datasets activate terminal-bench-2 \
  --profile terminal-bench-2@tb2.1-r6 \
  --audit-json "$PWD/tb21-audit.json" \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
  --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
  --minio-secret-key "$LOOM_MINIO_SECRET_KEY"
```

Acceptance:

- registration leaves the physical profile `pending` and direct submission is
  rejected;
- audit and activation each inspect all 89 packages and report
  `valid_bundles=89`,
  `missing_bundles=0`, `mismatched_bundles=0`.
- activation repeats the audit against current object-store bytes inside the
  alias transaction, then makes the profile `runnable`;
- a fresh post-activation audit reproduces the same snapshot identity;
- workers rehash materialized bundles before image or driver startup.

### Native task contract classification

Classify the current 89 packages from their preserved native schema-1.1
`upstream-task.toml`, then confirm the normalized Loom `task.toml` retains the
supported contract. Record at least these dimensions in the audit evidence:

- `[environment].docker_image` versus `dockerfile` plus
  `docker_build_context` and `build_timeout_sec`;
- `[environment].architecture`/`cpu_arch` and any GPU requirement;
- declared environment variables, network policy, DNS/hosts/tmpfs,
  healthcheck, user, and workdir;
- `[agent].timeout_sec`/`setup_timeout_sec` and
  `[verifier].timeout_sec`/`env_mode`.

Do not infer these classes from historical task names, old task YAML fields, or
Docker Compose sidecar assumptions. Service-mode trials must resolve exactly
the image/build and architecture recorded for the selected rev-6 package.

Authenticate first:

```bash
loom auth login --server "$LOOM_API_URL"
```

### G3 — Live cluster end-to-end (easy + hard task)

Select one known-pass and one verifier-sensitive task from the clean 89/89
audit. Record their physical IDs; do not infer names from historical TB2.0.
A successful trial must land verifier output with a numeric reward plus the
ATIF and trajectory in object storage.

```bash
mkdir -p ./tb2-evidence

for task in "$TB21_KNOWN_PASS_TASK_ID" "$TB21_VERIFIER_SENSITIVE_TASK_ID"; do
  case "$task" in terminal-bench-2@tb2.1-r6/*) ;; *) exit 1 ;; esac
  loom eval run --agent oracle --task "$task"
  # `loom eval run` prints the trial_id; wait for terminal state, then:
  #   loom eval trial show <trial_id> --json > "./tb2-evidence/${task//\//_}.json"
  #   loom eval artifact get <trial_id> atif > "./tb2-evidence/${task//\//_}.atif.json"
done
```

Acceptance:

- Both trials end with platform `state=succeeded` and a finite numeric reward.
  Reward `0` is a valid scored result; missing reward is a platform/verifier
  failure.
- ATIF JSON and trajectory blobs are downloadable through the Run Library SPA.
- The trial's `verifier.rewards` JSON validates against the
  `loom.models.verifier.VerifierResult` shape — `to_tb2_report()` consumes
  it to produce the canonical TB-2 `BenchmarkResults` shape.

Store new evidence in the candidate-bound #749 evidence root and link it from
#749. Existing `docs/evidence/issue-217/**` files remain historical and are not
rewritten; #749 acceptance does not close or re-adjudicate #217.

### Architecture and environment-sensitive tasks

Select coverage from every distinct native `task.toml` image/build,
architecture, and environment class recorded above. Run the complete selected
class matrix; do not reduce it to remembered TB2.0 names or compose-service
categories.

```bash
for task in ${TB21_ENVIRONMENT_SENSITIVE_TASK_IDS:?}; do
  case "$task" in terminal-bench-2@tb2.1-r6/*) ;; *) exit 1 ;; esac
  loom eval run --agent oracle --task "$task"
done
```

Acceptance:

- resolved task image/build digest and runtime architecture match the audited
  native package and recorded Loom image provenance;
- environment fields supported by the normalizer are present unchanged in the
  runnable config and effective driver inputs;
- unsupported or incompatible architecture/image/environment contracts fail
  before agent execution with task-level classification rather than silently
  substituting a default image or host;
- private `solution/`, `tests/`, `verifier/`, and `upstream-task.toml` paths
  appear only in the fresh verifier driver.

### G6 — Provider × Terminal-Bench-2 matrix

The staging agent catalog (PR #177) ships Claude Opus 4.7, Sonnet 4.6,
and Haiku 4.5. Run one TB-2 task per provider to confirm tool-loop reach to
verifier output. Set `TB21_PROVIDER_SMOKE_TASK_ID` to one short physical task
from the clean current audit for cost discipline.

```bash
# Replace --provider/--model with the staging connection name for each
# Claude SKU; `loom providers list` shows what's configured.
case "${TB21_PROVIDER_SMOKE_TASK_ID:?}" in
  terminal-bench-2@tb2.1-r6/*) ;;
  *) exit 1 ;;
esac
for agent_model in \
  "claude-code|anthropic|claude-opus-4-7-20260101" \
  "claude-code|anthropic|claude-sonnet-4-6-20251202" \
  "claude-code|anthropic|claude-haiku-4-5-20251001"; do
  IFS='|' read -r agent provider model <<< "$agent_model"
  loom eval run \
    --agent "$agent" \
    --provider "$provider" \
    --model "$model" \
    --task "$TB21_PROVIDER_SMOKE_TASK_ID"
done
```

Acceptance:

- Each trial reaches `verifier_output_emitted=true` with a numeric reward
  (pass or fail is fine — provider×TB2 reachability is what we are
  verifying, not the score).
- No trial fails with `provider tool-loop incompatibility` or
  `unsupported tool-use schema`.
- Link the run IDs into #35's evidence comment.

### G9 — Resource-budget profiling

TB2.1 preserves native schema-1.1 `[agent].timeout_sec` and
`setup_timeout_sec`, `[verifier].timeout_sec`, and environment
`build_timeout_sec` where declared. Profile the normalized values; do not read
legacy `max_*_timeout_sec` task YAML fields.

Profile a representative slice (one short, one medium, one long task):

```bash
for task in \
  "$TB21_SHORT_TASK_ID" \
  "$TB21_MEDIUM_TASK_ID" \
  "$TB21_LONG_TASK_ID"; do
  case "$task" in terminal-bench-2@tb2.1-r6/*) ;; *) exit 1 ;; esac
  trial_id=$(loom eval run --agent oracle --task "$task" --json \
    | jq -r .trial_id)
  loom eval trial show "$trial_id" --json \
    > "./tb2-profile/${task//\//_}.trial.json"
done
```

Then for each `.observe.jsonl`:

```bash
python -m scripts.ops.summarize_observe \
  --observe ./tb2-profile/<task>.observe.jsonl \
  --emit-budget-table
```

Acceptance:

- For each profiled task, the observed build/setup/agent/verifier durations are
  within the corresponding native `task.toml` budgets.
- If any task exceeds the sandbox per-trial wall-clock, record the override
  in `deploy/environments/<env>.profile` under `[task_budget_overrides]` and
  re-run.

### Closing #749

When all five exercises above produce green evidence:

- Comment on #749 with the artifact links (`--tb2-report` outputs, run IDs,
  audit logs).
- Drop the `[WIP]` prefix from the title.
- Close the issue.

This closure applies only to #749's rev-6 profile scope. Do not close #217 or
rewrite its 86-task historical evidence as part of this acceptance pass.

If any exercise blocks, keep #749 open and record the failure mode. Do not
activate a reduced profile, substitute TB2.0, or merge incomplete evidence.

## Capacity planning

- 1 vCPU + 256 MiB per Control Plane replica handles ~200 RPS for
  PATCH/GET; bump if `state_patch_total{result="timeout"}` non-zero.
- Each Worker process runs `LOOM_WORKER_MAX_CONCURRENT` trials. In
  Kubernetes render output, set this through `[worker_capacity]` in
  `cluster-config.toml`; the default render is 3 worker replicas at 16
  trials each. Memory scales with the trajectory ring buffer + the
  largest artifact in flight.
- Monitor and `loom resources status` show concurrent task slots as
  `occupied / current_active_slots` and group them by worker resource pool.
  They also expose `pending_slots`, `desired_slots`, `max_slots` /
  `ceiling_slots`, the autoscaler actuator, last decision reason, and blocked
  reason. Kubernetes render labels the baseline pool as `k8s-worker`; remote
  workers should set `LOOM_WORKER_POOL_NAME` to stable names such as `oldlab`
  or `gb10`.
- Remote worker pools should set `LOOM_WORKER_HOSTNAME` to the physical or VM
  node name before startup. Otherwise Docker Compose workers may register with
  container hostnames, which makes Monitor and capacity evidence harder to map
  back to GB10/OLDLAB hosts.
- The Control Plane crash detector has two reclaim paths. Dead or stale worker
  heartbeat ownership requeues or retries the trial as before. The #378
  stale-running path is intentionally more conservative: it only fails a
  `running` trial when the worker heartbeat is still fresh, runtime exceeds the
  effective agent timeout backstop, and both trial events and LLM calls have
  been silent beyond the silence window. Defaults are
  `LOOM_CP_STALE_RUNNING_TRIAL_RECLAIM_ENABLED=true`,
  `LOOM_CP_STALE_RUNNING_TRIAL_TIMEOUT_MULTIPLIER=3.0`,
  `LOOM_CP_STALE_RUNNING_TRIAL_GRACE_SEC=900.0`, and
  `LOOM_CP_STALE_RUNNING_TRIAL_SILENCE_SEC=900.0`. Lower these only for a
  controlled validation batch; keep staging defaults conservative. The
  service debug/diagnosis projection uses the corresponding
  `LOOM_SVC_WORKER_HEARTBEAT_EXPIRY_SEC` and `LOOM_SVC_STALE_RUNNING_*`
  settings, so tune service and Control Plane values together when validating
  non-default reclaim behavior.
- Example higher-capacity render config:

  ```toml
  [replicas]
  worker = 8

  [worker_capacity]
  max_concurrent = 32
  cpu_request = "4"
  cpu_limit = "32"
  memory_request = "16Gi"
  memory_limit = "128Gi"
  ```

- Each Worker process also derives `LOOM_WORKER_BLOCKING_IO_MAX_WORKERS`
  as `max(32, min(LOOM_WORKER_MAX_CONCURRENT * 4, 256))` when unset.
  This is the thread pool for blocking Docker, S3/MinIO, Hugging Face,
  and filesystem calls; it is not additional trial capacity.
- Cold Docker setup/build work has its own daemon-wide cap:
  `LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT`. Leave it at `1` for shared
  OLDLAB/k8s Docker daemons so task-image builds, layered trial-cache builds,
  and sidecar image pulls/builds serialize even when
  `LOOM_WORKER_MAX_CONCURRENT` admits many warm trials. The setup-health guard
  also blocks or delays this work under high I/O pressure, low swap, or high
  D-state counts. Raise the cap only for isolated Docker daemons with measured
  disk/containerd headroom.
- Docker and S3 have separate worker-side timeout/pool knobs. Use
  `LOOM_WORKER_DOCKER_API_TIMEOUT_SEC` for docker-py read timeouts during
  large pulls/builds/sidecars, and the `LOOM_WORKER_MINIO_*` timeout and
  pool knobs for object-store materialization, artifact upload, and
  trajectory flush pressure. These do not increase trial capacity; they
  keep each admitted trial from failing because the SDK default is too
  small for high-concurrency benchmark sweeps.
- Docker-backed workers need a high open-file limit for high sandbox
  concurrency. The dev and remote-worker compose files set
  `nofile=65536`; equivalent production deployments should set the same
  limit at the container runtime or node service layer before sweeps.
- For shared-dev or staging hosts outside Kubernetes, use
  [remote-worker-pool.md](remote-worker-pool.md). For OLDLAB-style
  production capacity, inventory every candidate node, generate
  `worker-plan.csv`, and attach every usable node unless excluded with a
  reason. Use `scripts/ops/worker_pool_slurm_submit.sh` for manual smoke
  launches; use the Control Plane elastic Slurm controller for normal
  OLDLAB capacity so Slurm latency stays out of the batch submit path.
- The default production shape is a fixed Kubernetes worker baseline plus
  OLDLAB 1-5 as elastic capacity. The staged OLDLAB inventory, launch plan,
  and controller env example live in `deploy/worker-pools/oldlab/`. Configure
  each environment independently. The env-driven Slurm controller uses one
  fixed concurrency slice per submitted job:

  ```bash
  LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=true
  LOOM_CP_SLURM_WORKER_CONTROLLER_ENVIRONMENT=production
  LOOM_CP_SLURM_WORKER_CONTROLLER_POOL_NAME=oldlab
  LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES=oldlab-1,oldlab-2,oldlab-3,oldlab-4,oldlab-5
  LOOM_CP_SLURM_WORKER_CONTROLLER_ENV_FILE=/secure/path/.env.remote-worker
  LOOM_CP_SLURM_WORKER_CONTROLLER_REPO_DIR=/opt/loom
  LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_CPUS=12
  LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_MEMORY_MIB=58000
  LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_CONCURRENCY=6
  LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS=5
  LOOM_CP_SLURM_WORKER_CONTROLLER_PENDING_JOB_CAP=2
  ```

  `MAX_JOBS` bounds running plus pending Slurm jobs. `PENDING_JOB_CAP` stops
  new submits when existing pending jobs already reach the threshold, which
  avoids piling up Slurm queue entries when OLDLAB is busy. Raise per-node
  concurrency only after CPU, RAM, Docker cleanup, MinIO throughput,
  Gateway/provider error rate, and Control Plane state-patch health are clean.
  The remote-worker env file and `REPO_DIR` must be readable from every
  included Slurm node. For OLDLAB staging capacity, use a shared checkout
  path such as `/shared_work/<operator>/loom-remote-worker-${IMAGE_TAG}`; a
  control-node `/home` checkout can be incomplete on OLDLAB 4/5 and must not be
  assumed valid without a Slurm-side check.
  Autoscaler Slurm jobs default to exclusive node allocation. For shared
  OLDLAB validation where a node already has another small Slurm job, set the
  policy `actuator_config.exclusive=false` only with a reduced, tested CPU,
  memory, and worker-concurrency slice; leave exclusive allocation enabled for
  full-node production capacity.
  Slurm worker jobs install an `EXIT`/`INT`/`TERM` trap around Docker Compose;
  normal exits, idle exits, and `scancel` release the compose worker container
  with `docker compose down --remove-orphans` instead of leaving an orphaned
  worker outside Slurm accounting.
  For normal shared OLDLAB autoscaling, prefer a worker-pool autoscaler policy
  with `actuator_config.resource_aware=true` instead of raising
  `REQUESTED_CONCURRENCY` manually. The conservative OLDLAB policy keeps
  `min_slots=1`, sets `max_slots=40`, allows only OLDLAB-1..5, and computes
  each submitted worker's concurrency from live `sinfo` data:

  ```json
  {
    "actuator": "slurm",
    "enabled": true,
    "min_slots": 1,
    "max_slots": 40,
    "actuator_config": {
      "external_runner": true,
      "allowed_nodes": [
        "TRT-EAI-OLDLAB-1",
        "trt-EAI-OLDLAB-2",
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5"
      ],
      "env_file": "/shared_work/qianyi/loom-worker-capacity/staging-oldlab-worker-${IMAGE_TAG}.env",
      "repo_dir": "/shared_work/qianyi/loom-remote-worker-${IMAGE_TAG}",
      "requested_cpus": 2,
      "requested_memory_mib": 8192,
      "requested_concurrency": 1,
      "max_jobs": 5,
      "pending_job_cap": 2,
      "resource_aware": true,
      "cpu_per_slot": 2,
      "memory_mib_per_slot": 8192,
      "reserved_cpus": 4,
      "reserved_memory_mib": 24576,
      "max_concurrency_per_node": 8,
      "max_cpu_load_ratio": 1.0,
      "exclusive": true
    }
  }
  ```

  The safety formula is
  `min((idle_cpus_or_total_cpus - reserved_cpus) / cpu_per_slot,
  (free_memory_mib - reserved_memory_mib) / memory_mib_per_slot,
  max_concurrency_per_node)`, floored to whole slots. Nodes are excluded when
  they already have a Loom Slurm job, Slurm state is not idle/mixed, resource
  data is missing, CPU load is above `cpus_total * max_cpu_load_ratio`, free
  memory is below the reserve plus one slot, or idle CPU is below the reserve
  plus one slot. With `max_concurrency_per_node=8`, five safe OLDLAB nodes can
  reach 40 slots; if current node load is near 24 on 24-CPU hosts, the
  conservative policy blocks scale-up with
  `last_blocked_reason=no_safe_slurm_nodes` and records
  `last_blocked_details.node_exclusions` in autoscaler status JSON. Text output
  from `loom admin worker-pools autoscaler status` and `loom resources status`
  summarizes those exclusions as `node:reason`, so operators do not have to
  reconstruct CPU, memory, state, or active-job exclusions from raw `sinfo`.
  The same hard blockers appear in `environment-state check --format json` as
  `autoscaler_blockers`, and `loom cluster release-gate` fails the
  environment-state convergence row while they are present.
  Temporarily exclude a node by removing it from `ALLOWED_NODES`; lower
  footprint with `MAX_JOBS`; lower per-node pressure with
  `REQUESTED_CONCURRENCY`; disable the pool with
  `LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=false`.
- For GB10 ARM64 capacity, use the staged runbook and evidence under
  `deploy/worker-pools/gb10/`, but manage normal scale-up/scale-down through
  the Slurm autoscaler policy with `actuator=slurm`,
  `actuator_config.partition=gb10`, `actuator_config.cpu_arch=arm64`, and
  `pool_name=gb10`. The backend still displays as `docker` because the
  workers run Docker sandboxes; the autoscaler actuator displays as `slurm`
  because capacity comes from the GB10 Slurm partition. GB10 hosts attach
  through private loopback worker-service tunnels, run the worker compose
  service with `network_mode: host`, and set
  `LOOM_WORKER_HOSTNAME=trt-gb10-N` so Monitor and database evidence map
  workers to physical hosts. Set `LOOM_WORKER_POOL_NAME=gb10` so slot
  summaries and metrics group the hosts together. Keep Docker data-root, worker
  trajectory cache, benchmark cache, and trial scratch on each node's local
  ext4 disk; do not put those hot paths on `/shared_work`. Current staging
  sandbox validation uses all 15 `trt-gb10-1..15` hosts at
  `LOOM_WORKER_MAX_CONCURRENT=8`, for 120 configured ARM64 Slurm slots, with
  `excluded_nodes=[]`. If a host is busy, candidate-owned drain/quiescence
  defers disruptive convergence without cancelling or preempting external
  work. The 150-slot figure elsewhere is the physical/legacy node-agent
  ceiling, not the sandbox policy. Every shared-staging rollout must use the broker,
  which applies and checks the versioned environment profile, verifies the
  OLDLAB tunnel and Slurm prerequisites, prepares all declared GB10 hosts, and
  evaluates the release gate inside the candidate-bound request envelope.
  Operators inspect `environment-state-check`, `gb10-workers-status`, tunnel,
  and release-gate evidence through `loom-staging-rollout status` and `logs`;
  they do not run the underlying admin or per-host apply commands directly.

  The GB10 gate checks image tag, env-config version, desired
  source git commit, active-vs-draining host intent, worker-token/env drift,
  and clean source-checkout provenance. Active nodes with missing provenance,
  dirty source, or a git commit that does not match desired `source_git_commit`
  must be treated as stale even if their image/env fields look current. Active
  GB10 nodes must also include a fresh worker registry link: `worker_id`
  present, `worker_status=active`, `worker_fresh=true`, and
  `worker_backend_names` containing `docker`. A node-agent `already current`
  result without that linked worker evidence is not sufficient for release.
  For the GB10 compatibility node-agent path, active/no-drift apply still runs
  `docker compose up -d worker` and waits for the compose service to become
  `running`; release-gate retries direct `gb10-workers status` target
  mismatches while worker registration and heartbeat converge. If stale
  heartbeats persist beyond the retry window, inspect whether the worker exited
  after registration rather than accepting node-agent metadata alone.

  Worker-token rotation must update the protected canonical token source and
  any repository-owned configuration through the approved change path, merge
  that change to `dev`, and start a broker rollout. It must not be performed as
  a per-host interactive apply against shared staging.

  Treat `/home/qianyi/loom-worker-build-staging/gb10-node-agent.env` and
  node-agent transient compose env files as host-local runtime material, not
  release source. Current node-agent apply writes transient env files under the
  user runtime/tmp directory and removes legacy repo-root `..env.*.tmp` files on
  rerun; these files must not be the reason a GB10 node reports
  `source_git_dirty=true`. Node-agent apply also checks active-intent Docker
  Compose liveness on no-drift reruns and restarts a missing worker container,
  so rerunning the service is the durable recovery path when metadata is current
  but `/api/v1/backends` has no active `docker` worker.

- For elastic Slurm pools, the Control Plane records submitted worker jobs in
  `slurm_worker_jobs` and exposes safe capacity status with:

  ```bash
  loom admin slurm-workers status \
    --cp-url http://control-node.lan:18081 \
    --admin-token file:/secure/path/admin-token
  ```

  Use this before submitting more capacity. A pending or running job with the
  same environment, pool, nodelist, CPU, memory, and concurrency is an active
  capacity request and should not be duplicated. Stale worker-heartbeat or
  missing-Slurm records are exposed as `stale=<slots>` and
  `stale_jobs=<count>` in the CLI plus
  `loom_slurm_worker_stale_slots` / `loom_slurm_worker_stale_jobs` metrics.
  `loom admin environment-state check` also fails when a pending/running Slurm
  job's node is absent from the current policy `allowed_nodes`, when its
  redacted `LOOM_REMOTE_WORKER_ENV_FILE` or
  `LOOM_REMOTE_WORKER_REPO_DIR` differs from the profile, when its non-secret
  `LOOM_WORKER_AUTH_FINGERPRINT` differs from the active `--worker-token`
  fingerprint, or when the profile's external runner env file is absent, the
  repo checkout is on the wrong release, the checkout is dirty, or a declared
  external Slurm autoscaler supervisor has stale unit content, an unscoped
  command, a missing or unexecutable `ExecStart` command path, a recent failed
  service result such as `status=203/EXEC`, a disabled timer, or an inactive
  timer.
  The worker-pool autoscaler uses the same Slurm job release-state evidence
  before computing healthy capacity. Pending or running jobs outside
  `allowed_nodes`, or whose redacted `LOOM_REMOTE_WORKER_ENV_FILE`,
  `LOOM_REMOTE_WORKER_REPO_DIR`, or worker-token fingerprint does not match the
  active policy, are excluded from `actual_slots` and legal active-node/job
  caps. The autoscaler records `release_state_drift` instead of treating the
  stale job as warm capacity. A safely linked running worker is drained
  normally; once it is `draining` and in-flight trials reach zero, the
  autoscaler cancels the Slurm job (or observes that it already exited) and
  marks the worker `drained`. Pending or unlinked jobs stay blocked for
  operator reconciliation.
  Rerun `environment-state check` before submitting staging/full100 validation
  batches.
  Use `--format json` for release evidence or automation. If Loom backlog has
  drained but Slurm still has pending elastic jobs, cancel those Slurm job ids
  with `scancel`; the controller will record cancellation on its next
  reconcile.
- When OLDLAB elastic workers are enabled for a staging or production release
  candidate, the release gate `worker_capacity_smoke` evidence must include the
  smoke batch id, runtime, failure count, and one record per OLDLAB worker with
  node name, Slurm job id, Loom worker id, configured concurrency, and claimed
  trial count.
- Fixed Kubernetes workers should leave `LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS`
  unset. Elastic Slurm workers should set it in the remote-worker env file:
  use 300 seconds for dev/staging pools and 600-900 seconds for production
  OLDLAB capacity. Keep Slurm `--time` as the hard safety bound; idle-exit
  only releases allocations after queue drain.
- Release-managed staging GB10 Docker Compose workers use a 7200-second idle
  window from `deploy/environment-state/staging.toml`. That is the validation
  lease bound for the exact 15 hosts x 10 slots gate; shorter values can let early
  hosts exit during bounded-parallel prep before release-gate observes fresh
  workers.
- For shared OLDLAB and GB10 pools, prefer the worker-pool autoscaler over
  manual SSH, Docker Compose, or Slurm operations. Check current policy and
  decisions with:

  ```bash
  loom admin worker-pools autoscaler status \
    --cp-url http://control-node.lan:18081 \
    --admin-token file:/secure/path/admin-token
  loom resources status --json
  ```

  The policy records desired slots, actual claimable slots, pending slots,
  draining slots, occupied slots, queued slots, idle-window age, last decision,
  blocked reason, blocked details, and actuator error. Normal scale-down marks workers
  `draining` first; those workers stop claiming new work, finish assigned
  trials, and are released only after in-flight count reaches zero. To roll
  back, disable the policy or raise `min_slots`, then wait for Slurm jobs to
  converge. If the GB10 node-agent compatibility path was used, restore host
  intents to `active` only after confirming the autoscaler policy is disabled
  or intentionally bypassed. Manual `scancel` and Docker Compose stop remain
  break-glass actions and must not target workers that still own claimed or
  running trials.
- If a Slurm pool's submit commands work only on the OLDLAB submit host, set
  `actuator_config.external_runner=true`. The Kubernetes Control Plane loop
  intentionally skips those policies; run the autoscaler reconciler on the
  submit host with `include_external_policies=True` and `external_only=True`
  so `sbatch`, `squeue`, `sacct`, `scancel`, and munge stay on the machine
  that owns those credentials. The submit-host reconciler refreshes active
  Slurm job records before each autoscaler decision, so pending/running/stale
  job status and `loom_slurm_worker_*` metrics do not depend on an in-pod
  Slurm controller.
  `actuator_config.exclusive` defaults to `true`; set it to `false` only when
  the requested CPU, memory, and concurrency are intentionally sized for a
  shared Slurm node.
- Until Docker sandbox CPU/RAM limits are enforced per trial, treat
  higher worker concurrency as an operator decision backed by load-test
  evidence, not just a CPU-count formula.
- Postgres: 50 GiB volume covers ~10M trial rows. Trial rows are
  small (< 4 KiB); trajectory JSONL lives in MinIO, not Postgres.
- MinIO: depends entirely on trajectory + artifact volume. 500 GiB
  PV in the manifest is a starting point — switch to a distributed
  MinIO deployment past ~10 TiB.

## Family runs (#672)

Batches can opt into ordered, adaptive execution across related trials
via `trial_config.family_run`. See `docs/architecture/family-runs.md`
for the design; the operator-facing shape is:

```json
{
  "trial_config": {
    "family_run": {
      "enabled": true,
      "family_key_extractor": {"name": "instance_id_prefix"},
      "sequencer": {"name": "alphabetical"},
      "advance_predicate": {"name": "always_on_terminal"},
      "adapter": {"name": "noop"},
      "failure_policy": {"name": "stall_family"},
      "state_backend": {"name": "s3_artifacts"}
    }
  }
}
```

The framework ships six plugin roles and default plugins for each; a
benchmark's catalog entry can supply `family_run_defaults` so common
cases work zero-config. When enabled, tasks are grouped by
`family_key_extractor`, ordered by `sequencer`, and each family runs
serially with the adapter deciding cross-trial state between them.

PR-1 (framework skeleton) ships the `noop` adapter, plugin protocols,
migrations (`batches.family_run_spec`, `trials.family_key`,
`batch_family_state`), scheduler predicate, CP finalize hook, batch-
submit seeder, and worker pre-start helper. The orchestrator service
and `skill_patcher_llm` adapter ship in PR-2.

### Orchestrator gateway credential and BYO routing (#695)

`skill_patcher_llm` (the reference LLM-driven adapter) evolves the
shared-skill directory between trials by calling the LLM gateway.
The Deployment uses one dedicated, teamless family-orchestrator worker
credential, not a represented team identity. An administrator with
`admin:tokens` issues it through Control Plane
`POST /admin/family-orchestrator-tokens`; the returned token has only the
internal `family:evolve` scope and must be stored as the
`family-orchestrator-token` key in `loom-secrets`:

```
kubectl -n <ns> patch secret loom-secrets --type=merge -p '{
  "data": {
    "family-orchestrator-token": "<b64-loom_fo_...-token>"
  }
}'
kubectl -n <ns> rollout restart deploy/loom-family-orchestrator
```

Treat the one-time token response as a credential: do not print it to logs,
paste it into issue comments, or retain it in evidence. The key is schema-owned
so `loom cluster preflight` does not report it as an orphan, but
`loom cluster bootstrap-secrets` intentionally does not emit a placeholder.
Leave it absent until the operator is ready to enable an LLM-backed family
adapter.

The key is marked `optional: true` on the Deployment so the orchestrator boots
without it — it just logs
`family_orchestrator_gateway_unconfigured` and refuses to call
`SkillPatcherLLMAdapter.evolve` (non-LLM adapters still advance).

The `family:evolve` credential cannot call the Gateway directly. For each
evolve operation, `OrchestratorGatewayClient` sends the real completed trial
id, represented batch team, `step_id="family_evolver"`, and an explicit
`provider_connection_id` value (including null) to Control Plane
`POST /admin/step-tokens`. The Control Plane loads the trial, requires its team
to match the represented batch team, verifies that a configured provider is
owned by or shared with that team, and returns a short-lived `llm:call` step
JWT. That JWT binds the trial, team, family-evolver step, and provider before
the request reaches the Gateway.

For BYO provider routing (the operator wants the evolver to call
their own upstream, not the platform default), set only
`family_run.adapter.params.provider_connection_id`. At batch acceptance,
`normalize_evolver_provider_connection()` canonicalizes the UUID and validates
the connection against the represented batch team's owner/share boundary.
Secret-like adapter parameter keys fail closed recursively; never put an API
key, bearer token, authorization header, credential, password, cookie, or
secret reference in `family_run.adapter.params`.

The Gateway treats the step-JWT provider claim as authoritative. If the client
also sends `x-loom-provider-connection-id` or
`loom.provider_connection_id`, both must match the JWT or the request is
rejected before dispatch. If the adapter has no configured evolver provider,
the Control Plane receives an explicit null, the JWT carries an authoritative
null provider claim, the header and body field are omitted, and the Gateway
uses the platform path;
it does not inherit the completed trial's provider. Cross-team provider ids
remain existence-hiding failures, and errors/evidence must not record or echo
credentials or secret references.
