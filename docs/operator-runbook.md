# Loom Operator Runbook

For operators of a production Loom deployment. Local dev → see the
top-level README + `deploy/docker-compose.dev.yml`.

> **Cross-repo issue/PR refs:** bare `#N` in this document may point to
> the pre-2026-06-26 `carinrc/loom` archive tracker (numbering was reset
> on the new canonical repo `qianyi-sun/loom`). See
> [`repo-migration.md`](repo-migration.md).

## Environment isolation

Loom uses three logical deployment environments. Treat the names below as the
operator contract; do not reuse kubeconfigs, databases, object buckets,
SecretStore keys, worker tokens, provider connections, or deploy credentials
across rows.

| Environment | Git branch / ref | GitHub Environment | Namespace | Public host | DB name | Object buckets |
|---|---|---|---|---|---|---|
| `development` | `dev` only | `development` | `loom-dev` | `dev.yylx.world` | `loom_dev` | `loom-dev-trajectories`, `loom-dev-artifacts` |
| `staging` | pinned `dev` SHA | `staging` | `loom-staging` | `staging.yylx.world` | `loom_staging` | `loom-staging-trajectories`, `loom-staging-artifacts` |
| `production` | `main` or `release-*` tag only | `production` | `loom-prod` | `yylx.world` | `loom_prod` | `loom-prod-trajectories`, `loom-prod-artifacts` |

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
| `LOOM_WORKER_TOKEN` | Worker bearer token for only that environment. |

The workflow `.github/workflows/deploy-environment.yml` binds each job to the
matching GitHub Environment. Because GitHub only exposes environment secrets
after the selected job enters that environment, a `development` or `staging`
deploy job cannot read production kubeconfig, database credentials,
object-store credentials, provider secrets, SecretStore keys, or worker tokens.
Configure the `production` GitHub Environment with required reviewer approval.

Before a production release, run the static boundary validator:

```bash
python scripts/validate_environment_isolation.py \
  --profiles-dir deploy/environments \
  --workflow .github/workflows/deploy-environment.yml
```

It verifies the committed environment profile names, namespaces, domains,
database names, object buckets, SecretStore key refs, worker-token refs,
provider-connection namespaces, cluster render inputs, and workflow branch
guards. The same check runs in repository CI through `tests/ops`.

### Release promotion gate

A production release is a promotion from a pinned `dev` candidate to `main`,
not an automatic deploy of every `dev` merge. The release owner must collect
the heavy staging evidence, encode it as a release gate manifest, and run
`.github/workflows/release-promotion-gate.yml` before opening or merging the
release PR.

Normal flow:

1. Pick a 40-character candidate SHA from `dev`.
2. Build or identify the image tag/digests for that candidate.
3. Deploy that exact image tag to `staging` using the staging GitHub
   Environment.
4. Run the staging smoke checklist below, including migration dry-run,
   public API/SPA smoke, provider smoke, benchmark reward gate, benchmark
   score-alignment gate, redaction scan, worker-capacity smoke, and rollback
   evidence.
5. Write a JSON manifest with `schema_version=1`, `candidate_sha`,
   `image_tag`, `staging_url`, image digests for every Loom image, and pass
   records for every required check:
   `repository_ci`, `image_build`, `cluster_render_audit`,
   `migration_dry_run`, `public_api_spa_smoke`, `secret_redaction`,
   `provider_smoke`, `benchmark_reward_gate`, `benchmark_score_alignment`,
   `worker_capacity_smoke`, `rollback_plan`, and `release_owner_approval`.
6. Run the release gate workflow:
   ```bash
   base64_manifest="$(base64 < release-gate-input.json | tr -d '\n')"
   gh workflow run release-promotion-gate.yml \
     --ref dev \
     -f candidate_sha="$CANDIDATE_SHA" \
     -f image_tag="$IMAGE_TAG" \
     -f evidence_manifest_b64="$base64_manifest"
   ```
7. Attach the workflow run, `release-gate-evidence` artifact, staging URL,
   candidate SHA, image digests, and rollback notes to the release PR from
   `dev` to `main`.
8. Merge the release PR only after the release owner accepts the evidence.
9. Deploy production from `main` with the same candidate SHA, image tag, and
   release gate workflow run id. The production deploy preflight downloads the
   `release-gate-evidence` artifact, verifies the candidate/image match, scans
   for leaked bearer/provider keys, signed URLs, internal service URLs, and
   secret refs, and confirms the candidate SHA is an ancestor of the
   production ref before it can reach `loom cluster up`.

Failed gate path: keep the release on `dev`, record the failing check and
evidence link on the release issue or PR, fix the owning subsystem, rerun the
staging gate with a new manifest, and only then retry promotion.

Failed deploy path: do not edit production secrets or reuse staging
credentials. Inspect the failed deployment logs, keep the release gate artifact
attached, and either rerun the production deploy with the same validated gate
artifact after fixing an operator error, or execute the rollback plan recorded
in the manifest.

Hotfix path: branch from `main`, apply the minimal fix, run repository CI plus
the same release gate against staging for the hotfix SHA, open a hotfix PR to
`main`, and deploy production with that hotfix candidate SHA and gate run id.
Back-merge or cherry-pick the hotfix to `dev` after production is stable.

### Deploy, inspect, and rollback by environment

Deploys run through the GitHub Actions workflow:

```bash
gh workflow run deploy-environment.yml \
  --ref dev \
  -f environment=development \
  -f image_tag="$IMAGE_TAG" \
  -f dry_run=false

gh workflow run deploy-environment.yml \
  --ref dev \
  -f environment=staging \
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

Use `dry_run=true` to render and audit with the environment secret config
without applying. Every deploy job writes `rollout-evidence/rendered.yaml` and
`rollout-evidence/release-manifest-<image-tag>.json` before apply, then uploads
that directory as a workflow artifact for the operator review trail. Production
deploys from any ref other than `main` or a `release-*` tag are skipped by the
workflow condition and still require the protected `production` environment
approval when they do run. Production deploys also refuse to run without a
successful release gate artifact for the
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
PREVIOUS_IMAGE_TAG=public-beta-known-good
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

## At-a-glance: deploy a fresh cluster

The fastest path uses the `loom cluster` CLI (shipped via #76). Install
the optional `cluster` extra and point at your kubeconfig context:

```bash
pip install "loom[cluster]"   # or `uv sync --extra cluster`
export KUBECONFIG=~/.kube/config   # standard kubectl config

# 1. Configure
cat > cluster-config.toml <<EOF
namespace = "loom"
image_tag = "0.7"
ingress_host = "loom.example.com"
ingress_class_name = "nginx"
ingress_tls_secret_name = "loom-tls"
# Optional when cert-manager manages the TLS Secret:
# ingress_cert_manager_cluster_issuer = "letsencrypt-prod"
EOF

# 2. One-time bootstrap (Secrets) — see "Bootstrap Secrets" below
# 3. Verify the cluster is ready to receive Loom
loom cluster preflight --namespace loom

# 4. Audit the manifests against the public/internal boundary
loom cluster audit --config cluster-config.toml

# 5. Deploy
loom cluster up \
  --config cluster-config.toml \
  --context $YOUR_CTX \
  --environment staging \
  --rollout-id "$IMAGE_TAG" \
  --rollout-lock-evidence "$ROLLOUT_DIR/rollout-mutation-lock-$IMAGE_TAG.json"

# 6. Verify
loom cluster status --namespace loom --format table
```

Each verb:

| Command | What it does | Exit codes |
|---|---|---|
| `loom cluster preflight` | API-side checks: namespace exists, required Secrets present, IngressClass installed, default StorageClass available, PSS labels OK, and schema-doctor reconciliation for rendered env/Secret drift. With `--config`, preflight validates live `loom-secrets` but checks env vars against the target rendered Deployments so rollouts that add schema-backed env vars are not blocked by old live pods. Protected environments also check the live critical PVC/PV storage boundary and a recent backup manifest; pass `--config cluster-config.toml` so first deploys can prove static host-path Retain PVs before the PVCs exist. Optional runtime-derived worker env such as hostname, idle-exit, fixtures root, benchmark cache, and blocking-I/O executor override may stay unset. | 0 pass / 1 fail / 2 cluster unreachable |
| `loom cluster backup manifest/check` | Write or verify metadata-only backup manifests for public-beta/staging destructive-operation guards | 0 verified / 1 invalid manifest / 2 bad input |
| `loom cluster render` | Print the rendered YAML to stdout (no cluster contact) | 0 / 2 on bad config |
| `loom cluster release-manifest` | Write a safe pre-apply rollout artifact with the candidate git SHA/image tag, CLI version, cluster-config and rendered-manifest hashes, intended Deployment images, optional expected image digests/IDs from `--expected-image-identities-json`, Alembic heads, and environment-state worker desired-state fingerprints | 0 written / 2 bad input |
| `loom cluster release-gate` | Compare the release manifest against the saved rendered/config hashes, live target-generation image evidence, live DB Alembic heads queried through `deploy/loom-control-plane`, and the `loom admin environment-state check --format json` artifact when the manifest records external-worker desired state. Running Deployments use exact Ready-pod runtime digest/image-ID comparison when available; kind-loaded `import-YYYY-MM-DD@sha256:...` runtime identities are accepted only with matching target-generation pod spec and Deployment template images; zero-replica managed Deployments use template-image convergence evidence. JSON output includes `component_evidence`; `--format markdown` writes the pasteable per-component release evidence table for issue comments. | 0 pass / 1 hard-check fail / 2 bad input or unreachable |
| `loom cluster audit` | Static public/internal boundary check on rendered manifests: TLS ingress, only `/api/v1` → `loom-service` and `/` → `loom-web`, no LoadBalancer/NodePort, no unsafe hostPort, required NetworkPolicies present | 0 clean / 1 violation / 2 bad config |
| `loom cluster up` | Preflight → render → protected-environment rollout mutation lease acquisition → `kubectl apply` → wait for components ready, Deployment generations observed, updated replicas converged, managed Deployment pods inspectable and free of blocking CrashLoop/image/config/start failures, kube-system rollout controllers healthy, and live Deployment images matching the rendered manifests; prints rendered/live image evidence for managed Deployments. For public-beta/staging/production, pass `--rollout-id` and `--rollout-lock-evidence` so evidence records acquisition and release/failure state. | 0 ready / 1 lock contention, not-ready, or image drift / 2 unreachable or kubectl missing |
| `loom cluster status` | Live readiness snapshot with ingress endpoints; marks stale Deployment generations, incomplete updated replicas, failed managed-pod inspection, managed Deployment pod CrashLoop/image/config/start failures, and visible kube-system controller/scheduler/etcd/API pod failures as not-ready | 0 all-ready / 1 not-ready / 2 unreachable |
| `loom cluster down` | `kubectl delete` of the rendered manifests; opt-in `--with-volumes` (PVCs) and `--delete-namespace` for full teardown. Protected environments require `--backup-manifest` and `--acknowledge-data-loss` before destructive flags. | 0 / 1 on failure, invalid backup guard, or operator-cancelled prompt |

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
   Keep operator-local directories such as `.public-beta-staging` and
   `.worktrees` out of the Docker context; the repository `.dockerignore`
   excludes them so public-beta evidence, benchmark caches, and local worktrees
   do not make image builds hang while sending context.
   `Dockerfile.web` is multi-stage (node-slim builds the Vite bundle
   → nginx-alpine serves it). Push to your registry, then update
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

8. **Apply versioned environment state.** Kubernetes manifests do not own
   every rollout-critical runtime row. After the images and secrets are live,
   apply the repository profile for the target environment, then check for
   drift before trusting Monitor capacity or benchmark validation evidence.
   Run the check from the Slurm submit/shared-storage host when the profile
   contains external Slurm runner pools; the gate also verifies runner env
   files, worker-token fingerprints, shared git checkouts, clean git status,
   and active Slurm job launch env:

   ```bash
   # Optional but recommended for protected environments: compute this from
   # the canonical live secret source, not from the operator .env being tested.
   ADMIN_TOKEN_FINGERPRINT="sha256:<12-hex> len=<N>"

   loom admin environment-state apply \
     --cp-url http://localhost:8080 \
     --admin-token env:ADMIN_TOKEN \
     --expect-admin-token-fingerprint "$ADMIN_TOKEN_FINGERPRINT" \
     --environment public-beta \
     --file deploy/environment-state/public-beta.toml \
     --var IMAGE_TAG="$IMAGE_TAG" \
     --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
     --rollout-id "$IMAGE_TAG" \
     --rollout-lock-evidence "$ROLLOUT_DIR/environment-state-apply-lock-$IMAGE_TAG.json"

   loom admin environment-state check \
     --cp-url http://localhost:8080 \
     --admin-token env:ADMIN_TOKEN \
     --expect-admin-token-fingerprint "$ADMIN_TOKEN_FINGERPRINT" \
     --environment public-beta \
     --file deploy/environment-state/public-beta.toml \
     --var IMAGE_TAG="$IMAGE_TAG" \
     --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
     --worker-token file:/secure/path/worker-token \
     --rollout-id "$IMAGE_TAG" \
     --rollout-lock-evidence "$ROLLOUT_DIR/environment-state-check-lock-$IMAGE_TAG.json" \
     --format json \
     > "$ROLLOUT_DIR/environment-state-check-$IMAGE_TAG.json"
   ```

   The command is idempotent and uses the existing Control Plane admin APIs for
   worker-pool autoscaler policies, GB10 desired state, and Slurm worker job
   status. A drift failure is actionable, for example desired `gb10-arm64`
   actuator `slurm` but live `gb10`, or an active OLDLAB Slurm job still
   pointing at an older `LOOM_REMOTE_WORKER_REPO_DIR`; fix it with the profile
   apply and by draining/replacing stale Slurm jobs rather than a one-off SQL
   patch. If `admin_token_fingerprint` fails, refresh the operator token
   source from the canonical protected-environment secret before rerunning;
   do not work around it by switching to an untracked token.
   The public-beta profile currently targets the legacy Control Plane
   environment name `production` because existing GB10 node agents read that
   desired-state key; the CLI still requires `--environment public-beta` so
   operators do not accidentally apply the staging profile.
   When a release manifest records this profile, pass the JSON check artifact
   to `loom cluster release-gate --environment-state-check`; a missing artifact,
   `ok=false`, or non-empty `drift` array keeps the protected release gate red
   and blocks workload-validation anchors.
   Protected `environment-state apply/check` uses the same per-environment
   rollout lease as `loom cluster up`, defaulting to
   `$LOOM_ROLLOUT_LOCK_DIR` or `~/.loom/rollout-locks`. Set a shared
   `LOOM_ROLLOUT_LOCK_DIR` on hosts where multiple operators or Codex threads
   can mutate the same public-beta/staging target. If the command reports an
   active owner, do not force it until the recorded owner is proven stale and
   that evidence is saved in the rollout directory; then rerun with
   `--force-rollout-lock` and keep the lock evidence JSON with the release
   artifacts. The force flag replaces an abandoned durable record only when no
   active process holds the advisory lock. For GitHub staging/production deploy
   workflows, configure the environment variable `LOOM_ROLLOUT_LOCK_DIR` to the
   same shared path; the deploy helper fails closed for protected environments
   when it is unset.
   Staging uses the same flow with `--environment staging` and
   `deploy/environment-state/staging.toml`. Keep the catalog provisioning
   command printed by the profile in the rollout evidence; that gate remains
   separate because catalog data lives in the service DB/object store, not the
   Control Plane admin API. The profile also lists the operator-only env keys
   required by that gate: `HF_TOKEN`, `LOOM_SVC_DB_URL`, and
   `LOOM_SVC_MINIO_*`. Those credentials belong in the operator context, not
   the runtime worker pods.

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
   raw MinIO/S3 signed URLs. The debug command should return a stable
   `failure.reason_code`, lifecycle state, token usage summary, scoped evidence
   links, and redacted next actions without bearer tokens, provider keys,
   internal service URLs, or signed object-store URLs.

9. **Approve account requests into fixed teams.** Public registration is
   default-closed. A researcher can submit an account request without a bearer
   token; the team must already exist:
   ```bash
   curl -X POST https://loom.example.com/api/v1/auth/registration-requests \
     -H "Content-Type: application/json" \
     -d '{"username":"Mark", "team_id":"00000000-0000-0000-0000-000000000000"}'
   ```
   Internal teams are admin-managed ahead of time. List or create them before
   approving a request:
   ```bash
   curl https://loom.example.com/api/v1/admin/teams \
     -H "Authorization: Bearer $ADMIN_TOKEN"

   curl -X POST https://loom.example.com/api/v1/admin/teams \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: qianyi" \
     -H "Content-Type: application/json" \
     -d '{"name":"Research Platform"}'
   ```
   An admin lists and approves pending requests into one existing team and role:
   ```bash
   curl https://loom.example.com/api/v1/admin/team-registrations?status=pending \
     -H "Authorization: Bearer $ADMIN_TOKEN"

   curl -X POST https://loom.example.com/api/v1/admin/team-registrations/$REG_ID/approve \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: qianyi" \
     -H "Content-Type: application/json" \
     -d '{"team_id":"'"$TEAM_ID"'", "role":"member"}'
   ```
   The approval response reveals a raw `loom_invite_...` code and browser invite
   link exactly once. Deliver the link to the requested contact; the contact
   accepts it to create their user session and the selected team membership
   without seeing raw API credentials. Loom does not email the link in public beta.
   If the link is lost, resend the invite to rotate the stored hash and reveal a
   replacement:
   ```bash
   curl -X POST https://loom.example.com/api/v1/invites/$INVITE_ID/resend \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: qianyi"
   ```
   Reject accidental requests with `POST .../$REG_ID/reject` and the same actor
   header. Review backend audit evidence with:
   ```bash
   curl https://loom.example.com/api/v1/admin/audit-events?limit=20 \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Audit rows include the operator-attested actor and safe metadata such as
   invite prefixes, never raw bearer or invite tokens.

10. **Public-beta incident controls.** Use the same admin token plus an
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

## Upgrade

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
not skip preflight during public-beta/staging storage migration or restore
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
YibuAPI provider connections should use `rate_card_provider=yibuapi`; user
self-deployed/private API connections should normally stay `tokens-only`, which
reports token totals and `cost_status=not_applicable` without assigning a
fabricated dollar amount.

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

and install a durable managed subprocess Gateway tunnel from `30444` to
`svc/loom-llm-gateway:9100`:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig \
  --subprocess-gateway-local-port 30444
```

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
| `claimed_without_start_expiry_sec` | `300` | Control Plane crash-detector threshold for reclaiming a trial stuck in `claimed` with `started_at IS NULL`, even when the owning worker heartbeat is still fresh. Keep above normal claim-to-start latency, including task materialization and image/cache setup. |
| `trial_cache_registry_repo` | `""` (unset) | Registry path to share layered images across workers (Docker Hub / GHCR / ECR / self-hosted). When unset, each worker caches locally. |
| `trial_cache_registry_pull_timeout_sec` | `15.0` | Per-attempt timeout for the registry pull. |
| `trial_cache_base_image_pull_timeout_sec` | `1800.0` | Per-attempt timeout for the underlying task-image pull (SWE-Bench instance images are 1–2 GB). |
| `trial_cache_ttl_hours` | `168` (7d) | Layered images older than this are pruned on the next eviction sweep. |
| `trial_cache_min_free_gb` | `20` | Capacity backstop — when free disk drops below this, oldest-by-creation entries are evicted first. |
| `trial_cache_build_lock_timeout_sec` | `1800.0` | Cluster-wide builder-slot TTL. The slot's owner refreshes every 60 s while building. |
| `trial_cache_build_max_concurrent` | `1` | Daemon-wide cap for concurrent layered trial-cache Docker builds across different cache keys. Keep `1` on shared OLDLAB/k8s Docker daemons; raise only for isolated daemon hosts after load testing. |
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
- **Concurrent layered image builds fail with apt/dpkg/containerd errors or
  killed setup containers on a shared worker host** → keep
  `trial_cache_build_max_concurrent=1` for that daemon group and check
  Docker/containerd disk and I/O pressure before raising it. This knob limits
  different cold `(task_image, agent)` cache keys that would otherwise build at
  the same time against one host daemon; it does not reduce already-warm trial
  concurrency.
- **Trial fails with `failure_reason=task_image_build_timeout` or a message
  like `building Docker image ... exceeded 1800s`** → treat it as a platform
  setup failure, not benchmark/model evidence. For GB10/ARM64 or mixed-arch
  full100 gates, warm the task image on each required architecture first:
  keep the shared trial-cache registry enabled, run a small architecture-targeted
  canary for the representative task image, and confirm subsequent trials pull
  or reuse the cache before launching the high-concurrency batch. Only raise
  task `build_timeout_sec` after confirming the Docker daemon, registry auth,
  disk, and CPU pressure are healthy enough that the longer build is expected.
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
  running. When the crash detector reclaims a pre-start claim, it records
  `failure_reason=worker_lost_claim` and a `claimed_without_started_reclaimed`
  message with the prior worker id, claim time, expiry window, and
  `started_at=NULL`; if the retry budget is already exhausted, the terminal
  `retry_exhausted` row preserves that message for attribution.
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
# Distribute the same token to attached remote-worker env files
# (GB10/OLDLAB) without printing it, restart those pools, then prove parity:
loom admin environment-state check \
  --cp-url http://localhost:8080 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment public-beta \
  --file deploy/environment-state/public-beta.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --worker-token file:/secure/path/worker-token
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

> The IRSA path requires #251 (boto3 credentials abstraction) before
> it's wired end-to-end. Until then, the operator falls back to static
> access keys provisioned via `loom-secrets`.

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

> GCS requires #254 (GCS lifecycle renderer) before
> `bootstrap-storage-lifecycle` works against it natively. Until
> then, operators using GCS apply lifecycle manually via `gsutil
> lifecycle set`.

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

MinIO ships as a single-replica `StatefulSet` with a 500Gi PVC
(`deploy/k8s/minio.yaml`). The PVC name is `data-loom-minio-0` and
appears in `kubelet_volume_stats_*` metrics from that name.

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
string like `env:PUBLIC_BETA_SMOKE_OPENAI` instead; the gateway
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

loom providers update together-prod --rate-card-provider together
```

Then switch `pricing_source` to `rate-card` only after the Gateway's
`rate_cards` table has matching `(provider, model)` entries. Facade calls
with a missing entry still record tokens and use
`rate_card_hash='facade:rate-card:missing'` with `cost_usd=0`.

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
- Untested rows remain selectable. Rows with a known failed preflight show a
  warning in New Batch, and `POST /api/v1/batches` rejects that
  provider/model pair until it passes preflight or the user chooses another
  model.

The service launch catalog at `GET /api/v1/models` defaults to
agent-capable models only. Raw provider entries are still available with
`GET /api/v1/models?view=raw`; suppressed entries include
`hidden_reason` values such as `classifier-non-llm` so operators can
debug noisy OpenAI-compatible catalogs.

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
normal launch flow selects one provider connection and one concrete
model id for a batch; the current backend contract stores that override
at batch level, so all BYO-provider combinations in one batch must share
the same connection/model.

## GPU-cluster checkpoint provider onboarding

For Lux-like clusters, prefer the user-facing bundle generator documented in
[`provider-onboarding.md`](provider-onboarding.md#gpu-cluster-checkpoint):

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

Then render, audit, and redeploy from the same config:

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
[`docs/architecture/observability.md`](architecture/observability.md).

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
| `POST /api/v1/batches` returns 400 `agent×task capability mismatch` | Selected agent's `requires_capabilities` (from `/api/v1/agents`) isn't satisfied by every task in the filter — e.g. `oracle` against an aime/gpqa task that doesn't ship `solution/solve.sh`; Terminal-Bench-2 is allowed because its adapter emits a wrapper | Resubmit with a compatible task slate (drop the listed incompatible tasks), or drop the incompatible agent from `combinations` |
| Worker logs `state_patch_error`, CP returns 400 requiring `result`, or DB rejects with `trials_succeeded_has_result` | A writeback path patched `state='succeeded'` before persisting/providing `result` (#416 Slice 4). CP should reject this as a clear 400 before the database constraint is reached; the constraint still blocks direct inconsistent writes | Inspect `select id, state, result, finished_at from trials where state='succeeded' and result is null` — should be empty post-#416. If non-empty, audit recent worker code for an out-of-order writeback |
| 429 from Gateway | Provider rate limit | Check `loom_llm_calls_total{provider,result}` panel |
| Trajectory or artifact uploads failing | MinIO credentials wrong or runtime bucket bootstrap failed | `kubectl logs deploy/loom-worker --tail=200` for `ensure_bucket`, `trajectory_flush_failed`, or `artifact_upload_failed`; verify `mc ls loom-minio/trajectories` and `mc ls loom-minio/artifacts` |

## Trial state/result consistency (#416 Slice 4)

Migration `0039_trials_succeeded_has_result.py` ships a `CHECK
(state != 'succeeded' OR result IS NOT NULL)` constraint as
`NOT VALID` — new writes are blocked, but pre-existing legacy rows
(present in public-beta DBs from before #416 ships) are not
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
  `claimed_without_start_expiry_sec` (default 300s), even if the worker
  heartbeat is fresh. This covers setup/materialization paths that claim a
  trial but never reach the worker's started writeback. Reclaim records
  `worker_lost_claim` plus a `claimed_without_started_reclaimed` diagnostic
  before clearing `worker_id`; a subsequent claim clears stale failure fields,
  while terminal retry exhaustion preserves the last reclaim message.

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

## Backup + restore

Public-beta and staging are protected environments. Before any operation that
can destroy or orphan cluster state, create a fresh backup bundle and metadata
manifest. The first-phase guard is intentionally conservative: `loom cluster
down --with-volumes` or `--delete-namespace` refuses to run against
`public-beta`, `staging`, or `production` unless a recent verified manifest is
provided and the operator passes an explicit acknowledgement.

Backup bundle layout:

```bash
export ENVIRONMENT=public-beta
export NAMESPACE=loom-public-beta
export BACKUP_ROOT=/data/loom-public-beta/backups/$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$BACKUP_ROOT"/{postgres,minio,secrets}
```

Protected embedded deployments should also use host-managed static PVs instead
of the kind/local-path default. Keep the data root outside the kind node's
Docker volume boundary:

```toml
namespace = "loom-public-beta"
persistent_storage_backend = "static-host-path"
persistent_storage_host_path_root = "/data/loom-public-beta"
```

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
      - hostPath: /data/loom-public-beta
        containerPath: /data/loom-public-beta
```

Create the host directories before first apply:

```bash
install -d -m 700 \
  /data/loom-public-beta/postgres \
  /data/loom-public-beta/minio \
  /data/loom-public-beta/trajectories \
  /data/loom-public-beta/backups
```

For an existing public-beta/staging namespace that already has local-path PVCs,
do not assume changing `cluster-config.toml` is enough: StatefulSet
`volumeClaimTemplates` and PVC binding fields are effectively immutable. Take a
fresh backup, pause writers, and treat the move to static PVs as a restore or
data-copy operation before deleting old PVCs/PVs.

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
  --manifest "$BACKUP_ROOT/backup-manifest.json"
```

The manifest records paths, sizes, and hashes only; it must not contain raw
secret values. Use it for protected preflight:

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
boundary.

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
Docker volume cleanup, or `loom cluster down --with-volumes` for public-beta or
staging. Ordinary pod/service restarts and `loom cluster down --yes` without
`--with-volumes` or `--delete-namespace` preserve PVCs and do not require the
destructive-operation acknowledgement.

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

Before promoting a release from `dev` to `main`, exercise the public beta
account flow on a staging cluster and attach the evidence to the release issue or
PR. The gate has two parts:

- **Operator/browser evidence** for DNS/TLS, account request approval, password
  setup/reset, SPA submission, and visual checks that require a real browser
  session.
- **Repeatable API evidence** from `scripts/public_beta_smoke_gate.py`, which
  verifies public API auth, provider discovery, service-proxied downloads, Run
  Library sharing, cross-team denials, provenance, claimed-without-started
  batch diagnostics, and leak scanning.

Quota and rate-limit enforcement are intentionally not part of this beta gate.
Team remains the execution, cost, provider credential, member, and API-token
boundary; spend response is handled through alerts and operator controls until a
separate product policy exists.

### Prereqs

- A staging cluster deployed via `loom cluster up` against the
  candidate image tag.
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
- A ready benchmark catalog provisioned into the staging/public-beta database
  and object store. This is release data, not test fixture data, and must not
  be created through `scripts/seed_test_data.py`.
- If the staging/public-beta deployment has a remote-worker pool outside the
  Kubernetes cluster, durable private tunnels are installed for Control Plane,
  Gateway, and MinIO. See [remote-worker-pool.md](remote-worker-pool.md).
- One seeded blocked artifact on the source trial, marked
  `share_status=blocked`, whose raw object body contains a fake secret such as
  `seeded-public-beta-secret`. The release evidence should prove Team B cannot
  download it and that the fake secret does not appear in API responses.
- One private source trial or batch with a safe artifact that Team A can read
  and Team B cannot read through Run Library.

### Benchmark catalog provisioning

Before inviting beta users or starting manual New Batch testing, restore the
ready benchmark catalog through one of the official catalog paths below. Do not
insert benchmark/task rows manually, patch JSON in SQL, or seed public-beta
with `scripts/seed_test_data.py`; missing credentials or source artifacts are
release blockers that should be fixed through the deployment Secret/profile and
tracked in the launch issue.

**Path A: copy from a known-good source catalog and object store.** Use this
when the source environment already has runnable task rows and bundle objects:

```bash
export LOOM_CATALOG_SOURCE_DB_URL="$SOURCE_LOOM_DB_URL"
export LOOM_CATALOG_SOURCE_MINIO_ENDPOINT="$SOURCE_LOOM_MINIO_ENDPOINT"
export LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY="$SOURCE_LOOM_MINIO_ACCESS_KEY"
export LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY="$SOURCE_LOOM_MINIO_SECRET_KEY"

# Outside Kubernetes, provide the target values explicitly:
export LOOM_DB_URL="$PUBLIC_BETA_DB_URL"
export LOOM_MINIO_ENDPOINT="$PUBLIC_BETA_MINIO_ENDPOINT"
export LOOM_MINIO_ACCESS_KEY="$PUBLIC_BETA_MINIO_ACCESS_KEY"
export LOOM_MINIO_SECRET_KEY="$PUBLIC_BETA_MINIO_SECRET_KEY"

# Inside a deployed loom-service pod, the command also accepts the service
# Secret names LOOM_SVC_DB_URL and LOOM_SVC_MINIO_* for target values.
loom datasets provision-public-beta-catalog \
  --target-bucket loom-benchmarks \
  --imported-by "release:${IMAGE_TAG:-manual}"
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

# Outside Kubernetes, provide the target DB and object-store values explicitly:
export LOOM_DB_URL="$PUBLIC_BETA_DB_URL"
export LOOM_MINIO_ENDPOINT="$PUBLIC_BETA_MINIO_ENDPOINT"
export LOOM_MINIO_ACCESS_KEY="$PUBLIC_BETA_MINIO_ACCESS_KEY"
export LOOM_MINIO_SECRET_KEY="$PUBLIC_BETA_MINIO_SECRET_KEY"

# Inside loom-service pods, the command also accepts LOOM_SVC_DB_URL from the
# service Secret and LOOM_SVC_MINIO_* for target object storage. For gated
# repos, HF_TOKEN must already be present in the operator context.
loom datasets register skilllearnbench \
  --revision "$PUBLISHED_SHA" \
  --mirror-to-object-store \
  --bucket loom-benchmarks \
  --registered-by "release:${IMAGE_TAG:-manual}"
```

If the HF repo is private/gated and the pod lacks `HF_TOKEN`, the 401/403 is a
real rollout blocker. Fix it by updating the Secret/profile and restarting the
operator context; do not replace it with hand-written DB rows.

The mirror path is idempotent. It downloads the exact HF revision with the
operator token, writes bundle objects under deterministic internal keys, stores
`s3://...` runtime sources in `tasks.source`, and preserves HF repo/revision/
path/checksum provenance in task tags without storing tokens.

Runtime workers also support private/gated `hf://` sources as a compatibility
path. `loom cluster render` injects the optional `loom-secrets` key
`huggingface-api-key` into the worker Deployment as the standard `HF_TOKEN`
environment variable used by `huggingface_hub`. Public-only deployments can
omit this Secret key because the reference is optional. To rotate the read
token, update only `loom-secrets/huggingface-api-key`, then roll any pods that
need to read it:

```bash
HF_READ_TOKEN="$(security find-generic-password -w -s loom-hf-read-token)"
kubectl -n loom patch secret loom-secrets --type merge \
  --patch "{\"stringData\":{\"huggingface-api-key\":\"${HF_READ_TOKEN}\"}}"
unset HF_READ_TOKEN
kubectl -n loom rollout restart deploy/loom-worker deploy/loom-llm-gateway
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

Score credibility has a separate Layer 1 gate that is independent of live model
quality. Before using benchmark scores as release evidence, verify that every
v1.0-supported benchmark has a canonical scoring reference, score-semantics
contract, Harbor/upstream parity decision, and same-output replay case:

```bash
python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/benchmark-score-alignment.json
```

Layer 1 answers whether Loom scores a fixed output correctly. Layer 2 matched
Loom-vs-Harbor or Loom-vs-upstream runs remain separate run evidence.

### Checklist

1. **Cluster healthy.** `loom cluster status --namespace loom` reports
   `all_ready=True`. `kubectl get pods -n loom` shows no `CrashLoopBackOff`.
2. **Public surface reachable.** `curl -sf https://<ingress_host>/api/v1/health`
   returns `200`. `curl -sf https://<ingress_host>/` returns the SPA
   index when `loom-web` replicas > 0.
3. **Boundary holds.** `loom cluster audit` exits 0. `kubectl get svc -n loom`
   shows no `LoadBalancer` / `NodePort` services. `kubectl get ingress -n loom`
   shows TLS enabled and backends only for `loom-service` at `/api/v1` and
   `loom-web` at `/`. `loom-llm-gateway`, Control Plane, Postgres, MinIO,
   workers, worker-token admin routes, and batch-runner bootstrap routes stay
   internal-only.
4. **Remote-worker private tunnels hold.** If remote workers are attached,
   collect watchdog evidence, then verify the exact worker-facing URLs from the
   control node and at least one worker host. The evidence command resolves the
   active `--env-file` path from the systemd unit without reading or printing
   secret values, checks the watchdog script path, and records timer state. If
   `LOOM_WORKER_SUBPROCESS_GATEWAY_URL` is set, this includes the
   `subprocess-gateway` facade probe used by Docker sandboxes:
   ```bash
   scripts/ops/worker_service_tunnels.py watchdog-evidence \
     --expected-script-path "$PWD/scripts/ops/worker_service_tunnels.py" \
     | tee "$ROLLOUT_DIR/watchdog-evidence.json"

   export REMOTE_WORKER_ENV_FILE="$(
     jq -r '.env_file.path' "$ROLLOUT_DIR/watchdog-evidence.json"
   )"

   scripts/ops/worker_service_tunnels.py check \
     --env-file "$REMOTE_WORKER_ENV_FILE"

   scripts/ops/worker_service_tunnels.py check-remote worker-hosts.txt \
     --env-file "$REMOTE_WORKER_ENV_FILE"
   ```
   This check is required after every rollout because a public ingress health
   check can pass while private worker tunnels are down.
5. **Invite-only onboarding.** From the operator/admin browser session, confirm
   fixed teams such as Team A and Team B exist, then submit username requests
   for each team. Approve each request in Admin access -> Accounts, open the
   setup link in a fresh browser profile, set a password, and confirm the user
   lands in the selected team without seeing raw API credentials. Capture only
   safe prefixes and redacted links in shared evidence.
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
   export SMOKE_TOKEN_NAME="public-beta-team-a-$(date -u +%Y%m%dT%H%M%SZ)"
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
7. **Provider connection create + test.** As Team A, create the public-beta
   smoke provider through the CLI (or `POST /api/v1/provider-connections`),
   then probe. The release fixture uses YibuAPI through Loom's
   OpenAI-compatible provider path so the same connection can run the Codex
   SkillLearnBench smoke:
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
   non-empty catalog, including `gpt-4o-mini`. `curl /api/v1/models` from a
   user-owned API token shows the agent-capable view with provider namespace
   `yibuapi`.
9. **Model preflight.** Run
   `loom providers models mz_tn_canada_qianyi --preflight gpt-4o-mini`.
   The model row
   should show `preflight=valid`. A 401/403 should show `access-denied` without
   raw provider keys.
10. **Submit small batches from SPA and CLI.** Pick `hello-world` (or another
    canonical fixture). Submit once from the SPA New Batch page and once from the
    CLI. Before submitting either canary, run the object-store write gate by
    itself so MinIO free-space failures are caught before trial execution:
    ```bash
    python scripts/public_beta_smoke_gate.py \
      --server-url https://loom.example.com \
      --team-a-token env:TEAM_A_TOKEN \
      --team-b-token env:TEAM_B_TOKEN \
      --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
      --catalog-minio-access-key env:PUBLIC_BETA_MINIO_ACCESS_KEY \
      --catalog-minio-secret-key env:PUBLIC_BETA_MINIO_SECRET_KEY \
      --object-store-write-check-only \
      --object-store-write-check-bucket trajectories \
      --object-store-write-check-count 64 \
      --object-store-write-check-concurrency 16 \
      --fail-on-skip \
      --markdown-output public-beta-object-store-preflight.md \
      --json-output public-beta-object-store-preflight.json
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
     --task-filter '{"task_ids":["hello-world"]}' \
     --agent oracle \
     --n-per-task 1

   # Model-backed path through the provider gateway. This is the release
   # provider smoke because it exercises codex + YibuAPI OpenAI-compatible.
   # Platform admins must pass the provider owner's team id explicitly;
   # provider-name lookup is scoped to that team.
   loom eval batch create \
     --team-id <agentic-rl-team-id> \
     --name-suffix codex-yibuapi-smoke \
     --task-filter '{"task_ids":["skilllearnbench/anthropic-poster-design/anthropic-poster-design-1"]}' \
     --provider mz_tn_canada_qianyi \
     --model gpt-4o-mini \
     --agent codex \
     --n-per-task 1 \
     --backend docker \
     --required-worker-pool oldlab
   # then tail it:
   loom eval batch show <batch-id>
   ```
   For mixed-pool release evidence, repeat `--required-worker-pool` for every
   pool that must produce terminal evidence, for example `oldlab`,
   `k8s-worker`, and `gb10-arm64`. The service adds one extra pool-pinned
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
   service-mode `codex` batch as long as the target public-beta service catalog
   lists `codex` as ready.
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
   `cost_status`, `pricing_modes`, `usage_estimate_confidence`); use
   `/api/v1/usage` with optional `include_batches=true` for admin totals
   and per-batch drilldown. Failed upstream audit rows surface as
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
13. **Trajectory + artifact download.** `GET /api/v1/trials/{id}/trajectory`
    streams event pages; `GET /api/v1/trials/{id}/trajectory/download`
    returns raw JSONL; `GET /api/v1/trials/{id}/atif` returns the ATIF JSON;
    artifact `download_url` entries from trial detail return object bodies. The
    URLs must stay on `/api/v1/trials/...`, not raw MinIO/S3 signed URLs, and
    cross-team callers must not be able to use owner-team artifact proxy URLs.
    Verify the public CLI path with `loom eval trial download ...`; it should
    write the object body locally without printing internal object-store URLs.
14. **Run Library sharing.** Confirm the completed source run appears in Run
    Library -> My team for Team A and Run Library -> All teams for Team B.
    Evidence must include the owner-team label, completed state, score/cost
    summary, task/agent/model summary, bounded artifact badges, diagnosis,
    debug evidence, and artifact groups. Team B must be able to download a safe
    artifact only through the Run Library service URL.
15. **Clone and reuse.** From Team B, clone config from Team A's completed run.
    If the source run used a provider connection, select a Team B-owned
    provider connection before cloning. Then reuse a safe artifact from the
    source trial. Both created records must belong to Team B and show
    `source_provenance` with the source batch/trial/artifact key.
16. **Blocked and private access denied.** Team B must be denied when trying to:
    download the seeded blocked artifact through Run Library; download Team A's
    artifact through the normal owner-team trial route; mutate Team A's original
    batch, such as cancelling it; or inspect/download private or blocked source
    artifacts. Denials should include safe reasons only.
17. **Provider error surfaces.** Temporarily rotate the provider key
    to an invalid value, re-run a trial, and confirm the SPA + API
    surface a clear `provider_error` reason rather than a generic 500. Confirm
    diagnostic text does not contain raw provider keys, bearer tokens, signed
    URL query parameters, or internal service hostnames.
    For transient provider/gateway transport drops, confirm diagnostics use
    `provider_transport_disconnect` rather than `internal_error` and that the
    trial retry budget is consumed before a terminal failure is recorded.
18. **Automated evidence script.** After steps 4-16, run the repeatable API
    gate. Use disposable staging data because clone/reuse checks create Team B
    records. Pass smoke-gate tokens, MinIO credentials, and explicit secret
    needles as `env:VAR`, `file:PATH`, or `-` sources; do not expand raw secret
    values into argv:
    ```bash
    python scripts/public_beta_smoke_gate.py \
      --server-url https://loom.example.com \
      --team-a-token env:TEAM_A_TOKEN \
      --team-b-token env:TEAM_B_TOKEN \
      --provider-connection-name mz_tn_canada_qianyi \
      --provider-model-provider yibuapi \
      --provider-model-name gpt-4o-mini \
      --batch-id "$TEAM_A_BATCH_ID" \
      --trial-id "$TEAM_A_TRIAL_ID" \
      --safe-artifact-key "$SAFE_ARTIFACT_KEY" \
      --blocked-artifact-key "$BLOCKED_ARTIFACT_KEY" \
      --private-trial-id "$PRIVATE_TRIAL_ID" \
      --private-artifact-key "$PRIVATE_ARTIFACT_KEY" \
      --clone-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
      --reuse-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
      --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
      --catalog-minio-access-key env:PUBLIC_BETA_MINIO_ACCESS_KEY \
      --catalog-minio-secret-key env:PUBLIC_BETA_MINIO_SECRET_KEY \
      --object-store-write-check \
      --object-store-write-check-bucket trajectories \
      --object-store-write-check-count 64 \
      --object-store-write-check-concurrency 16 \
      --k8s-namespace loom-public-beta \
      --required-worker-pool oldlab \
      --secret-needle env:PUBLIC_BETA_SECRET_NEEDLE \
      --internal-url-needle loom-minio.loom.svc.cluster.local \
      --allow-mutating-checks \
      --fail-on-skip \
      --markdown-output public-beta-smoke.md \
      --json-output public-beta-smoke.json
    ```
    Attach `public-beta-smoke.md` or paste its table into the release comment.
    Store `public-beta-smoke.json` with release artifacts if the environment has
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
    trajectory or artifact upload. The `service.no_oom_restarts` row must pass
    for full100/release evidence; if it
    reports an `OOMKilled` last state or unexpected current restart count,
    inspect `loom-service` memory, previous pod logs, and large batch
    detail/cancel traffic before accepting the gate. For OLDLAB-required
    full100/release evidence, the `runs.worker_pool_coverage` row must pass
    with `--required-worker-pool oldlab`; a missing pool means the batch did
    not produce deterministic terminal evidence on that worker pool. The
    source batch should have been created with the same
    `loom eval batch create --required-worker-pool oldlab` constraint so this
    is a deterministic gate rather than a post-hoc DB distribution check.
19. **Teardown clean.** `loom cluster down --yes` removes every applied
    object; PVCs survive (verify via `kubectl get pvc -n loom`). For
    public-beta or staging, pass `--with-volumes` or `--delete-namespace` only
    with a fresh `loom cluster backup manifest` and
    `--acknowledge-data-loss <environment>`.

A staging release that fails any check is NOT eligible for `main`.
Capture artifact links + a brief note for each pass in the
`docs/release-history.md` entry (or the equivalent for your fork).

### Automation status

Two CI workflows plus the public-beta smoke script automate parts of this
checklist:

- **`cluster-smoke`** (kind, label-gated `cluster-smoke`) — covers
  steps 1, 3, 17. Uses placeholder images, `--no-wait` apply, schema
  + boundary + apply + status + down round-trip. Fast (~1 min).
- **`staging-smoke`** (kind, label-gated `staging-smoke`) — builds
  REAL images, applies them, waits for every pod to reach Ready,
  probes `/healthz` + `/metrics` on every component. Closes the
  cold-start regression gap (~15-20 min).
- **`scripts/public_beta_smoke_gate.py`** — covers public health, logged-out SPA
  reachability, two-team non-admin user-owned API-token auth, provider/model
  discovery, runnable benchmark catalog presence, sampled ready benchmark bundle objects,
  concurrent object-store write/delete probing, service pod restart/OOM status,
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

For the final public-beta #49/#129 full/max-slot three-cluster canary, use
[`docs/full-max-slot-canary-runbook.md`](full-max-slot-canary-runbook.md).
That runbook is GO-gated: prepare the commands, preflight checklist, stop
conditions, and evidence directory up front, but do not submit the canary until
the coordinator confirms the clean anchor and #190 targeted durability evidence.

## Terminal-Bench 2.0 public-beta readiness

The TB-2 adapter (`packages/loom-benchmark-terminal-bench-2`) and its 86-task
pinned bundle (`terminal-bench-core` v0.1.1, commit
`91e10457b5410f16c44364da1a34cb6de8c488a5`) ship with the public-beta
catalog. The exercises in this section are the cluster-side acceptance gates
for issue #217 that cannot be covered by unit CI: real worker image builds,
provider trials, MinIO mirroring, sidecar plumbing, and resource-budget
profiling all need a deployed environment.

Run them against `development` first; promote to `staging` and `production`
by changing the target environment block at the top of each subsection.

Prerequisites:

- `kubectl` configured against the target cluster (use the env-scoped
  kubeconfig from `LOOM_KUBECONFIG_B64`, not your personal one).
- `loom` CLI on PATH (`uv pip install -e .` from the repo root, or the
  release tarball).
- A team API token with permission to launch batches.
- The MinIO endpoint, access key, and secret key for the target environment.
- A Hugging Face token if publishing the bundle from outside the cluster.

### G5 — Mirror the TB-2 bundle into the object store

The bundle is large enough that pulling it from Hugging Face at trial-time
saturates the worker setup budget. Publish once, register, mirror, and audit:

```bash
# Replace the env vars below with the target environment's values.
export LOOM_HF_ORG=loom-public-beta
export LOOM_DB_URL="postgresql+psycopg://loom:$LOOM_DB_PASS@dev-db.yylx.world:5432/loom_dev"
export LOOM_MINIO_ENDPOINT=https://minio.dev.yylx.world
export LOOM_MINIO_ACCESS_KEY=...
export LOOM_MINIO_SECRET_KEY=...

loom datasets publish terminal-bench-2 --hf-org "$LOOM_HF_ORG"
loom datasets register terminal-bench-2 --hf-org "$LOOM_HF_ORG" \
  --db-url "$LOOM_DB_URL" \
  --mirror-to-object-store \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
  --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
  --minio-secret-key "$LOOM_MINIO_SECRET_KEY"
loom datasets audit terminal-bench-2 --db-url "$LOOM_DB_URL" \
  --verify-bundles \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
  --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
  --minio-secret-key "$LOOM_MINIO_SECRET_KEY"
```

Acceptance:

- `loom datasets audit` exits 0 and reports `valid_bundles=86`,
  `missing_bundles=0`, `mismatched_bundles=0`.
- The object store path `<bucket>/benchmarks/terminal-bench-2/<sha>/` exists
  for each of the 86 tasks.

### Service vs. local CLI — IMPORTANT

These exercises submit trials to the deployed Loom service (control plane
+ worker), NOT the standalone `loom run` CLI. `loom run` runs locally on
DockerDriver and does NOT build task Dockerfiles — it silently falls back
to `alpine` when a task ships a `dockerfile` (every TB-2 task does), so an
oracle smoke against `loom run` fails with
`env: can't execute 'bash': No such file or directory`. Tracked as #232.
Service-mode trials route through
`src/loom_worker/task_image.py:resolve_task_image`, which builds the
upstream Dockerfile and runs the bundle in the correct image.

Authenticate first:

```bash
loom auth login --server "$LOOM_API_URL"
```

### G3 — Live cluster end-to-end (easy + hard task)

Pick one short task and one long task. `hello-world` is the canonical short
case; `simple-web-scraper` is a representative long case (it pulls a sidecar).
A successful trial must land verifier output with a numeric reward plus the
ATIF and trajectory in object storage.

```bash
mkdir -p ./tb2-evidence

for task in terminal-bench-2/hello-world terminal-bench-2/simple-web-scraper; do
  loom eval run --agent oracle --task "$task"
  # `loom eval run` prints the trial_id; wait for terminal state, then:
  #   loom eval trial show <trial_id> --json > "./tb2-evidence/${task//\//_}.json"
  #   loom eval artifact get <trial_id> atif > "./tb2-evidence/${task//\//_}.atif.json"
done
```

Acceptance:

- Both trials end with `state=succeeded` and `reward >= 0`.
- ATIF JSON and trajectory blobs are downloadable through the Run Library SPA.
- The trial's `verifier.rewards` JSON validates against the
  `loom.models.verifier.VerifierResult` shape — `to_tb2_report()` consumes
  it to produce the canonical TB-2 `BenchmarkResults` shape.

Archive the ATIFs under `docs/evidence/issue-217/` and link them in the
closing comment on #217.

### G4 — Sidecar tasks against the public-beta sandbox

Three pinned tasks declare compose sidecars (`security-vulhub-minio`,
`simple-sheets-put`, `simple-web-scraper`). Run each individually so the
worker exercises the per-trial network, DNS propagation, and `extra_hosts`
plumbing.

```bash
for task in \
  terminal-bench-2/security-vulhub-minio \
  terminal-bench-2/simple-sheets-put \
  terminal-bench-2/simple-web-scraper; do
  loom eval run --agent oracle --task "$task"
done
```

Acceptance:

- Each trial logs `started sidecar <name>` for every non-`client` service.
- No trial fails with `sandbox: service <name> not reachable`.
- The sidecar containers terminate when the trial ends (verify via
  `kubectl -n loom-dev get pods -l loom.role=sidecar -w` until the trial
  finishes, then assert the list is empty).

### G6 — Provider × Terminal-Bench-2 matrix

The public-beta agent catalog (PR #177) ships Claude Opus 4.7, Sonnet 4.6,
and Haiku 4.5. Run one TB-2 task per provider to confirm tool-loop reach to
verifier output. Use `hello-world` for cost discipline.

```bash
# Replace --provider/--model with the public-beta connection name for each
# Claude SKU; `loom providers list` shows what's configured.
for agent_model in \
  "claude-code|anthropic|claude-opus-4-7-20260101" \
  "claude-code|anthropic|claude-sonnet-4-6-20251202" \
  "claude-code|anthropic|claude-haiku-4-5-20251001"; do
  IFS='|' read -r agent provider model <<< "$agent_model"
  loom eval run \
    --agent "$agent" \
    --provider "$provider" \
    --model "$model" \
    --task terminal-bench-2/hello-world
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

TB-2 inherits `max_agent_timeout_sec` and `max_test_timeout_sec` from
upstream task YAML. Some tasks reserve 30-minute agent budgets, which
collide with the default per-trial wall-clock on the public-beta sandbox
class.

Profile a representative slice (one short, one medium, one long task):

```bash
for task in \
  terminal-bench-2/hello-world \
  terminal-bench-2/chess-best-move \
  terminal-bench-2/security-vulhub-minio; do
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

- For each profiled task, the observed `agent_wall_seconds` and
  `verifier_wall_seconds` are within the upstream-declared budgets.
- If any task exceeds the sandbox per-trial wall-clock, record the override
  in `deploy/environments/<env>.profile` under `[task_budget_overrides]` and
  re-run.

### Closing #217

When all five exercises above produce green evidence:

- Comment on #217 with the artifact links (`--tb2-report` outputs, run IDs,
  audit logs).
- Drop the `[WIP]` prefix from the title.
- Close the issue.

If any exercise blocks, open a focused sub-issue with the failure mode and
link it from #217; do not merge incomplete evidence.

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
  or `gb10-arm64`.
- Remote worker pools should set `LOOM_WORKER_HOSTNAME` to the physical or VM
  node name before startup. Otherwise Docker Compose workers may register with
  container hostnames, which makes Monitor and capacity evidence harder to map
  back to GB10/OLDLAB hosts.
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
- Layered trial-cache image builds have their own daemon-wide cap:
  `LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT`. Leave it at `1` for shared
  OLDLAB/k8s Docker daemons so cold cache-key setup containers serialize even
  when `LOOM_WORKER_MAX_CONCURRENT` admits many warm trials. Raise it only for
  isolated Docker daemons with measured disk/containerd headroom.
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
  included Slurm node. For OLDLAB public-beta capacity, use a shared checkout
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
      "env_file": "/shared_work/qianyi/loom-worker-capacity/public-beta-oldlab-worker-${IMAGE_TAG}.env",
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
  `pool_name=gb10-arm64`. The backend still displays as `docker` because the
  workers run Docker sandboxes; the autoscaler actuator displays as `slurm`
  because capacity comes from the GB10 Slurm partition. GB10 hosts attach
  through private loopback worker-service tunnels, run the worker compose
  service with `network_mode: host`, and set
  `LOOM_WORKER_HOSTNAME=trt-gb10-N` so Monitor and database evidence map
  workers to physical hosts. Set `LOOM_WORKER_POOL_NAME=gb10-arm64` so slot
  summaries and metrics group the hosts together. Keep Docker data-root, worker
  trajectory cache, benchmark cache, and trial scratch on each node's local
  ext4 disk; do not put those hot paths on `/shared_work`. Current public-beta
  validation uses `trt-gb10-1..15` at `LOOM_WORKER_MAX_CONCURRENT=10`, for 150
  configured ARM64 slots. After every rollout, first apply and check the
  versioned environment profile so DB-backed policy converges with the image
  rollout. Set `ADMIN_TOKEN_FINGERPRINT` from the canonical live admin secret
  source for the protected environment before running these commands:

  ```bash
  loom admin environment-state apply \
    --cp-url http://control-node.lan:18081 \
    --admin-token file:/secure/path/admin-token \
    --expect-admin-token-fingerprint "$ADMIN_TOKEN_FINGERPRINT" \
    --environment public-beta \
    --file deploy/environment-state/public-beta.toml \
    --var IMAGE_TAG="$IMAGE_TAG" \
    --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}"
  loom admin environment-state check \
    --cp-url http://control-node.lan:18081 \
    --admin-token file:/secure/path/admin-token \
    --expect-admin-token-fingerprint "$ADMIN_TOKEN_FINGERPRINT" \
    --environment public-beta \
    --file deploy/environment-state/public-beta.toml \
    --var IMAGE_TAG="$IMAGE_TAG" \
    --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
    --worker-token file:/secure/path/worker-token
  ```

  Then confirm the OLDLAB-1 `loom-remote-worker-tunnel-watchdog.timer` is
  active, run the local plus GB10 `check-remote` tunnel gates, verify Slurm
  worker status, then gate the node-agent release target before treating the
  pool as healthy. The GB10 gate checks image tag, env-config version, and
  clean source-checkout provenance; active nodes with missing provenance or a
  git commit that does not match the trailing SHA in `--release-image-tag` must
  be treated as stale even if their image/env fields look current.

  ```bash
  loom admin slurm-workers status \
    --cp-url http://control-node.lan:18081 \
    --admin-token file:/secure/path/admin-token
  loom admin gb10-workers status \
    --cp-url http://control-node.lan:18081 \
    --admin-token file:/secure/path/admin-token \
    --environment production \
    --pool-name gb10-arm64 \
    --release-image-tag "$IMAGE_TAG" \
    --release-env-config-version "$ENV_CONFIG_VERSION"
  # On each GB10 host during worker-token rotation:
  loom worker gb10-agent apply \
    --cp-url http://127.0.0.1:18081 \
    --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
    --environment production \
    --pool-name gb10-arm64 \
    --env-file /home/trt/loom-remote-worker/.env.remote-worker \
    --compose-file deploy/docker-compose.remote-worker.yml \
    --compose-file /home/trt/loom-remote-worker/docker-compose.gb10-hostnet.yml \
    --worker-token file:/secure/path/worker-token
  loom resources status --json
  ```
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
  `loom admin environment-state check` also fails when a running Slurm job's
  redacted `LOOM_REMOTE_WORKER_ENV_FILE` or
  `LOOM_REMOTE_WORKER_REPO_DIR` differs from the profile, when its non-secret
  `LOOM_WORKER_AUTH_FINGERPRINT` differs from the active `--worker-token`
  fingerprint, or when the profile's external runner env file is absent, the
  repo checkout is on the wrong release, the checkout is dirty, or a declared
  external Slurm autoscaler supervisor has stale unit content, an unscoped
  command, a disabled timer, or an inactive timer.
  The worker-pool autoscaler uses the same Slurm job release-state evidence
  before computing healthy capacity. Pending or running jobs whose redacted
  `LOOM_REMOTE_WORKER_ENV_FILE`, `LOOM_REMOTE_WORKER_REPO_DIR`, or worker-token
  fingerprint does not match the active policy are excluded from
  `actual_slots`; the autoscaler records a hard `release_state_drift` blocked
  decision instead of treating the stale job as warm capacity. Replace or
  cancel those jobs and rerun `environment-state check` before submitting
  public-beta/full100 validation batches.
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
