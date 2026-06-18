# Loom Operator Runbook

For operators of a production Loom deployment. Local dev → see the
top-level README + `deploy/docker-compose.dev.yml`.

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
# gateway_public_host = "gateway.loom.example.com"  # opt in to expose
EOF

# 2. One-time bootstrap (Secrets) — see "Bootstrap Secrets" below
# 3. Verify the cluster is ready to receive Loom
loom cluster preflight --namespace loom

# 4. Audit the manifests against the public/internal boundary
loom cluster audit --config cluster-config.toml

# 5. Deploy
loom cluster up --config cluster-config.toml --context $YOUR_CTX

# 6. Verify
loom cluster status --namespace loom --format table
```

Each verb:

| Command | What it does | Exit codes |
|---|---|---|
| `loom cluster preflight` | API-side checks: namespace exists, required Secrets present, IngressClass installed, default StorageClass available, PSS labels OK | 0 pass / 1 fail / 2 cluster unreachable |
| `loom cluster render` | Print the rendered YAML to stdout (no cluster contact) | 0 / 2 on bad config |
| `loom cluster audit` | Static public/internal boundary check on rendered manifests (no LoadBalancer/NodePort, Ingress backends on allowlist, no hostPort) | 0 clean / 1 violation / 2 bad config |
| `loom cluster up` | Preflight → render → `kubectl apply` → wait for components ready | 0 ready / 1 not-ready / 2 unreachable or kubectl missing |
| `loom cluster status` | Live readiness snapshot with ingress endpoints | 0 all-ready / 1 not-ready / 2 unreachable |
| `loom cluster down` | `kubectl delete` of the rendered manifests; opt-in `--with-volumes` (PVCs) and `--delete-namespace` for full teardown | 0 / 1 on failure or operator-cancelled prompt |

The detailed manual flow (build images → create Secrets → apply
each manifest → mint tokens → approve registrations) below documents
the bootstrap and operator steps the CLI doesn't yet automate. It's
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
   `Dockerfile.web` is multi-stage (node-slim builds the Vite bundle
   → nginx-alpine serves it). Push to your registry, then update
   `image:` refs in `deploy/k8s/*.yaml`.

2. **Create the `loom-secrets` Secret.** Required keys are declared in
   `config/loom-schema.toml` — the canonical source of truth. Generate and
   apply the full Secret in one step:

   ```bash
   loom cluster bootstrap-secrets --rotate
   ```

   `--rotate` mints fresh values for entries that carry a `generate:` command
   (currently `step-jwt-signing-key`). For other required secrets (DB URLs,
   MinIO credentials, provider API keys) the placeholder `<EDIT_ME>` appears —
   replace with your values before piping to kubectl.

   To preview and edit before applying:

   ```bash
   loom cluster bootstrap-secrets > /tmp/loom-secret.sh
   $EDITOR /tmp/loom-secret.sh
   bash /tmp/loom-secret.sh
   ```

   The `worker-token` value is overwritten in step 5.

   Create the singleton admin secret file with the operator CLI and mount it as
   `loom-admin-secret`:

   ```bash
   loom service init-admin --secret-file ./secrets.toml
   ADMIN_TOKEN="$(loom service reveal-admin --secret-file ./secrets.toml --yes)"
   kubectl create secret generic loom-admin-secret \
     --from-file=secrets.toml=./secrets.toml
   ```

3. **Apply manifests in dependency order:**
   ```bash
   kubectl apply -f deploy/k8s/postgres.yaml
   kubectl apply -f deploy/k8s/minio.yaml
   # wait for postgres + minio ready
   kubectl apply -f deploy/k8s/llm-gateway.yaml
   kubectl apply -f deploy/k8s/control-plane.yaml
   kubectl apply -f deploy/k8s/loom-service.yaml
   kubectl apply -f deploy/k8s/web.yaml
   kubectl apply -f deploy/k8s/worker.yaml
   kubectl apply -f deploy/k8s/ingress.yaml
   ```

4. **Run migrations** (one-off Job, from a pod with the Control Plane image):
   ```bash
   kubectl exec deploy/loom-control-plane -- alembic upgrade head
   ```

5. **Mint a worker token** via the admin API. The admin credential is the
   singleton `loom-admin-secret` mounted into `loom_service`, the Control Plane,
   and the LLM Gateway. Use the same `ADMIN_TOKEN` revealed in step 2; do not
   create a database-backed admin row for this bootstrap path.

   The Control Plane's `POST /admin/worker-tokens` route is
   intentionally NOT exposed via Ingress (see
   `deploy/k8s/ingress.yaml`). Reach it via port-forward:
   ```bash
   kubectl port-forward deploy/loom-control-plane 8080:8080 &
   curl -X POST http://localhost:8080/admin/worker-tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"expires_in_days": 365}'
   ```
   Update the `worker-token` key in `loom-secrets` with the returned
   raw token, then `kubectl rollout restart deploy/loom-worker`.

6. **(Optional) Provision the batch-runner CP token.** The
   `loom_service` batch-runner needs a `submit:batch` internal token
   to fan out trials from batches. Without it, the runner skips
   its tick with a warning — batches will not advance. The token is
   team-less; Control Plane derives each child trial's `team_id` from
   the parent batch row, so one runner can safely process multiple teams.

   Mint it through Control Plane's admin endpoint, then patch the
   service secret:
   ```bash
   kubectl port-forward deploy/loom-control-plane 8080:8080 &
   RAW=$(curl -sS -X POST http://localhost:8080/admin/batch-runner-tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"expires_in_days": 365}' \
     | jq -r .token)
   kubectl patch secret loom-secrets \
     -p "{\"stringData\":{\"batch-runner-cp-token\":\"$RAW\"}}"
   kubectl rollout restart deploy/loom-service
   ```

7. **Configure a user-facing MinIO endpoint for presigned URLs.** Local dev
   compose sets `LOOM_SVC_MINIO_PUBLIC_ENDPOINT=http://localhost:9000` by
   default, so links returned to the SPA and host CLI are resolvable from the
   developer machine. Production clusters must set the value explicitly. Without
   it, `loom_service` generates presigned URLs using the cluster-internal MinIO
   address (`http://loom-minio:9000`), which is not resolvable from outside the
   cluster.

   If MinIO is reachable from the internet — either via its own Ingress or as a
   path under the main Loom Ingress — set `LOOM_SVC_MINIO_PUBLIC_ENDPOINT` to
   the public base URL. `loom_service` uses `LOOM_SVC_MINIO_ENDPOINT` for
   internal bucket operations and a separate presign client pointed at
   `LOOM_SVC_MINIO_PUBLIC_ENDPOINT` for URLs returned to API callers. This is
   required because SigV4 presigned URLs bind the request `Host` header; signing
   against the internal hostname and rewriting the URL afterward will fail with
   `SignatureDoesNotMatch` from browsers and laptop CLIs.

   ```bash
   kubectl patch secret loom-secrets \
     -p '{"stringData":{"minio-public-endpoint":"https://minio.loom.example.com"}}'
   # (or add LOOM_SVC_MINIO_PUBLIC_ENDPOINT to loom-service's env block)
   kubectl rollout restart deploy/loom-service
   ```

   If the env var is unset, all presigned URLs point at the internal hostname.
   Keep that only for deployments where every API caller is inside the cluster.
   Browser, laptop CLI, and shared-dev deployments need a user-facing endpoint
   or a proxy path that forwards to MinIO.

8. **Approve team registration requests.** Public registration is
   default-closed. A researcher can submit a request without a bearer token:
   ```bash
   curl -X POST https://loom.example.com/api/v1/teams/register \
     -H "Content-Type: application/json" \
     -d '{"name":"latent-reasoning", "contact_email":"owner@example.com"}'
   ```
   An admin lists and approves pending requests through `loom_service`:
   ```bash
   curl https://loom.example.com/api/v1/admin/team-registrations?status=pending \
     -H "Authorization: Bearer $ADMIN_TOKEN"

   curl -X POST https://loom.example.com/api/v1/admin/team-registrations/$REG_ID/approve \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: qianyi"
   ```
   The approval response reveals the raw `loom_team_...` token exactly once;
   store or deliver it through an operator-approved secure channel. Reject
   accidental requests with `POST .../$REG_ID/reject` and the same actor header.
   Review backend audit evidence with:
   ```bash
   curl https://loom.example.com/api/v1/admin/audit-events?limit=20 \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Audit rows include the operator-attested actor and safe metadata such as
   token hash prefixes, never raw bearer tokens.

## Upgrade

### Breaking changes by release

**v0.3 (PR #150, 2026-06-17) — config consolidation.** Two operator-visible breakages when upgrading from a pre-#150 cluster:

1. **`worker_max_concurrent` removed from `cluster-config.toml`.** The field no longer exists on `[render_config]`. Worker concurrency is now controlled by `LOOM_WORKER_MAX_CONCURRENT` (the existing env var). If your `cluster-config.toml` set this field, delete the line — otherwise `loom cluster render` exits with `unknown keys in cluster config: ['worker_max_concurrent']`. To override the default of 5, patch the worker Deployment's env block or set the value in `config/loom-schema.toml`'s `service_config.max_concurrent` and regenerate.

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

3. **New Secret keys required.** `postgres-user` and `postgres-password` are now declared in `[infra_secrets]` and must exist in `loom-secrets` (previously the postgres template assumed they existed but nothing checked). If they're already populated, no action. If not, add them — `loom cluster bootstrap-secrets --rotate` mints fresh postgres-password and emits `<EDIT_ME>` for postgres-user.

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

`--skip-preflight` is safe here because preflight checks (Secrets,
IngressClass, StorageClass) don't change between rollouts.

Migration rollbacks: `alembic downgrade -1` from a Control Plane pod.
DB-level downgrades that drop columns are NOT reversible without
restore from snapshot — gate destructive migrations behind a flag.

## Rate-card management

Costs are computed from versioned rate cards. To bump prices:

```bash
curl -X POST https://gateway.loom.example.com/admin/rate-cards \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d @new-rate-card.json
```

The Gateway's in-memory cache invalidates immediately; in-flight
requests use whatever card was active when they started.

## Token rotation

### Worker tokens — `loom admin tokens worker`

The CP admin surface (`/admin/worker-tokens`) is NOT exposed via
Ingress; reach it through a port-forward:

```bash
kubectl port-forward deploy/loom-control-plane 8080:8080 &
export LOOM_ADMIN_TOKEN=$(yq '.[admin].token' loom-admin-secret.toml)
```

Then use the CLI (since #80):

```bash
# Mint + print rollout checklist (does NOT revoke the old token).
loom admin tokens worker rotate --expires-in-days 365

# Rollout: update Secret + restart workers (the checklist above tells
# you the kubectl commands).

# After workers re-register cleanly, revoke the old token by prefix:
loom admin tokens worker revoke <OLD_PREFIX>
```

For one-off mint or revoke without the rollout reminder:

```bash
loom admin tokens worker mint --expires-in-days 365
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

### Team tokens — `loom admin tokens team`

Team-token rotation hits `loom_service`'s public `/api/v1/tokens`
route, so it uses the bearer + server URL from `loom auth login`
(no port-forward). Admin callers must supply `--admin-actor NAME`
which the server records in `admin_audit_events`.

```bash
loom auth login --server https://loom.example.com --token env:LOOM_ADMIN_TOKEN

# Mint a fresh team token + print the rollout checklist.
loom admin tokens team rotate \
  --team-id <UUID> \
  --scopes read:own,submit \
  --expires-in-days 90 \
  --admin-actor qianyi

# After clients have moved over, revoke the old token by its 8-hex prefix:
loom admin tokens team revoke <OLD_PREFIX> --admin-actor qianyi
```

One-off mint / revoke without the rollout reminder:

```bash
loom admin tokens team mint --team-id <UUID> --admin-actor qianyi
loom admin tokens team revoke 01234567 --admin-actor qianyi
```

`--scopes` accepts a comma-separated list. Known team scopes: `read:own` and
`submit`; anything else is rejected client-side before the round-trip. `--type`
defaults to `team`, and admin credentials are managed only by
`loom service init-admin`, `loom service reveal-admin`, and
`loom service rotate-admin`. Default lifetime is 90 days.

Raw curl is still supported for scripted automation:
```bash
curl -X POST https://loom.example.com/api/v1/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "X-Loom-Admin-Actor: qianyi" \
  -d '{"type": "team", "team_id": "...",
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
are still readable via the fallback.

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
loom providers models refresh lab-vllm
loom providers models list lab-vllm
```

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
| `LoomLLMGatewayDown` | **critical** | `up{job=~".*loom-llm-gateway.*"} == 0` for 5m | Prometheus cannot scrape Gateway, so provider-call metrics are blind. | `kubectl get pods -n loom -l app=loom-llm-gateway`; `kubectl logs -n loom -l app=loom-llm-gateway --tail=200`. |
| `LoomGatewayProviderErrorRate` | warning | provider-level `loom_gateway_llm_calls_total{result!="ok"}` ratio > 5% for 10m | A provider is failing calls; common causes are expired keys, provider outage, SSRF/egress policy, or dialect drift. | `kubectl logs -n loom -l app=loom-llm-gateway --since=15m`; run `loom providers test <connection-name>`; check provider status. |
| `LoomServiceDown` | **critical** | `up{job=~".*loom-service.*"} == 0` for 5m | Prometheus cannot scrape the public API service. | `kubectl get pods -n loom -l app=loom-service`; `kubectl describe svc -n loom loom-service`; verify ServiceMonitor selectors. |
| `LoomServiceHighErrorRate` | warning | `loom_svc_http_requests_total{status_class="5xx"}` ratio > 2% for 10m | The public API is returning elevated 5xx responses. | `kubectl logs -n loom -l app=loom-service --since=15m \| grep -i "500\|exception\|traceback"`; check Control Plane and Gateway dependencies. |
| `LoomWorkerProcessDown` | warning | `up{job=~".*loom-worker.*"} == 0` for 5m | A worker scrape target is silent. `LoomNoWorkersActive` remains the page for full capacity loss. | `kubectl get pod -n loom -l app=loom-worker`; `kubectl logs -n loom -l app=loom-worker --previous`. |
| `LoomWorkerHeartbeatFailing` | warning | `rate(loom_worker_heartbeat_failures_total[5m]) > 0` for 10m | Worker heartbeats to CP are failing; CP will eventually reclaim that worker's trials. | Verify worker-to-CP reachability; check `loom_worker_claim_loop_iterations_total{result="error"}` for related connectivity failures. |
| `LoomWorkerTrialFailureRateHigh` | warning | worker `loom_worker_trials_completed_total{result!="succeeded"}` ratio > 20% for 15m | Many worker-run trials are failing, cancelling, or crashing. | Inspect `sum by (result) (rate(loom_worker_trials_completed_total[5m]))`; compare recent trajectories for common failure reasons. |
| `LoomRetryExhaustedSpiking` | warning | `rate(loom_retry_exhausted_total[5m]) > 0.1` for 10m | CP's retry-exhausted sweeper is transitioning > 6 trials/min to `failed/retry_exhausted`. Indicates either workloads consistently hitting the team's `max_attempts` quota, or a flaky upstream causing real failures across many trials. | Inspect `sum by (team_id, task_id) (rate(loom_trials_state_total{to_state="failed"}[15m]))`; correlate with `LoomWorkerTrialFailureRateHigh` + recent provider/sandbox failures; consider raising team `max_attempts` if the workload is genuinely retry-heavy. |

Thresholds are starting points — tune per team's trial volume +
workload shape. Halve the `for:` durations for staging.

## Alarm response (troubleshooting matrix)

| Symptom | Likely cause | First check |
|---|---|---|
| `loom_trials_inflight{state="claimed"}` rising, no `running` | Workers can't reach gateway/MinIO | `kubectl logs deploy/loom-worker --tail=200` for connect errors |
| `loom_worker_reclaim_total` spiking | Workers crashing or heartbeat thread blocked | Check worker memory pressure + `state_patch_error` log lines |
| 502 from Control Plane | Postgres unreachable | `kubectl exec deploy/loom-control-plane -- pg_isready -h loom-postgres` |
| Trials stuck queued | No worker matches `requires_caps` | Inspect `trials.requires_caps` vs registered `workers.capabilities` |
| 429 from Gateway | Provider rate limit | Check `loom_llm_calls_total{provider,result}` panel |
| Trajectory or artifact uploads failing | MinIO credentials wrong or runtime bucket bootstrap failed | `kubectl logs deploy/loom-worker --tail=200` for `ensure_bucket`, `trajectory_flush_failed`, or `artifact_upload_failed`; verify `mc ls loom-minio/trajectories` and `mc ls loom-minio/artifacts` |

## Backup + restore

- **Postgres:** standard `pg_dump` of the `loom` DB on a cron. Restore
  via `pg_restore` into a fresh StatefulSet; bump the deployment to
  pick up the new ReadWriteOnce volume.
- **MinIO:** workers create the `trajectories` and `artifacts` buckets
  idempotently at startup. Both buckets are immutable once a trial reaches a
  terminal state. Mirror with `mc mirror loom-minio/trajectories
  backup-store/trajectories` and `mc mirror loom-minio/artifacts
  backup-store/artifacts` on a cron. Restore: re-create the buckets and
  `mc mirror` back.

## Staging smoke gate

Before promoting a release from `dev` → `main`, exercise each user-facing
flow on a staging cluster. The gate is a manual checklist today
(automation tracked in a follow-up); every item below maps to a
concrete command or UI action.

### Prereqs

- A staging cluster deployed via `loom cluster up` against the
  candidate image tag.
- A real provider key for at least one provider (OpenAI works for the
  default benchmark sweep).
- One of the canonical task fixtures registered: `hello-world` is
  enough.

### Checklist

1. **Cluster healthy.** `loom cluster status --namespace loom` reports
   `all_ready=True`. `kubectl get pods -n loom` shows no `CrashLoopBackOff`.
2. **Public surface reachable.** `curl -sf https://<ingress_host>/api/v1/health`
   returns `200`. `curl -sf https://<ingress_host>/` returns the SPA
   index when `loom-web` replicas > 0.
3. **Boundary holds.** `loom cluster audit` exits 0. `kubectl get svc -n loom`
   shows no `LoadBalancer` / `NodePort` services. `kubectl get ingress -n loom`
   shows backends only for `loom-service` and `loom-web` (plus
   `loom-llm-gateway` when `gateway_public_host` is configured).
4. **API token issuance.** Mint a team token and verify a 401 turns
   into a 200:
   ```bash
   curl -X POST https://loom.example.com/api/v1/tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "X-Loom-Admin-Actor: smoke-operator" \
     -d '{"type":"team","team_id":"'$TEAM_ID'",
          "scopes":["read:own","submit"],"expires_in_days":7}'
   curl -sf -H "Authorization: Bearer $TEAM_TOKEN" \
     https://loom.example.com/api/v1/trials
   ```
5. **Provider connection create + test.** Create a connection through
   the CLI (or `POST /api/v1/provider-connections`), then probe:
   ```bash
   loom providers create --name smoke-openai --type openai-compatible \
     --base-url https://api.openai.com/v1 --api-key env:OPENAI_API_KEY
   loom providers test smoke-openai
   ```
   `test` must return `status=valid`; `http_status` shows the upstream
   HTTP response code. Exit code is 0 for valid, 1 for invalid.
6. **Model discovery.** `loom providers models refresh smoke-openai`
   followed by `loom providers models list smoke-openai` returns a
   non-empty catalog. `curl /api/v1/models` from a team token shows
   the agent-capable view.
7. **Submit a small batch.** Pick `hello-world` (or another canonical
   fixture) and submit:
   ```bash
   loom eval batch create \
     --name smoke-$(date +%s) \
     --benchmark hello-world \
     --provider smoke-openai --model gpt-4o-mini --agent oracle \
     --n-per-task 1
   # then tail it:
   loom eval batch show <batch-id>
   ```
   Re-run `batch show` until `state` reaches a terminal value.
8. **Live progress visibility.** While the batch runs, the SPA Tasks
   page shows the trial advancing through `queued → claimed → running`,
   and `GET /api/v1/trials/{id}` echoes the same state.
9. **Final evaluator output.** Trial reaches `succeeded` (or `failed`
   with a sensible reason). `GET /api/v1/trials/{id}` carries
   `aggregate_reward`, `cost_usd`, and `failure_reason` (when
   applicable), plus `atif_url`, `trajectory_url`, `atif_ready`,
   `trajectory_ready`, and `artifacts` for download.
10. **Trajectory + artifact download.** `GET /api/v1/trials/{id}/trajectory`
    streams events; `GET /api/v1/trials/{id}/atif` returns the ATIF
    JSON. Both bodies parse cleanly.
11. **Provider error surfaces.** Temporarily rotate the provider key
    to an invalid value, re-run a trial, and confirm the SPA + API
    surface a clear `provider_error` reason rather than a generic 500.
12. **Teardown clean.** `loom cluster down --yes` removes every applied
    object; PVCs survive (verify via `kubectl get pvc -n loom`). Pass
    `--with-volumes` only when wiping staging state intentionally.

A staging release that fails any check is NOT eligible for `main`.
Capture artifact links + a brief note for each pass in the
`docs/release-history.md` entry (or the equivalent for your fork).

### Automation status

Two CI workflows automate parts of this checklist:

- **`cluster-smoke`** (kind, label-gated `cluster-smoke`) — covers
  steps 1, 3, 12. Uses placeholder images, `--no-wait` apply, schema
  + boundary + apply + status + down round-trip. Fast (~1 min).
- **`staging-smoke`** (kind, label-gated `staging-smoke`) — builds
  REAL images, applies them, waits for every pod to reach Ready,
  probes `/healthz` + `/metrics` on every component. Closes the
  cold-start regression gap (~15-20 min).

Steps 4-11 (provider connection create + test, model discovery,
batch submission, trajectory + ATIF download, provider error
visibility) still require either a real provider key in CI secrets
or a mock OpenAI server in the staging cluster — tracked as a
follow-up to #111.

## Capacity planning

- 1 vCPU + 256 MiB per Control Plane replica handles ~200 RPS for
  PATCH/GET; bump if `state_patch_total{result="timeout"}` non-zero.
- Each Worker process runs `LOOM_WORKER_MAX_CONCURRENT` trials
  (default 5). Memory scales with the trajectory ring buffer + the
  largest artifact in flight; 8 GiB limit covers most workloads.
- For shared-dev or staging hosts outside Kubernetes, use
  [remote-worker-pool.md](remote-worker-pool.md). Start at concurrency
  5 per worker host, then raise only after CPU, RAM, Docker cleanup,
  MinIO throughput, and provider rate limits are healthy.
- Until Docker sandbox CPU/RAM limits are enforced per trial, treat
  higher worker concurrency as an operator decision backed by load-test
  evidence, not just a CPU-count formula.
- Postgres: 50 GiB volume covers ~10M trial rows. Trial rows are
  small (< 4 KiB); trajectory JSONL lives in MinIO, not Postgres.
- MinIO: depends entirely on trajectory + artifact volume. 500 GiB
  PV in the manifest is a starting point — switch to a distributed
  MinIO deployment past ~10 TiB.
