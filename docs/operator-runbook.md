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
loom cluster up --config cluster-config.toml --context $YOUR_CTX

# 6. Verify
loom cluster status --namespace loom --format table
```

Each verb:

| Command | What it does | Exit codes |
|---|---|---|
| `loom cluster preflight` | API-side checks: namespace exists, required Secrets present, IngressClass installed, default StorageClass available, PSS labels OK | 0 pass / 1 fail / 2 cluster unreachable |
| `loom cluster render` | Print the rendered YAML to stdout (no cluster contact) | 0 / 2 on bad config |
| `loom cluster audit` | Static public/internal boundary check on rendered manifests: TLS ingress, only `/api/v1` → `loom-service` and `/` → `loom-web`, no LoadBalancer/NodePort, no unsafe hostPort, required NetworkPolicies present | 0 clean / 1 violation / 2 bad config |
| `loom cluster up` | Preflight → render → `kubectl apply` → wait for components ready | 0 ready / 1 not-ready / 2 unreachable or kubectl missing |
| `loom cluster status` | Live readiness snapshot with ingress endpoints | 0 all-ready / 1 not-ready / 2 unreachable |
| `loom cluster down` | `kubectl delete` of the rendered manifests; opt-in `--with-volumes` (PVCs) and `--delete-namespace` for full teardown | 0 / 1 on failure or operator-cancelled prompt |

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
   serving requests that later fail with missing-column errors.

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
   `deploy/k8s/ingress.yaml`). Reach it via port-forward:
   ```bash
   kubectl port-forward deploy/loom-control-plane 8080:8080 &
   curl -X POST http://localhost:8080/admin/worker-tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"expires_in_days": 365}'
   ```
   Update the `worker-token` key in `loom-secrets` with the returned
   raw token, then `kubectl rollout restart deploy/loom-worker`.

7. **(Optional) Provision the batch-runner CP token.** The
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

8. **Verify service-proxied downloads.** `loom_service` should use the
   cluster-internal MinIO endpoint for object reads, then stream ATIF,
   trajectory, and artifact downloads through authenticated API routes. Browser
   and laptop CLI users should not need direct access to the MinIO S3 port.

   ```bash
   curl -H "Authorization: Bearer $TEAM_TOKEN" \
     "$LOOM_API/api/v1/trials/$TRIAL_ID" \
     | jq -r '.trajectory_url,.atif_url,.artifacts[].download_url'

   loom eval trial show "$TRIAL_ID"
   loom eval trial download "$TRIAL_ID" --kind atif --output atif.json
   loom eval trial download "$TRIAL_ID" --kind trajectory --output events.jsonl
   ```

   Every returned URL should stay on the Loom API host, and a normal authorized
   `curl -L` against those URLs should return the object body without opening a
   separate MinIO tunnel. The CLI should print download commands rather than
   raw MinIO/S3 signed URLs.

9. **Approve team registration requests.** Public registration is
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
   The approval response reveals a raw `loom_invite_...` code and browser invite
   link exactly once. Deliver the link to the requested contact; the contact
   accepts it to create their user session and owner membership without seeing a
   raw team token. If the link is lost, resend the invite to rotate the stored
   hash and reveal a replacement:
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

`--skip-preflight` is safe here because preflight checks (Secrets,
IngressClass, StorageClass) don't change between rollouts.

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
OpenAI-compatible facade URL. The k8s manifest uses
`http://host.docker.internal:30443/openai/v1` so Docker sandboxes reach
the node-local gateway-router hostPort; `DockerDriver` injects the
Linux host-gateway alias for that hostname.

See `docs/architecture/agent-adapter.md` for the architecture.

### Config knobs (`config/loom-schema.toml`, `[service_config]`)

| Key | Default | What it does |
|---|---|---|
| `trial_cache_registry_repo` | `""` (unset) | Registry path to share layered images across workers (Docker Hub / GHCR / ECR / self-hosted). When unset, each worker caches locally. |
| `trial_cache_registry_pull_timeout_sec` | `15.0` | Per-attempt timeout for the registry pull. |
| `trial_cache_base_image_pull_timeout_sec` | `1800.0` | Per-attempt timeout for the underlying task-image pull (SWE-Bench instance images are 1–2 GB). |
| `trial_cache_ttl_hours` | `168` (7d) | Layered images older than this are pruned on the next eviction sweep. |
| `trial_cache_min_free_gb` | `20` | Capacity backstop — when free disk drops below this, oldest-by-creation entries are evicted first. |
| `trial_cache_build_lock_timeout_sec` | `1800.0` | Cluster-wide builder-slot TTL. The slot's owner refreshes every 60 s while building. |
| `subprocess_gateway_url` | unset; k8s manifest sets `http://host.docker.internal:30443/openai/v1` | Sandbox-facing OpenAI-compatible gateway facade URL for subprocess agents. |

### Setting up the optional shared registry (Docker Hub example)

1. Create a Docker Hub team (or pick an existing organization) and a
   private repository — e.g. `loomops/trial-cache`.
2. Create a robot account / access token with `read+write` to that
   repo.
3. Mount a `docker-config` Secret on each worker with the credentials
   the docker daemon expects:
   ```bash
   kubectl -n loom create secret docker-registry docker-config \
     --docker-server=https://index.docker.io/v1/ \
     --docker-username=<robot-user> \
     --docker-password=<robot-token>
   ```
   (`loom cluster render` wires the Secret in already — see the
   workers' StatefulSet manifest.)
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

### Team API tokens — `loom admin tokens team`

Team API-token rotation hits `loom_service`'s public `/api/v1/tokens`
route, so it uses the bearer + server URL from `loom auth login`
(no port-forward). Tokens are named, scoped, hash-stored, and reveal the raw
`loom_api_...` value only on mint/rotate. Admin callers must supply
`--admin-actor NAME`, which the server records in `admin_audit_events`.

```bash
loom auth login --server https://loom.example.com --token env:LOOM_ADMIN_TOKEN

# Mint a fresh team token + print the rollout checklist.
loom admin tokens team rotate \
  --name nightly-cli \
  --team-id <UUID> \
  --scopes read:own,submit,providers:manage \
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
  --scopes read:own,submit \
  --admin-actor qianyi
loom admin tokens team revoke 01234567 --admin-actor qianyi
```

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
public `443` or `80` work with the default gateway policy, but GPU-cluster
bastion forwards often use ports such as `18001`. Approve those endpoints in
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

Thresholds are starting points — tune per team's trial volume +
workload shape. Halve the `for:` durations for staging.

## Alarm response (troubleshooting matrix)

| Symptom | Likely cause | First check |
|---|---|---|
| `loom_trials_inflight{state="claimed"}` rising, no `running` | Workers can't reach gateway/MinIO | `kubectl logs deploy/loom-worker --tail=200` for connect errors |
| `loom_worker_reclaim_total` spiking | Workers crashing or heartbeat thread blocked | Check worker memory pressure + `state_patch_error` log lines |
| 502 from Control Plane | Postgres unreachable | `kubectl exec deploy/loom-control-plane -- pg_isready -h loom-postgres` |
| Trials stuck queued | No worker matches `requires_caps` | Inspect `trials.requires_caps` vs registered `workers.capabilities` |
| Batch finishes `all_failed` with zero child trials | Batch fan-out was rejected by deterministic submission policy, such as a hard license allowlist, after preview-time checks were bypassed or state changed | Open Batch Detail or `loom eval batch show <id>` and inspect `failure_reason`, `failure_message`, and `fanout_errors`; update policy/config, use an approved notice-only benchmark mirror, or choose a compatible benchmark |
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

Before promoting a release from `dev` to `main`, exercise the invite-only
public beta on a staging cluster and attach the evidence to the release issue or
PR. The gate has two parts:

- **Operator/browser evidence** for DNS/TLS, invite acceptance, SPA submission,
  and visual checks that require a real browser session.
- **Repeatable API evidence** from `scripts/public_beta_smoke_gate.py`, which
  verifies public API auth, provider discovery, service-proxied downloads, Run
  Library sharing, cross-team denials, provenance, and leak scanning.

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
- A real or mock OpenAI-compatible provider key for Team A. Use an environment
  reference such as `env:OPENAI_API_KEY`; do not paste raw provider keys into
  issue comments, shell history, or committed files.
- One canonical task fixture registered. `hello-world` is enough for the gate;
  another tiny task is fine if it produces ATIF, trajectory, and at least one
  safe artifact.
- A ready benchmark catalog provisioned into the staging/public-beta database
  and object store. This is release data, not test fixture data, and must not
  be created through `scripts/seed_test_data.py`.
- One seeded blocked artifact on the source trial, marked
  `share_status=blocked`, whose raw object body contains a fake secret such as
  `seeded-public-beta-secret`. The release evidence should prove Team B cannot
  download it and that the fake secret does not appear in API responses.
- One private source trial or batch with a safe artifact that Team A can read
  and Team B cannot read through Run Library.

### Benchmark catalog provisioning

Before inviting beta users or starting manual New Batch testing, copy the
known-good ready catalog and referenced task bundles into the target
environment:

```bash
export LOOM_CATALOG_SOURCE_DB_URL="$SOURCE_LOOM_DB_URL"
export LOOM_CATALOG_SOURCE_MINIO_ENDPOINT="$SOURCE_LOOM_MINIO_ENDPOINT"
export LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY="$SOURCE_LOOM_MINIO_ACCESS_KEY"
export LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY="$SOURCE_LOOM_MINIO_SECRET_KEY"

export LOOM_DB_URL="$PUBLIC_BETA_DB_URL"
export LOOM_MINIO_ENDPOINT="$PUBLIC_BETA_MINIO_ENDPOINT"
export LOOM_MINIO_ACCESS_KEY="$PUBLIC_BETA_MINIO_ACCESS_KEY"
export LOOM_MINIO_SECRET_KEY="$PUBLIC_BETA_MINIO_SECRET_KEY"

loom datasets provision-public-beta-catalog \
  --target-bucket loom-benchmarks \
  --imported-by "release:${IMAGE_TAG:-manual}"
```

The command is idempotent. It upserts only benchmarks whose stored task rows are
fully runnable, creates the target bucket when needed, copies missing
`s3://...` bundle objects, skips matching target objects, and exits non-zero if
any source task bundle prefix has no objects. A healthy run reports non-zero
`ready_benchmarks`, non-zero `ready_tasks`, and `missing=0`.

Verify the target before continuing:

```bash
loom datasets audit --all --db-url "$LOOM_DB_URL"
curl -sf -H "Authorization: Bearer $TEAM_A_TOKEN" \
  "$PUBLIC_SERVER_URL/api/v1/benchmarks?limit=200"
```

The public API response must include at least one benchmark with
team-license-allowed `task_count > 0`, and the New Batch page must show
selectable benchmark choices after sign-in.

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
4. **Invite-only onboarding.** From the operator/admin browser session, create
   an invite for a new Team A owner. Open the invite link in a fresh browser
   profile, accept it, and confirm the user lands in Team A without seeing a raw
   team token. Repeat for Team B. Capture the invite id/prefix only; do not
   capture raw invite codes.
5. **Scoped CLI tokens.** In Team Settings -> Team access, each team owner
   creates a named API token with `read:own` and `submit`; Team A also needs
   `providers:manage` for provider setup. In a fresh shell:
   ```bash
   export LOOM_API_TOKEN=$TEAM_A_TOKEN
   loom auth login --server https://loom.example.com --token env:LOOM_API_TOKEN
   loom auth whoami
   ```
   Repeat with Team B's token. Evidence should show token names/scopes/prefixes,
   never raw token values.
6. **Provider connection create + test.** As Team A, create a connection through
   the CLI (or `POST /api/v1/provider-connections`), then probe:
   ```bash
   loom providers create --name smoke-openai --type openai-compatible \
     --base-url https://api.openai.com/v1 --api-key env:OPENAI_API_KEY
   loom providers test smoke-openai
   ```
   `test` must return `status=valid`; `http_status` shows the upstream
   HTTP response code. Exit code is 0 for valid, 1 for invalid.
7. **Model discovery.** `loom providers models smoke-openai --refresh`
   followed by `loom providers models smoke-openai` returns a
   non-empty catalog. `curl /api/v1/models` from a team token shows
   the agent-capable view.
8. **Model preflight.** Run
   `loom providers models smoke-openai --preflight gpt-4o-mini`. The model row
   should show `preflight=valid`. A 401/403 should show `access-denied` without
   raw provider keys.
9. **Submit small batches from SPA and CLI.** Pick `hello-world` (or another
   canonical fixture). Submit once from the SPA New Batch page and once from the
   CLI. The CLI commands below keep the no-model canary separate from the
   provider-backed path:
   ```bash
   # No-model oracle canary; no provider/model flags are needed.
   loom eval batch create \
     --name oracle-smoke-$(date +%s) \
    --task-filter '{"task_ids":["hello-world"]}' \
     --agent oracle \
     --n-per-task 1

   # Model-backed path through the provider gateway.
   loom eval batch create \
     --name smoke-$(date +%s) \
    --task-filter '{"task_ids":["hello-world"]}' \
     --provider smoke-openai --model gpt-4o-mini --agent litellm \
     --n-per-task 1
   # then tail it:
   loom eval batch show <batch-id>
   ```
   Re-run `batch show` until `state` reaches a terminal value.
10. **Live progress visibility.** While the batch runs, the SPA Monitor page
   shows planned trials and current state transitions, and
   `GET /api/v1/trials/{id}` echoes the same state.
11. **Final evaluator output.** Trial reaches `succeeded` (or `failed`
   with a sensible reason). `GET /api/v1/trials/{id}` carries
   `aggregate_reward`, `failure_reason` (when applicable),
   `total_prompt_tokens`, `total_completion_tokens`,
   `llm_calls_count`, plus `atif_url`, `trajectory_url`,
   `atif_ready`, `trajectory_ready`, and `artifacts` for download.
   Artifact rows include `share_status` and a safe `blocked_reason`
   when org-wide sharing is blocked. Use `/api/v1/usage` for
   cost views rather than trial or batch detail responses.
12. **Trajectory + artifact download.** `GET /api/v1/trials/{id}/trajectory`
    streams event pages; `GET /api/v1/trials/{id}/trajectory/download`
    returns raw JSONL; `GET /api/v1/trials/{id}/atif` returns the ATIF JSON;
    artifact `download_url` entries from trial detail return object bodies. The
    URLs must stay on `/api/v1/trials/...`, not raw MinIO/S3 signed URLs, and
    cross-team callers must not be able to use owner-team artifact proxy URLs.
    Verify the public CLI path with `loom eval trial download ...`; it should
    write the object body locally without printing internal object-store URLs.
13. **Run Library sharing.** Confirm the completed source run appears in Run
    Library -> My team for Team A and Run Library -> All teams for Team B.
    Evidence must include the owner-team label, completed state, score/cost
    summary, task/agent/model summary, and artifact groups. Team B must be able
    to download a safe artifact only through the Run Library service URL.
14. **Clone and reuse.** From Team B, clone config from Team A's completed run.
    If the source run used a provider connection, select a Team B-owned
    provider connection before cloning. Then reuse a safe artifact from the
    source trial. Both created records must belong to Team B and show
    `source_provenance` with the source batch/trial/artifact key.
15. **Blocked and private access denied.** Team B must be denied when trying to:
    download the seeded blocked artifact through Run Library; download Team A's
    artifact through the normal owner-team trial route; mutate Team A's original
    batch, such as cancelling it; or inspect/download private or blocked source
    artifacts. Denials should include safe reasons only.
16. **Provider error surfaces.** Temporarily rotate the provider key
    to an invalid value, re-run a trial, and confirm the SPA + API
    surface a clear `provider_error` reason rather than a generic 500. Confirm
    diagnostic text does not contain raw provider keys, bearer tokens, signed
    URL query parameters, or internal service hostnames.
17. **Automated evidence script.** After steps 4-15, run the repeatable API
    gate. Use disposable staging data because clone/reuse checks create Team B
    records:
    ```bash
    python scripts/public_beta_smoke_gate.py \
      --server-url https://loom.example.com \
      --team-a-token "$TEAM_A_TOKEN" \
      --team-b-token "$TEAM_B_TOKEN" \
      --provider-connection-name smoke-openai \
      --batch-id "$TEAM_A_BATCH_ID" \
      --trial-id "$TEAM_A_TRIAL_ID" \
      --safe-artifact-key "$SAFE_ARTIFACT_KEY" \
      --blocked-artifact-key "$BLOCKED_ARTIFACT_KEY" \
      --private-trial-id "$PRIVATE_TRIAL_ID" \
      --private-artifact-key "$PRIVATE_ARTIFACT_KEY" \
      --clone-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
      --reuse-provider-connection-id "$TEAM_B_PROVIDER_CONNECTION_ID" \
      --catalog-minio-endpoint "$PUBLIC_BETA_MINIO_ENDPOINT" \
      --catalog-minio-access-key "$PUBLIC_BETA_MINIO_ACCESS_KEY" \
      --catalog-minio-secret-key "$PUBLIC_BETA_MINIO_SECRET_KEY" \
      --secret-needle seeded-public-beta-secret \
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
    service URLs before writing evidence.
17. **Teardown clean.** `loom cluster down --yes` removes every applied
    object; PVCs survive (verify via `kubectl get pvc -n loom`). Pass
    `--with-volumes` only when wiping staging state intentionally.

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
  reachability, two-team API-token auth, provider/model discovery, runnable
  benchmark catalog presence, sampled ready benchmark bundle objects,
  batch/trial detail, service-proxied ATIF/trajectory downloads, My team and
  All teams Run Library visibility, owner-team label, cross-team safe artifact
  download, direct-route denial, clone config, reuse artifact, provenance,
  blocked artifact denial, private artifact denial, cross-team mutation denial,
  and response leak scanning.

Browser-only invite acceptance, SPA visual submission, and provider-error UI
screenshots remain manual release evidence unless the staging environment adds a
mock provider and browser automation job.

## Capacity planning

- 1 vCPU + 256 MiB per Control Plane replica handles ~200 RPS for
  PATCH/GET; bump if `state_patch_total{result="timeout"}` non-zero.
- Each Worker process runs `LOOM_WORKER_MAX_CONCURRENT` trials
  (default 5). Memory scales with the trajectory ring buffer + the
  largest artifact in flight; 8 GiB limit covers most workloads.
- Each Worker process also derives `LOOM_WORKER_BLOCKING_IO_MAX_WORKERS`
  as `max(32, min(LOOM_WORKER_MAX_CONCURRENT * 4, 256))` when unset.
  This is the thread pool for blocking Docker, S3/MinIO, Hugging Face,
  and filesystem calls; it is not additional trial capacity.
- Docker-backed workers need a high open-file limit for high sandbox
  concurrency. The dev and remote-worker compose files set
  `nofile=65536`; equivalent production deployments should set the same
  limit at the container runtime or node service layer before sweeps.
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
