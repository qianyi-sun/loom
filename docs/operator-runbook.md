# Loom Operator Runbook

For operators of a production Loom deployment. Local dev → see the
top-level README + `deploy/docker-compose.dev.yml`.

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

2. **Create the `loom-secrets` Secret.** Required keys:

   ```bash
   kubectl create secret generic loom-secrets \
     --from-literal=postgres-user=loom \
     --from-literal=postgres-password=$(openssl rand -hex 32) \
     --from-literal=cp-db-url=postgresql+psycopg://loom:PWD@loom-postgres/loom \
     --from-literal=gw-db-url=postgresql+psycopg://loom:PWD@loom-postgres/loom \
     --from-literal=minio-access-key=loom \
     --from-literal=minio-secret-key=$(openssl rand -hex 32) \
     --from-literal=anthropic-api-key=YOUR_KEY \
     --from-literal=openai-api-key=YOUR_KEY \
     --from-literal=worker-token=PLACEHOLDER
   ```
   The `worker-token` value is overwritten in step 5.

   Create the singleton admin secret file separately. This first #10 slice
   lets `loom_service` read the file, while later #10 slices add
   `loom service init-admin` and rotation commands. Until then, generate a
   high-entropy token manually and mount it as `loom-admin-secret`:

   ```bash
   ADMIN_TOKEN="loom_admin_$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
   cat > secrets.toml <<EOF
[admin]
token = "$ADMIN_TOKEN"
created_at = "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
version = 1
EOF
   chmod 0600 secrets.toml
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
   singleton `loom-admin-secret` mounted into both `loom_service` and the
   Control Plane. Use the same `ADMIN_TOKEN` generated in step 2; do not create
   a temporary database-backed admin row for this bootstrap path.

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
   `loom_service` batch-runner needs a `submit`-scoped team token
   to fan out trials from batches. Without it, the runner skips
   its tick with a warning — batches will not advance.

   CP's `POST /admin/worker-tokens` hardcodes `type=worker` /
   `scopes=[worker:*]` so it cannot mint a submit-scoped token. Mint
   instead via `loom_service`'s `POST /api/v1/tokens`, which DOES
   accept `type` + `scopes` + `team_id`. Port-forward `loom-service`
   (NOT CP), pick the team that will own the batches, then patch
   the secret:
   ```bash
   kubectl port-forward deploy/loom-service 8090:8090 &
   # SYSTEM_TEAM_UUID is the team_id you want batches to be
   # attributed to — typically a dedicated "system" team you
   # created via the SQL bootstrap (similar to step 5's admin
   # token), or any existing team.
   RAW=$(curl -sS -X POST http://localhost:8090/api/v1/tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d "{\"type\": \"team\", \"team_id\": \"$SYSTEM_TEAM_UUID\",
          \"scopes\": [\"submit\"], \"expires_in_days\": 365}" \
     | jq -r .token)
   kubectl patch secret loom-secrets \
     -p "{\"stringData\":{\"svc-batch-runner-cp-token\":\"$RAW\"}}"
   kubectl rollout restart deploy/loom-service
   ```

7. **Approve team registration requests.** Public registration is
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

## Upgrade

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

```bash
kubectl rollout undo deploy/loom-control-plane
kubectl rollout undo deploy/loom-llm-gateway
kubectl rollout undo deploy/loom-service
kubectl rollout undo deploy/loom-web
kubectl rollout undo deploy/loom-worker
```

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

Worker tokens (admin route is CP-internal; reach via port-forward):
1. Mint a new token via `POST /admin/worker-tokens` (port-forward CP
   first: `kubectl port-forward deploy/loom-control-plane 8080:8080 &`).
2. Update `loom-secrets` and `kubectl rollout restart deploy/loom-worker`.
3. Revoke the old token by its hash prefix:
   ```bash
   curl -X DELETE http://localhost:8080/admin/worker-tokens/$OLD_PREFIX \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Prefix is the 4–64 hex chars from `token_hash_prefix` returned at issue.

Team tokens: managed via `loom_service`'s public API:
```bash
curl -X POST https://loom.example.com/api/v1/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"type": "team", "team_id": "...",
       "scopes": ["submit"], "expires_in_days": 90}'
```
`type` is required; allowed values are `team` and `admin`.
Recognized `scopes`: `read:own`, `submit`, `admin:tokens`,
`admin:rate_cards`. Unrecognized scopes 400 at the route.

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

## Alarm response (troubleshooting matrix)

| Symptom | Likely cause | First check |
|---|---|---|
| `loom_trials_inflight{state="claimed"}` rising, no `running` | Workers can't reach gateway/MinIO | `kubectl logs deploy/loom-worker --tail=200` for connect errors |
| `loom_worker_reclaim_total` spiking | Workers crashing or heartbeat thread blocked | Check worker memory pressure + `state_patch_error` log lines |
| 502 from Control Plane | Postgres unreachable | `kubectl exec deploy/loom-control-plane -- pg_isready -h loom-postgres` |
| Trials stuck queued | No worker matches `requires_caps` | Inspect `trials.requires_caps` vs registered `workers.capabilities` |
| 429 from Gateway | Provider rate limit | Check `loom_llm_calls_total{provider,result}` panel |
| Trajectory uploads failing | MinIO credentials wrong or bucket missing | `mc alias set` against `loom-minio:9000` and `mc ls loom-minio/trajectories` |

## Backup + restore

- **Postgres:** standard `pg_dump` of the `loom` DB on a cron. Restore
  via `pg_restore` into a fresh StatefulSet; bump the deployment to
  pick up the new ReadWriteOnce volume.
- **MinIO:** the `trajectories` and `artifacts` buckets are immutable
  once a trial reaches a terminal state. Mirror with
  `mc mirror loom-minio/trajectories backup-store/trajectories` on a
  cron. Restore: re-create the buckets and `mc mirror` back.

## Capacity planning

- 1 vCPU + 256 MiB per Control Plane replica handles ~200 RPS for
  PATCH/GET; bump if `state_patch_total{result="timeout"}` non-zero.
- Each Worker pod runs `max_concurrent` (default 5) trials. Memory
  scales with the trajectory ring buffer + the largest artifact in
  flight; 8 GiB limit covers most workloads.
- Postgres: 50 GiB volume covers ~10M trial rows. Trial rows are
  small (< 4 KiB); trajectory JSONL lives in MinIO, not Postgres.
- MinIO: depends entirely on trajectory + artifact volume. 500 GiB
  PV in the manifest is a starting point — switch to a distributed
  MinIO deployment past ~10 TiB.
