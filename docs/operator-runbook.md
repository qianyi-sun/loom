# Loom Operator Runbook

For operators of a production Loom deployment. Local dev → see the
top-level README + `deploy/docker-compose.dev.yml`.

## Initial deployment

1. **Build images.** From repo root:
   ```bash
   docker build -f deploy/Dockerfile.control-plane -t loom-control-plane:0.7 .
   docker build -f deploy/Dockerfile.gateway       -t loom-llm-gateway:0.7   .
   docker build -f deploy/Dockerfile.worker        -t loom-worker:0.7        .
   ```
   Push to your registry, then update `image:` refs in `deploy/k8s/*.yaml`.

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

3. **Apply manifests in dependency order:**
   ```bash
   kubectl apply -f deploy/k8s/postgres.yaml
   kubectl apply -f deploy/k8s/minio.yaml
   # wait for postgres + minio ready
   kubectl apply -f deploy/k8s/llm-gateway.yaml
   kubectl apply -f deploy/k8s/control-plane.yaml
   kubectl apply -f deploy/k8s/worker.yaml
   kubectl apply -f deploy/k8s/ingress.yaml
   ```

4. **Run migrations** (one-off Job, from a pod with the Control Plane image):
   ```bash
   kubectl exec deploy/loom-control-plane -- alembic upgrade head
   ```

5. **Mint an admin + worker token** via the admin API. An admin token
   has scope `admin:tokens` and is created by inserting one row into
   `tokens` directly (bootstrap problem):
   ```sql
   INSERT INTO tokens (token_hash, type, scopes, issued_at)
   VALUES (decode(sha256_hex('admin_TOKEN_RAW_VALUE'), 'hex'),
           'admin', ARRAY['admin:tokens'], now());
   ```
   Then use `POST /admin/worker-tokens` to issue worker tokens:
   ```bash
   curl -X POST https://loom.example.com/admin/worker-tokens \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -d '{"expires_in_days": 365}'
   ```
   Update the `worker-token` key in `loom-secrets` with the returned
   raw token, then `kubectl rollout restart deploy/loom-worker`.

## Upgrade

```bash
# Build + push new images
NEW_TAG=0.8
docker build -t loom-control-plane:${NEW_TAG} -f deploy/Dockerfile.control-plane .
# ... push, then bump image refs:
kubectl set image deploy/loom-control-plane control-plane=loom-control-plane:${NEW_TAG}
kubectl set image deploy/loom-llm-gateway   gateway=loom-llm-gateway:${NEW_TAG}
kubectl set image deploy/loom-worker        worker=loom-worker:${NEW_TAG}
```

Workers drain on SIGTERM (default 600 s); k8s sends SIGTERM during rollout.

## Rollback

```bash
kubectl rollout undo deploy/loom-control-plane
kubectl rollout undo deploy/loom-llm-gateway
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

Worker tokens:
1. Mint a new token via `POST /admin/worker-tokens`.
2. Update `loom-secrets` and `kubectl rollout restart deploy/loom-worker`.
3. Revoke the old token by its hash prefix:
   ```bash
   curl -X DELETE https://loom.example.com/admin/worker-tokens/$OLD_PREFIX \
     -H "Authorization: Bearer $ADMIN_TOKEN"
   ```
   Prefix is the 4–64 hex chars from `token_hash_prefix` returned at issue.

Team tokens: same flow against the `tokens` table, but the admin
endpoint for team tokens is not yet shipped — insert directly via SQL.

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
