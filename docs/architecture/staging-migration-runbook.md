# Live migration runbook: `public-beta` → `staging`

**Status: not-yet-executed.** Run this when the pilot cluster is drained. See #417 for background.

The repo has already renamed `public-beta` → `staging` in every current-state code path, deploy config, and doc. Migration filenames and historical evidence directories retain the old name because they document past events.

The live pilot cluster on `platform-dev` is still under the old name: `kind-loom-public-beta` cluster, `loom-public-beta` k8s namespace, DB rows with `environment='public-beta'`, systemd units pointing at the old paths, GHCR image tags of the form `public-beta-<sha>`, and rollout evidence under `/data/loom-public-beta/rollouts/*`. Coordinated cutover consolidates all of that under the new name.

## Precondition — hard gate

**All of the following must be true before starting:**

- Zero non-terminal trials in the pilot cluster
  ```bash
  loom admin batches list --state running,queued --environment public-beta
  # expect: empty
  ```
- No pending Slurm jobs on OLDLAB from this environment
  ```bash
  squeue -u qianyi -o "%.9i %.9P %.20j %.8u %.8T %.10M" | grep -v "^JOBID" | wc -l
  # expect: 0 for loom-* jobs
  ```
- No active setup / build containers on shared hosts
  ```bash
  ssh oldlab1 'docker ps --format "{{.Names}}"' | grep -E "loom|task-cache|base-images"
  # expect: only steady-state control-plane / worker daemon
  ```
- Internal teams notified of the Do Not Submit window; new submissions closed at the API layer.

## Migration steps

### 1. Freeze new submissions

Bump the LLM-gateway rate-card to reject new batch creation cluster-wide, or run the coordinator playbook that closes `POST /api/v1/batches` with HTTP 503. This step is reversible — restoring the previous rate-card re-opens submissions.

### 2. Capture snapshots

Snapshot the pilot's Postgres + MinIO PVCs. These are the rollback anchors:

```bash
# Postgres logical backup
kubectl -n loom-public-beta exec loom-postgres-0 -- pg_dump -Fc loom > \
  /data/loom-public-beta/backups/pre-staging-migration-$(date -u +%Y%m%dT%H%M%SZ).dump

# MinIO bucket-list snapshot for cross-check
kubectl -n loom-public-beta exec loom-minio-0 -- mc ls loom-minio-alias > \
  /data/loom-public-beta/backups/pre-staging-migration-minio-listing.txt
```

### 3. Migrate DB rows

Rename the environment string in every table that carries it:

```sql
BEGIN;

UPDATE environment_state
   SET environment = 'staging'
 WHERE environment = 'public-beta';

UPDATE slurm_worker_jobs
   SET environment = 'staging'
 WHERE environment = 'public-beta';

UPDATE gb10_worker_pool_desired_states
   SET environment = 'staging'
 WHERE environment = 'public-beta';

UPDATE worker_pool_autoscaler_policies
   SET environment = 'staging'
 WHERE environment = 'public-beta';

-- Verify counts before committing
SELECT 'environment_state', COUNT(*) FROM environment_state WHERE environment = 'staging'
UNION ALL
SELECT 'slurm_worker_jobs', COUNT(*) FROM slurm_worker_jobs WHERE environment = 'staging'
UNION ALL
SELECT 'gb10_worker_pool_desired_states', COUNT(*) FROM gb10_worker_pool_desired_states WHERE environment = 'staging'
UNION ALL
SELECT 'worker_pool_autoscaler_policies', COUNT(*) FROM worker_pool_autoscaler_policies WHERE environment = 'staging';

COMMIT;
```

Grep the codebase to catch any additional tables added after this runbook was written:

```bash
grep -rn "environment.*public-beta\|environment.*staging" src/loom/db/schema.py
```

### 4. Stop live tunnels + autoscaler timer

On the tunnel-hosting user account (currently `qianyi` on OLDLAB / platform-dev):

```bash
systemctl --user stop loom-remote-worker-tunnel-control-plane.service
systemctl --user stop loom-remote-worker-tunnel-gateway.service
systemctl --user stop loom-remote-worker-tunnel-minio.service
systemctl --user stop loom-remote-worker-tunnel-watchdog.timer
systemctl --user stop loom-oldlab-autoscaler.timer
```

### 5. Stand up the fresh `loom-staging` cluster

```bash
kind create cluster --name loom-staging \
  --config .claude/plans/kind-loom-staging.yaml   # copy the loom-public-beta cluster shape

# Load the current release images with the new tags
IMAGE_TAG="staging-$(git rev-parse --short=12 HEAD)"
for img in control-plane service llm-gateway worker web gateway-router; do
  docker pull ghcr.io/qianyi-sun/loom-${img}:${IMAGE_TAG}
  kind load docker-image ghcr.io/qianyi-sun/loom-${img}:${IMAGE_TAG} --name loom-staging
done

loom cluster up --config deploy/environments/staging.cluster.toml \
  --namespace loom-staging \
  --image-tag ${IMAGE_TAG}
```

### 6. Restore Postgres + MinIO state

Choose one restoration route:

**a) Restore from backup (safer, requires downtime = backup + restore time):**

```bash
kubectl -n loom-staging exec loom-postgres-0 -- pg_restore -c -d loom < \
  /data/loom-public-beta/backups/pre-staging-migration-<timestamp>.dump
```

MinIO objects need `mc mirror`:

```bash
mc mirror loom-public-beta-minio/ loom-staging-minio/ --overwrite
```

**b) Migrate PVCs in place (faster, riskier):**

If using `static-host-path` persistent storage, physically move `/data/loom-public-beta/postgres` → `/data/loom-staging/postgres` and `/data/loom-public-beta/minio` → `/data/loom-staging/minio`, then have the new PVCs bind to those paths via `volumeName`.

**Recommended: route (a).** The extra time is worth not losing the rollback anchor.

### 7. Restart tunnels + autoscaler with new paths

Regenerate the tunnel systemd units targeting the new namespace:

```bash
uv run python scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --context kind-loom-staging \
  --output-dir ~/.config/systemd/user/

systemctl --user daemon-reload
systemctl --user start loom-remote-worker-tunnel-control-plane.service
systemctl --user start loom-remote-worker-tunnel-gateway.service
systemctl --user start loom-remote-worker-tunnel-minio.service
systemctl --user start loom-remote-worker-tunnel-watchdog.timer
systemctl --user start loom-oldlab-autoscaler.timer
```

Reconfigure the OLDLAB autoscaler working directory:

```bash
# The existing autoscaler timer points at /home/qianyi/dev/loom-worktrees/public-beta-<sha>
# Update the systemd unit's ExecStart working dir to a fresh checkout tagged staging-<sha>
```

### 8. Environment-state apply

```bash
loom admin environment-state apply \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG=${IMAGE_TAG} \
  --var ENV_CONFIG_VERSION=${IMAGE_TAG} \
  --var GIT_SHA=${RELEASE_SHA}
```

### 9. Release-gate verification

```bash
loom cluster release-gate --format json \
  --environment staging \
  --image-tag ${IMAGE_TAG} \
  > /data/loom-staging/rollouts/$(date -u +%Y%m%dT%H%M%SZ)-staging-first-gate.json
```

**Acceptance:** the JSON reports `all_pass=true`, `environment=staging`, `drift=[]`. If any check fails, roll back immediately (step 11).

### 10. Reopen submissions

Restore the LLM-gateway rate-card. Send internal-teams the staging route (`https://yylx.world/dev`), the new evidence directory (`/data/loom-staging/rollouts`), and a summary of the rename.

### 11. Retire the old cluster

Once one successful test batch runs end-to-end on `loom-staging`:

```bash
kind delete cluster --name loom-public-beta
kubectl delete ns loom-public-beta   # if any stray namespace exists

# Preserve evidence — DO NOT delete /data/loom-public-beta/
# It contains the last-known-good rollout evidence + backup snapshots.
# Archive as read-only:
sudo chmod -R a-w /data/loom-public-beta
```

Old GHCR image tags stay in place — they're immutable references from past rollout evidence.

## Rollback

If any step 6+ fails:

1. Stop the new tunnels + autoscaler.
2. Delete the fresh `kind-loom-staging` cluster.
3. Revert the DB migration (step 3) — the values were `public-beta`, restore them:
   ```sql
   BEGIN;
   UPDATE environment_state SET environment='public-beta' WHERE environment='staging';
   -- same for the other 3 tables
   COMMIT;
   ```
4. Restart the old tunnels + autoscaler.
5. If the old `kind-loom-public-beta` cluster was already deleted (only possible at step 11), restore Postgres from the pre-migration snapshot into a rebuilt cluster.

Rollback window closes at step 11. Everything before that is fully reversible.

## Rollback anchor lifecycle

| When | Retain | Delete |
|---|---|---|
| During cutover | Everything under `/data/loom-public-beta/*` and the Postgres/MinIO snapshots taken in step 2 | Nothing |
| After successful cutover + 1 week of clean rollouts on `loom-staging` | `/data/loom-public-beta/rollouts/*` (historical evidence) | `/data/loom-public-beta/backups/pre-staging-migration-*` snapshots may be moved to cold storage |
| After 3 months | Historical evidence stays (permanent record) | Backups can be deleted if no rollback needed |

## Timing estimate

- Steps 1–4 (drain + freeze + backup): 15–30 minutes
- Step 5 (fresh cluster + image load): 10 minutes
- Step 6 (restore from backup): 10–30 minutes depending on data volume
- Steps 7–10 (start tunnels, apply state, gate, reopen): 10 minutes

**Total: ~1 hour of downtime for internal teams.** Longer if PVCs are large.

## Related

- Repo PR: (link here after opening)
- Parent tracking: #417
- Precondition validation: run the `staging` variants of `docs/staging-launch.md` §"Preflight" section
