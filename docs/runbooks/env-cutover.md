# Env cutover runbook

Executes the operational side of #857 (env redesign). Do this AFTER the code PRs merge.

## Prereqs

- `#858` (path convention refactor), `#860` (multi-controller Slurm schema), `#861` (rename → local), and the env-config batch PR all merged to `dev`.
- Access to bb8-1 as `hongjian` (has kubectl for the k3s cluster).
- SSH keys to bb8-2, bb8-3, and the GB10 hosts (or coordination with someone who does).

## Order

Each step is reversible. Do NOT skip validation before the next step.

### Step 1 — Filesystem convention move

`/shared_work/qianyi/` and `/home/qianyi/loom-worker-build-*` retire in favour of `/shared_work/loom/`.

```bash
# On bb8-1 (or wherever the shared_work volume mounts)
sudo mkdir -p /shared_work/loom
sudo mv /shared_work/qianyi/loom-* /shared_work/loom/
sudo chown -R hongjian:loom-rollout /shared_work/loom/  # or your team group
```

On each GB10 host (14 hosts: trt-gb10-1..15 minus -7):

```bash
for host in trt-gb10-{1,2,3,4,5,6,8,9,10,11,12,13,14,15}; do
  ssh $host 'sudo mv /home/qianyi/loom-worker-build-staging /shared_work/loom/ 2>/dev/null && sudo systemctl restart loom-gb10-node-agent.service'
done
```

Verify: `loom-gb10-node-agent.service` restarted cleanly on each host.

### Step 2 — GitHub Environments

In repo Settings → Environments:

1. Rename `development` → `local` (or archive `development` and create fresh `local`).
2. Create new `dev` environment.
3. Set secrets on `dev`:
   - `LOOM_KUBECONFIG_B64`, `LOOM_CLUSTER_CONFIG_B64`, `LOOM_DEPLOY_TOKEN`
   - `LOOM_SECRET_STORE_MASTER_KEY`, `LOOM_SERVICE_API_TOKEN`, `LOOM_WORKER_TOKEN`
   - `LOOM_PROVIDER_SECRET_REF`, `YIBUAPI_API_KEY`
4. Set var: `LOOM_ROLLOUT_LOCK_DIR` on both `dev` and `staging` (if not inherited).

### Step 3 — Deploy new staging on k3s multi-node

```bash
# From GH Actions: workflow_dispatch on deploy-environment.yml
# - environment: staging
# - image_tag: <latest built tag>
# - dry_run: true  (first pass; verify manifests render cleanly)
```

Once dry-run looks good, re-run with `dry_run: false`. This:
- Creates `loom-staging` namespace on k3s
- Brings up CNPG postgres (3 replicas), MinIO (3 replicas), control-plane, etc.
- **Does NOT yet set the Slurm multi-controller env var** (post-deploy manual step)

Post-deploy:

```bash
# Configure multi-controller Slurm dispatch — see the block in staging.cluster.toml
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl -n loom-staging set env deploy/loom-control-plane deploy/loom-family-orchestrator \
  LOOM_CP_SLURM_WORKER_CONTROLLERS_JSON='[
    {"enabled":true,"environment":"staging","pool_name":"oldlab",
     "allowed_nodes":["TRT-EAI-OLDLAB-1","trt-EAI-OLDLAB-2","trt-eai-oldlab-3","trt-eai-oldlab-4","trt-eai-oldlab-5"],
     "env_file":"/shared_work/loom/loom-worker-capacity/staging-oldlab.env",
     "repo_dir":"/shared_work/loom/loom-remote-worker",
     "time_limit":"3-00:00:00","requested_cpus":8,"requested_memory_mib":32000,
     "requested_concurrency":4,"max_jobs":5,"pending_job_cap":2,
     "min_queued_trials":1,"stale_after_seconds":300,"command_timeout_seconds":20.0}]'
```

Then wait for the control-plane pods to roll (~30s). Verify:

```bash
kubectl -n loom-staging logs -l app=loom-control-plane --tail=20 | grep -i slurm
# Should show "loom-cp-elastic-slurm-worker-controller-oldlab" task started
```

### Step 4 — Migrate task rows from `loom-multinode-test` to `loom-staging`

Both namespaces are on the same k3s cluster and use the same MinIO instance. Object-store paths are identical (`s3://loom-benchmarks/...`), so only the DB rows need to move.

```bash
# Dump Benchmark + Task rows from loom-multinode-test's postgres
kubectl -n loom-multinode-test exec loom-postgres-1 -- \
  pg_dump -t benchmarks -t tasks --data-only -d loom \
  > /tmp/mnt-benchmarks-tasks.sql

# Load into loom-staging's postgres
kubectl -n loom-staging exec loom-postgres-1 -- \
  psql -d loom_staging < /tmp/mnt-benchmarks-tasks.sql
```

Verify counts match:

```bash
for ns in loom-multinode-test loom-staging; do
  kubectl -n $ns exec loom-postgres-1 -- psql -d loom -c 'select count(*) from tasks;'
done
```

### Step 5 — Deploy new dev on the same k3s cluster

```bash
# workflow_dispatch: environment=dev, image_tag=<latest>
```

Creates `loom-dev` namespace. Empty DB — team seeds via `loom datasets sync-config` or per-benchmark publish/register commands.

Post-deploy: set the multi-controller env var (block from dev.cluster.toml comment).

### Step 6 — Team cutover

Announce to team, then have each dev:

1. Update their local kubeconfig / env vars to point at:
   - Frontend: `https://yylx.world:8443/dev` (or `/staging` for integration tests)
   - API base: same
2. Verify auth works (each dev logs in as their own user).
3. Migrate any in-flight batches (finish them, or accept they were dev experiments).

### Step 7 — Retire the kind cluster + loom-multinode-test namespace

**After team confirms new envs are working AND no active batches on the old ones:**

```bash
# Stop + remove the kind cluster
docker stop loom-staging-control-plane
docker rm loom-staging-control-plane
# Frees ports 80/443 on bb8-1

# Delete the test namespace
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl delete namespace loom-multinode-test
```

Once ports 80/443 are free, the ingress in `loom-staging` / `loom-dev` can move off :8443 → 443, restoring the standard-URL story (`https://yylx.world/staging`, `https://yylx.world/dev`).

## Rollback

Each step is reversible:

- **Step 1**: `mv /shared_work/loom/loom-* /shared_work/qianyi/` (or symlink)
- **Step 2**: GH environment secrets in git via `terraform` if you use it, otherwise re-enter from a password manager
- **Step 3**: `kubectl delete namespace loom-staging` — old kind cluster still serves as fallback
- **Step 5**: `kubectl delete namespace loom-dev`
- **Step 7**: `docker start loom-staging-control-plane` (data persists on Docker volume) + `kubectl create namespace loom-multinode-test` + restore DB dump

## Related

- Umbrella: #857
- Follow-up: cluster-render support for `[[slurm_worker_controllers]]` array-of-tables (avoid `kubectl set env` step)
- Follow-up: retire `sudo -u qianyi` in `staging_rollout_host.py` (multi-operator whitelist)
