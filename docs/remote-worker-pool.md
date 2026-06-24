# Remote Worker Pool

This runbook is for joining extra Docker-capable hosts to an existing
Loom control node. It is useful for shared development, staging, or a
small internal deployment before moving to full Kubernetes cluster mode.

The model is horizontal scaling: each worker host runs one Loom Worker
process and starts trial sandboxes through that host's Docker engine.
CPU and memory are not combined into one larger machine; total capacity
comes from more workers claiming independent trials.

## Topology

```
control node
  postgres
  minio
  loom-control-plane
  loom-llm-gateway
  loom-service / web

worker host A
  loom-worker
  docker sandbox containers

worker host B
  loom-worker
  docker sandbox containers
```

Remote workers connect to the control node through these URLs:

- `LOOM_WORKER_CONTROL_PLANE_URL`
- `LOOM_WORKER_GATEWAY_URL`
- `LOOM_WORKER_SUBPROCESS_GATEWAY_URL` when subprocess agents need a
  different gateway URL from inside Docker sandboxes
- `LOOM_WORKER_MINIO_ENDPOINT`

Do not expose those worker-facing endpoints to the public internet. Use
a private network, VPN, or firewall rules that allow only trusted worker
hosts to reach them.

## Control-node Service Tunnels

When the control node is a Kubernetes cluster and remote workers live outside
that cluster, expose the worker-facing services through durable private
tunnels. Do not leave these as terminal-owned `kubectl port-forward` processes:
they disconnect when the target pod is recreated during rollout and silently
detach the remote worker pool.

`scripts/ops/worker_service_tunnels.py` renders or installs systemd user units
for the three private dependencies:

| Unit | Private port | Kubernetes service |
|---|---:|---|
| `loom-remote-worker-tunnel-control-plane.service` | `18081` | `loom-control-plane:8080` |
| `loom-remote-worker-tunnel-gateway.service` | `19100` | `loom-llm-gateway:9100` |
| `loom-remote-worker-tunnel-minio.service` | `19000` | `loom-minio:9000` |

Render units for review:

```bash
scripts/ops/worker_service_tunnels.py render-systemd \
  --output-dir ./loom-remote-worker-tunnels \
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig
```

Install and start them as user services on the control node:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig
```

Use durable paths for `--kubectl` and `--kubeconfig`. `install-systemd`
rejects `/tmp`-style paths by default because those units must survive host
reboot. For a disposable test only, pass `--allow-volatile-paths`.

For user services to survive host reboot, enable lingering for the deploy user
with the host's normal privileged administration path:

```bash
loginctl enable-linger "$USER"
```

Check the units after every cluster rollout:

```bash
systemctl --user status \
  loom-remote-worker-tunnel-control-plane.service \
  loom-remote-worker-tunnel-gateway.service \
  loom-remote-worker-tunnel-minio.service
```

The remote-worker env file should point at the private control-node address and
the managed local ports:

```bash
LOOM_WORKER_CONTROL_PLANE_URL=http://control-node.lan:18081
LOOM_WORKER_GATEWAY_URL=http://control-node.lan:19100
LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000
```

The same script provides the rollout gate for those exact URLs:

```bash
scripts/ops/worker_service_tunnels.py check \
  --env-file .env.remote-worker
```

Validate from worker hosts too, not only from the control node:

```bash
scripts/ops/worker_service_tunnels.py check-remote worker-hosts.txt \
  --env-file .env.remote-worker
```

`check-remote` sends only the derived health URLs over SSH. It does not send or
print worker tokens, MinIO secret keys, or provider credentials.

If workers are reachable only through Slurm allocations rather than SSH, print
the same secret-free check script and pipe it into `srun`:

```bash
scripts/ops/worker_service_tunnels.py print-check-script \
  --env-file .env.remote-worker \
  | srun --jobid "$REMOTE_WORKER_JOB_ID" --overlap --ntasks=1 bash -s
```

## Prerequisites

On each worker host:

- Docker Engine is installed and running.
- The deploy user can read `/var/run/docker.sock`.
- The host can reach the control node's Control Plane, Gateway, and
  MinIO endpoints.
- The host has either a Loom checkout that can build `deploy/Dockerfile.worker`
  or access to a registry image tagged as `loom-worker:<tag>`.
- A worker token has been minted by an operator. See
  [operator-runbook.md](operator-runbook.md#worker-tokens--loom-admin-tokens-worker).

## Inventory Check

Create a hostfile containing SSH targets you are allowed to inspect:

```text
worker-a.example.internal
worker-b.example.internal
worker-c.example.internal
```

Run the non-destructive inventory script from an operator machine that
can SSH to the candidates:

```bash
export LOOM_WORKER_CONTROL_PLANE_URL=http://control-node.lan:18081
export LOOM_WORKER_GATEWAY_URL=http://control-node.lan:19100
# Optional when the sandbox's network view differs from the worker process.
# For example, use a node-local router or host-gateway URL.
# export LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30443/openai/v1
export LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000

scripts/ops/worker_pool_inventory.sh worker-hosts.txt
```

For first-time SSH contact, keep host-key handling explicit. For
example, use a temporary known-hosts file during discovery:

```bash
kh=$(mktemp)
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$kh" \
  scripts/ops/worker_pool_inventory.sh worker-hosts.txt
rm -f "$kh"
```

The script does not scan a subnet. It only connects to the hosts listed
in the hostfile and prints CPU, memory, disk, Docker status, and
open-file limits plus reachability back to the control node endpoints.

For production/public-beta capacity, the hostfile should include every
candidate worker node the operator is allowed to use. Exclude a node
only with a recorded reason such as missing Docker, failed endpoint
reachability, insufficient disk, or Slurm reservation policy.

The staged OLDLAB public-beta plan is recorded under
`deploy/worker-pools/oldlab/`. It intentionally requests only `12 CPU` and
`58000M` per node with `LOOM_WORKER_MAX_CONCURRENT=6`, even when inventory
shows more total host capacity.

## Capacity Plan

Convert the inventory output into an initial per-node concurrency plan:

```bash
scripts/ops/worker_pool_inventory.sh worker-hosts.txt > worker-inventory.txt

scripts/ops/worker_pool_plan.py \
  --inventory worker-inventory.txt \
  --cpu-per-trial 2 \
  --mem-mib-per-trial 8192 \
  --max-per-host 96 \
  > worker-plan.csv
```

The planner emits CSV:

```text
host,status,cpus,mem_total_mib,docker_cpus,recommended_concurrency,reason
worker-a,include,64,262144,64,32,
worker-b,exclude,,,,0,ssh failed
```

The heuristic is intentionally an initial setting, not a final ceiling:
it chooses the minimum of Docker/host CPU, RAM, and `--max-per-host`.
Operators should raise or lower the per-host value after real benchmark
load tests show CPU, RAM, Docker cleanup, MinIO/object-store writes,
Gateway/provider calls, and Control Plane state updates are healthy.

## Slurm Launch From A Plan

On Slurm-managed pools, dry-run one worker job per included plan row:

```bash
scripts/ops/worker_pool_slurm_submit.sh worker-plan.csv \
  --env-file /secure/path/.env.remote-worker \
  --repo-dir /opt/loom \
  --dry-run
```

After reviewing the printed `sbatch` commands, submit:

```bash
scripts/ops/worker_pool_slurm_submit.sh worker-plan.csv \
  --env-file /secure/path/.env.remote-worker \
  --repo-dir /opt/loom \
  --yes
```

The script uses `--nodelist=<host>` for each included row and exports
the row's `recommended_concurrency` as `LOOM_WORKER_MAX_CONCURRENT`.
It requests the row's CPU and memory values and `--exclusive` so a
remote worker can consume the node up to the measured stable boundary.
Keep the env file untracked and available on each worker node. The `--repo-dir`
path must also exist with `deploy/docker-compose.remote-worker.yml` on every
included node; prefer a shared checkout path such as
`/shared_work/<operator>/loom-remote-worker` for OLDLAB-style pools. A
control-node-local `/home/.../loom` checkout is not sufficient unless a Slurm
job has verified it on each target node. This script is for manual or staged
launches. For elastic pools, prefer the Control Plane controller below so batch
submission stays independent of Slurm latency.

## Elastic Slurm Controller

The Control Plane can run an internal elastic Slurm worker controller loop. It
observes queued Loom trials and the `slurm_worker_jobs` registry, submits
worker jobs when backlog exceeds active capacity, and cancels still-pending
jobs after the queue drains. Batch creation never calls Slurm synchronously.

Enable it only in environments where the Control Plane process can run
`sbatch`, `squeue`, `sacct`, and `scancel` with the intended Slurm identity:

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
LOOM_CP_SLURM_WORKER_CONTROLLER_TIME_LIMIT=7-00:00:00
# Optional when the site uses a named partition:
# LOOM_CP_SLURM_WORKER_CONTROLLER_PARTITION=cpu
```

For OLDLAB 1-5, start from
`deploy/worker-pools/oldlab/controller.env.example` and replace only the
environment-specific remote-worker env file and repo directory paths. Both
paths must be visible from every allowed node, not just from the control node.

The controller submits at most one active Loom worker job per allowed node and
relies on the registry's active-capacity uniqueness guard for dedupe. Existing
pending jobs pause new submissions once `PENDING_JOB_CAP` is reached; running
plus pending jobs are also bounded by `MAX_JOBS`. Pending jobs are cancelled
when there are no ready queued trials. Workers that start late still register
and claim normally through the standard worker token and service URLs in the
remote-worker env file.

Keep worker tokens, MinIO credentials, and provider credentials only in the
remote-worker env file. The registry stores a redacted env snapshot for
operator diagnostics.

Operational controls:

- Temporarily exclude a node by removing it from
  `LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES`; mirror that exclusion in
  `deploy/worker-pools/oldlab/worker-plan.csv` with a reason before the next
  release evidence update.
- Lower concurrency by changing
  `LOOM_CP_SLURM_WORKER_CONTROLLER_REQUESTED_CONCURRENCY` and the
  `recommended_concurrency` column in the matching plan.
- Lower total elastic footprint with
  `LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS`.
- Cancel currently pending work by reading job ids from
  `loom admin slurm-workers status --format json` and running `scancel` for the
  pending Slurm jobs. The controller will also cancel pending jobs when the
  Loom queue drains.
- Disable the pool by setting
  `LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=false` and rolling the Control Plane.

## Slurm Job Registry

The Control Plane records every Loom-submitted Slurm job in the registry after
`sbatch` returns. The registry stores the environment, pool name, nodelist,
requested CPU, requested memory, requested concurrency, Slurm job id/state,
optional worker id, timestamps, and a redacted copy of the submitted worker
environment. Secret-looking env keys such as tokens, passwords, credentials,
and keys are stored as `<redacted>`.

Inspect the registry through the CP admin surface:

```bash
loom admin slurm-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token
```

For scripting, use `--format json`. The output is safe for issue comments and
release evidence because it contains only redacted env values.

The registry's normalized states are:

| State | Meaning |
|---|---|
| `pending` | Slurm reports queued/configuring and no worker has usable capacity yet. |
| `running` | Slurm reports running; capacity is counted as active slots. |
| `completed` | Slurm completed the worker job. With idle-exit this is expected after queue drain. |
| `failed` | Slurm reported failed, timed out, node failure, OOM, preempted, or submission failed before a job id existed. |
| `cancelled` | Slurm reported cancellation. Use pending reason/logs to distinguish operator cancellation from policy preemption. |
| `stale` | Loom had an active record, but the Slurm reconcile pass no longer saw the job after the stale window. |

The elastic controller reconciles the table from `squeue` and `sacct`,
including pending reasons where Slurm reports them. It must not submit another
job for the same environment, pool, nodelist, CPU, memory, and concurrency
while an active `pending` or `running` record already exists.

## Start A Remote Worker

On the worker host, copy the example env file to an untracked file:

```bash
cp deploy/remote-worker.env.example .env.remote-worker
```

Edit `.env.remote-worker`:

```bash
LOOM_IMAGE_TAG=dev
LOOM_WORKER_CONTROL_PLANE_URL=http://control-node.lan:18081
LOOM_WORKER_GATEWAY_URL=http://control-node.lan:19100
# Leave unset when the sandbox can use the same gateway URL. Set when
# subprocess agents run in Docker sandboxes that need a host-gateway or
# node-local router endpoint.
# LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30443/openai/v1
LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000
LOOM_WORKER_TOKEN=loom_w_...
LOOM_WORKER_MINIO_ACCESS_KEY=...
LOOM_WORKER_MINIO_SECRET_KEY=...
LOOM_WORKER_MAX_CONCURRENT=5
# Fixed workers should leave this unset. Elastic Slurm workers should opt in.
# LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS=600
# Optional: leave unset unless a capacity sweep says blocking I/O is the bottleneck.
# LOOM_WORKER_BLOCKING_IO_MAX_WORKERS=128
```

Start only the worker service:

```bash
docker compose \
  --env-file .env.remote-worker \
  -f deploy/docker-compose.remote-worker.yml \
  up -d --build
```

Watch registration and claim activity:

```bash
docker compose \
  --env-file .env.remote-worker \
  -f deploy/docker-compose.remote-worker.yml \
  logs -f worker
```

Stop the worker without deleting cached trajectory or benchmark data:

```bash
docker compose \
  --env-file .env.remote-worker \
  -f deploy/docker-compose.remote-worker.yml \
  down
```

## Local-folder benchmarks

If operators want this worker to evaluate `[[local]]` benchmarks
registered via `config/benchmarks.toml`, the worker host needs:

1. `LOOM_WORKER_FIXTURES_ROOT` set to a directory containing
   `<benchmark-id>/<task>/task.toml` bundles for every registered
   `[[local]]` benchmark.
2. The same data populated on disk (host bind-mount in compose; PV
   or hostPath in k8s).

Sync from the control-plane side runs on `loom service up` (dev) or
via `loom datasets sync-config` (operator-driven on k8s). Without
the fixtures-root data, the worker can claim trials for that
benchmark but `FixtureMaterializer` will log a warning and leave
the task dir empty — the trial then fails at agent start.

## Capacity Settings

Per-host trial concurrency is controlled by `LOOM_WORKER_MAX_CONCURRENT`.
The remote-worker compose default is 5 for first contact, but production
capacity should come from the inventory and capacity-plan flow above.

Elastic Slurm workers should also set
`LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS` in the remote-worker env file. When
the worker has no in-flight trials and repeated claim attempts find no
work for that window, it logs `worker_idle_exit`, updates its Control
Plane heartbeat status to `idle-exit`, drains, and exits with success so
Slurm records the job as completed. Leave this value unset for fixed
Kubernetes workers or manually managed remote workers that should stay
online.

Recommended idle-exit values:

| Environment | Setting | Rationale |
|---|---:|---|
| Fixed Kubernetes worker | unset | Keep baseline capacity online. |
| Dev or staging elastic Slurm | 300 seconds | Release idle allocations quickly while preserving short queue bursts. |
| Production OLDLAB elastic Slurm | 600-900 seconds | Avoid churn during real batch bursts; use 900 seconds when submissions are bursty. |

Keep Slurm `--time` as a hard upper bound even when idle-exit is enabled.
Idle-exit releases allocations after queue drain; `--time` still protects
against stuck jobs, host leaks, or worker bugs.

Idle-exit also appears in the Slurm capacity registry when the worker heartbeat
status is `idle-exit`. Operators should treat `completed` Slurm jobs with
`idle-exit` worker status as normal elastic shrink, not as capacity failure.

The Worker also configures Python's default blocking-I/O executor for
Docker, S3/MinIO, Hugging Face, and filesystem calls. Leave
`LOOM_WORKER_BLOCKING_IO_MAX_WORKERS` unset for normal operation; the
Worker derives it from trial concurrency as:

```text
max(32, min(LOOM_WORKER_MAX_CONCURRENT * 4, 256))
```

This executor setting is not additional trial capacity. It only prevents
blocking setup and sandbox calls from capping admission around Python's
small default thread pool. Override it only when a single-worker sweep
shows blocking I/O threads are still the first bottleneck.

Service-mode tasks that carry `environment.dockerfile` are built on the worker
host from the materialized task bundle, or from
`environment.docker_build_context` when the task declares one. Keep
`LOOM_TASK_IMAGE_BUILD_MAX_FILES` and `LOOM_TASK_IMAGE_BUILD_MAX_BYTES` at their
defaults (2000 files and 536870912 bytes) unless a capacity test proves the
host can safely absorb larger Docker build contexts. Exceeding either limit
fails the trial during setup with a diagnostic before Docker build starts.
Tasks with `environment.sidecars` require Docker networking support on the
worker host; each trial starts the sidecars on the same per-trial bridge as the
primary sandbox and removes them during teardown.

The remote-worker compose file also raises the worker container's open-file
limit to `nofile=65536`. High sandbox concurrency opens Docker socket,
HTTP, object-store, and filesystem descriptors at the same time; the common
default soft limit of 1024 can make Docker cleanup fail with `Too many open
files`, which in turn leaves sandbox containers behind. Verify a new worker
host with:

```bash
docker compose --env-file .env.remote-worker \
  -f deploy/docker-compose.remote-worker.yml \
  exec worker sh -c 'ulimit -n'
```

The output should be at least `65536` before running high-concurrency sweeps.

Use this formula for the initial ceiling:

```text
total_trial_concurrency = worker_host_count * LOOM_WORKER_MAX_CONCURRENT
```

Recommended rollout:

| Stage | Per-host setting | Purpose |
|---|---:|---|
| Smoke | 1 | Prove one remote worker can claim and finish a trial. |
| Conservative | 5 | Match the remote-worker compose default and validate stable shared-dev capacity. |
| Planned | `worker-plan.csv` | Use every healthy node at its recommended starting concurrency. |
| Higher | above plan | Requires explicit load-test evidence from CPU, RAM, Docker cleanup, MinIO, gateway/provider, and Control Plane state-patch health. |

Do not raise concurrency only because host CPU appears idle. API-model
evaluations can still bottleneck on provider rate limits, artifact IO,
Postgres state updates, MinIO writes, or sandbox cleanup.

Until Docker sandbox CPU/RAM limits are enforced per trial, keep shared
worker hosts conservative. A single workload can otherwise consume more
than its fair share of the host.

## Single-Worker Capacity Sweep

When validating a new worker image or host class, isolate one worker
host first and sweep upward before scaling the fleet. The goal is to find
the stable per-container ceiling and the first real bottleneck, not just
to prove one target succeeds.

Use a low-cost S3-backed oracle task such as `qa255-sleep-60s`, then run
increasing targets such as 64, 96, 128, 160, 192, 224, and 256 trials.
Continue or binary-search if the host is still healthy. Stop when one of
these happens:

- Success rate drops below the operator threshold.
- Peak overlap stops increasing materially across at least two higher
  targets.
- Tail latency, claim/start latency, Docker/MinIO errors, or cleanup
  leakage crosses the operator threshold.
- Host CPU, memory, disk, file descriptors, or Docker daemon pressure
  reaches the safety limit.

Record target concurrency, submitted trials, succeeded/failed/cancelled
counts, peak overlap from `started_at`/`finished_at`, claim span, start
span, p95/p99 runtime, tail latency, host CPU/memory/disk pressure,
Docker daemon errors, MinIO/S3 errors, and cleanup results. Every stage
must finish with no leaked sandbox containers, Docker networks, worker
temp dirs, or trajectory cache files.

## Validation Gate

Before treating a remote worker pool as usable:

1. Install or verify durable control-node service tunnels with
   `scripts/ops/worker_service_tunnels.py check`.
2. Run `scripts/ops/worker_service_tunnels.py check-remote` from every
   candidate worker context and record endpoint reachability.
3. Inventory every candidate worker and record CPU, memory, disk, Docker,
   and endpoint reachability.
4. Generate `worker-plan.csv`; every usable node should be `include`,
   and every excluded node needs a reason.
5. Start one remote worker at `LOOM_WORKER_MAX_CONCURRENT=1`.
6. Submit a tiny API-model + Docker-terminal evaluation and verify the
   remote worker claims it.
7. Confirm the trial reaches a terminal state and artifacts/trajectory
   downloads work. Workers bootstrap both runtime buckets
   (`trajectories` and `artifacts`) before claiming trials; a missing
   bucket or artifact upload failure should produce a terminal failed
   trial, not a succeeded trial with missing outputs.
8. Scale to the rest of the included worker hosts at the planned
   concurrency.
9. Run a real supported-benchmark load test sized to exceed the planned
   slot count.
10. Check there are no stuck `claimed` / `running` trials, leaked Docker
   containers, missing artifacts, provider rate-limit storms, or host
   swap pressure.

If any gate fails, keep the pool below the last stable concurrency and
record the failure on the deployment issue before raising the limit.

## Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| Worker never registers | Bad token or cannot reach Control Plane | Worker logs; `curl $LOOM_WORKER_CONTROL_PLANE_URL/healthz` from the worker host. |
| Claims happen but trials fail immediately | Docker unavailable or sandbox image missing | `docker info`; worker logs around sandbox start. |
| Trials upload no trajectory/artifacts | MinIO endpoint, credentials, or runtime bucket bootstrap failure | `curl $LOOM_WORKER_MINIO_ENDPOINT/minio/health/live`; worker logs for S3 errors; trial `failure_reason` should be `trajectory_flush_failed` or `artifact_upload_failed`. |
| Queue grows while hosts look idle | Workers not matching task capabilities or provider limits throttling | Control Plane worker table, queue depth, gateway/provider errors. |
| Host becomes unstable | Concurrency too high or missing sandbox resource limits | Lower `LOOM_WORKER_MAX_CONCURRENT`; inspect memory, swap, and Docker container count. |
