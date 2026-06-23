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
export LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:8080
export LOOM_WORKER_GATEWAY_URL=http://control-node:9100
# Optional when the sandbox's network view differs from the worker process.
# For example, use a node-local router or host-gateway URL.
# export LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30443/openai/v1
export LOOM_WORKER_MINIO_ENDPOINT=http://control-node:9000

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
reachability back to the control node endpoints.

## Start A Remote Worker

On the worker host, copy the example env file to an untracked file:

```bash
cp deploy/remote-worker.env.example .env.remote-worker
```

Edit `.env.remote-worker`:

```bash
LOOM_IMAGE_TAG=dev
LOOM_WORKER_CONTROL_PLANE_URL=http://control-node:8080
LOOM_WORKER_GATEWAY_URL=http://control-node:9100
# Leave unset when the sandbox can use the same gateway URL. Set when
# subprocess agents run in Docker sandboxes that need a host-gateway or
# node-local router endpoint.
# LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30443/openai/v1
LOOM_WORKER_MINIO_ENDPOINT=http://control-node:9000
LOOM_WORKER_TOKEN=loom_w_...
LOOM_WORKER_MINIO_ACCESS_KEY=...
LOOM_WORKER_MINIO_SECRET_KEY=...
LOOM_WORKER_MAX_CONCURRENT=5
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
The default is 5, matching `WorkerSettings.max_concurrent`.

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
| Conservative | 5 | Match the runtime default and validate stable shared-dev capacity. |
| Medium | 8 | Raise only after CPU, RAM, Docker cleanup, MinIO, and provider limits look healthy. |
| Higher | 10+ | Requires explicit load-test evidence and sandbox resource-limit follow-up. |

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

1. Inventory every candidate worker and record CPU, memory, disk, Docker,
   and endpoint reachability.
2. Start one remote worker at `LOOM_WORKER_MAX_CONCURRENT=1`.
3. Submit a tiny API-model + Docker-terminal evaluation and verify the
   remote worker claims it.
4. Confirm the trial reaches a terminal state and artifacts/trajectory
   downloads work. Workers bootstrap both runtime buckets
   (`trajectories` and `artifacts`) before claiming trials; a missing
   bucket or artifact upload failure should produce a terminal failed
   trial, not a succeeded trial with missing outputs.
5. Scale to the rest of the worker hosts at concurrency 5.
6. Run a 25-trial batch or equivalent load test.
7. Check there are no stuck `claimed` / `running` trials, leaked Docker
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
