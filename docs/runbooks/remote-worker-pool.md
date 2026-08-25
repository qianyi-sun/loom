# Remote worker pool

Remote workers add Docker-capable execution hosts to an existing Loom control
plane. Each host runs a Loom Worker and starts independent trial containers on
that host's Docker daemon; CPUs and memory are not combined into one machine.

## Network and identity contract

A worker needs private access to:

- `LOOM_WORKER_CONTROL_PLANE_URL` for registration, heartbeats, claims, and
  trial state;
- `LOOM_WORKER_GATEWAY_URL` for worker-side Gateway calls;
- optional `LOOM_WORKER_SUBPROCESS_GATEWAY_URL` when Docker sandboxes need a
  different Gateway address;
- `LOOM_WORKER_MINIO_ENDPOINT` for internally mirrored task bundles,
  trajectories, and artifacts;
- `LOOM_WORKER_TRAJECTORIES_BUCKET` and `LOOM_WORKER_ARTIFACTS_BUCKET`, which
  must exactly match the connected control plane's environment-scoped buckets.

Keep these endpoints on a private network, VPN, or host allowlist. Do not
publish the Control Plane, Gateway, or object store to the internet.

Set `LOOM_WORKER_HOSTNAME` to the physical or VM host name and
`LOOM_WORKER_POOL_NAME` to the stable resource pool. The worker advertises its
CPU architecture. Trials requiring `x86_64` are not claimed by ARM64 workers;
use task requirement `arm64` or `any` only when the image and verifier support
that architecture.

Workers must receive internal `s3://` task sources. Do not place `HF_TOKEN` on
worker hosts; gated Hugging Face access belongs to catalog mirror provisioning.

## Fixed Docker worker

Prerequisites are Docker with Compose, enough disk for images and task caches,
private connectivity to the control node, a worker token, and object-store
credentials scoped to the target environment.

Copy the example outside the repository and restrict it to the service user:

```bash
install -m 0600 deploy/remote-worker.env.example /secure/path/loom-worker.env
```

Fill the required endpoints, credentials, and exact bucket names. Start
conservatively with the example's `LOOM_WORKER_MAX_CONCURRENT=5`; change it
only from measured host capacity. Keep `LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS`
unset for a fixed worker.

Start the worker from the checkout matching the deployed candidate:

```bash
docker compose \
  --env-file /secure/path/loom-worker.env \
  -f deploy/docker-compose.remote-worker.yml \
  up -d --build
```

Verify registration, pool identity, and capacity:

```bash
docker compose \
  --env-file /secure/path/loom-worker.env \
  -f deploy/docker-compose.remote-worker.yml logs --tail=200 worker
loom resources status --format json
loom admin worker-pools autoscaler status
```

Run one representative trial for the host architecture and pool before raising
concurrency. Confirm claim, sandbox creation, Gateway access, object download,
trajectory upload, terminal state, and cleanup.

To stop a fixed worker without deleting its cache volumes:

```bash
docker compose \
  --env-file /secure/path/loom-worker.env \
  -f deploy/docker-compose.remote-worker.yml down
```

## Managed Kubernetes service tunnels

When workers are outside the Kubernetes network, run durable private tunnels
on the control node. The helper manages user-level systemd units for the
Control Plane, Gateway, object store, and optional distinct subprocess Gateway.

Render units for review, then install from durable `kubectl` and kubeconfig
paths:

```bash
uv run --no-sync python scripts/ops/worker_service_tunnels.py render-systemd \
  --output-dir ./loom-remote-worker-tunnels \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig

uv run --no-sync python scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig
```

The default local ports are `18081` for the Control Plane, `19100` for the
Gateway, and `19000` for MinIO. Use `--gateway-local-port` to change the normal
Gateway port. Use `--subprocess-gateway-local-port` only when sandbox traffic
needs a separate address.

Install the watchdog with the same worker environment file:

```bash
uv run --no-sync python scripts/ops/worker_service_tunnels.py \
  install-watchdog-systemd --env-file /secure/path/loom-worker.env
loginctl enable-linger "$USER"
```

Check the private path after every cluster rollout and before restoring worker
capacity:

```bash
uv run --no-sync python scripts/ops/worker_service_tunnels.py check \
  --env-file /secure/path/loom-worker.env
systemctl --user status \
  loom-remote-worker-tunnel-control-plane.service \
  loom-remote-worker-tunnel-gateway.service \
  loom-remote-worker-tunnel-minio.service \
  loom-remote-worker-tunnel-watchdog.timer
```

Use durable paths for systemd units. `--allow-volatile-paths` is for disposable
tests only.

## Elastic Slurm workers

Elastic Slurm workers are allocation-bound processes rather than fixed
services. The Control Plane's elastic controller and worker-pool autoscaler
submit from the checked-in environment desired state, register the Slurm job,
and set a bounded idle exit so capacity returns after the queue drains.

For non-exclusive allocations, the policy must provide positive per-container
CPU, memory, and PID limits plus an allocation-owned cgroup parent. The worker
fails closed when required cgroup identity is absent or does not match the
Slurm job. GPU jobs also propagate only the allocation's device IDs to trial
and setup containers.

Inspect desired and actual state without changing it:

```bash
loom admin worker-pools autoscaler status
loom resources status --format json
```

Apply autoscaler or Slurm desired state through the environment's authorized
reconciliation path. Shared staging changes are owned by its installed rollout
authority; do not submit parallel manual workers for that environment.

## Capacity and concurrency

`LOOM_WORKER_MAX_CONCURRENT` limits in-flight trials for one worker process.
The effective host limit also depends on CPU, memory, PID, GPU, Docker, disk,
network, object-store, and trial-cache build capacity. Keep
`LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT=1` on a shared Docker daemon
unless a measured sweep justifies more.

Raise capacity in small steps while watching:

- claim latency, queue backlog, heartbeat age, reclaim and retry rates;
- host load, available memory, cgroup limits, PID pressure, and GPU allocation;
- Docker API latency, image-build errors, filesystem usage, and inode pressure;
- MinIO connection pool, download/upload latency, and request failures;
- provider rate limits and Gateway errors.

Stop increasing at the first sustained bottleneck and keep the last stable
setting. A busy healthy host may advertise reduced or zero availability and
become eligible again automatically after resource release; do not remove it
from inventory solely because it is busy.

## Token rotation

Mint or rotate the environment's worker token through the admin CLI, install
the new value on every fixed and elastic consumer, and restart workers one at a
time. Verify new registrations and successful claims before revoking the old
prefix:

```text
loom admin tokens worker rotate --help
loom admin tokens worker revoke --help
```

Never put raw tokens in command history, reports, or documentation.

## Recovery

| Symptom | Checks | Recovery |
| --- | --- | --- |
| worker absent | process logs, token, Control Plane URL, tunnel health | repair connectivity or credential, then restart one worker |
| heartbeat stale | host load, Docker stall, network, process liveness | drain new work, preserve logs, restart after the cause is bounded |
| sandbox cannot reach Gateway | compare worker and Docker network views | set or repair `LOOM_WORKER_SUBPROCESS_GATEWAY_URL`; verify the managed tunnel |
| task materialization fails | internal object URL, MinIO credentials, disk, timeout | repair mirror/object access; do not add `HF_TOKEN` to the worker |
| wrong architecture remains queued | trial CPU requirement and worker capability | publish a compatible task or add the required architecture |
| Docker API timeouts | daemon health, disk, build concurrency | reduce concurrency and clear only verified unused Docker objects |
| Slurm worker exits immediately | allocation, cgroup parent, container limits, job registry | correct desired state and submit through the controller |
| old token generation remains | worker env and restart status | finish overlap rollout, verify claims, then revoke old token |

Preserve sanitized worker, allocation, and trial identifiers for incident
analysis. Store run-specific evidence outside the active documentation tree.
