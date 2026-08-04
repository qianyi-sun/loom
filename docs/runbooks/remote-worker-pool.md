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

Remote workers also advertise their host CPU architecture in
`workers.capabilities[].cpu_arch`. The worker process auto-detects common Linux
values (`x86_64`, `aarch64`/`arm64`) and the scheduler matches that against
`trials.requires_caps.cpu_arch`. Missing legacy trial requirements are treated
as `x86_64`, so attaching ARM64 hosts such as GB10 cannot accidentally drain
queued x86-specific work. Only tasks submitted with
`environment.cpu_arch = "arm64"` or `"any"` can be claimed by ARM64 workers;
use `"any"` only after the image and verifier have been proven credible on
both architectures.

For Terminus 2 task bundles that inherit from
`mictern2/terminus2-full:latest`, workers attached to an ARM64 Docker daemon
prepare a Loom-managed local compatibility base image before building the task
Dockerfile. The upstream `mictern2/terminus2-full:latest` image is currently
amd64-only, so this worker-side prewarm keeps GB10 tasks runnable without
expanding x86 capacity or changing normal x86 behavior. Operators should still
treat a first build on a fresh ARM64 host as a warmup event because the local
base image is created once per Docker daemon.

Set `LOOM_WORKER_HOSTNAME` in each remote worker env file to the physical or
VM host name, for example `trt-gb10-7`. If it is unset, the worker registers
with the runtime hostname, which may be only a container ID in Docker Compose.
Using the host name keeps Monitor, worker inventory, and capacity evidence
readable when many remote workers attach through the same control plane.
Set `LOOM_WORKER_POOL_NAME` to the stable pool identity shared by similar
workers, for example `gb10`, `oldlab`, or `remote-worker`. Monitor,
`loom resources status`, and Prometheus slot metrics group capacity by this
pool name plus backend and CPU architecture.

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

The default worker-process Gateway tunnel port is `19100`. If the worker
process and Docker sandbox can share one port, override the managed Gateway
tunnel instead of leaving a separate ad-hoc port-forward running:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig \
  --gateway-local-port 30444
```

If the worker process must keep using `19100` while subprocess agents in
Docker sandboxes use a separate host bridge such as
`http://host.docker.internal:30444/openai/v1`, install an additional managed
`subprocess-gateway` tunnel:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig \
  --subprocess-gateway-local-port 30444
```

This creates `loom-remote-worker-tunnel-subprocess-gateway.service` and keeps
the normal `loom-remote-worker-tunnel-gateway.service` on `19100`.

Rollout may recreate the Control Plane and Gateway pods during `cluster-up`.
The tunnel units should restart automatically, but CP-backed rollout consumers
must wait for `http://<control-node>:18081/healthz` before mutating desired
state. The protected rollout `env-state` step records this as
`control-plane-readiness.json`; a manual operator flow should perform the same
health check before running `loom admin environment-state apply`.

Render units for review:

```bash
scripts/ops/worker_service_tunnels.py render-systemd \
  --output-dir ./loom-remote-worker-tunnels \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig
```

Install and start them as user services on the control node:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig
```

Install the watchdog timer on the same host after the tunnels are installed.
The watchdog probes the worker-facing health URLs every 30 seconds, keeps a
small consecutive-failure counter under `~/.local/state/loom/`, and restarts
only the failed tunnel unit after three failed probes. This catches
active-looking but stale `kubectl port-forward` processes after host restarts,
pod recreation, or Kubernetes rollouts.

```bash
scripts/ops/worker_service_tunnels.py install-watchdog-systemd \
  --env-file /secure/path/.env.remote-worker
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
  loom-remote-worker-tunnel-minio.service \
  loom-remote-worker-tunnel-watchdog.timer
```

The remote-worker env file should point at the private control-node address and
the managed local ports:

```bash
LOOM_WORKER_CONTROL_PLANE_URL=http://control-node.lan:18081
LOOM_WORKER_GATEWAY_URL=http://control-node.lan:19100
LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000
# Optional when the sandbox network view differs from the worker process.
# LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30444/openai/v1
```

`LOOM_WORKER_SUBPROCESS_GATEWAY_URL` is optional. When it is set,
`worker_service_tunnels.py check`, `check-remote`, `print-check-script`,
`watchdog`, and `watchdog-evidence` add or report a `subprocess-gateway` probe
against the Gateway health endpoint.
For `host.docker.internal` URLs, the script runs outside the sandbox and probes
the equivalent host-side loopback URL, for example
`http://127.0.0.1:30444/healthz`. Repeated `subprocess-gateway` failures cause
the watchdog to restart `loom-remote-worker-tunnel-subprocess-gateway.service`
when the subprocess port differs from `LOOM_WORKER_GATEWAY_URL`; otherwise it
restarts `loom-remote-worker-tunnel-gateway.service`.
Set the URL to the gateway root or to the adapter-compatible facade root. Codex
and other OpenAI-compatible subprocess agents use `/openai/v1`; Claude Code
uses `/anthropic`; Gemini adapters use `/google`. An explicit incompatible
facade, such as a Codex worker env pointing at `/anthropic`, is rejected during
agent startup.

The same script provides the rollout gate for those exact URLs:

```bash
scripts/ops/worker_service_tunnels.py watchdog-evidence \
  --expected-script-path "$PWD/scripts/ops/worker_service_tunnels.py" \
  | tee remote-worker-watchdog-evidence.json

export REMOTE_WORKER_ENV_FILE="$(
  jq -r '.env_file.path' remote-worker-watchdog-evidence.json
)"

scripts/ops/worker_service_tunnels.py check \
  --env-file "$REMOTE_WORKER_ENV_FILE"
```

The checker treats an empty 2xx response from `minio` as healthy because
MinIO's `/minio/health/*` endpoints return no response body. Empty 2xx
responses from Control Plane, Gateway, or subprocess Gateway still fail the
probe because they indicate the stale `kubectl port-forward` pattern.

For immediate local self-heal testing without waiting for the timer, run:

```bash
scripts/ops/worker_service_tunnels.py watchdog \
  --env-file .env.remote-worker
```

Validate from worker hosts too, not only from the control node:

```bash
scripts/ops/worker_service_tunnels.py check-remote worker-hosts.txt \
  --env-file "$REMOTE_WORKER_ENV_FILE"
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
# For example, use a node-local router or host-gateway URL; replace the
# port with the operator-selected sandbox bridge port.
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

For production/staging capacity, the hostfile should include every
candidate worker node the operator is allowed to use. Exclude a node
only with a recorded reason such as missing Docker, failed endpoint
reachability, insufficient disk, or Slurm reservation policy.

The staged OLDLAB staging manual plan is recorded under
`deploy/worker-pools/oldlab/`. It intentionally requests only `12 CPU` and
`58000M` per node with `LOOM_WORKER_MAX_CONCURRENT=6`, even when inventory
shows more total host capacity. For normal shared OLDLAB capacity, prefer the
worker-pool autoscaler policy described below; it can use live Slurm node
resources instead of this fixed manual slice.

The staged GB10 staging plan is recorded under
`deploy/worker-pools/gb10/`. GB10 workers execute Docker sandboxes on ARM64
hosts, but normal capacity management should still use the same Slurm
autoscaler policy shape as OLDLAB: `actuator=slurm`, `pool_name=gb10`,
`actuator_config.partition=gb10`, `actuator_config.cpu_arch=arm64`, and one
allowed node per `trt-gb10-N` host. GB10's Docker data-root plus worker scratch
must stay on each node's local ext4 disk. Do not use `/shared_work` for GB10
Docker overlay2, worker scratch, Postgres, MinIO backend data, or kind/k8s
volumes; `/shared_work` is NFSv4 and is suitable only for read-mostly cache
staging, a shared Loom checkout, env files, or evidence transfer.

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
  --sandbox-identity staging \
  --candidate-sha "$CANDIDATE_SHA" \
  --container-cpus 2 \
  --container-memory-mib 4096 \
  --container-pids 512 \
  --dry-run
```

After reviewing the printed `sbatch` commands, submit:

```bash
scripts/ops/worker_pool_slurm_submit.sh worker-plan.csv \
  --env-file /secure/path/.env.remote-worker \
  --repo-dir /opt/loom \
  --sandbox-identity staging \
  --candidate-sha "$CANDIDATE_SHA" \
  --container-cpus 2 \
  --container-memory-mib 4096 \
  --container-pids 512 \
  --yes
```

The script uses `--nodelist=<host>` for each included row and exports
the row's `recommended_concurrency` as `LOOM_WORKER_MAX_CONCURRENT`.
It requests the row's CPU and memory values without `--exclusive`, binds the
exact candidate and sandbox identity, and requires positive per-container CPU,
memory, and PID ceilings. Keep submission disabled until the target pool has
passed #896 containment acceptance; whole-node exclusivity is not a fallback.
Keep the env file untracked and available on each worker node. The `--repo-dir`
path must also exist with `deploy/docker-compose.remote-worker.yml` on every
included node; prefer a shared checkout path such as
`/shared_work/<operator>/loom-remote-worker` for OLDLAB-style pools. A
control-node-local `/home/.../loom` checkout is not sufficient unless a Slurm
job has verified it on each target node. This script is for manual or staged
launches. For elastic pools, prefer the Control Plane controller below so batch
submission stays independent of Slurm latency.

## Candidate Publication Contract (service-owned root)

Per-environment worker candidates published under
`/shared_work/loom/candidates/<environment>/<sha>/` follow an all-or-nothing,
service-owned contract (#874):

- The rollout **service** (`loom-rollout`) owns the root and every level below
  it (mode `2750`); workers read via the `sharedwork` group but never write —
  that is the immutability guarantee for a published candidate.
- Privileged setup (`staging_rollout_shared_repo.py service-ensure
  --environment <env> --candidate-sha <sha>`) validates the environment
  (development/staging/production) and a full 40-hex SHA, hardcodes
  `/shared_work/loom`, and ensures **only** `candidates/<environment>`. It never
  pre-creates the final `<sha>` directory and accepts no arbitrary root/path.
- The publisher/materializer builds the complete candidate in a private
  temporary tree, then **atomically** rename-no-replaces it into
  `candidates/<environment>/<sha>`. An already-present target fails closed (no
  overwrite, no partial content becomes visible).
- **Rollback** is merge-revert + a fresh candidate: revert on the authoritative
  branch and publish a new SHA. The broker never re-points at or reuses a
  retained historical `<sha>`; retained directories are forensic only.

## Elastic Slurm Controller

The Control Plane can run an internal elastic Slurm worker controller loop. It
observes queued Loom trials and the `slurm_worker_jobs` registry, submits
worker jobs when backlog exceeds active capacity, and cancels still-pending
jobs after the queue drains. Batch creation never calls Slurm synchronously.
This legacy env-driven controller uses one fixed concurrency slice per
submitted job. For OLDLAB resource-aware scaling across the five shared nodes,
use the worker-pool autoscaler policy section instead.

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
| `stale` | Loom had an active record, but the Slurm reconcile pass no longer saw the job after the stale window, or the Slurm job still reported running while the associated Loom worker heartbeat was missing or stale. |

The elastic controller reconciles the table from `squeue` and `sacct`,
including pending reasons where Slurm reports them. It must not submit another
job for the same environment, pool, nodelist, CPU, memory, and concurrency
while an active `pending` or `running` record already exists.

Stale records are visible in `loom admin slurm-workers status` as
`stale=<slots>` and `stale_jobs=<count>`, and in Prometheus as
`loom_slurm_worker_stale_slots` and `loom_slurm_worker_stale_jobs`. A recent
stale record temporarily blocks replacement on the same nodelist so the
controller does not double-submit during a noisy Slurm or heartbeat transition;
the next reconcile can replace missing capacity on another allowed node.

## Prod-First Shared Capacity Contract

First production runs on a separate prod control plane and state profile, but
GB10/OLDLAB machines remain shared physical capacity. The release contract is
`deploy/worker-capacity/prod-first.toml`: by default every eligible host slot
belongs to production, staging has `staging_slots = 0`, and any staging borrow must
be explicit, bounded to at most one slot per host, and drained back before the
borrow window ends. The physical v1.0 GB10 inventory remains all 15 hosts at 10
theoretical slots each. All 15 remain heartbeat-managed. A healthy busy node
advertises zero or reduced free capacity, receives no new Loom allocation, and
returns automatically after resource release. The repo-owned
`deploy/worker-pools/gb10/ssh_config` routes
the private `trt-gb10-2..15` addresses on port `22` through
`ProxyJump trt-gb10-1`; `trt-gb10-1` is the only public entrypoint and uses
port `2221`.
The manifest must distinguish inventory membership, health, and current
capacity. It may represent temporary health or drain states without removing a
node from the complete 15-host expected set.

Generate the secret-safe desired-vs-observed evidence before a production
promotion:

```bash
uv run --no-sync python scripts/ops/worker_capacity_manifest.py \
  --manifest deploy/worker-capacity/prod-first.toml \
  --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
  --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
  --var STAGING_IMAGE_TAG="$STAGING_IMAGE_TAG" \
  --var STAGING_SOURCE_COMMIT="$STAGING_RELEASE_SHA" \
  --observed-json "$ROLLOUT_DIR/worker-registrations.json" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-prod-first.json"
```

The observed artifact may come from Control Plane worker registration/status,
GB10 node-agent status, or a composed release evidence collector, but it must
not include raw service tokens, provider keys, MinIO credentials, or signed
URLs. The validator redacts secret-bearing fields before writing JSON or
Markdown and fails on production/staging crosses in worker identity, API URL, image tag,
source commit, compose service, Kubernetes deployment, host state, or observed
slot counts.

Short-lived staging capacity borrows use the same repo-only manifest helper.
The commands below preview first and only write a new desired-state file when
`--apply` is present; they do not contact a live control plane, mutate worker
pools, or read secrets.

Preview a two-slot staging smoke lease before a rollout:

```bash
uv run --no-sync python scripts/ops/worker_capacity_manifest.py lease-staging \
  --manifest deploy/worker-capacity/prod-first.toml \
  --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
  --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
  --var STAGING_IMAGE_TAG="$STAGING_IMAGE_TAG" \
  --var STAGING_SOURCE_COMMIT="$STAGING_RELEASE_SHA" \
  --reason "staging rollout smoke $STAGING_IMAGE_TAG" \
  --ttl 45m \
  --slots-per-host 1 \
  --max-total-slots 2 \
  --preemptible
```

After reviewing the JSON preview, write the bounded lease manifest and
sanitized evidence:

```bash
LEASE_MANIFEST="$ROLLOUT_DIR/worker-capacity-staging-lease.toml"

uv run --no-sync python scripts/ops/worker_capacity_manifest.py lease-staging \
  --manifest deploy/worker-capacity/prod-first.toml \
  --var PROD_IMAGE_TAG="$PROD_IMAGE_TAG" \
  --var PROD_SOURCE_COMMIT="$PROD_RELEASE_SHA" \
  --var STAGING_IMAGE_TAG="$STAGING_IMAGE_TAG" \
  --var STAGING_SOURCE_COMMIT="$STAGING_RELEASE_SHA" \
  --reason "staging rollout smoke $STAGING_IMAGE_TAG" \
  --ttl 45m \
  --slots-per-host 1 \
  --max-total-slots 2 \
  --preemptible \
  --apply \
  --output-manifest "$LEASE_MANIFEST" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-lease.json"
```

Check lease status, including automatic TTL-expiry and prod-pressure handling,
with the latest secret-free worker registration/status artifact when
available. Treat `prod_pending_count > 0`, `prod_active_count > 0`, or a
positive prod capacity shortfall as prod pressure: `status` stops new staging
claims in the desired state, releases idle staging slots back to prod, and reports
running staging slots as draining. Preemptible running staging trials are only marked
retryable in evidence after the configured grace period; this repo helper does
not cancel live work.

```bash
uv run --no-sync python scripts/ops/worker_capacity_manifest.py status \
  --manifest "$LEASE_MANIFEST" \
  --observed-json "$ROLLOUT_DIR/worker-registrations.json" \
  --prod-pending-count "${PROD_PENDING_COUNT:-0}" \
  --prod-active-count "${PROD_ACTIVE_COUNT:-0}" \
  --prod-capacity-shortfall "${PROD_CAPACITY_SHORTFALL:-0}" \
  --prod-pressure-source "control-plane prod queue summary" \
  --preemptible-grace-period 10m \
  --apply \
  --output-manifest "$ROLLOUT_DIR/worker-capacity-staging-status.toml" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-status.json"
```

The evidence distinguishes prod-driven pauses from staging rollout failures:
`prod_pressure.cause=prod_capacity_pressure` means prod demand triggered the
drain, while `drift` or `errors` still indicate manifest/worker mismatch. If
pressure clears before the lease TTL expires, rerun `status` with zero
prod-pressure counts to recover the bounded staging desired slots from the lease
metadata.

The file helper does not control live workers. Continuous enforcement is the
root-installed `deploy/worker-capacity/loom-prod-pressure-worker-control.timer`;
its oneshot service runs `scripts/ops/prod_pressure_worker_control.py` every 30
seconds from the authorized fixed runner and persists sanitized health evidence.
It reads `/admin/worker-pools/<pool>/prod-pressure` from production and posts
the sanitized counts to staging's
`/admin/gb10-worker-pools/<environment>/<pool>/prod-pressure`. Desired
`draining`/`stopped` intent immediately fences every matching worker registry
row by hostname and pool, including unlinked or duplicate registrations; the
claim SQL already requires `Worker.drain_state = active`. `draining` keeps
Compose alive for in-flight work, while `stopped` always runs Compose stop and
verifies the service is no longer running. Idle hosts become stopped
immediately. Busy non-preemptible hosts stop only after in-flight work reaches
zero; preemptible hosts may become stopped after grace with a durable
prod-pressure retry diagnostic.

Use an explicit manual drain only for operator-initiated pause scenarios that
are not represented by the prod-pressure counts. The report separates
`running_staging_trials` from `idle_leased_slots` so operators can see what is
still active and what was released from the desired state:

```bash
uv run --no-sync python scripts/ops/worker_capacity_manifest.py drain-staging \
  --manifest "$LEASE_MANIFEST" \
  --observed-json "$ROLLOUT_DIR/worker-registrations.json" \
  --reason "prod pressure before release gate" \
  --apply \
  --output-manifest "$ROLLOUT_DIR/worker-capacity-staging-draining.toml" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-drain.json"
```

Immediately release staging capacity after validation finishes. This action is
idempotent: rerunning it keeps staging desired slots at zero and leaves prod slots
on eligible hosts:

```bash
uv run --no-sync python scripts/ops/worker_capacity_manifest.py release-staging \
  --manifest "$LEASE_MANIFEST" \
  --reason "staging smoke complete" \
  --apply \
  --output-manifest "$ROLLOUT_DIR/worker-capacity-staging-released.toml" \
  --evidence-out "$ROLLOUT_DIR/worker-capacity-staging-release.json"
```

Before production promotion, copy the latest `status`, `drain-staging`, or
`release-staging` summary into the release-promotion manifest under
`checks.prod_staging_isolation.staging_capacity`. The production gate accepts
`lease_state=none`, `released`, or an expired/drained state only when
`staging_slots=0` and `new_staging_claims_allowed=false`. Any active staging lease,
remaining staging slot, or open staging claims fail promotion unless the same object
contains a documented override with `approved=true`, a reason, and an HTTPS
approval/evidence URL.

## GB10 Node-Agent Compatibility Lifecycle

Normal GB10 capacity should be managed through the Slurm autoscaler policy
above. The GB10 node-agent remains available for Docker Compose rollout
validation, legacy compatibility, and break-glass operation when Slurm is not
available. Its lifecycle manager is pull-based: the Control Plane stores
desired non-secret state, and a host-local node-agent applies it from each GB10
node. The Control Plane does not SSH into GB10 and does not store worker
tokens, MinIO credentials, provider keys, or sudo material.

Desired state is stored per `(environment, pool_name)` and includes:

- worker image tag (`LOOM_IMAGE_TAG`);
- desired source checkout commit (`source_git_commit`);
- pool name (`LOOM_WORKER_POOL_NAME`);
- per-worker trial concurrency (`LOOM_WORKER_MAX_CONCURRENT`);
- env/config version (`LOOM_WORKER_ENV_CONFIG_VERSION`, node-agent local only);
- rollout policy such as canary hosts;
- optional compatibility target slots and per-host intents:
  `active`, `draining`, or `stopped`.

For staging and staging release rollouts, apply the repository
environment-state profile so the Slurm autoscaler policy and GB10 compatibility
desired state converge together. The staging profile must write the
`staging/gb10` CP key; `production/gb10` belongs to production and
is drift in current-path staging evidence. Run this from the Slurm submit host when the
profile declares an external Slurm autoscaler supervisor; staging uses that
path to install and check the OLDLAB user timer, and the rendered service must
call the repo one-shot with `--pool-name oldlab` so it cannot reconcile GB10 as
a side effect. Protected `current-gb10` rollouts must point
cluster-config `env_state_profile` at this profile. The release gate treats an
empty `gb10_worker_pool_desired_states` section as a failure, even when a
GB10 status artifact exists, because an empty manifest would otherwise allow
external workers to drift outside the release contract.
The check also compares the active environment worker token to
remote-worker env files and active Slurm job fingerprints using only redacted
sha256-prefix output, so pass the token through `env:` or `file:` indirection:

```bash
loom admin environment-state apply \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA"

loom admin environment-state check \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA" \
  --worker-token env:LOOM_WORKER_TOKEN
```

For a one-off node-agent canary experiment, write desired state through the CP
admin API:

```bash
curl -sS -X PUT \
  -H "Authorization: Bearer $LOOM_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  http://control-node.lan:18081/admin/gb10-worker-pools/staging/gb10/desired-state \
  -d '{
    "image_tag": "staging-<commit>",
    "source_git_commit": "<full-release-commit>",
    "max_concurrent": 10,
    "env_config_version": "gb10-env-2026-06-26",
    "rollout_policy": {
      "mode": "canary",
      "canary_hosts": ["trt-gb10-1"]
    }
  }'
```

Inspect desired state and per-host reports:

```bash
loom admin gb10-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN
```

For release rollouts, make the status check a convergence gate by passing the
expected image tag and env config version. The command exits non-zero if any
active GB10 node, or the desired state itself, is still on the previous target;
capacity marked draining/stopped is ignored. Active nodes must also report
source checkout provenance: the node-agent records `compose_project_dir`, the
checkout's git commit, and whether the tree is dirty. When desired state
contains `source_git_commit`, active node source commits must match it exactly
and the checkout must be clean. Missing provenance is a release-gate failure
because it means the operator cannot prove that a local build fallback used the
desired source tree. The status artifact also links node reports to the worker
registry by hostname/pool; active release nodes must show `worker_id`,
`worker_status=active`, `worker_fresh=true`, and `worker_backend_names`
containing `docker`. This is the evidence that `/api/v1/backends` will expose a
usable docker backend for smoke and user submissions.

```bash
loom admin gb10-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --pool-name gb10 \
  --release-image-tag "$IMAGE_TAG" \
  --release-env-config-version "$ENV_CONFIG_VERSION"
```

On each GB10 host, the node-agent reads the host-local staging env file
`/home/qianyi/loom-worker-build-staging/.env`, compares it with Control Plane desired state, writes
only non-secret env updates, then runs Docker Compose locally. If desired state
contains `source_git_commit`, the apply path fetches `origin` and checks out
that commit before pull/build/restart, so local-build fallback cannot silently
reuse a stale source tree. The apply path uses
`docker compose stop --timeout <seconds> worker`, so the worker receives
SIGTERM and uses the existing drain path before restart.
The host-local node-agent env file and legacy `..env.*.tmp` files are ignored by
the repo, and new transient compose env files are written under the user
runtime/tmp directory instead of the source checkout. They must not make GB10
release-gate source convergence fail as `source_git_dirty=true`.

```bash
loom worker gb10-agent plan \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment staging \
  --pool-name gb10 \
  --env-file /home/qianyi/loom-worker-build-staging/.env \
  --source-dir /home/qianyi/loom-worker-build-staging

loom worker gb10-agent apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment staging \
  --pool-name gb10 \
  --env-file /home/qianyi/loom-worker-build-staging/.env \
  --compose-file deploy/docker-compose.remote-worker.yml \
  --compose-file deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml \
  --source-dir /home/qianyi/loom-worker-build-staging
```

For worker-token rotation, run the same host-local plan/apply with the active
environment token supplied through `env:`, `file:`, or stdin. The token is not
published to the Control Plane; it is compared against and, on apply, written
only to the host-local `.env` file before restarting the worker.
Output shows only changed key names and redacted values.

```bash
loom worker gb10-agent plan \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment staging \
  --pool-name gb10 \
  --env-file /home/qianyi/loom-worker-build-staging/.env \
  --source-dir /home/qianyi/loom-worker-build-staging \
  --worker-token file:/secure/path/current-worker-token

loom worker gb10-agent apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment staging \
  --pool-name gb10 \
  --env-file /home/qianyi/loom-worker-build-staging/.env \
  --compose-file deploy/docker-compose.remote-worker.yml \
  --compose-file deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml \
  --source-dir /home/qianyi/loom-worker-build-staging \
  --worker-token file:/secure/path/current-worker-token
```

`apply --dry-run` prints only the non-secret env keys that would be changed and
the Docker Compose commands that would run. Rollback apply publishes the
previous desired image/concurrency/env version back to the Control Plane before
local Compose changes, so the periodic timer does not reapply the bad desired
state.

For systemd service/timer templates and GB10-specific paths, see
`deploy/worker-pools/gb10/`. Manual Docker Compose remains the break-glass
fallback when the node-agent timer, token, or CP desired-state API is
unavailable.

The protected rollout requires `loginctl show-user "$USER" -p Linger --value`
to return `yes` before it changes either unit. It installs the candidate's
service and timer into `~/.config/systemd/user`, starts the oneshot once for
immediate convergence, enables/restarts the timer, and verifies both installed
bytes and live systemd properties. Bootstrap linger once with the host's normal
user/admin path:

```bash
loginctl enable-linger "$USER"
```

An active host is reconciled every timer period. If its current or normally
idle-exited container already uses the desired image, the node-agent reuses it
and runs Compose reconciliation without a new pull/build. Missing containers or
runtime-image drift still trigger the candidate-bound pull/build. `draining`
and `stopped` hosts do not pull, build, or start a worker.

After installation, disconnect the operator SSH session, wait longer than one
timer period, stop one active canary worker, and require the timer to restore a
fresh linked worker within the next period. Confirm excluded/stopped hosts stay
absent. A legacy node-agent for another environment must be stopped or fully
isolated from this environment's tunnel ports and Compose root before tunnel
connectivity is restored.

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
# node-local router endpoint; replace the port with the operator-selected
# sandbox bridge port.
# LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:30443/openai/v1
# Do not set HF_TOKEN on worker hosts. Gated HF access belongs to catalog
# mirror provisioning; workers should receive internal s3:// task sources.
LOOM_WORKER_MINIO_ENDPOINT=http://control-node.lan:19000
LOOM_WORKER_TOKEN=loom_w_...
LOOM_WORKER_MINIO_ACCESS_KEY=...
LOOM_WORKER_MINIO_SECRET_KEY=...
LOOM_WORKER_MAX_CONCURRENT=5
LOOM_WORKER_POOL_NAME=remote-worker
# Optional, but recommended for remote pools so UI/DB worker rows identify the
# physical or VM host instead of the container hostname.
# LOOM_WORKER_HOSTNAME=worker-host-a
# Keep these defaults for normal sweeps; raise only after logs show SDK timeouts.
LOOM_WORKER_DOCKER_API_TIMEOUT_SEC=1800
LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS=256
LOOM_WORKER_MINIO_CONNECT_TIMEOUT_SEC=5
LOOM_WORKER_MINIO_READ_TIMEOUT_SEC=120
LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC=300
LOOM_WORKER_MINIO_OPERATION_ATTEMPTS=3
LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC=300
LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT=1
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

When the worker-facing URLs are node-local loopback tunnels such as
`http://127.0.0.1:18081`, run the worker container with host networking so the
container's loopback is the host loopback:

```yaml
services:
  worker:
    network_mode: host
```

Then start with both compose files:

```bash
docker compose \
  --env-file .env.remote-worker \
  -f deploy/docker-compose.remote-worker.yml \
  -f docker-compose.gb10-hostnet.yml \
  up -d --build
```

For GB10-style hosts, keep `LOOM_WORKER_GATEWAY_URL` pointed at
`http://127.0.0.1:19100` for the worker process and set
`LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:19100` so
subprocess agents inside trial sandboxes can also reach the gateway. The
worker normalizes that sandbox-facing URL by adapter dialect before
injecting SDK env vars, so OpenAI-compatible adapters receive
`http://host.docker.internal:19100/openai/v1`, Anthropic adapters receive
`http://host.docker.internal:19100/anthropic`, and Gemini adapters receive
`http://host.docker.internal:19100/google`. Do not pre-normalize a
Codex/OpenAI env file to `/anthropic`; that path is now treated as a bad
release configuration and fails before the agent subprocess starts.

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
Remote workers also advertise `LOOM_WORKER_POOL_NAME`; use one stable value
per capacity pool so `loom resources status --json`, Monitor, and
`loom_worker_pool_*` metrics report slots by the same grouping operators use
for deployment and evidence.

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
| Staging GB10 release-managed Compose | 7200 seconds | Keep all 15 inventory hosts represented in heartbeat/health evidence through bounded-parallel prep, release-gate, and smoke validation. |
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

Docker SDK timeouts and S3 operation timeouts are independent of the
blocking-I/O executor. Use `LOOM_WORKER_DOCKER_API_TIMEOUT_SEC` when logs show
docker-py read timeouts during large base-image pulls, task Dockerfile builds,
or sidecar startup. Use `LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC`,
`LOOM_WORKER_MINIO_OPERATION_ATTEMPTS`, and
`LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS` when logs show `download_prefix`,
artifact upload, or trajectory flush timeouts under high trial concurrency.
The S3 operation attempt budget also covers transient boto3 socket/client
disconnects and retryable S3 5xx/throttle responses; each retry replaces the
client connection before trying again. `download_prefix` applies that budget
separately to prefix listing and each object download, so one object disconnect
does not restart the whole prefix download.
Use `LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC` only when pre-start bundle
materialization is reachable but legitimately slow; `hf://` sources that need
private internet access should normally be mirrored into internal object
storage before runtime.

Cold Docker setup work has a daemon-wide admission boundary separate from warm
trial execution concurrency. `LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT`
sets the number of concurrent setup/build slots per daemon fingerprint
(`pool_name`, hostname, Docker socket). Keep the default `1` on shared OLDLAB,
shared k8s node, or host-network remote-worker daemons so task Dockerfile
builds, layered trial-cache builds, and sidecar image pulls/builds serialize
their setup pressure even when `LOOM_WORKER_MAX_CONCURRENT` admits many warm
trials. Raise it only for isolated Docker daemons after a load-test issue
records CPU, memory, disk I/O, containerd, and cleanup headroom.

The #275 OLDLAB incident was caused by that boundary being incomplete: cold
setup/build paths could fan out apt/dpkg work before a trial reached
`started_at`, saturating host I/O and swap while unrelated users saw SSH or
login hangs. Workers now also run a setup-health gate before admitting Docker
setup/build work. The guard reads Linux `/proc/pressure/io` full avg10,
`SwapTotal`/`SwapFree`, and D-state process counts; if a threshold is crossed,
the claimed trial keeps pre-start heartbeats running, waits up to
`LOOM_WORKER_SETUP_HEALTH_WAIT_TIMEOUT_SEC`, then fails setup as
`failure_reason=node_setup_health` if the node does not recover. Tune:

- `LOOM_WORKER_SETUP_HEALTH_IO_FULL_AVG10_MAX` (default `50.0`)
- `LOOM_WORKER_SETUP_HEALTH_MIN_SWAP_FREE_MB` (default `1024`; ignored when
  `SwapTotal=0`)
- `LOOM_WORKER_SETUP_HEALTH_DSTATE_MAX` (default `32`)
- `LOOM_WORKER_SETUP_HEALTH_WAIT_TIMEOUT_SEC` (default `300.0`)
- `LOOM_WORKER_SETUP_HEALTH_POLL_INTERVAL_SEC` (default `5.0`)

Run `loom worker setup status` on the worker host to inspect the current
setup-health decision and Loom-labeled setup/trial containers. Use it together
with `loom admin worker-pools autoscaler status` or the environment-state drain
controls before doing targeted manual Docker cleanup; do not prune retained
`loom.trial-cache=true` images as part of setup-container reaping.

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

After startup, verify the same slot accounting exposed in the browser:

```bash
loom resources status
loom resources status --json
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

### Slurm cgroup parent for non-exclusive workers

Per-container CPU, memory, and PID limits are necessary but do not prove that
host-daemon containers belong to the Slurm allocation. For every
`exclusive=false` submission, the batch wrapper now discovers its unified
cgroup v2 membership from `/proc/self/cgroup`, identifies the allocation-owned
job scope, and verifies all of the following before Docker starts:

- the process is below an identifiable Slurm job scope, never `/`, a user
  slice, Docker's default parent, or another ambient cgroup;
- a named `job_<id>` scope matches `SLURM_JOB_ID`; opaque scopes that cannot
  be cryptographically or structurally bound to that job ID fail closed;
- the job scope is a domain cgroup with no internal processes; and
- `cpu`, `memory`, and `pids` are present in both `cgroup.controllers` and
  `cgroup.subtree_control`; and
- the job scope's actual `pids.max` exactly matches the controller-requested
  aggregate PID ceiling.

The wrapper exports the validated path as `LOOM_WORKER_CGROUP_PARENT`, adds
`deploy/docker-compose.remote-worker.cgroup-parent.yml`, and sets the
controller-owned `LOOM_WORKER_REQUIRE_CGROUP_PARENT=1` marker. The overlay
places the Compose worker beneath that parent. The worker validates the same
binding before registration and passes it unchanged to Docker
`HostConfig.CgroupParent` for every trial, verifier-only driver, and task
sidecar. The worker requires a Slurm marker followed by `job_<SLURM_JOB_ID>`
(or the base job component for an array task); a different job, a `job_*`
outside Slurm, a marker without a job component, or an opaque scope fails
startup. There is no fallback to Docker's default cgroup parent.

**Image builds on contained workers (#1146):** the cgroup parent binds every
*runtime* container (worker, trial, verifier-only driver, sidecar run). Image
**builds** can't be bound the same way — `docker-py`'s `images.build()` exposes
no `cgroup_parent`, so a build's `RUN` steps would run in a host-daemon
container outside the `job_<id>` cgroup and could escape the allocation caps.
A containment-required (non-exclusive) worker therefore **refuses to build**
task-Dockerfile, layered trial-cache, and sidecar images at runtime
(`ImageBuildForbiddenError`, `loom.driver.build_containment`). Such a worker
can still *run* any pre-built or pulled image — the image must be pre-built and
pushed to the shared trial-image cache (#547) rather than built on a packed
node. (Exclusive / unconstrained workers build as before.)

Every non-exclusive actuator config must also provide `job_pids_max` as a
positive JSON integer. It must be at least `container_pids` multiplied by the
configured concurrency ceiling (`requested_concurrency`, or
`max_concurrency_per_node` for a resource-aware policy). The controller emits
only the closed, versioned Slurm comment
`loom-cgroup-v1:pids=<job_pids_max>`. A persistent, administrator-owned root
cgroup-guard daemon must accept that exact grammar, reject all other comment
forms, wait for the allocation cgroup to exist, and apply the value there.
Ordinary Slurm 23.11 Prolog is not a valid implementation: it runs outside the
job cgroup and `_run_prolog` precedes creation of the extern step; the later
`RunInJob` facility is unavailable in 23.11. Before Compose starts, the batch
wrapper performs structural validation once, then waits up to 30 seconds using
monotonic 100 ms polling only for `pids.max` to appear and exactly match. A
wrong job, unsafe path, missing controller, or invalid delegation fails
immediately; a missing/stopped guard, an unbounded `max`, or a stale/different
value fails when the bounded wait expires.

Cluster administrators must first configure cgroup v2, Slurm
`proctrack/cgroup` and `task/cgroup`, and a delegated job scope whose subtree
controls include `cpu memory pids`. Compute nodes must also provide
`/usr/bin/python3`; the wrapper uses that fixed interpreter path so cgroup
discovery never depends on a login shell, virtual environment, or ambient
`PATH`. The repository does not change
`slurm.conf`, `cgroup.conf`, systemd delegation, the root guard service, or
Docker daemon policy. A
missing controller, non-delegated scope, wrong job identity, cgroup v1/hybrid
host, or unsafe path stops the batch before `docker compose up`.

Exclusive and unmanaged remote workers do not add the overlay and preserve
their current Docker parent. Do not set `LOOM_WORKER_CGROUP_PARENT` manually in
the remote-worker env file to activate packing; only the Slurm batch wrapper's
validated export is an accepted source.

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

### Non-exclusive Slurm containment acceptance

Non-exclusive Slurm workers stay disabled until a candidate-bound evidence
artifact passes the repository acceptance tool. The tool is intentionally
non-mutating: it does not run SSH, Slurm, Docker, or shell commands, and it
cannot activate workers. Live observation is a separate operator action that
requires its own authority.

Review the contract before collecting anything:

```bash
python scripts/ops/nonexclusive_slurm_acceptance.py plan
```

The evidence contract is
[`docs/evidence/nonexclusive-slurm-acceptance-v1.schema.json`](../evidence/nonexclusive-slurm-acceptance-v1.schema.json).
Build a secret-free JSON snapshot from separately authorized, read-only
observations. Do not store raw `docker inspect`, environment variables,
headers, URLs with credentials, command transcripts, or service responses.
Record only the bounded fields allowed by the schema.

The snapshot must bind one full candidate SHA to the exact sandbox, node,
Slurm job, and Compose project. It must include exactly the worker, trial,
verifier, and sidecar containers; prove that every container is a strict child
of the finite Slurm job cgroup; and account conservatively for the sum of all
CPU, memory, PID, and device caps. It also requires:

- allocated-device usability and denial of an unallocated device;
- reviewed node CPU, memory, PID, concurrency, Kubernetes, MinIO, and Longhorn
  headroom;
- every ordered cross-sandbox pair for worker identity, object storage, and
  result paths, with every probe denied;
- a mixed Loom, non-Loom Slurm, Kubernetes, MinIO, and Longhorn soak; and
- cancellation, TTL-expiry, worker-crash, and submit-host-restart cleanup
  checkpoints with no surviving job/container and durable retryable trial
  state.

After the read-only observation is complete, canonicalize it:

```bash
python scripts/ops/nonexclusive_slurm_acceptance.py collect \
  --input /secure/operator/nonexclusive-observed.json \
  --output /secure/operator/nonexclusive-evidence.json
```

`collect` writes nothing when any required field or checkpoint fails and will
not overwrite an existing output artifact. Verify the resulting artifact again
on any offline review host:

```bash
python scripts/ops/nonexclusive_slurm_acceptance.py verify \
  --evidence /secure/operator/nonexclusive-evidence.json
```

Stop and keep non-exclusive workers disabled if the candidate identity is
ambiguous, any cgroup ancestry or controller is missing, aggregate caps exceed
the Slurm allocation, device isolation is incomplete, a headroom/soak/cleanup
threshold is missed, a cross-sandbox probe succeeds, or any secret-like field
or value is present. A passing artifact is acceptance evidence only; this
repository tool does not grant rollout or activation authority.

Before treating a remote worker pool as usable:

1. Install or verify durable control-node service tunnels with
   `scripts/ops/worker_service_tunnels.py watchdog-evidence` and
   `scripts/ops/worker_service_tunnels.py check`.
2. Run `scripts/ops/worker_service_tunnels.py check-remote` from every
   candidate worker context and record endpoint reachability.
3. Inventory every candidate worker and record CPU, memory, disk, Docker,
   and endpoint reachability.
4. Generate `worker-plan.csv`; every usable node should be `include`,
   and every excluded node needs a reason.
5. Start one remote worker at `LOOM_WORKER_MAX_CONCURRENT=1`.
6. Submit a tiny API-model + Docker-terminal evaluation whose task config
   declares `environment.cpu_arch` compatible with that worker. For ARM64
   worker pools, do not rely on legacy tasks with no `cpu_arch` field; they
   are intentionally treated as x86_64-only.
7. Verify the remote worker claims it and that incompatible queued work remains
   untouched. For example, an ARM64 worker must not claim SWE-Bench Verified
   tasks that use `swebench/sweb.eval.x86_64.*` images.
8. Confirm the trial reaches a terminal state and artifacts/trajectory
   downloads work. Workers bootstrap both runtime buckets
   (`trajectories` and `artifacts`) before claiming trials; a missing
   bucket or artifact upload failure should produce a terminal failed
   trial, not a succeeded trial with missing outputs.
9. Scale to the rest of the included worker hosts at the planned
   concurrency.
10. Run a real supported-benchmark load test sized to exceed the planned
   slot count.
11. Check there are no stuck `claimed` / `running` trials, leaked Docker
   containers, missing artifacts, provider rate-limit storms, or host
   swap pressure.

If any gate fails, keep the pool below the last stable concurrency and
record the failure on the deployment issue before raising the limit.

## Worker-Pool Autoscaler

Shared OLDLAB and GB10 pools can be controlled by a Control Plane
autoscaler policy. The policy is keyed by `environment` and `pool_name`
and records desired, actual, pending, draining, queued, occupied,
idle-window age, decision, blocked, and error state.

Inspect policy state with:

```bash
loom admin worker-pools autoscaler status \
  --cp-url http://control-node.lan:18081 \
  --admin-token file:/secure/path/admin-token
loom resources status
loom resources status --json
```

Normal scale-down first marks workers `draining`. Draining workers stop
claiming new trials but keep heartbeating until in-flight trials finish.
After a worker has no claimed or running trials, the autoscaler releases
the underlying Slurm job. Release prefers the registry `worker_id`; if Slurm
observations never linked the job to a worker, it falls back to the drained
worker hostname matching the Slurm job `nodelist`.
The Slurm submission script wraps Docker Compose in an `EXIT`/`INT`/`TERM` trap
and derives a per-job Compose project name from
`environment + candidate_sha[0:12] + SLURM_JOB_ID`. It runs `docker compose
down --remove-orphans` against that exact project, so autoscaler cancellation
and worker idle exit remove only that job's worker container. The same sandbox,
candidate, job, and Compose-project identity is stamped onto the worker, trial,
verifier, and task-sidecar containers. Startup orphan cleanup filters by the
current sandbox identity and preserves containers owned by another sandbox,
including trials unknown to the current Control Plane.
Forced termination is a break-glass operator action, not the default path.

For OLDLAB Slurm, autoscaler `actuator_config` must include allowed nodes,
remote worker env file, remote checkout path, requested CPU/memory, requested
worker concurrency, max jobs, pending-job cap, and Slurm command paths if
they differ from `sbatch`, `squeue`, `sacct`, `scancel`, and `sinfo`.
Autoscaler Slurm submissions always use `actuator_config.exclusive=false`.
Exclusive allocation is rejected. Keep a policy disabled until requested CPU,
memory, PID, GPU/TRES, per-container ceilings, and exact candidate identity are
complete and the load-tested slice safely coexists with other jobs.
Non-exclusive admission fails closed unless all of
`container_cpus`, `container_memory_mib`, and `container_pids` are positive,
`job_pids_max` is a positive integer satisfying the aggregate/concurrency
minimum described above, `environment` is a lowercase sandbox identity, and
`candidate_sha` is the exact 40-character lowercase Git commit. The
per-container caps apply to the worker, trial, verifier, and sidecars;
`job_pids_max` is the allocation-wide parent ceiling and is carried to the
root cgroup guard only as `loom-cgroup-v1:pids=N`. Neither setting substitutes
for reading back effective containment on the target Slurm/Docker host.

GPU pools may set `gpu_tres` using exactly `gpu:COUNT` or
`gpu:TYPE:COUNT`; the controller emits that value as `sbatch --gres`. The batch
script fails before Compose startup when a positive GPU request has no
`SLURM_JOB_GPUS`, and the Docker driver rejects any trial or verifier GPU
request above the recorded Slurm allocation. Containers receive only the
allocated device IDs, rather than an unconstrained Docker GPU count.

### Slurm scheduler fields (account / QoS / reservation)

`actuator_config` accepts optional Slurm scheduler bindings, all defaulting to
empty (no flag emitted, so existing configs are unchanged):

- `slurm_account` — emitted as `sbatch --account=<x>`. Use per-environment
  accounts (e.g. `loom-dev`, `loom-staging`, `loom-prod`) for attribution and
  scheduler-side limits.
- `slurm_reservation` — emitted as `sbatch --reservation=<x>`. Required if a
  Slurm reservation carves nodes out of the general pool; without it, jobs
  pinned via `--nodelist` to reserved nodes pend indefinitely.
- `slurm_qos`, `qos_normal`, `qos_boost` — QoS names emitted as `sbatch
  --qos=<x>`. The controller picks the QoS **per submission** from the pool's
  DB-sourced slot sum (`active_slots + pending_slots`, i.e. the summed
  `requested_concurrency` of live `SlurmWorkerJob` rows), NOT a `squeue` query:
  - if that sum is **below** `min_slots`, the pool is under its warm floor and
    the submission uses `qos_boost` (higher priority);
  - otherwise it uses `qos_normal` (falling back to `slurm_qos` if set).
  Note: with `min_slots=0` the boost condition is unreachable, so `qos_boost`
  never fires — set a positive `min_slots` if you want a boost floor.

### Environment priority: prod > staging > dev

On the shared GB10 + OLDLAB nodes, environments must not compete as equals.
Priority is enforced **Loom-side**, so it needs no change to the shared Slurm
controller (which on this cluster does not weight QoS — `PriorityWeightQOS=0` —
and runs with preemption off):

1. **Active reclaim (#892).** When production has queued demand it cannot
   satisfy, the prod-pressure bridge marks the lower-priority pools' policies
   `prod_pressure_state = "draining"`. The scheduler claim query then **fences
   new claims** on those pools (`claim.py`), so staging/dev stop taking work,
   drain gracefully (in-flight trials finish — no kills), and their desired slots
   recover once pressure clears. See "prod-pressure handling" above.
2. **Submit priority + min/max.** Within what a pool may run, `submit_priority`
   orders trials, and `min_slots` / `max_slots` bound each pool: the autoscaler
   keeps at least `min_slots` warm and never overshoots `max_slots` (see clamp
   below). Give production a positive `min_slots` at activation so it always
   holds a warm floor; staging/dev default to `min_slots = 0` (scale to zero) and
   yield first.

The `qos_normal` / `qos_boost` bindings above stay **empty** on these pools:
the `loom-*` QoS tiers do not exist on the controller, and adding them would
require a cluster-wide `PriorityWeightQOS` change affecting every other user —
unnecessary given the reclaim already delivers env-priority. Set a real
`qos_normal` (e.g. `normal`) only if a future controller enables QoS weighting.

### max_slots clamp (no overshoot)

The autoscaler never submits a worker that would push the pool's committed slot
sum past `max_slots`. When a submission would overshoot, its concurrency is
clamped to the remaining budget (`max_slots - active_plus_pending`), and its
CPU/memory are scaled **proportionally from the pre-clamp request** using an
integer ceiling — e.g. a 10-slot / 115000 MiB worker clamped to 4 slots
requests `ceil(115000 * 4 / 10) = 46000` MiB (never the per-slot default
`4 * memory_mib_per_slot`, and never a banker's-rounded under-request).
Resource-aware `safe_slots` selection is applied first, then this budget clamp.

OLDLAB 1-5 should use conservative resource-aware scale-up rather than a
static high `requested_concurrency`. With `resource_aware=true`, the autoscaler
queries `sinfo` before each scale-up and excludes nodes with an active Loom
Slurm job, unsafe Slurm state, missing resource data, high CPU load, too little
free memory, or too little idle CPU. The submitted worker concurrency is
computed per node:

```text
safe_slots = min(
  floor((idle_cpus_or_total_cpus - reserved_cpus) / cpu_per_slot),
  floor((free_memory_mib - reserved_memory_mib) / memory_mib_per_slot),
  max_concurrency_per_node
)
```

The conservative OLDLAB default is `cpu_per_slot=2`,
`memory_mib_per_slot=8192`, `reserved_cpus=4`,
`reserved_memory_mib=24576`, `max_concurrency_per_node=8`, and
`max_cpu_load_ratio=1.0`. Across five allowed OLDLAB nodes this caps the pool
at `5 * 8 = 40` slots, but only when all five nodes pass the live Slurm safety
checks. If the nodes are already loaded near their CPU count or a node has only
single-digit GiB free memory, the autoscaler should keep the warm minimum
instead of submitting more jobs. When every allowed node is excluded during a
desired Slurm scale-up, the autoscaler records `last_decision=blocked`,
`last_blocked_reason=no_safe_slurm_nodes`, and structured
`last_blocked_details.node_exclusions` entries such as
`oldlab-1:insufficient_memory`, `oldlab-2:cpu_load_high`, or
`oldlab-5:unsafe_state`. The text status commands summarize those entries, and
the JSON output includes the Slurm state, CPU load, idle CPU, free memory, and
safe-slot count when live `sinfo` data was available.
If the Slurm include list is empty or missing, scale-up is blocked with
`last_blocked_reason=missing_slurm_allowed_nodes` and details showing the
invalid `allowed_nodes` value.
`loom admin environment-state check --format json` copies these hard blockers
into `autoscaler_blockers`, and `loom cluster release-gate` fails the
environment-state convergence row until they are resolved.

If the Control Plane runs inside Kubernetes and the Slurm CLI/munge socket are
available only on the OLDLAB submit host, mark the policy as externally run:

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
    "env_file": "/shared_work/qianyi/loom-worker-capacity/staging-oldlab-worker-${IMAGE_TAG}.env",
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
    "exclusive": false,
    "container_cpus": 2,
    "container_memory_mib": 4096,
    "container_pids": 512,
    "candidate_sha": "<exact-40-character-candidate-sha>"
  }
}
```

The in-pod Control Plane autoscaler loop skips `external_runner` policies, so
it does not repeatedly fail on missing `sbatch`. Run the reconciler on the
Slurm submit host with the same deployed code and a local Control Plane DB
port-forward. The supported one-shot entrypoint is
`scripts/ops/worker_pool_autoscaler_external_once.py --environment
<environment> --pool-name <pool>`, which passes the exact environment and pool
filters together with `include_external_policies=True` and
`external_only=True` to `reconcile_worker_pool_autoscaler_once`. This keeps
policy, status, and API visibility in the Control Plane while executing Slurm
commands only where the cluster credentials exist and only for the intended
environment/pool authority.

For GB10, use the same Slurm actuator rather than the legacy `gb10` actuator
for normal capacity. The backend remains `docker` because each worker runs
Docker sandboxes, while the autoscaler actuator is `slurm` because capacity is
requested and released through the GB10 Slurm partition. The declared inventory
contains all 15 nodes. The 150-slot value below is a physical maximum; current
allocatable capacity remains resource-observation driven:

```json
{
  "actuator": "slurm",
  "enabled": false,
  "disabled_reason": "#827: service identity and exact-candidate allocation attestation are not yet proven for every allowed GB10 node",
  "min_slots": 0,
  "max_slots": 150,
  "actuator_config": {
    "backend": "docker",
    "cpu_arch": "arm64",
    "partition": "gb10",
    "allowed_nodes": [
      "trt-gb10-1",
      "trt-gb10-2",
      "trt-gb10-3",
      "trt-gb10-4",
      "trt-gb10-5",
      "trt-gb10-6",
      "trt-gb10-7",
      "trt-gb10-8",
      "trt-gb10-9",
      "trt-gb10-10",
      "trt-gb10-11",
      "trt-gb10-12",
      "trt-gb10-13",
      "trt-gb10-14",
      "trt-gb10-15"
    ],
    "env_file": "/var/lib/loom-staging-rollout/generated/staging-gb10-worker-${IMAGE_TAG}.env",
    "repo_dir": "/shared_work2/qianyi/.loom-staging-rollout/worker-repos/loom-remote-worker-${IMAGE_TAG}",
    "requested_cpus": 20,
    "requested_memory_mib": 115000,
    "requested_concurrency": 10,
    "max_jobs": 15,
    "pending_job_cap": 2,
    "time_limit": "2-00:00:00",
    "exclusive": false,
    "container_cpus": 2,
    "container_memory_mib": 4096,
    "container_pids": 512,
    "candidate_sha": "<exact-40-character-candidate-sha>"
  }
}
```

The staging environment-state profile keeps rollout-owned materialization
disabled with the policy and supervisor for #827. None of those three switches
may be enabled by candidate-owned environment-state fields. The current loader
and release gate fail with `external_slurm_acceptance_authority_unavailable`
until an independently installed authority verifies candidate-bound service
identity and allocation evidence for every `allowed_nodes` entry. Once that
external authority exists and accepts a candidate, `loom cluster rollout` step 11
first syncs the candidate environment-state profile into the rollout root,
then copies the latest matching staging GB10 env file template when the target
`env_file` is missing, rewrites only release keys plus the active worker token
from the replayable `--worker-token` source, forces mode `0600`, and prepares
a clean shared checkout at the target `repo_dir` before the environment-state
check validates existence, git HEAD, clean status, and token parity.

A pool listed under `external_slurm_runner_prerequisites.pools` remains in the
protected supervisor artifact even while its supervisor is fully disabled.
The rendered timer carries the exact `LoomDesiredState` marker, so a protected
rollout can journal and verify the transition from an older active canonical
timer to loaded-but-disabled unit files without invoking the external
actuator. Rehearsal runs the actuator validation command only for supervisors
whose desired state is both enabled and active. The next requestless preflight
reads the same marker and accepts only the corresponding active or disabled
runtime state; legacy canonical units without a marker retain their historical
active interpretation.

The platform-dev installer owns the dedicated GB10 staging checkout root at
`/shared_work2/qianyi/.loom-staging-rollout/worker-repos` as
`loom-rollout:sharedwork` mode `2750`. It creates or verifies that root only
while rollout admission is closed and no request is active. The service can
write the root, while the `qianyi` Slurm submitter can read and traverse it but
cannot write it. The checked-in exporter helper adds only
`192.168.50.103/32` to the existing `/shared_work2` export. The platform-dev
installer owns the exact `192.168.20.12:/shared_work2` NFSv4.2 systemd mount
and creates the absent `qianyi` parent with fixed owner/group/mode. Mountinfo,
filesystem type, options, and device identity are checked so a local empty
directory cannot pass. The install record binds both account names to their resolved UID/GID,
and readiness rechecks metadata plus effective service/consumer access.
Token and worker-env files remain private mode-`0600` local files; only the
immutable candidate checkout uses `/shared_work2`.

Step 11 accepts only the candidate-named direct child of that exact root. It
claims that final child with an atomic no-replace `mkdir` at private mode
`2700`, clones into it with `--no-hardlinks`, and normalizes the completed tree
without exposing it to the shared group. It then publishes with an inode-bound
publication mode transition to consumer-readable `0750` only after complete
validation. Tracked repository symlinks remain valid;
authority symlinks, foreign ownership, hard-linked or special files, extra
directories, wrong modes, and SHA drift fail closed. Existing targets are
immutable: only an exact fully validated target is reused, and rollout never
replaces, cleans, or takes over drifted or ambient directories.
Install/check runs a bounded, self-cleaning private-claim/access-gate/publish/
collision probe as the service identity before accepting the NFS publication
contract.

Broker preflight checks all 15 GB10 inventory nodes against the root as the
`qianyi` consumer. The NFS clients must report the exact source and NFSv4
identity, while `trt-gb10-2` must report its ext4 export backend. After
publication and before environment-state apply, step 11 reads the verifier
from the exact resolved commit's Git blob, not the mutable rollout worktree,
then streams the captured bytes to `/usr/bin/python3 -` over the same protected
SSH config, identity, and known-hosts boundary. The target checkout is data
only, never verifier authority. Every node must report the exact HEAD,
zero modified/untracked/ignored status, exact physical index and modes,
readability of the deterministic tracked probe file, and no qianyi write
capability. Content identities must agree 14/14; NFS device/inode evidence is
recorded per node without requiring equality across nodes. A non-zero remote
verifier or SSH exit is retried at most thirteen times over a bounded
390-second incremental-backoff window so a just-published shared checkout or
transient transport reset cannot fail the release on its first observation.
Valid but divergent structured evidence is never retried, and exhausted
evidence records only the host, attempt count, and a non-sensitive failure
class before failing closed.

For sealed-cumulative rollouts, step 12 then fetches the exact commit from that
verified shared checkout through the fixed system upload-pack with object fsck
enabled. It does not resolve or fetch `origin/dev`; merged-dev keeps the
existing GitHub-origin fetch path.

Keep the GB10 node-agent path only for Docker Compose rollout validation,
legacy compatibility, or break-glass operation when Slurm is unavailable. The
Control Plane does not SSH into hosts. Each `loom worker gb10-agent apply`
pulls desired state and applies its host intent:

- `active`: run the worker compose service.
- `draining`: write drain intent and keep Compose running while the registry
  claim fence prevents new work; the capacity controller advances an idle host
  to `stopped`.
- `stopped`: keep compose stopped.

The active path is liveness-aware as well as metadata-aware. If image/env/source
already match desired state, `apply` still prepares the image and runs
`docker compose up -d worker`. Compose no-ops when the service is truly current,
and recreates or starts it when rollout prep pre-wrote `.env`, the image/env
changed outside compose, or the previous worker exited. The active apply path
then waits for the compose worker service to become `running`; a created/stopped
container is a failed apply, not a successful node-agent report.

Rollback or disable:

- Set the policy `enabled=false` to stop new autoscaler actions.
- Raise `min_slots` to restore warm capacity.
- Use `loom admin slurm-workers status` before manual `scancel` so running
  jobs with active trials are not interrupted.
- If using the GB10 node-agent compatibility path, restore `host_intents` to
  `active` only after confirming the policy is disabled or intentionally
  bypassed.

## Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| Worker never registers | Bad token or cannot reach Control Plane | Worker logs; `curl $LOOM_WORKER_CONTROL_PLANE_URL/healthz` from the worker host. |
| Claims happen but trials fail immediately | Docker unavailable or sandbox image missing | `docker info`; worker logs around sandbox start. |
| Trials upload no trajectory/artifacts | MinIO endpoint, credentials, or runtime bucket bootstrap failure | `curl $LOOM_WORKER_MINIO_ENDPOINT/minio/health/live`; worker logs for S3 errors; trial `failure_reason` should be `trajectory_flush_failed` or `artifact_upload_failed`. |
| Queue grows while hosts look idle | Workers not matching task capabilities or provider limits throttling | Control Plane worker table, queue depth, gateway/provider errors. |
| Autoscaler does not scale up | Policy disabled, cooldown active, max slots reached, pending cap reached, missing Slurm include list, no safe Slurm nodes, external runner not active, release-state drift in an active Slurm job, or no compatible queued trials | `loom admin worker-pools autoscaler status --format json`; check `last_blocked_reason`, `last_blocked_details.node_exclusions`, `last_error`, queued caps, Slurm job status, and `loom resources status --json`. For `missing_slurm_allowed_nodes`, repair `actuator_config.allowed_nodes` before rerunning the external autoscaler. For `no_safe_slurm_nodes`, inspect each node exclusion reason before changing the allowlist or resource thresholds. For `release_state_drift`, confirm whether the listed job is outside `allowed_nodes` or has stale launch metadata. Safely linked running workers drain; once `draining` with zero claimed/running trials, the autoscaler cancels the job (or observes its exit) and marks the worker `drained`. Reconcile pending/unlinked jobs explicitly, then rerun `loom admin environment-state check` before release validation. |
| Worker remains draining | In-flight trial still assigned or Slurm release has not converged | `loom resources status --json`; inspect claimed/running trials by worker id and `loom admin slurm-workers status`. |
| Host becomes unstable | Concurrency too high or missing sandbox resource limits | Lower `LOOM_WORKER_MAX_CONCURRENT`; inspect memory, swap, and Docker container count. |
