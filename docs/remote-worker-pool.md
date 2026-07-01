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

Set `LOOM_WORKER_HOSTNAME` in each remote worker env file to the physical or
VM host name, for example `trt-gb10-7`. If it is unset, the worker registers
with the runtime hostname, which may be only a container ID in Docker Compose.
Using the host name keeps Monitor, worker inventory, and capacity evidence
readable when many remote workers attach through the same control plane.
Set `LOOM_WORKER_POOL_NAME` to the stable pool identity shared by similar
workers, for example `gb10-arm64`, `oldlab`, or `remote-worker`. Monitor,
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
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig \
  --gateway-local-port 30444
```

If the worker process must keep using `19100` while subprocess agents in
Docker sandboxes use a separate host bridge such as
`http://host.docker.internal:30444/openai/v1`, install an additional managed
`subprocess-gateway` tunnel:

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig \
  --subprocess-gateway-local-port 30444
```

This creates `loom-remote-worker-tunnel-subprocess-gateway.service` and keeps
the normal `loom-remote-worker-tunnel-gateway.service` on `19100`.

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
`worker_service_tunnels.py check`, `check-remote`, `print-check-script`, and
`watchdog` add a `subprocess-gateway` probe against the Gateway health endpoint.
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
scripts/ops/worker_service_tunnels.py check \
  --env-file .env.remote-worker
```

For immediate local self-heal testing without waiting for the timer, run:

```bash
scripts/ops/worker_service_tunnels.py watchdog \
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

For production/public-beta capacity, the hostfile should include every
candidate worker node the operator is allowed to use. Exclude a node
only with a recorded reason such as missing Docker, failed endpoint
reachability, insufficient disk, or Slurm reservation policy.

The staged OLDLAB public-beta manual plan is recorded under
`deploy/worker-pools/oldlab/`. It intentionally requests only `12 CPU` and
`58000M` per node with `LOOM_WORKER_MAX_CONCURRENT=6`, even when inventory
shows more total host capacity. For normal shared OLDLAB capacity, prefer the
worker-pool autoscaler policy described below; it can use live Slurm node
resources instead of this fixed manual slice.

The staged GB10 public-beta plan is recorded under
`deploy/worker-pools/gb10/`. GB10 workers execute Docker sandboxes on ARM64
hosts, but normal capacity management should still use the same Slurm
autoscaler policy shape as OLDLAB: `actuator=slurm`, `pool_name=gb10-arm64`,
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
- pool name (`LOOM_WORKER_POOL_NAME`);
- per-worker trial concurrency (`LOOM_WORKER_MAX_CONCURRENT`);
- env/config version (`LOOM_WORKER_ENV_CONFIG_VERSION`, node-agent local only);
- rollout policy such as canary hosts;
- optional compatibility target slots and per-host intents:
  `active`, `draining`, or `stopped`.

For public-beta and staging release rollouts, apply the repository
environment-state profile so the Slurm autoscaler policy and GB10 compatibility
desired state converge together. The public-beta profile writes the existing
`production/gb10-arm64` CP key until GB10 node-agent environment names are
renamed in a coordinated rollout. Run this from the Slurm submit host when the
profile declares an external Slurm autoscaler supervisor; public-beta uses that
path to install and check the OLDLAB user timer, and the rendered service must
call the repo one-shot with `--pool-name oldlab` so it cannot reconcile GB10 as
a side effect. The check also compares the active environment worker token to
remote-worker env files and active Slurm job fingerprints using only redacted
sha256-prefix output, so pass the token through `env:` or `file:` indirection:

```bash
loom admin environment-state apply \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment public-beta \
  --file deploy/environment-state/public-beta.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}"

loom admin environment-state check \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment public-beta \
  --file deploy/environment-state/public-beta.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --worker-token env:LOOM_WORKER_TOKEN
```

For a one-off node-agent canary experiment, write desired state through the CP
admin API:

```bash
curl -sS -X PUT \
  -H "Authorization: Bearer $LOOM_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  http://control-node.lan:18081/admin/gb10-worker-pools/production/gb10-arm64/desired-state \
  -d '{
    "image_tag": "public-beta-<commit>",
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
capacity marked draining/stopped is ignored.

```bash
loom admin gb10-workers status \
  --cp-url http://control-node.lan:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --release-image-tag "$IMAGE_TAG" \
  --release-env-config-version "$ENV_CONFIG_VERSION"
```

On each GB10 host, the node-agent reads the host-local
`.env.remote-worker` file, compares it with Control Plane desired state, writes
only non-secret env updates, then runs Docker Compose locally. The apply path
uses `docker compose stop --timeout <seconds> worker`, so the worker receives
SIGTERM and uses the existing drain path before restart.

```bash
loom worker gb10-agent plan \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --env-file /home/trt/loom-remote-worker/.env.remote-worker

loom worker gb10-agent apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --env-file /home/trt/loom-remote-worker/.env.remote-worker \
  --compose-file deploy/docker-compose.remote-worker.yml \
  --compose-file /home/trt/loom-remote-worker/docker-compose.gb10-hostnet.yml
```

For worker-token rotation, run the same host-local plan/apply with the active
environment token supplied through `env:`, `file:`, or stdin. The token is not
published to the Control Plane; it is compared against and, on apply, written
only to the host-local `.env.remote-worker` file before restarting the worker.
Output shows only changed key names and redacted values.

```bash
loom worker gb10-agent plan \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --env-file /home/trt/loom-remote-worker/.env.remote-worker \
  --worker-token file:/secure/path/current-worker-token

loom worker gb10-agent apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --env-file /home/trt/loom-remote-worker/.env.remote-worker \
  --compose-file deploy/docker-compose.remote-worker.yml \
  --compose-file /home/trt/loom-remote-worker/docker-compose.gb10-hostnet.yml \
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
# Optional: only set for private/gated hf:// sources that have not yet been
# mirrored to internal object storage.
# HF_TOKEN=replace-with-read-token
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
client connection before trying again.
Use `LOOM_WORKER_TASK_MATERIALIZE_TIMEOUT_SEC` only when pre-start bundle
materialization is reachable but legitimately slow; `hf://` sources that need
private internet access should normally be mirrored into internal object
storage before runtime.

Layered trial-cache image builds also have a daemon-wide concurrency cap:
`LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT`. Keep the default `1` on shared
OLDLAB, shared k8s node, or host-network remote-worker daemons so different
cold cache keys serialize their setup containers. Raise it only for isolated
Docker daemons after a load-test issue records CPU, memory, disk I/O,
containerd, and cleanup headroom. This cap does not reduce warm trial
concurrency controlled by `LOOM_WORKER_MAX_CONCURRENT`.

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
and runs `docker compose down --remove-orphans`, so autoscaler cancellation and
worker idle exit remove the compose worker container as well as the Slurm job.
Forced termination is a break-glass operator action, not the default path.

For OLDLAB Slurm, autoscaler `actuator_config` must include allowed nodes,
remote worker env file, remote checkout path, requested CPU/memory, requested
worker concurrency, max jobs, pending-job cap, and Slurm command paths if
they differ from `sbatch`, `squeue`, `sacct`, `scancel`, and `sinfo`.
Autoscaler Slurm submissions request exclusive node allocation by default.
Set `actuator_config.exclusive=false` only for deliberately shared Slurm nodes
after lowering `requested_cpus`, `requested_memory_mib`, and
`requested_concurrency` to a load-tested slice that coexists with other jobs.

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
instead of submitting more jobs.

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
    "env_file": "/shared_work/qianyi/loom-worker-capacity/public-beta-oldlab-worker-${IMAGE_TAG}.env",
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
    "exclusive": true
  }
}
```

The in-pod Control Plane autoscaler loop skips `external_runner` policies, so
it does not repeatedly fail on missing `sbatch`. Run the reconciler on the
Slurm submit host with the same deployed code and a local Control Plane DB
port-forward. The supported one-shot entrypoint is
`scripts/ops/worker_pool_autoscaler_external_once.py --pool-name <pool>`, which
passes `include_external_policies=True`, `external_only=True`, and a pool filter
to `reconcile_worker_pool_autoscaler_once`. This keeps policy, status, and API
visibility in the Control Plane while executing Slurm commands only where the
cluster credentials exist and only for the intended pool.

For GB10, use the same Slurm actuator rather than the legacy `gb10` actuator
for normal capacity. The backend remains `docker` because each worker runs
Docker sandboxes, while the autoscaler actuator is `slurm` because capacity is
requested and released through the GB10 Slurm partition. A 15-node, 10-slot
per node policy has a theoretical ceiling of 150 slots:

```json
{
  "actuator": "slurm",
  "enabled": true,
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
    "env_file": "/shared_work/qianyi/loom-worker-capacity/public-beta-gb10-worker-${IMAGE_TAG}.env",
    "repo_dir": "/shared_work/qianyi/loom-remote-worker-${IMAGE_TAG}",
    "requested_cpus": 20,
    "requested_memory_mib": 115000,
    "requested_concurrency": 10,
    "max_jobs": 15,
    "pending_job_cap": 2,
    "time_limit": "2-00:00:00",
    "exclusive": true
  }
}
```

Keep the GB10 node-agent path only for Docker Compose rollout validation,
legacy compatibility, or break-glass operation when Slurm is unavailable. The
Control Plane does not SSH into hosts. Each `loom worker gb10-agent apply`
pulls desired state and applies its host intent:

- `active`: run the worker compose service.
- `draining`: write drain intent, stop compose with the drain timeout, and
  let the worker finish in-flight trials before exit.
- `stopped`: keep compose stopped.

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
| Autoscaler does not scale up | Policy disabled, cooldown active, max slots reached, pending cap reached, no safe Slurm nodes, external runner not active, release-state drift in an active Slurm job, or no compatible queued trials | `loom admin worker-pools autoscaler status --format json`; check `last_blocked_reason`, `last_error`, queued caps, Slurm job status, and `loom resources status --json`. For `release_state_drift`, replace/cancel the listed stale Slurm jobs and rerun `loom admin environment-state check` before release validation. |
| Worker remains draining | In-flight trial still assigned or Slurm release has not converged | `loom resources status --json`; inspect claimed/running trials by worker id and `loom admin slurm-workers status`. |
| Host becomes unstable | Concurrency too high or missing sandbox resource limits | Lower `LOOM_WORKER_MAX_CONCURRENT`; inspect memory, swap, and Docker container count. |
