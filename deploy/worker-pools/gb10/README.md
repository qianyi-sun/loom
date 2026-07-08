# GB10 Remote Worker Pool

This directory records the staging GB10 worker-pool policy validated for
issue #518. The pool attaches ARM64 GB10 hosts to the OLDLAB-1 staging
control plane as Slurm-managed Docker workers. Slurm owns normal capacity
request/release; Docker Compose and the node-agent remain documented here for
host-local worker execution, rollout validation, legacy compatibility, and
break-glass operation.

## Topology

- Control plane host: OLDLAB-1, reached by the operator as `platform-dev`,
  `oldlab-1`, or `oldlab1`.
- Kubernetes namespace: `loom-staging` in the `kind-loom-staging`
  cluster.
- Worker hosts: `trt-gb10-1` through `trt-gb10-15`.
- Slurm partition: `gb10`.
- Jump path: `deploy/worker-pools/gb10/ssh_config` is the release-managed SSH
  topology for `trt-gb10-N`; `trt-gb10-14` specifically must use
  `ProxyJump trt-gb10-1`.
- Worker process path on every GB10 host:
  `/home/qianyi/loom-worker-build-staging`.
- Loom checkout path on every GB10 host:
  `/home/qianyi/loom-worker-build-staging`.
- Remote-worker env file on every GB10 host:
  `/home/qianyi/loom-worker-build-staging/.env`, mode `600`.

The worker-facing OLDLAB-1 services stay private. Public internet traffic must
continue to reach only Web/API over TLS. Remote workers use loopback endpoints
provided by the existing GB10 tunnel services:

```bash
LOOM_WORKER_CONTROL_PLANE_URL=http://127.0.0.1:18081
LOOM_WORKER_GATEWAY_URL=http://127.0.0.1:19100
LOOM_WORKER_MINIO_ENDPOINT=http://127.0.0.1:19000
```

Because the worker process runs in Docker Compose, the GB10 deployment adds a
Compose override with `network_mode: host`. Subprocess agents running inside
trial sandboxes still need a sandbox-visible gateway URL, so the env file sets:

```bash
LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:19100
```

The worker normalizes this sandbox-facing gateway root by agent adapter:
Codex/OpenAI-compatible agents receive `/openai/v1`, Claude Code receives
`/anthropic`, and Gemini receives `/google`. A GB10 env file that pins Codex to
an explicit `/anthropic` facade is invalid and fails before the subprocess
starts.

## SSH Trust And Tunnel Recovery

Protected rollouts run from `platform-dev` as the fixed rollout runner. Step 11
uses the repo-owned `deploy/worker-pools/gb10/ssh_config` with `ssh -F` plus
the platform-dev-local deploy identity declared by
`[gb10_pool].ssh_identity_file`; do not rely on `platform-dev` having local
`trt-gb10-*` aliases or a Mac forwarded-agent session. Keep `trt-gb10-14`
routed through `ProxyJump trt-gb10-1`; the older `trt-gb10-8` jump path does
not reach `10.42.0.12:22`.

Do not background the protected rollout on `platform-dev` while depending on
the operator Mac's forwarded SSH agent. The formal rollout path is a detached
`systemd-run --user` unit on `platform-dev` using the local deploy identity.
If rollout state remains `running` after the process exits, rerun the same
fixed image tag with `--resume` instead of hand-editing rollout evidence or
host state.

Verify non-interactive SSH before starting or restarting workers:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config \
    -i /shared_work/qianyi/loom-worker-capacity/staging-gb10-rollout-ed25519 \
    -o IdentitiesOnly=yes \
    -o BatchMode=yes -o ConnectTimeout=8 "$h" 'hostname >/dev/null'
done
```

On OLDLAB-1, install the durable private service tunnels with the shared
remote-worker tunnel helper. Use durable paths for `kubectl` and `kubeconfig`;
do not install units that depend on files under `/tmp`. Install the watchdog
timer after the tunnel units so stale active-looking port-forwards are restarted
automatically after host restarts, pod recreation, or staging rollouts.

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-staging \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/staging.kubeconfig

scripts/ops/worker_service_tunnels.py install-watchdog-systemd \
  --env-file /secure/path/.env.remote-worker
```

After every staging rollout, check the OLDLAB-1 tunnel units, watchdog
timer, and worker-facing health URLs. Manual restart is a fallback when the
watchdog reports repeated failures instead of a normal first step.

```bash
systemctl --user is-active \
  loom-remote-worker-tunnel-control-plane.service \
  loom-remote-worker-tunnel-gateway.service \
  loom-remote-worker-tunnel-minio.service \
  loom-remote-worker-tunnel-watchdog.timer

scripts/ops/worker_service_tunnels.py check \
  --env-file /secure/path/.env.remote-worker
```

From the worker side, validate the same private URLs through SSH. The helper
derives and sends only health-check URLs; it does not send worker tokens, MinIO
secret keys, or provider credentials.

```bash
seq -f 'trt-gb10-%.0f' 1 15 > gb10-hosts.txt

scripts/ops/worker_service_tunnels.py check-remote gb10-hosts.txt \
  --env-file /secure/path/.env.remote-worker
```

## Current Validated State

Evidence date: 2026-06-25.

- `trt-gb10-1..15` are all `worker-enabled`.
- Every host is `aarch64`; every Loom worker registers as `cpu_arch=arm64`.
- `trt` is a member of the `docker` group on every host.
- Docker is accessible from a fresh SSH session on every host.
- Control Plane, Gateway, and MinIO health checks pass from every host through
  the local tunnel endpoints.
- Every host runs one Docker Compose worker container.
- Historical issue #518 smoke used per-host trial concurrency
  `LOOM_WORKER_MAX_CONCURRENT=2`.
- Current staging capacity after the node-agent/autoscaler rollout uses
  `LOOM_WORKER_MAX_CONCURRENT=10`.
- Every host advertises `LOOM_WORKER_POOL_NAME=gb10-arm64`.
- Total configured GB10 trial capacity is therefore `15 * 10 = 150` slots.
- During cold source-useful/Terminus setup, a worker may hold all 10 local
  slots while `LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT=1` serializes
  layered trial-cache Docker builds before `started_at` is set. Upgraded
  workers refresh `pre_start_heartbeat_at` every
  `LOOM_WORKER_PRE_START_HEARTBEAT_INTERVAL_SEC` seconds so the Control Plane
  does not reclaim legitimate local pre-start setup as a dead claim. The
  Control Plane default `LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC=3600` is a
  fallback window; do not rely on live manual `kubectl set env` patches as the
  durable rollout mechanism.
- Normal capacity policy uses `actuator=slurm`,
  `actuator_config.partition=gb10`, `actuator_config.cpu_arch=arm64`,
  `requested_concurrency=10`, `max_jobs=15`, and `max_slots=150`.
- Canary batch `b18d1a92-909a-443f-a768-f0aae8229cea` finished `succeeded`.
  Trial `6e833772-ae85-4bf0-9621-904cb9bca0ea` was claimed by
  `trt-gb10-6`, used the real Lux provider model
  `qwen2.5-coder-7b-instruct`, made one LLM call, and produced reward `1.0`.
- GB10 workers claimed no legacy or `x86_64` trials during the smoke; the only
  GB10-claimed trial required `cpu_arch=any`.
- Fresh recheck at `2026-06-25T20:15:40Z`: all 15 hosts passed Control Plane,
  Gateway, MinIO, Docker daemon, and running worker-container checks; OLDLAB-1
  tunnel units were active; DB worker rows reported `15/15` active ARM64
  workers.

See `inventory-2026-06-25.txt` and `smoke-evidence-2026-06-25.json` for
non-secret evidence details.

## Storage Policy

Use each GB10 node's local ext4 root disk for Docker data and worker hot paths.

Do not put these on `/shared_work`:

- Docker `overlay2` or Docker data-root.
- Worker trajectory cache.
- Worker benchmark cache.
- Trial scratch directories.
- Postgres, MinIO backend data, kind volumes, or Kubernetes PV data.

`/shared_work` is NFSv4 exported from `trt-gb10-1`; it is useful for
read-mostly cache staging or transferring evidence, not for high-churn Docker
or trial execution paths. Trial artifacts should return through Loom's
artifact/trajectory object-store path.

## Setup And Restart

The protected rollout owns normal checkout/env/service convergence. The
commands below are break-glass/manual equivalents and must be run from
`platform-dev` with the repo-owned SSH config plus the local deploy identity,
not from a Mac forwarded-agent session.

Prepare or refresh the checkout on every host:

```bash
COMMIT=cbbe6ff213da492fcef4348121b941944547c188
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" "
    set -euo pipefail
    if [ ! -d ~/loom-worker-build-staging/.git ]; then
      git clone https://github.com/qianyi-sun/loom.git ~/loom-worker-build-staging
    fi
    cd ~/loom-worker-build-staging
    git fetch origin dev
    git checkout --detach $COMMIT
  "
done
```

The host-network override lives in the checked-out repo at
`deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml`:

```yaml
services:
  worker:
    network_mode: host
```

Keep `.env` untracked and mode `600`. Required non-secret shape:

```bash
LOOM_IMAGE_TAG=staging-cbbe6ff
LOOM_WORKER_CONTROL_PLANE_URL=http://127.0.0.1:18081
LOOM_WORKER_GATEWAY_URL=http://127.0.0.1:19100
LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:19100
LOOM_WORKER_MINIO_ENDPOINT=http://127.0.0.1:19000
LOOM_WORKER_MAX_CONCURRENT=2
LOOM_WORKER_POOL_NAME=gb10-arm64
LOOM_WORKER_ENV_CONFIG_VERSION=manual-v1
LOOM_WORKER_HOSTNAME=trt-gb10-N
LOOM_WORKER_SANDBOX_WORKER_INDEX=N
LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS=7200
LOOM_WORKER_LOG_LEVEL=info
LOOM_WORKER_DOCKER_API_TIMEOUT_SEC=1800
LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT=1
LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS=256
LOOM_WORKER_MINIO_CONNECT_TIMEOUT_SEC=5
LOOM_WORKER_MINIO_READ_TIMEOUT_SEC=120
LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC=300
LOOM_WORKER_MINIO_OPERATION_ATTEMPTS=3
```

The same file must also contain the environment's worker token and MinIO
credentials. Do not print those values in issue comments, logs, or PRs.
After worker-token rotation, update this file on every host before restarting
workers. The release gate checks the shared GB10 Slurm runner env file and
active Slurm job `LOOM_WORKER_AUTH_FINGERPRINT` values with:

```bash
loom admin environment-state check \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA" \
  --worker-token file:/secure/path/worker-token
```

For the host-local node-agent path, `LOOM_WORKER_ENV_CONFIG_VERSION` must change
with the token rollout so `loom admin gb10-workers status
--release-env-config-version "$ENV_CONFIG_VERSION"` proves the updated
`.env` was applied on each GB10 host without storing the raw token
in the Control Plane.

`LOOM_WORKER_ENV_CONFIG_VERSION` is a host-local lifecycle marker for the
GB10 node-agent. It is read from the env file by Docker Compose and the
node-agent, but it is not passed into the worker container. This lets the
node-agent compare local env/config state with Control Plane desired state
without storing worker tokens or MinIO credentials in the Control Plane.

Start or restart workers:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" "
    set -euo pipefail
    cd ~/loom-worker-build-staging
    docker compose \
      --env-file .env \
      -f deploy/docker-compose.remote-worker.yml \
      -f deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml \
      up -d --build
  " &
done
wait
```

Install the worker-side user service so each GB10 host re-runs the same Compose
startup after host reboot. This service does not store secrets; it reads the
existing `/home/qianyi/loom-worker-build-staging/.env` file.

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" "
    set -euo pipefail
    mkdir -p ~/.config/systemd/user
    cp ~/loom-worker-build-staging/deploy/worker-pools/gb10/loom-gb10-worker.service \
      ~/.config/systemd/user/loom-gb10-worker.service
    systemctl --user daemon-reload
    systemctl --user enable --now loom-gb10-worker.service
  " &
done
wait
```

## Node-Agent Lifecycle Manager

The first lifecycle-management slice uses a host-local pull agent. Operators
write desired state to the Control Plane, and each GB10 host periodically runs
`loom worker gb10-agent apply` to compare the desired image tag, pool name,
trial concurrency, and env/config version with its local env file. The Control
Plane does not SSH into GB10 hosts and does not store worker tokens or MinIO
credentials. Apply is restartable: when the local release metadata already
matches the Control Plane and host intent is active, the agent still reconciles
Docker Compose with `docker compose up -d worker`. This covers both missing or
exited worker containers and rollout prep that pre-wrote `.env` before the
actual compose container was recreated for the target image/env.

Create `/home/qianyi/loom-worker-build-staging/gb10-node-agent.env` on every
host with mode `600`:

```bash
LOOM_GB10_CP_URL=http://127.0.0.1:18081
LOOM_GB10_ENVIRONMENT=staging
LOOM_GB10_POOL_NAME=gb10-arm64
LOOM_GB10_DRAIN_TIMEOUT_SEC=600
LOOM_GB10_NODE_AGENT_TOKEN=loom_admin_...
```

The node-agent token currently uses the CP admin surface and must include the
`admin:gb10_workers` scope. Keep it host-local and rotate it with the same
care as other admin credentials.

`gb10-node-agent.env` is host-local runtime configuration, not release source.
The repository ignores this file, along with legacy `..env.*.tmp` compose env
files from older node-agent versions, so they do not make the release-managed
checkout report `source_git_dirty=true`.

GB10 hosts install `uv` under `/home/qianyi/.local/bin`; the node-agent systemd
unit sets PATH explicitly so the timer works from a non-interactive user
service environment.

Install the node-agent service and timer only after the Control Plane desired
state has already been updated for the target release. In the standard
protected rollout, `loom cluster rollout` does that ordering for you: step 10
materializes the current candidate profile under the rollout root, applies
`deploy/environment-state/staging.toml`, then step 11 performs GB10 prep and
starts the host-local node-agent. Do not enable fleet-wide node-agent apply
before env-state points at the target image/source, or hosts can correctly
apply the previous release target.

Manual install after env-state is current:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" "
    set -euo pipefail
    mkdir -p ~/.config/systemd/user
    cp ~/loom-worker-build-staging/deploy/worker-pools/gb10/loom-gb10-node-agent.service \
      ~/.config/systemd/user/loom-gb10-node-agent.service
    cp ~/loom-worker-build-staging/deploy/worker-pools/gb10/loom-gb10-node-agent.timer \
      ~/.config/systemd/user/loom-gb10-node-agent.timer
    systemctl --user daemon-reload
    systemctl --user enable --now loom-gb10-node-agent.timer
  " &
done
wait
```

Before enabling fleet-wide apply, inspect one host:

```bash
cd /home/qianyi/loom-worker-build-staging
uv run loom worker gb10-agent plan \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment staging \
  --pool-name gb10-arm64 \
  --hostname trt-gb10-1 \
  --env-file /home/qianyi/loom-worker-build-staging/.env
```

Use `apply --dry-run` to preview the non-secret env keys and Docker Compose
commands that would run. Dry-run output does not print the full
host-local `.env` file because that file also contains worker and MinIO
credentials.

For normal staging and staging rollouts, apply the repository
environment-state profile instead of hand-patching Control Plane rows with
one-off SQL or `curl`. The profile converges the GB10 Slurm autoscaler policy
and the GB10 node-agent desired state together. Current staging evidence must
show the `staging/gb10-arm64` CP desired-state key; `production/gb10-arm64` is
production-only and is drift for a staging rollout:

```bash
loom admin environment-state apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA"

loom admin environment-state check \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --file deploy/environment-state/staging.toml \
  --var IMAGE_TAG="$IMAGE_TAG" \
  --var ENV_CONFIG_VERSION="${ENV_CONFIG_VERSION:-$IMAGE_TAG}" \
  --var GIT_SHA="$RELEASE_SHA" \
  --worker-token file:/secure/path/worker-token
```

For manual canary experiments, set desired state through the CP admin API.
This example canaries only
`trt-gb10-1`; after it reports `applied`, change `rollout_policy` to
`{"mode":"all"}` or expand `canary_hosts`. Autoscaler-managed policies may also
write `target_slots` plus per-host `host_intents` of `active`, `draining`, or
`stopped`; the node-agent applies those through the same pull-based path.

```bash
curl -sS -X PUT \
  -H "Authorization: Bearer $LOOM_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:18081/admin/gb10-worker-pools/staging/gb10-arm64/desired-state \
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

Inspect rollout state:

```bash
loom admin gb10-workers status \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN

loom admin gb10-workers status \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --format json
```

For release rollouts, gate convergence against the intended image/env/source
target and source checkout provenance. This exits non-zero if active nodes or
Control Plane desired state are still on the old release, if a declared active
host has no node report, if an active node is not `applied`, if capacity differs
from the desired max concurrency, if active node-agent reports do not include
source provenance, if the checkout is dirty, or if the reported git commit does
not match desired `source_git_commit`. Save the JSON form and pass it to
`loom cluster release-gate --gb10-workers-status` for protected rollouts.

```bash
loom admin gb10-workers status \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_ADMIN_TOKEN \
  --environment staging \
  --pool-name gb10-arm64 \
  --release-image-tag "$IMAGE_TAG" \
  --release-env-config-version "$ENV_CONFIG_VERSION" \
  --format json \
  > "$ROLLOUT_DIR/gb10-workers-status-$IMAGE_TAG.json"
```

The node-agent applies updates by first fetching `origin` and checking out the
desired `source_git_commit` in the host-local checkout when desired state
requires a source change or the tree is dirty. It then writes non-secret keys in
the host-local `.env` file and runs `docker compose pull`. During apply it uses a
temporary compose env file under the user runtime/tmp directory with mode `600`,
not inside `/home/qianyi/loom-worker-build-staging`; reruns also remove legacy
repo-root `..env.*.tmp` files. If the worker image tag is not available from a
registry, it falls back to `docker compose build` from the checked-out source
before running `docker compose stop --timeout <drain-timeout> worker` and
`docker compose up -d worker`. The stop path sends SIGTERM to the worker, which
uses the existing worker drain logic before the container exits. Use `--force`
only for an explicit emergency override.

When no metadata drift remains and host intent is active, the node-agent skips
the drain/stop phase but still prepares the image and runs
`docker compose up -d worker`. Compose no-ops when the service is already
current, and recreates or starts it when the env/image changed outside compose
or the previous worker exited. After every active `up -d`, node-agent waits for
the compose service to report `running`; if it remains only created/stopped, the
apply fails so rollout step 11 can retry or stop before release-gate.

Retry a failed rollout by fixing the local cause and restarting the service:

```bash
systemctl --user start loom-gb10-node-agent.service
```

`loom-gb10-node-agent.service` is a `Type=oneshot` unit. A successful apply
normally finishes as `ActiveState=inactive` / `SubState=dead`; check
`systemctl --user show loom-gb10-node-agent.service -p Result -p ExecMainStatus`
and expect `Result=success` with `ExecMainStatus=0`. Do not treat `is-active`
returning inactive as a failed prep by itself. Validate worker availability from
the Control Plane status artifact instead: active release hosts must have a
fresh `worker_id`, `worker_status=active`, `worker_fresh=true`, and
`docker` in `worker_backend_names`. Protected rollout retries release-target
mismatches from `loom admin gb10-workers status` while a fresh compose worker
registers and emits its first heartbeats; persistent stale worker evidence after
that retry window is a host-runtime failure to repair, not a gate to bypass.
Durable rollout and validation runners queue node-agent starts with
`systemctl --user start --no-block` and bound the SSH start command. The CP
status gate, not a synchronous `systemctl start` return, is the convergence
contract for release/resume automation.
Protected rollout also disables the older `loom-gb10-worker.service` before
starting node-agent. That legacy service is a direct Docker Compose path and
must not remain enabled, because user-manager/default-target starts during SSH
can race node-agent and recreate the worker outside the release-state boundary.
The staging desired-state profile sets
`LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS=7200` so the earliest host in a 15-host
bounded-parallel prep does not idle-exit before release-gate and smoke can
observe all 150 slots. `loom cluster rollout` step 11 keeps each host's prep
sequence ordered, but submits independent hosts with bounded concurrency. Tune
that host-level fanout through `--gb10-prep-concurrency N`; do not make it
unbounded, and do not reduce the idle-exit window below the protected rollout
convergence plus smoke window.

Rollback publishes the previous desired image/concurrency/env version back to
the Control Plane first, then applies it locally. This keeps the periodic
node-agent timer from reapplying the bad desired state after a manual rollback:

```bash
uv run loom worker gb10-agent apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment staging \
  --pool-name gb10-arm64 \
  --env-file /home/qianyi/loom-worker-build-staging/.env \
  --compose-file deploy/docker-compose.remote-worker.yml \
  --compose-file deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml \
  --source-dir /home/qianyi/loom-worker-build-staging \
  --rollback \
  --force
```

The manual Docker Compose commands remain the break-glass fallback when the
node-agent token, timer, or CP desired-state API is unavailable.

Enable lingering for the `qianyi` user on every GB10 host through the site's
privileged admin path so the user service starts after reboot even before an
operator logs in:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" 'sudo loginctl enable-linger qianyi' &
done
wait
```

Stop without deleting cached Docker volumes:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" "
    cd ~/loom-worker-build-staging &&
    docker compose \
      --env-file .env \
      -f deploy/docker-compose.remote-worker.yml \
      -f deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml \
      down
  " &
done
wait
```

## Health Checks

From each GB10 host:

```bash
curl -fsS http://127.0.0.1:18081/healthz
curl -fsS http://127.0.0.1:19100/healthz
curl -fsS http://127.0.0.1:19000/minio/health/live
docker version --format '{{.Server.Version}}'
systemctl --user is-active loom-gb10-worker.service
```

Check running worker containers:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -F deploy/worker-pools/gb10/ssh_config "$h" "
    cd ~/loom-worker-build-staging &&
    docker compose \
      --env-file .env \
      -f deploy/docker-compose.remote-worker.yml \
      -f deploy/worker-pools/gb10/docker-compose.gb10-hostnet.yml \
      ps --status running
  "
done
```

On OLDLAB-1, verify Control Plane rows:

```sql
select hostname, status, capabilities->0->>'cpu_arch' as cpu_arch,
       now() - last_seen_at as heartbeat_age
from workers
where hostname like 'trt-gb10-%'
order by split_part(hostname, 'trt-gb10-', 2)::int;
```

Expected result: 15 rows, `status=active`, `cpu_arch=arm64`, and fresh
heartbeats.

Check the scheduler guard after ARM64 workers are attached:

```sql
select coalesce(t.requires_caps->>'cpu_arch', '<missing>') as required_arch,
       count(*)
from trials t
join workers w on t.worker_id = w.id
where w.hostname like 'trt-gb10-%'
group by 1
order by 1;
```

Legacy or `x86_64` claims by `trt-gb10-%` should remain `0` unless a future
issue explicitly adds credible x86 compatibility or emulation.

## ARM64 Scheduling Policy

GB10 workers must not claim legacy tasks that lack `environment.cpu_arch`; Loom
treats those legacy requirements as `x86_64`. Only tasks explicitly marked
`environment.cpu_arch = "arm64"` or `"any"` can run on GB10.

Use `"any"` only after the task image, verifier, and artifacts are proven
credible on both architectures. Do not mark SWE-Bench x86 images or other
x86-specific benchmark images as `"any"`.

The issue #518 canary created an ops shadow task based on
`humaneval/HumanEval/0` with `environment.cpu_arch = "any"` so that it would
not alter the official HumanEval task count or score denominator.

## Cleanup Policy

Run these only when the worker is idle or drained:

```bash
docker ps --filter label=loom.trial_id
docker network ls --filter label=loom.trial_id
docker container prune
docker image prune
docker builder prune --filter until=168h
docker volume ls | grep remote_worker_
```

Do not delete `remote_worker_trajectories` or `remote_worker_benchmarks` during
a normal restart. Delete those volumes only when intentionally wiping local
worker cache state.

If a host becomes unstable, lower `LOOM_WORKER_MAX_CONCURRENT` first, restart
that host's worker, and record the observed bottleneck before raising capacity
again. Use `loom resources status` to confirm the GB10 row reports the expected
occupied/total slot count plus autoscaler desired, pending, draining, idle, and
decision state before starting another sweep.
If instability happens during cold layered-image setup, keep
`LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT=1` so each host serializes
trial-cache Docker builds even when normal warm-trial concurrency is higher.
Use `loom resources status --json` to inspect
`pre_start_heartbeat_fresh_tasks` and `oldest_starting_task_age_sec` before
changing `LOOM_CP_CLAIMED_WITHOUT_START_EXPIRY_SEC`; fresh pre-start
heartbeats mean the worker-local queue is still alive, while no heartbeat
means the row still ages from `claimed_at` and should be eligible for reclaim.
