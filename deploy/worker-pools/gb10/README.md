# GB10 Remote Worker Pool

This directory records the public-beta GB10 worker-pool policy validated for
issue #518. The pool attaches ARM64 GB10 hosts to the OLDLAB-1 public-beta
control plane as fixed Docker Compose remote workers.

## Topology

- Control plane host: OLDLAB-1, reached by the operator as `platform-dev`,
  `oldlab-1`, or `oldlab1`.
- Kubernetes namespace: `loom-public-beta` in the `kind-loom-public-beta`
  cluster.
- Worker hosts: `trt-gb10-1` through `trt-gb10-15`.
- Jump path: operator Mac -> `cudo-sudo-trt` -> `trt-gb10-N`.
- Worker process path on every GB10 host: `/home/trt/loom-remote-worker`.
- Loom checkout path on every GB10 host:
  `/home/trt/loom-remote-worker/loom`.
- Remote-worker env file on every GB10 host:
  `/home/trt/loom-remote-worker/.env.remote-worker`, mode `600`.

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

## SSH Trust And Tunnel Recovery

The operator path has two SSH hops: the Mac reaches `cudo-sudo-trt`, then
`cudo-sudo-trt` reaches `trt-gb10-N`. Load the GB10 ssh-agent on
`cudo-sudo-trt` before running fleet commands:

```bash
source ~/.ssh/gb10_agent.env
```

Verify non-interactive SSH before starting or restarting workers:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$h" 'hostname >/dev/null'
done
```

On OLDLAB-1, install the durable private service tunnels with the shared
remote-worker tunnel helper. Use durable paths for `kubectl` and `kubeconfig`;
do not install units that depend on files under `/tmp`. Install the watchdog
timer after the tunnel units so stale active-looking port-forwards are restarted
automatically after host restarts, pod recreation, or public-beta rollouts.

```bash
scripts/ops/worker_service_tunnels.py install-systemd \
  --namespace loom-public-beta \
  --kubectl /usr/local/bin/kubectl \
  --kubeconfig /secure/path/public-beta.kubeconfig

scripts/ops/worker_service_tunnels.py install-watchdog-systemd \
  --env-file /secure/path/.env.remote-worker
```

After every public-beta rollout, check the OLDLAB-1 tunnel units, watchdog
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
- Per-host trial concurrency is currently `LOOM_WORKER_MAX_CONCURRENT=2`.
- Every host advertises `LOOM_WORKER_POOL_NAME=gb10-arm64`.
- Total configured GB10 trial capacity is therefore `15 * 2 = 30` slots.
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

Run the following from `cudo-sudo-trt` after loading the GB10 ssh-agent:

```bash
source ~/.ssh/gb10_agent.env
```

Prepare or refresh the checkout on every host:

```bash
COMMIT=cbbe6ff213da492fcef4348121b941944547c188
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh "$h" "
    set -euo pipefail
    mkdir -p ~/loom-remote-worker
    if [ ! -d ~/loom-remote-worker/loom/.git ]; then
      git clone https://github.com/qianyi-sun/loom.git ~/loom-remote-worker/loom
    fi
    cd ~/loom-remote-worker/loom
    git fetch origin dev
    git checkout --detach $COMMIT
  "
done
```

Create `/home/trt/loom-remote-worker/docker-compose.gb10-hostnet.yml` on every
host:

```yaml
services:
  worker:
    network_mode: host
```

Keep `.env.remote-worker` untracked and mode `600`. Required non-secret shape:

```bash
LOOM_IMAGE_TAG=public-beta-cbbe6ff
LOOM_WORKER_CONTROL_PLANE_URL=http://127.0.0.1:18081
LOOM_WORKER_GATEWAY_URL=http://127.0.0.1:19100
LOOM_WORKER_SUBPROCESS_GATEWAY_URL=http://host.docker.internal:19100
LOOM_WORKER_MINIO_ENDPOINT=http://127.0.0.1:19000
LOOM_WORKER_MAX_CONCURRENT=2
LOOM_WORKER_POOL_NAME=gb10-arm64
LOOM_WORKER_ENV_CONFIG_VERSION=manual-v1
LOOM_WORKER_HOSTNAME=trt-gb10-N
LOOM_WORKER_SANDBOX_WORKER_INDEX=N
LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS=31536000
LOOM_WORKER_LOG_LEVEL=info
LOOM_WORKER_DOCKER_API_TIMEOUT_SEC=1800
LOOM_WORKER_MINIO_MAX_POOL_CONNECTIONS=256
LOOM_WORKER_MINIO_CONNECT_TIMEOUT_SEC=5
LOOM_WORKER_MINIO_READ_TIMEOUT_SEC=120
LOOM_WORKER_MINIO_OPERATION_TIMEOUT_SEC=300
LOOM_WORKER_MINIO_OPERATION_ATTEMPTS=3
```

The same file must also contain the environment's worker token and MinIO
credentials. Do not print those values in issue comments, logs, or PRs.

`LOOM_WORKER_ENV_CONFIG_VERSION` is a host-local lifecycle marker for the
GB10 node-agent. It is read from the env file by Docker Compose and the
node-agent, but it is not passed into the worker container. This lets the
node-agent compare local env/config state with Control Plane desired state
without storing worker tokens or MinIO credentials in the Control Plane.

Start or restart workers:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh "$h" "
    set -euo pipefail
    cd ~/loom-remote-worker/loom
    docker compose \
      --env-file ../.env.remote-worker \
      -f deploy/docker-compose.remote-worker.yml \
      -f ../docker-compose.gb10-hostnet.yml \
      up -d --build
  " &
done
wait
```

Install the worker-side user service so each GB10 host re-runs the same Compose
startup after host reboot. This service does not store secrets; it reads the
existing `/home/trt/loom-remote-worker/.env.remote-worker` file.

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh "$h" "
    set -euo pipefail
    mkdir -p ~/.config/systemd/user
    cp ~/loom-remote-worker/loom/deploy/worker-pools/gb10/loom-gb10-worker.service \
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
credentials.

Create `/home/trt/loom-remote-worker/gb10-node-agent.env` on every host with
mode `600`:

```bash
LOOM_GB10_CP_URL=http://127.0.0.1:18081
LOOM_GB10_ENVIRONMENT=production
LOOM_GB10_POOL_NAME=gb10-arm64
LOOM_GB10_DRAIN_TIMEOUT_SEC=600
LOOM_GB10_NODE_AGENT_TOKEN=loom_admin_...
```

The node-agent token currently uses the CP admin surface and must include the
`admin:gb10_workers` scope. Keep it host-local and rotate it with the same
care as other admin credentials.

Install the node-agent service and timer:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh "$h" "
    set -euo pipefail
    mkdir -p ~/.config/systemd/user
    cp ~/loom-remote-worker/loom/deploy/worker-pools/gb10/loom-gb10-node-agent.service \
      ~/.config/systemd/user/loom-gb10-node-agent.service
    cp ~/loom-remote-worker/loom/deploy/worker-pools/gb10/loom-gb10-node-agent.timer \
      ~/.config/systemd/user/loom-gb10-node-agent.timer
    systemctl --user daemon-reload
    systemctl --user enable --now loom-gb10-node-agent.timer
  " &
done
wait
```

Before enabling fleet-wide apply, inspect one host:

```bash
cd /home/trt/loom-remote-worker/loom
uv run loom worker gb10-agent plan \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --hostname trt-gb10-1 \
  --env-file /home/trt/loom-remote-worker/.env.remote-worker
```

Use `apply --dry-run` to preview the non-secret env keys and Docker Compose
commands that would run. Dry-run output does not print the full
`.env.remote-worker` file because that file also contains worker and MinIO
credentials.

Set desired state through the CP admin API. This example canaries only
`trt-gb10-1`; after it reports `applied`, change `rollout_policy` to
`{"mode":"all"}` or expand `canary_hosts`.

```bash
curl -sS -X PUT \
  -H "Authorization: Bearer $LOOM_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  http://127.0.0.1:18081/admin/gb10-worker-pools/production/gb10-arm64/desired-state \
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

The node-agent applies updates by writing non-secret keys in
`.env.remote-worker`, then running `docker compose pull`. If the worker image
tag is not available from a registry, it falls back to `docker compose build`
from the host-local checkout before running `docker compose stop --timeout
<drain-timeout> worker` and `docker compose up -d worker`. The stop path sends
SIGTERM to the worker, which uses the existing worker drain logic before the
container exits. Use `--force` only for an explicit emergency override.

Retry a failed rollout by fixing the local cause and restarting the service:

```bash
systemctl --user start loom-gb10-node-agent.service
```

Rollback publishes the previous desired image/concurrency/env version back to
the Control Plane first, then applies it locally. This keeps the periodic
node-agent timer from reapplying the bad desired state after a manual rollback:

```bash
uv run loom worker gb10-agent apply \
  --cp-url http://127.0.0.1:18081 \
  --admin-token env:LOOM_GB10_NODE_AGENT_TOKEN \
  --environment production \
  --pool-name gb10-arm64 \
  --env-file /home/trt/loom-remote-worker/.env.remote-worker \
  --compose-file deploy/docker-compose.remote-worker.yml \
  --compose-file /home/trt/loom-remote-worker/docker-compose.gb10-hostnet.yml \
  --rollback \
  --force
```

The manual Docker Compose commands remain the break-glass fallback when the
node-agent token, timer, or CP desired-state API is unavailable.

Enable lingering for the `trt` user on every GB10 host through the site's
privileged admin path so the user service starts after reboot even before an
operator logs in:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh "$h" 'sudo loginctl enable-linger trt' &
done
wait
```

Stop without deleting cached Docker volumes:

```bash
for i in $(seq 1 15); do
  h=trt-gb10-$i
  ssh "$h" "
    cd ~/loom-remote-worker/loom &&
    docker compose \
      --env-file ../.env.remote-worker \
      -f deploy/docker-compose.remote-worker.yml \
      -f ../docker-compose.gb10-hostnet.yml \
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
  ssh "$h" "
    cd ~/loom-remote-worker/loom &&
    docker compose \
      --env-file ../.env.remote-worker \
      -f deploy/docker-compose.remote-worker.yml \
      -f ../docker-compose.gb10-hostnet.yml \
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
occupied/total slot count before starting another sweep.
