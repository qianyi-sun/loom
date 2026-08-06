# OLDLAB Elastic Worker Pool

This directory records the staged OLDLAB 1-5 worker policy for staging and
production-like validation.

- `inventory-2026-06-24.txt` records the include decision and evidence notes for
  each node.
- `worker-plan.csv` is the Slurm launch plan consumed by
  `scripts/ops/worker_pool_slurm_submit.sh`.
- `dry-run-2026-06-24.txt` is immutable historical evidence of the retired
  whole-node launch contract. Do not replay it; current Loom Slurm submissions
  omit `--exclusive` and require the non-exclusive containment inputs below.
- `smoke-evidence-2026-06-24.json` records the staging OLDLAB 4/5 worker
  smoke batch id, worker ids, Slurm job ids, runtime, trial counts, and failure
  count.
- `controller.env.example` is the Control Plane override set for the native
  elastic Slurm controller from #432. For #45 and later, use the worker-pool
  autoscaler policy as the normal desired-capacity control surface; the same
  Slurm node, checkout, env-file, CPU, memory, concurrency, max-job, and
  pending-cap settings map into the policy `actuator_config`. Loom Slurm
  submissions always use `exclusive=false`; keep the policy disabled until
  shared-node containment and positive per-container caps are proven.

The initial capacity slice is intentionally conservative: one Slurm job per
node, `12 CPU`, `58000M`, and `LOOM_WORKER_MAX_CONCURRENT=6`. Raise concurrency
only after a separate load-test issue records CPU, RAM, Docker cleanup, MinIO,
Gateway/provider, and Control Plane state-patch health.
Keep `LOOM_WORKER_TRIAL_CACHE_BUILD_MAX_CONCURRENT=1` for OLDLAB shared Docker
daemons unless a focused load-test issue proves concurrent layered image builds
do not saturate Docker/containerd or node disk I/O.
The #275 root cause was that trial execution slots and cold setup/build work
were not the same resource: a worker could keep claiming warm-trial capacity
while task Dockerfile builds, layered agent-cache builds, and sidecar image
preparation all created Docker setup pressure before `started_at`. On shared
OLDLAB this manifested as apt/dpkg build containers driving high I/O pressure,
full swap, and SSH/login symptoms. Keep the setup-health guard enabled so new
setup work waits before launching Docker setup/build work when
`/proc/pressure/io` full avg10, free swap, or D-state process counts cross the
configured thresholds. Use `loom worker setup status` on a worker host to see
the current guard decision and Loom-labeled setup/trial containers before doing
any targeted manual cleanup.

For the staging worker-pool autoscaler, use the bounded resource-aware policy
in `deploy/environment-state/staging.toml`: the four fixed workers provide 24
slots, while one dynamic job on OLDLAB-5 may raise the pool to 25 slots. The
dynamic lease requests 2 CPU and 8192 MiB with one worker slot, keeps 4 CPU and
20480 MiB reserved for the shared-node services, and permits at most one active
or pending autoscaler job. Do not expand the allowed-node set or slot ceiling
without a new shared-node acceptance result.

The corresponding staging `actuator_config` uses:

```json
{
  "resource_aware": true,
  "cpu_per_slot": 2,
  "memory_mib_per_slot": 8192,
  "reserved_cpus": 4,
  "reserved_memory_mib": 20480,
  "max_concurrency_per_node": 8,
  "max_cpu_load_ratio": 1.0,
  "requested_cpus": 2,
  "requested_memory_mib": 8192,
  "requested_concurrency": 1,
  "max_jobs": 1,
  "pending_job_cap": 1
}
```

The autoscaler queries `sinfo` before scale-up, excludes nodes that already
have an active Loom Slurm job, unsafe Slurm state, missing resource data, high
CPU load, low free memory, or low idle CPU, and then submits each worker with
that node's computed safe slot count. In staging, only OLDLAB-5 is eligible and
the pool tops out at 25 slots. When OLDLAB-5 is already occupied or lacks the
reserved headroom, the expected behavior is to keep the 24 fixed slots and
record the exclusion reason instead of forcing a Slurm job.

For staging release gates, supervise OLDLAB through the
`external_slurm_autoscaler_supervisors` section in
`deploy/environment-state/staging.toml`. `loom admin environment-state
apply` writes the user systemd service/timer on the Slurm submit host and
`check` fails if the timer is inactive, the unit points at a stale rollout
checkout, or the ExecStart command omits the exact environment and pool
bindings. Do not leave a free-floating ops script as the timer target; use the
repo entrypoint `scripts/ops/worker_pool_autoscaler_external_once.py
--environment <environment> --pool-name oldlab`.

The remote-worker env file, Loom checkout, and job-output directory must be
readable and writable as appropriate from every included Slurm node. Staging
uses the service-owned paths under `/shared_work/loom/staging-rollout/`; do not
use a personal checkout under `/shared_work*/qianyi/` or a control-node-local
home directory. Keep `LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS` in the remote-worker
env file so elastic jobs release the allocation after the Loom queue drains.
The generated Slurm job script also traps `EXIT`, `INT`, and `TERM` and runs
`docker compose down --remove-orphans`, so `scancel` and idle exits clean up the
worker container instead of leaving a live worker outside Slurm accounting.

Shared-resource smoke evidence from 2026-06-24 is stored on platform-dev at
`/shared_work/qianyi/loom-worker-capacity/issue435-20260624T164151Z/`. It used
reduced shared Slurm slices because OLDLAB 4/5 were partially occupied, proving
worker registration, heartbeat, claim, gateway call, result writeback, and
trajectory index writeback on the BFCL smoke batch. During later opportunistic
#426 backlog processing, one OLDLAB5 Hendrycks Math trial failed artifact upload
while the MinIO tunnel restarted; keep that separate from the BFCL capacity
smoke and track the in-flight retry/drain gap with the existing rollout-failure
work. Full production capacity still uses the 12 CPU / 58000M plan above.

## External autoscaler supervisor (systemd)

Each environment's env-state profile carries an
`external_slurm_autoscaler_supervisors` section. Every entry renders a
`systemctl --user` service plus timer that periodically runs the repo
entrypoint `scripts/ops/worker_pool_autoscaler_external_once.py` for one pool.
`loom admin environment-state apply` writes the unit files under
`~/.config/systemd/user`, and `check` reports drift when a unit is missing,
points at a stale checkout, omits the exact `--environment` or `--pool-name`
binding, or is not enabled/active as declared.

Each supervisor tunnels to the environment's Postgres on a reserved local port,
so no two supervisors on one host collide. The `--db-local-port` scheme is:

| pool   | development | staging | production |
| ------ | ----------- | ------- | ---------- |
| oldlab | 15447       | 15448   | 15449      |
| gb10   | 15450       | 15451   | 15452      |

Protected release rehearsals use the corresponding isolated range
`25447`–`25452` (live port plus `10000`). This prevents the validate-only
database tunnel from colliding with a live 30-second supervisor run.

Supporting layout, shared across environments:

- Runner checkout and virtualenv: `/opt/loom-<environment>-runner/repo` and
  `/opt/loom-<environment>-runner/venv`.
- Kubeconfig: the environment runner's least-privilege kubeconfig. Staging uses
  `/var/lib/loom-staging-rollout/kubeconfig`.
- Health check: `systemctl --user is-active loom-autoscaler-oldlab-<env>.timer`.

The staging OLDLAB supervisor ships `enabled=true` and `active=true` after the
bounded OLDLAB-5 launch, registration, cgroup-containment, targeted drain, and
Slurm cancellation acceptance. Development remains fail-closed, and there is
no committed production OLDLAB activation.

To temporarily exclude a node, remove it from both `worker-plan.csv` and
`LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES`, or lower
`LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS` below the node count. To disable the
elastic pool entirely, disable the worker-pool autoscaler policy and set
`LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=false` for older controller-only
deployments.
