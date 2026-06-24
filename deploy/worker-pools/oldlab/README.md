# OLDLAB Elastic Worker Pool

This directory records the staged OLDLAB 1-5 worker policy for public-beta and
production-like validation.

- `inventory-2026-06-24.txt` records the include decision and evidence notes for
  each node.
- `worker-plan.csv` is the Slurm launch plan consumed by
  `scripts/ops/worker_pool_slurm_submit.sh`.
- `dry-run-2026-06-24.txt` records generated `sbatch` commands for all included
  nodes using the shared checkout path validated during issue #435.
- `smoke-evidence-2026-06-24.json` records the public-beta OLDLAB 4/5 worker
  smoke batch id, worker ids, Slurm job ids, runtime, trial counts, and failure
  count.
- `controller.env.example` is the Control Plane override set for the native
  elastic Slurm controller from #432.

The initial capacity slice is intentionally conservative: one Slurm job per
node, `12 CPU`, `58000M`, and `LOOM_WORKER_MAX_CONCURRENT=6`. Raise concurrency
only after a separate load-test issue records CPU, RAM, Docker cleanup, MinIO,
Gateway/provider, and Control Plane state-patch health.

The remote-worker env file and Loom checkout path must be readable from every
included Slurm node. For OLDLAB 4/5, do not use a control-node-local checkout
such as `/home/qianyi/dev/loom` unless a Slurm job on that node has verified it
contains `deploy/docker-compose.remote-worker.yml`. Use a shared checkout path
such as `/shared_work/<operator>/loom-remote-worker` for public-beta style
capacity. Keep `LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS` in the remote-worker env
file so elastic jobs release the allocation after the Loom queue drains.

Shared-resource smoke evidence from 2026-06-24 is stored on platform-dev at
`/shared_work/qianyi/loom-worker-capacity/issue435-20260624T164151Z/`. It used
reduced shared Slurm slices because OLDLAB 4/5 were partially occupied, proving
worker registration, heartbeat, claim, gateway call, result writeback, and
trajectory index writeback on the BFCL smoke batch. During later opportunistic
#426 backlog processing, one OLDLAB5 Hendrycks Math trial failed artifact upload
while the MinIO tunnel restarted; keep that separate from the BFCL capacity
smoke and track the in-flight retry/drain gap with the existing rollout-failure
work. Full production capacity still uses the 12 CPU / 58000M plan above.

To temporarily exclude a node, remove it from both `worker-plan.csv` and
`LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES`, or lower
`LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS` below the node count. To disable the
elastic pool entirely, set `LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=false`.
