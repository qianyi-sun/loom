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
  elastic Slurm controller from #432. For #45 and later, use the worker-pool
  autoscaler policy as the normal desired-capacity control surface; the same
  Slurm node, checkout, env-file, CPU, memory, concurrency, max-job, and
  pending-cap settings map into the policy `actuator_config`. Slurm submissions
  are exclusive by default; set `actuator_config.exclusive=false` only for
  deliberately shared partial-node validation slices.

The initial capacity slice is intentionally conservative: one Slurm job per
node, `12 CPU`, `58000M`, and `LOOM_WORKER_MAX_CONCURRENT=6`. Raise concurrency
only after a separate load-test issue records CPU, RAM, Docker cleanup, MinIO,
Gateway/provider, and Control Plane state-patch health.

For the worker-pool autoscaler, prefer the resource-aware policy rather than
raising the fixed slice. Use `min_slots=1`, `max_slots=40`, `max_jobs=5`, and
`pending_job_cap=2` for OLDLAB-1..5. In `actuator_config`, set:

```json
{
  "resource_aware": true,
  "cpu_per_slot": 2,
  "memory_mib_per_slot": 8192,
  "reserved_cpus": 4,
  "reserved_memory_mib": 24576,
  "max_concurrency_per_node": 8,
  "max_cpu_load_ratio": 1.0,
  "requested_cpus": 2,
  "requested_memory_mib": 8192,
  "requested_concurrency": 1
}
```

The autoscaler queries `sinfo` before scale-up, excludes nodes that already
have an active Loom Slurm job, unsafe Slurm state, missing resource data, high
CPU load, low free memory, or low idle CPU, and then submits each worker with
that node's computed safe slot count. With five safe nodes and
`max_concurrency_per_node=8`, OLDLAB tops out at 40 slots. When shared OLDLAB
load is already near the CPU count or free memory is low, the expected behavior
is to keep the warm minimum and record the exclusion reason instead of forcing
more Slurm jobs.

The remote-worker env file and Loom checkout path must be readable from every
included Slurm node. For OLDLAB 4/5, do not use a control-node-local checkout
such as `/home/qianyi/dev/loom` unless a Slurm job on that node has verified it
contains `deploy/docker-compose.remote-worker.yml`. Use a shared checkout path
such as `/shared_work/<operator>/loom-remote-worker` for public-beta style
capacity. Keep `LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS` in the remote-worker env
file so elastic jobs release the allocation after the Loom queue drains.
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

To temporarily exclude a node, remove it from both `worker-plan.csv` and
`LOOM_CP_SLURM_WORKER_CONTROLLER_ALLOWED_NODES`, or lower
`LOOM_CP_SLURM_WORKER_CONTROLLER_MAX_JOBS` below the node count. To disable the
elastic pool entirely, disable the worker-pool autoscaler policy and set
`LOOM_CP_SLURM_WORKER_CONTROLLER_ENABLED=false` for older controller-only
deployments.
