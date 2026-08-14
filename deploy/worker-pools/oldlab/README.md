# OLDLAB elastic worker pool

The staging OLDLAB pool is x86_64 Docker capacity allocated through Slurm. Its
authoritative policy is the `oldlab` worker-pool entry in
`deploy/environment-state/staging.toml`; the external autoscaler supervisor on
`TRT-EAI-OLDLAB-1` is the capacity actuator.

## Current staging policy

The policy scales from zero to 18 slots on `trt-eai-oldlab-3`,
`trt-eai-oldlab-4`, and `trt-eai-oldlab-5`. Each non-exclusive job requests
12 CPUs, 49152 MiB, and concurrency 6. Trial containers are capped at 2 CPUs,
4096 MiB, and 512 PIDs, with a job-level ceiling of 4096 PIDs.

Resource-aware admission reserves 4 CPUs and 20480 MiB per node for other
services, permits at most one pending autoscaler job, and excludes nodes with
unsafe Slurm state, an active Loom job, missing resource data, high load, or
insufficient free CPU/memory. While sizing an inactive, otherwise-safe node,
the OLDLAB policy uses a tiny immediate Slurm probe of Linux `MemAvailable`,
which includes safely reclaimable page cache. A probe that cannot run or
returns malformed data falls back to `FreeMem` and remains closed. The worker
setup guard also delays Docker setup and build work when I/O pressure, swap, or
D-state process thresholds are unsafe.

Autoscaled jobs use the dedicated `loom-staging` Slurm partition on those same
three nodes. The partition has `PriorityTier=100`, is limited to the
`loom-rollout` group, and shares already-running allocations without
preemption. It gives new on-demand Loom work precedence over future shared
partition reservations only while Loom demand exists; the zero-slot minimum
and five-minute idle drain bound that use. The controller-root
`deploy/slurm/converge-loom-oldlab-slurm-partition.sh` transition is bounded.
It never cancels or preempts foreign jobs and fails closed on configuration
drift.

## Disabled Pipeline GPU admission surface

Issue #1213 adds a separate repository-owned Pipeline contract in
`worker-plan.csv` and `slurm/`. It freezes `trt-eai-oldlab-1` through
`trt-eai-oldlab-5`, one exclusive `gpu:rtx5080:2` job and one concurrency-1
`behavior-gpu-oldlab` worker per allocation. It merges disabled with desired
slots zero and does not change the live legacy Trial policy above. Installation
and activation remain a later candidate-bound rollout action.

The service-owned candidate checkout, worker env, and job output directories
are under `/shared_work/loom/staging-rollout/` and must be visible on every
allowed compute node. Personal checkouts and control-host-only paths are not
valid worker inputs.

The candidate-owned worker env pins the Control Plane, Gateway, subprocess
Gateway, and MinIO to OLDLAB-1's `192.168.50.103` private fleet forwards. It
uses the TLS trial-cache repository
`192.168.50.103:5443/loom-trial-cache`; per-node loopback tunnels and personal
network services are not valid substitutes.

## Supervisor

Environment-state renders and owns:

```text
loom-autoscaler-oldlab-staging.service
loom-autoscaler-oldlab-staging.timer
```

The timer runs
`scripts/ops/worker_pool_autoscaler_external_once.py --environment staging
--pool-name oldlab`, using local database tunnel port `15448`. Drift checks
require the declared exact-candidate paths, environment/pool binding,
enabled/active unit state, and Slurm cluster identity.

Inspect without mutation:

```bash
loom admin worker-pools autoscaler status
loom resources status --format json
loom worker setup status
```

Shared-staging changes are applied by the installed rollout authority. Do not
submit parallel manual workers, edit desired-state rows directly, or point a
timer at a free-floating checkout.

## Allocation lifecycle

Each submitted job registers its Slurm allocation and worker identity, starts
the candidate-bound remote-worker Compose process inside the allocation's
cgroup, and exits after the configured idle window when no trial is in flight.
The job traps normal exit and termination to run Compose cleanup, preventing a
worker from surviving outside Slurm accounting.

Non-exclusive containment requires positive container caps and a matching
allocation-owned cgroup parent. Missing allocation, cgroup, sandbox, candidate,
or GPU identity fails closed.

## Maintenance

When setup pressure rises, stop new work and use `loom worker setup status` to
correlate guard state with Loom-labeled setup and trial containers. Reduce
concurrency before any cleanup. Drain or prove a job idle before canceling it;
preserve its worker and trial evidence.

Do not expand allowed nodes, slot ceilings, container limits, or build
concurrency outside the environment-state review and acceptance path. Never
delete shared worker cache volumes as part of a normal restart.
