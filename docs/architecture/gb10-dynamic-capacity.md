# GB10 Capacity and Membership

The GB10 inventory is the exact set `trt-gb10-1` through `trt-gb10-15`.
Inventory membership is stable; health, workload safety, and allocatable
resources vary independently.

Each node heartbeat reports bounded health and resource observations. A
healthy node may advertise zero capacity while foreign or Loom work consumes
its resources. A stale or unhealthy node is ineligible for new work but
remains visible in fleet status and release evidence.

The scheduler uses currently allocatable CPU, memory, GPU, and slot values
rather than assuming a fixed slot count per host. Explicit architecture or
pool requirements remain hard eligibility constraints. Resource-neutral work
can be placed only where the current worker and scheduler contracts report a
compatible shape.

Protected rollouts, environment-state checks, trust preparation, node-agent
convergence, and release gates all use the same 15-host inventory. Missing
hosts, duplicate identities, stale heartbeats, invalid observations, or
candidate/generation drift fail the relevant readiness check; they are not
hidden by shrinking the inventory.

Node-agent and worker status are separate. A host can have a fresh node-agent
heartbeat but no active worker registration, or an active worker whose
reported capacity is zero. Release checks that require runnable capacity must
verify both layers.

The checked-in inventory and pool configuration under `deploy/worker-pools/`
are the operator authority for host identities and transport. Current runtime
health and capacity come from service observations, not from archived rollout
reports.
