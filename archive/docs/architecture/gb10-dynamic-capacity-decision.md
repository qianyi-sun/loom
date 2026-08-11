# GB10 Dynamic Capacity and Membership Contract

> Archived on 2026-08-11. This version included decision and supersession
> history. See the current
> [`gb10-dynamic-capacity.md`](../../../docs/architecture/gb10-dynamic-capacity.md).

Status: accepted design contract

Decision date: 2026-07-31

Tracking: [#822](https://github.com/qianyi-sun/loom/issues/822)

## Membership

The GB10 worker inventory is always the exact set `trt-gb10-1..15`. Every
machine remains in the worker-health model and reports a heartbeat plus bounded
resource observations. Resource contention does not change inventory
membership.

The former active-14/140-slot policy and static `trt-gb10-7` exclusion are
superseded. The checked-in inventory, worker-pool configuration, trust
topology, readiness gates, tests, and runbooks now use the complete 15-host
membership. Historical runtime evidence that expresses the former policy is
dated evidence only, not the target contract. #822 continues to own dynamic
resource observation and placement acceptance across that fixed membership.

## Health and capacity are separate

Each inventory node has independent health and capacity state:

- **healthy with capacity**: eligible for compatible Loom placement;
- **healthy but busy**: remains heartbeat-managed and visible, advertises zero
  or reduced free CPU, memory, PID, GPU/TRES, and trial slots, and receives no
  new Loom work until resources are available;
- **temporarily unhealthy**: network, heartbeat, service, or resource-
  observation failure blocks placement while the node remains visible in the
  inventory and self-heal state;
- **recovered**: fresh health and resource observations restore placement
  eligibility automatically without an inventory edit or re-admission change.

The physical maximum may be 150 trial slots when all 15 nodes each safely
provide 10 slots. This is not a guaranteed or fixed allocatable value. Current
capacity is derived from fresh resource observations and may be lower without
changing membership.

## Scheduling

When a node cannot satisfy a job's requested resources, the scheduler keeps the
job pending or places it on another compatible node with sufficient capacity.
It must not mark the busy node stopped, remove it from `allowed_nodes`, or
require operator re-admission after resources are released.

No Loom controller, rollout, acceptance test, or recovery path may cancel,
preempt, stop, or kill a foreign job to make room. A disruptive Loom-owned
convergence step waits for a safe window while heartbeat and non-invasive
health/resource observation continue.

## Evidence and release gates

Candidate-bound evidence must:

1. expect exactly `trt-gb10-1..15` with `excluded_nodes=[]`;
2. record heartbeat and health state for all 15 nodes;
3. report theoretical maximum, currently allocatable, busy, unhealthy, and
   pending capacity separately;
4. prove a healthy busy node receives no new Loom placement and another
   eligible node can receive the work;
5. prove resource release restores eligibility within the reviewed reconcile
   bound;
6. prove a health failure remains visible and recovers through the supported
   self-heal path.

Historical 14-host rollout artifacts remain dated evidence of those runs. They
must be labelled historical and cannot define current topology or acceptance.

## Implementation boundary

This document records the target contract only. It does not authorize live
Slurm, node-agent, systemd, environment-state, rollout, or staging mutation.
Code, configuration, tests, and live activation converge separately through
#822 and their existing authority gates.
