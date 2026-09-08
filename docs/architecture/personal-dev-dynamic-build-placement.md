# Dynamically placed personal-development builds

Status: implementation in progress; no production activation.

## Decision and scope

Personal ARM64 builds use certified, dynamically selected GB10 compute nodes,
not a service permanently pinned to the controller. Preserve the existing
personal source intake, owner identity, candidate/attempt leases, artifact
verification, application deployment, and OLDLAB native AMD64 path. The global
capacity manager remains the only Loom allocation authority; pool-local Slurm
execution performs placement within its allowance. Do not add an independent
personal-build autoscaler or send personal candidates through task materialization.

Initial policy permits a subset of `trt-gb10-3` through `trt-gb10-15`. Exclude
controller node 1 and reserved task-builder node 2. This is an operator eligibility
boundary, not a preferred-host list or a promise that every member is ready.

Alternatives rejected: resizing node 1 to satisfy an unmeasured disk constant;
replacing its fixed hostname with node 2; sharing the task worker's credentials
or daemon; starting a host-level service beside an exclusive Slurm allocation.

## Evidence

Read-only observations on 2026-09-08 found all nodes 3–15 reachable, aarch64,
with KVM, cgroup v2, Docker 29.2.1, and 2.43–3.54 TB free on `/var/lib`'s backing
filesystem. Slurm showed nodes 7, 9, 10 and 13 idle and other nodes mixed.
These are feasibility observations, not runtime certifications or reservations.
Existing personal runtime pins Docker 28.3.3 and a controller-level cgroup;
it cannot be retargeted without a successor runtime and allocation proof.

## Trust boundaries

1. The management service owns the candidate, owner, immutable source and attempt
   lease. The global manager grants bounded CPU/memory/build concurrency; no
   builder chooses its own subject, pool weights, or priority.
2. Slurm owns the physical node and outer cgroup. Submit held, bind the exact job,
   then release only under the current global execution fence. An ambiguous
   submission is reconciled, never blindly repeated.
3. The node runtime authenticates a manager-approved allocation, boot identity,
   attempt and runtime profile. Every BuildKit/client/helper process must descend
   from that job's cgroup. A host-level `cgroup_parent` is not sufficient.
4. Preserve separate KVM-gVisor build and capability-client sandboxes, private
   per-grant state, no host runtime socket in untrusted containers, and existing
   credential separation. A rootless shared-kernel task provider is not an
   equivalent isolation boundary.
5. Lease expiry, node reboot, job loss, stale proof or cancellation closes new
   work and publication. Owner cancellation affects only its exact attempt.

## First implementation slice: non-executable placement contract

Add a pure contract that is usable before any authority or infrastructure
mutation. Inputs are trusted policy and normalized observations from a future
authenticated observer. Parsing an observation does not authenticate it.

`NativeBuildPlacementPolicy` defines a nonempty, unique subset of compute nodes,
an exact runtime-profile digest, positive per-allocation CPU millicores/memory,
positive disk/inode headroom, and a positive maximum observation age. Resource
quantities are explicit: there is no replacement magic disk threshold.

`NativeBuildNodeObservation` binds node ID, nonzero boot UUID, timezone-aware
observation time, architecture, Slurm state/reservation flag, KVM availability,
available CPU/memory/disk/inodes, and optional certified runtime-profile digest.
Available CPU/memory must already be the conservative intersection of Slurm
remaining resources and host headroom; this layer must not infer availability
from total hardware. A matching digest means upstream certification was
verified for this node/boot, not merely that a config file exists.

`eligible_native_build_nodes(policy, observations, now=...)` returns sorted
eligible observations without picking a favorite host. It rejects malformed or
duplicate node observations. It excludes unlisted nodes, stale/future reports,
non-aarch64 nodes, reserved nodes, states other than IDLE/MIXED, absent KVM,
absent/mismatched runtime certification and insufficient resources. Age exactly
at the policy boundary is accepted; any older report is rejected. Resource
equality is sufficient. No IO, environment lookup, shell or scheduler call.

`NativeBuildAllocationBinding` records nonzero manager reservation, candidate,
attempt and owner UUIDs; positive attempt lease epoch; candidate digest; exact
runtime-profile digest; cluster `trt-gb10`; positive canonical decimal Slurm job
ID (no arrays/steps); compute-node identity and boot UUID. It is a strictly
validated, immutable identity record, NOT a launch permit. The later runtime
gate must compare it with authenticated manager authority and live Slurm/cgroup
evidence. It must never be accepted by the v1 production provider implicitly.

This slice adds no executable composition and does not claim scheduling works.
Its purpose is to make the node/boot/attempt/allocation boundary explicit for
the following integration work.

## Remaining integration and acceptance

1. Project candidate build demand into manager-owned capabilities and resource
   shapes. Count pending, unknown and running build allocations beside existing
   workloads. Keep per-owner fairness and two-owner concurrency; no pool weights.
   Do not register fake runnable personal applications before deployment to
   obtain build capacity. Extend the management build subject explicitly.
2. Add held-job submission and durable binding to the existing pool executor,
   using manager permits and reconciliation. Slurm may select any certified node
   in the manager allowance; reconcile the actual placement without double
   counting topology reservations. No permanent/exclusive reservation is created.
3. Install a successor allocation-contained runtime on selected nodes and attach
   authenticated, boot-bound conformance evidence. Preserve the original provider
   inert for rollback. Correct workspace/output peak accounting and check fresh
   disk/inode pressure before accepting work. Keep trusted-release scoped GC.
4. Wire the management executor and signed completion to the new allocation
   identity. Reject stale leases, cross-owner publication, boot drift, duplicate
   jobs and ambiguous cleanup; retain commitments until cleanup is proved.
5. After CI and review, enter an explicitly bounded BUILD-capacity acceptance
   window. The old Task 6 zero-Slurm window cannot prove this provider: do not
   relabel it or bypass a zero global execution ceiling. Task/trial execution
   stays disabled during the build-only acceptance window.
6. Prove concurrent owners using committed and dirty/untracked feature sources,
   independent updates/cancellation/retry, namespace/data isolation, teardown and
   redeploy, then build scale-to-zero. Preserve working staging services and
   the node-2 task-image reservation throughout.
7. Complete the full environment goal's separate real architecture-specific and
   neutral workload acceptance and global OLDLAB/GB10 scale-to-zero. The build
   provider's tests alone do not prove the full environment operational.

Self-review: the first slice only validates identity/eligibility; it grants no
authority, changes no existing pins, and cannot be mistaken for production
activation. Subsequent work must close all seven integration/acceptance items.
