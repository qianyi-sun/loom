# Task-image builder Phase 1 isolation correction

**Status:** Approved design; Phase 1 prerequisite implementation and inert
operator runbook published; live convergence pending externally provisioned
storage and GB10 administrative authority

**Date:** 2026-08-19

**Target branch:** `dev`

## Decision

Loom will retain the selected allocation-scoped rootless BuildKit architecture,
but the Phase 1 prerequisite implementation merged by PR #1457 must be
corrected before either cluster is changed.

The correction separates the new rootless builder from every legacy Slurm
object, defines one exact submission principal, verifies Slurm aliases against
the physical host, and moves site-wide host changes into a journaled,
rollback-aware per-node convergence boundary. OLDLAB will converge first; GB10
will follow
only after administrative access exists and every inventory node, including
`trt-gb10-7`, is reachable.

Phase 1 remains inert throughout:

- `production_certification_allowed = false`;
- `certified_nodes = []`;
- `phase2_guard_provider_release_missing` remains unconditional;
- no rootless builder policy or supervisor is enabled;
- no builder node feature is advertised; and
- task `4139e767` is not rerun.

The operational sequence and its current stop conditions are published in the
[Phase 1 site-convergence runbook](../../../docs/runbooks/task-image-builder-phase1-site-convergence.md).
The runbook documents check/plan-first evidence collection only; publication
does not constitute a live controller/node check or authorization to apply.

## Why PR #1457 must not be applied

### Legacy QoS collision

The legacy exclusive backend owns QoS `loom-task-image-builder` with a
four-hour wall limit. PR #1457 assigns that same name to the rootless backend,
treats the legacy definition as a migration pre-state, and modifies it into a
two-hour, aggregate-TRES-limited QoS.

That would make the legacy converger reject its own QoS and would violate the
explicit requirement to retain legacy capacity and reservations as rollback
infrastructure. The existing parser failure prevented this mutation by
accident; correcting only the parser is unsafe.

### Byte-oriented Slurm readback

Slurm 23.11.4 emits an empty final `sacctmgr --parsable2` field without the
second terminal delimiter assumed by the fake-Slurm test. Exact whole-line
comparison therefore classifies the reviewed live QoS as unknown drift.

Readback must be semantic: parse one exact row into the requested fields,
normalize documented Slurm representations such as wall time and TRES memory,
and compare the resulting typed values. Missing fields, extra rows, unknown
flags, duplicate TRES keys, or extra limits remain fatal.

### Missing controller submission identity

The rootless partition admits Unix group `loom-task-builder`, while its Slurm
association names user `loom-builder`. The merged node installer creates that
identity only on eligible compute nodes. The controller therefore cannot run
the future provider as the intended principal or satisfy the partition group
gate.

### Partial host convergence

The merged node installer creates the identity and runtime before checking
UID-map helpers, quota storage, and global Slurm cgroup settings. It does not
provision those dependencies. On the observed hosts, `apply` would leave
partial state and then fail.

### Slurm-name and host-name mismatch

GB10 policy uses Slurm aliases such as `trt-gb10-1`, while the machines report
physical names in the `gx10-*` namespace. Direct comparison with
`hostname -s` cannot establish node identity.

## Invariants

The correction is governed by the following non-negotiable invariants.

1. The legacy QoS, account association, fixed nodes, named reservations,
   backend configuration, and supervisor state are never created, modified,
   or deleted by the rootless Phase 1 workflow.
2. Rootless Slurm objects have names distinct from all legacy objects and are
   additive from an absent pre-state. Existing non-exact rootless objects are
   rejected rather than modified.
3. Only `loom-builder` submits rootless builder jobs. `loom-rollout`, trial
   workers, ordinary users, and Docker-group members cannot use the rootless
   builder partition or QoS.
4. No Docker socket or privileged container engine is used to install,
   validate, or execute the rootless builder.
5. A host is never reported prepared unless every required prerequisite is
   present in readback. A prepared host is not a certified host.
6. Fleet convergence never requires all nodes to be idle simultaneously and
   never cancels an existing trial job.
7. A node that cannot be safely restored remains drained with an explicit
   operator reason; automation never resumes it after incomplete rollback.
8. Phase 1 completion requires exact positive evidence from both complete
   clusters. Partial OLDLAB or GB10 progress cannot remove the Phase 2 blocker.

## Corrected Slurm contract

### Names and limits

The rootless backend keeps account `loom-task-builder`, partition
`loom-task-builder`, Unix user `loom-builder`, and Unix group
`loom-task-builder`. It receives cluster-specific QoS names:

| Cluster | Rootless QoS |
| --- | --- |
| OLDLAB | `loom-task-image-builder-rootless-oldlab` |
| GB10 | `loom-task-image-builder-rootless-gb10` |

Cluster-specific names prevent accidental coupling through a shared Slurm
accounting database and make every operational command unambiguous. The legacy
name `loom-task-image-builder` is forbidden in the rootless policy.

Each rootless QoS has exactly:

- `Flags=DenyOnLimit`;
- `Priority=0`;
- `MaxJobsPU=1`;
- `MaxSubmitJobsPU=1`;
- `MaxWall=02:00:00`; and
- `GrpTRES=cpu=8,mem=32768M,node=1`.

The overlapping rootless partition retains `PriorityTier=200`, while the trial
partition remains at tier 100. It has `OverSubscribe=NO`,
`AllowAccounts=loom-task-builder`, and `AllowGroups=loom-task-builder`.

Phase 1 submits no builder jobs. The Phase 2 provider grammar must request the
rootless partition, cluster-specific QoS, account, CPU, memory, and wall time
without `--exclusive`, a reservation, or a fixed `--nodelist`. Slurm chooses an
eligible prepared node dynamically. A rootless capability constraint may be
added only after Phase 2 owns and verifies the corresponding node feature.

The higher tier prevents continuing trial arrivals from indefinitely overtaking
a queued builder. It does not preempt running trials and does not promise an
immediate build start. Backfill may start trial work only when it cannot delay
the higher-tier builder. Phase 2 acceptance must prove this behavior on both
controllers using real pending jobs and scheduler readback.

### Legacy immutability guard

Before and after any rootless Slurm apply, the converger reads and semantically
fingerprints the legacy QoS, legacy association, and named reservation. A
mismatch aborts the operation. The rootless converger contains no `modify` or
`delete` command targeting a legacy name.

For new rootless accounting objects, apply follows an absent-or-exact rule:

- absent: add the exact object;
- exact: make no change; or
- present but different: fail before mutation.

The partition follows the same absent-or-exact rule and retains the existing
durable configuration backup/reconfigure/readback behavior. Partially created
new objects are inert because neither activation nor certification exists; a
state receipt records which additive objects were created for later guarded
cleanup.

### Semantic readback

The parser requests explicit columns and accepts exactly zero or one data row,
depending on whether absence is valid. It accepts exactly seven semantic QoS
fields: either eight split parts with one trailing `parsable2` sentinel removed,
or the observed Slurm 23.11.4 seven-part form whose final semantic field is
empty. Every other field count is rejected. It compares typed fields rather
than raw output bytes. TRES is parsed as an unordered map with exact keys and
normalized memory units.

Regression fixtures include the exact OLDLAB Slurm 23.11.4 output, both legal
terminal-delimiter forms, reordered TRES keys, normalized memory units,
duplicate/extra rows, missing columns, extra flags, and unknown limits.

## Submission identity contract

The controller and every eligible compute node have the same fixed primary
identity: user `loom-builder` UID 993 and group `loom-task-builder` GID 980.
The account has a non-login shell and no membership in `docker`, `sudo`,
`root`, or any other supplementary group.

Only compute nodes receive subordinate UID/GID ranges and the rootless runtime.
The controller identity exists solely so the Phase 2 provider can run as
`User=loom-builder`, call unprivileged Slurm client commands, and satisfy the
Slurm user/account/QoS/partition and Unix-group gates. It receives no Slurm
administrative authority. The provider service, credentials, and activation
remain Phase 2 work.

Controller and compute-node identity checks occur before Slurm accounting
objects are applied. Numeric UID/GID conflicts, unexpected supplementary
groups, or an inconsistent existing identity are fatal.

## Node identity verification

The operator supplies the expected Slurm node name to the node converger. That
name must belong to the policy inventory. The converger then reads the exact
node from `scontrol show node` and verifies:

- the returned `NodeName` is the policy alias;
- its `NodeAddr` resolves to at least one non-loopback address assigned to the
  local machine;
- `NodeHostName`, when configured, matches a local host name or canonical name
  case-insensitively; and
- the local architecture matches the cluster policy.

This establishes the Slurm-alias-to-machine binding without assuming that a
physical host name equals its Slurm name. Ambiguous addresses, duplicate policy
aliases, controller unavailability, or an unresolvable mapping fail closed.

## Site-specific host convergence

Host convergence is a separate reviewed boundary from the Slurm isolation
repair. It covers the controller identity and, on compute nodes:

- the dedicated identity and subordinate ID ranges;
- signed, pinned `newuidmap` and `newgidmap` packages;
- the pinned BuildKit, RootlessKit, `slirp4netns`, and `fuse-overlayfs`
  release and its complete shared-library dependency closure;
- cgroup v2 controller availability and the reviewed Slurm cgroup settings;
- root-only bpffs prerequisites; and
- an ext4 or XFS project-quota-backed builder storage mount.

No host apply begins until all packages and runtime artifacts are locally
staged, signatures and digests validate, UID/GID and subordinate ranges are
conflict-free, an exact configuration backup can be created, and the storage
plan has been selected from observed disk and filesystem inventory.

Bundle verification produces an owner-private snapshot from descriptor-pinned
regular inputs after rejecting symlinks, special files, writable inputs, and
source changes. Host apply consumes only that verified snapshot and removes it
on every terminal path; it never reopens caller-controlled bundle paths after
verification.

The Phase 1 smoke owns only the controls Slurm can prove at this boundary: the
cardinality of `cpuset.cpus.effective`, exact `memory.max`, exact
`memory.swap.max`, and an attached cgroup-device BPF program. Exact CPU
bandwidth (`cpu.max`) and PID (`pids.max`) enforcement belong to the Phase 2
node guard and are not claimed by Phase 1 evidence.

The project-quota jobs root is exactly `/var/lib/loom-task-builder/jobs`, owned
by UID 993/GID 980, mode `0700`, project ID `300993` with inheritance, and empty
after smoke cleanup. Evidence re-reads that metadata, `lsattr`, and `repquota`;
it does not substitute an earlier host receipt for the fresh observation.

All producers, collectors, and verification inputs are bound through
`authority-components-v1.json`, which names every authority-bearing component
and its digest. Kernel and storage claims retain bounded raw syscall,
command, file, mount, quota, metadata, and cleanup observations so semantic
verification never trusts policy-derived booleans alone.

The converger never automatically repartitions a disk, resizes a filesystem,
enables quota on an unrelated root filesystem, or creates a loopback
filesystem. If no suitable existing or explicitly provisioned filesystem is
available, that node remains blocked.

Slurm cgroup changes are treated as site-wide workload changes, not builder-only
settings. Inventory must first determine whether `cgroup.conf` and its activation
are node-local or shared. Node-local configuration requires an idle-node canary,
exact configuration backup, `slurmd` restart or reconfigure as required by the
installed Slurm version, ordinary trial smoke validation, and readback before
the next node proceeds. If shared configuration cannot be activated on one
canary without changing other nodes, convergence stops and requires a separate
reviewed cluster-maintenance procedure; the per-node orchestrator must not
pretend that the change is isolated.

### Per-node maintenance protocol

For each node, the controller-side orchestrator performs this sequence:

1. Record the node's initial Slurm state and reason.
2. If another operator already owns a drain, stop without changing it.
3. Drain the node with a versioned Loom maintenance reason; do not cancel jobs.
4. Wait by condition until Slurm reports no running or completing jobs and no
   allocated resources on that node.
5. Run immutable preflight and produce a proposed-change record.
6. Apply identity, packages, runtime, storage, and configuration using
   root-owned backups and a state receipt.
7. Restart/reconfigure only the services required by the proposed change.
8. Run node-local readback, Slurm readback, and a contained smoke allocation.
9. Resume the node only if every check passes and Loom owns the drain.
10. Aggregate the node evidence into the cluster report.

On failure, the converger restores mutable configuration and mount state from
the receipt and revalidates the restoration. The inert dedicated identity and
verified runtime may remain installed because deleting a UID or files bearing
that UID is less safe than leaving an unprivileged, non-login capability
inactive. A rollback failure leaves the node drained.

Nodes converge one at a time. The fleet summary may show partial preparation,
but a cluster passes only when every policy node passes the same policy and
release digest.

## Delivery sequence

### PR A: correctness and isolation repair

PR A changes the dormant policy, administrator-invoked scripts, and tests.
Neither CI nor merge performs a live mutation. The corrected apply path exists
for the later authorized rollout, but operators run check mode only until PR B
has merged and host prerequisites are staged. PR A:

- assigns the two cluster-specific rootless QoS names;
- rejects any rootless reference to the legacy QoS or reservation names;
- replaces byte-oriented Slurm accounting comparisons with semantic parsing;
- adds legacy pre/post immutability checks;
- defines the controller submission identity readiness contract;
- replaces direct host-name equality with Slurm alias/address verification;
- adds live-output regression fixtures and negative collision tests; and
- keeps certification and activation unconditionally closed.

PR A is merged through the protected `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate` contexts. After merge, only
read-only checks run; they are expected to remain negative on missing host
prerequisites.

### PR B: site-specific prerequisite convergence

PR B is based on fresh controller and node storage/package/configuration
inventory. It adds the controller identity installer, offline signed package
supply, a journaled fail-closed host converger, per-node maintenance
orchestrator, rollback receipts, and canonical evidence collection. It contains
no builder provider, node guard, activation, or rerun.

PR B follows the same protected PR and squash-merge process. Repository changes
are made only in a worktree, and `docs/superpowers/**` is excluded.

The delivered [inert operator runbook](../../../docs/runbooks/task-image-builder-phase1-site-convergence.md)
binds this repository boundary to the future authorized sequence: verified
artifact staging, controller identity/readback, additive Slurm convergence,
OLDLAB-first one-node maintenance, rollback receipt inspection, complete
two-cluster evidence assembly, and the still-closed Phase 2 boundary.  It does
not record a fresh live check or authorize an apply.

### Operational rollout

After both PRs merge:

1. Stage verified artifacts without enabling services.
2. Converge the OLDLAB controller identity.
3. Converge OLDLAB compute nodes one at a time and collect cluster evidence.
4. Keep Phase 1 incomplete even after OLDLAB passes.
5. Obtain command-scoped GB10 root authority and restore reachability of
   `trt-gb10-7`.
6. Converge the GB10 controller and nodes one at a time.
7. Produce one canonical two-cluster Phase 1 evidence envelope.
8. Confirm certification remains false and the Phase 2 blocker remains.

Docker-group membership is never used as a substitute for administrative
authority. The legacy backend, capacity, and reservations remain unchanged.

## Verification and acceptance

PR A must prove:

- real Slurm 23.11.4 QoS output parses correctly;
- both rootless QoS definitions are exact and distinct from legacy;
- a legacy collision fails before any mutation;
- legacy fingerprints are unchanged across a successful fake apply;
- unknown existing new-object drift fails before mutation;
- controller identity absence fails readiness;
- physical/Slurm alias binding accepts a verified mapping and rejects ambiguous
  or foreign hosts;
- the future submission grammar contains no `--exclusive`, reservation, or
  fixed node selection;
- check mode performs no mutation; and
- every result still reports zero certified nodes and the Phase 2 blocker.

PR B and operational acceptance must prove, per node and then per cluster:

- exact identity, subordinate ranges, package signatures, and runtime digests;
- root-owned UID-map helpers with the exact reviewed setuid mode, no writable
  group/world bits, and no unexpected file capabilities;
- a complete, pinned runtime dependency closure with no host-Docker dependency;
- no privileged supplementary group or forbidden container socket;
- required cgroup v2 controllers and Slurm confinement settings;
- quota-backed storage and bounded cleanup behavior;
- drain ownership, condition-based idle waiting, rollback, and safe resume;
- an administrator-run `sbatch --test-only` as `loom-builder` accepts only the
  exact account/QoS/partition request, while the equivalent `loom-rollout`
  request is rejected;
- a credential-free smoke allocation verifies Slurm admission and cgroup
  placement, cpuset cardinality, memory/swap limits, and device containment
  without executing BuildKit or untrusted build input;
- CPU-bandwidth and PID-limit enforcement remain explicitly blocked on the
  Phase 2 node guard rather than being inferred from Phase 1 Slurm state;
- the authority-component manifest and bounded raw kernel/storage observations
  match the candidate, receipts, and derived evidence;
- the jobs root is UID 993/GID 980 mode `0700`, project-inheriting as 300993,
  and freshly observed empty after the receipt-bound cleanup command;
- no surviving build process, mount, credential, or writable cache after the
  contained smoke allocation; and
- ordinary trials remain schedulable after node restoration.

Phase 1 evidence is valid only when the exact OLDLAB and GB10 inventories pass.
It does not authorize production. Phase 2 must still deliver and accept the
node guard, build-environment provider, credential projection, network policy,
publication/retention path, and allocation-contained BuildKit execution before
any rootless builder activation or task rerun.

## Alternatives rejected

### Fix only the Slurm delimiter parser

Rejected because it would expose the unsafe mutation of the legacy QoS.

### Keep permanent exclusive builder nodes

Retained only as transitional rollback. It consumes permanently reserved
capacity and the host Docker daemon creates build processes outside the Slurm
allocation cgroup.

### Converge every node in one maintenance window

Rejected because it unnecessarily requires simultaneous fleet idleness and
increases blast radius. Per-node maintenance provides the same eventual fleet
state with observable rollback boundaries.

## Resulting safety posture

The corrected Phase 1 creates no production builder capability. It establishes
an auditable, non-colliding Slurm and host prerequisite foundation while
preserving the legacy rollback path. Dynamic rootless builders will share
eligible trial nodes only after Phase 2 proves that all build processes remain
beneath the Slurm allocation and that node-local containment, cleanup,
credentials, publication, and retention fail closed.
