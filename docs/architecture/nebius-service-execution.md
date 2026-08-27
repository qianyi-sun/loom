# Nebius service execution contract

Status: accepted hybrid target architecture for issue #1548. The
provider-neutral durable control plane, namespace-scoped Kubernetes Job
adapter, read-only capacity collector, and evidence-gated resource forecast
are implemented but not
traffic-enabled. Infrastructure,
sandbox-runtime acceptance, live canaries, routing-policy changes, maintenance
drains, and any future pool retirement remain separately authorized work.

## Decision

Loom service mode has one provider-neutral admission and authority model over
three accepted concurrent pools: `nebius-cpu`, `oldlab`, and `gb10`. Nebius
uses fenced Kubernetes Job execution units; OLDLAB and GB10 retain the existing
worker-claim adapter. A trial declares `WorkloadRequirementsV1`; Loom records
one versioned routing decision, then exactly one adapter may obtain execution
authority for that attempt. Trials never select a provider, region, cluster,
worker name, or reusable slot directly.

The checked contracts live in
`src/loom/execution_contract.py`. Their generated JSON schemas and the complete
repo-known compatibility report live under `docs/evidence/`. Unknown fields,
unknown schema versions, implicit resource limits, mutable images, or a
capability that would be silently weakened fail admission.

This decision does not retire, disable, or subordinate OLDLAB/GB10, and does
not make configured slots, a registered worker, or Nebius quota equivalent to
fresh executable capacity.

## Authority topology

| State | Authority | Rule |
| --- | --- | --- |
| Task, batch, trial, immutable workload requirements | Loom/Postgres | Written before fan-out; user input cannot write derived admission fields. |
| Execution class, pool candidates, routing decision, desired lease, attempt generation | Loom/Postgres | Versioned selection and fencing authority; a selected adapter/pool is immutable once execution authority is issued. |
| OLDLAB/GB10 worker claims | Existing Loom scheduler and workers | May claim only a queued Trial routed to their exact pool; they do not interpret a Nebius lease. |
| Provider project, Kubernetes cluster, node group, Pod/Job, RuntimeClass | Nebius/Kubernetes | Observed resource state only; never the source of trial intent. |
| Reconciliation cursor, observation, condition, retry | Loom/Postgres | The reconciler records every comparison and action outcome. |
| Trial artifacts and immutable image identity | Object store/registry plus Postgres digest references | A mutable tag or local-only build is not executable service identity. |

The Nebius actuator is the only adapter allowed to translate a
`kubernetes_job` route and lease into Kubernetes operations. Existing worker
claim paths remain the adapters for `legacy_worker_claim` routes. API handlers
persist intent; they do not call Nebius or create Pods inline. The reconciler reads one fenced
desired generation, observes the provider and Kubernetes objects, performs an
idempotent action, and records the resulting observation. A stale generation,
unknown object owner, target-health failure, or ambiguous action outcome stops
progress and never broadens placement.

The identity chain is:

`task revision -> workload requirements digest -> routing generation and digest ->`
`selected pool/adapter -> lease or worker claim -> attempt generation ->`
`provider resource identity -> artifact digests`.

Kubernetes garbage collection may remove children owned by a task-scoped
execution object. It cannot decide that a Loom attempt succeeded, failed, or
may be retried; those transitions remain fenced Loom state.

## Provider-neutral contracts

`WorkloadRequirementsV1` is the complete material declaration for one
execution unit:

- OS and CPU architecture;
- GPU vendor/count;
- positive CPU, RAM, and ephemeral-storage limits;
- minimum sandbox isolation;
- network mode;
- immutable image/runtime identity or an explicit non-admissible build mode;
- sidecar count and verifier topology;
- custom DNS, extra hosts, and tmpfs;
- privileged, hostPath, host network, nested-container, device, and
  host-specialized requirements.

`ExecutionClassV1` describes portable capabilities. It deliberately has no
provider, region, target, worker, pool, or reusable-slot field. A valid service
class cannot permit privileged mode, hostPath, host networking, nested
containers, host devices, or a shared-kernel isolation boundary.

`ExecutionTargetV1` is the later binding point for provider, environment,
region, failure domain, residency, and a target-specific health check. The
checked `ExecutionTopologyV1` requires isolated development and staging
targets, at least two production regions and failure domains, primary and
secondary roles, and unique health checks.

Admission compares the immutable requirement record with the selected class
and returns all structured rejection codes. It runs before a batch is persisted
or fanned out. Conversion tooling may produce a new immutable workload
revision; admission itself never edits, defaults away, or weakens a submitted
requirement.

The legacy `TaskConfig` does not express privileged mode, hostPath, host
network, nested containers, or host devices. Its projection can only preserve
fields that exist in that schema. Dynamic operator and user bundles therefore
remain `conversion_required` until materialization emits and validates the
complete new contract; absence from the legacy schema is not evidence that a
capability is false.

## Logical pool and regional policy

The accepted service pool identities and adapter boundaries are:

| Pool | Adapter | Eligibility evidence |
| --- | --- | --- |
| `oldlab` | Existing worker claim | Exact capability match plus a fresh compatible worker observation, or bounded configured autoscaler headroom explicitly recorded as such. |
| `gb10` | Existing worker claim | Exact capability match plus a fresh compatible worker observation, or bounded configured autoscaler headroom explicitly recorded as such. |
| `nebius-cpu` | Kubernetes Job lease | Compatible execution class, healthy target, accepted runtime/image evidence, and separately proven target capacity. |

Normal scheduling evaluates all compatible candidates without exposing a
physical-pool selector to users. It records candidate health, draining state,
configured/active/occupied/pending/assigned slots, capacity observation time,
adapter identity, environment/region/residency, budget eligibility, estimated
per-slot cost, operator weight, selected reason, and a canonical digest in
`ExecutionRoutingDecisionV1`. Fresh executable capacity is preferred over
configured scale headroom; within the same operator weight, lower known cost is
preferred. A budget-ineligible candidate is blocked. An operator-only weight
may order otherwise eligible candidates; it cannot override capability,
security, health, residency, budget, drain, or capacity blockers. An explicit
admin target binding is audited and is not a normal user workflow.

`Trial.execution_route_generation` advances while the Trial is queued. The
selected pool, adapter, target/class when applicable, reason, candidate
evidence, and digest are copied into the immutable execution lease/outbox or
honored by the legacy claim query. Once a worker claim or execution lease owns
the attempt, the route cannot change. Cross-pool recovery first revokes the old
authority, proves cleanup/seat release, returns the Trial to queued state, and
creates a new routing generation for the next attempt. It never creates two
authorities for one attempt generation.

## Capacity observation and concurrency admission

`loom.pool-capacity.v1` is the normalized operator projection for every legacy
autoscaler pool. It reports the configured ceiling and scale headroom
separately from observed active, occupied, pending, and route-assigned queued
slots. An observation older than the requested freshness window retains its
diagnostic values but reports `executable_free_slots=0`,
`capacity_is_fresh=false`, and is excluded from executable aggregation. This
prevents quota, configured slots, or stale worker counts from becoming a claim
of runnable capacity. `loom admin worker-pools autoscaler status` exposes the
same contract as `GET /admin/worker-pool-autoscalers/status`.

Concurrency admission is a separate persisted boundary. Operators can enable
independent positive ceilings for `global`, `environment`, `region`, `team`,
`batch`, `execution_class`, and `pool` scopes through
`PUT /admin/execution-admission-policies/{scope_kind}/{scope_key}`. Every
mutation is versioned and recorded in `admin_audit_events`; disabling a policy
preserves its history. `GET /admin/execution-admission/status` and
`loom admin worker-pools admission-status` report the durable counter, the
reservation-ledger count, and whether they agree.

Both legacy worker claim endpoints and Kubernetes Job lease reservation call
the same database admission function before changing Trial authority. The
function locks every matching policy row in canonical scope order, increments
all matching counters with reservation creation in the same transaction, and
fails closed when any scope is full. Disjoint scopes remain concurrent. A
reservation freezes team, batch, environment, region, execution class, pool,
attempt, role, and owner identity. Legacy reservations
release when a Trial leaves `claimed`/`running`; service reservations release
only on a terminal lease observation. Database triggers decrement every scope
counter and retain the released ledger row. The reservation is therefore both
the concurrency seat and the audit evidence, not a cache derived from worker
heartbeats.

Paid Nebius execution adds a second, independent finance admission boundary.
An immutable `execution_price_snapshots` row records provider, region, SKU,
USD rates, source URI/version, effective and observation timestamps, the full
canonical rate payload, and its digest. A target price binding is versioned and
must be explicitly enabled. Nebius lease reservation fails closed before the
create command when the binding is absent, mismatched, disabled, or not yet
effective; other pools are not changed by this provider-specific requirement.

The preflight estimate prices the complete requested Pod envelope for its full
deadline: execution container, native sidecars, runtime materializer,
workspace/runtime/output volumes, CPU, memory, and ephemeral storage. It is a
conservative reservation, not a provider bill. Both an enabled pool policy and
an enabled target policy must admit the estimate. Each policy independently
enforces per-attempt, daily, monthly, and maximum-duration limits plus an
emergency stop. Matching policy rows are locked before their current-period
counters and debit ledger are updated, so concurrent control-plane replicas
cannot overspend the same remaining budget. Policy mutation takes an exclusive
transaction lock while reservations, terminal release, and billing ingestion
take the shared form; routine paid reservations remain concurrent after that
brief shared acquisition. Reservations that cross midnight are split into UTC
daily debit rows, and status reads reconcile the current UTC day/month directly
from the debit and provider-bill ledgers instead of trusting cached counters.

Terminal leases that never started a Pod release the estimate immediately.
Started leases retain it as `awaiting_settlement` until provider billing covers
their complete runtime. `execution_node_cost_records` stores immutable
provider-billed node intervals and only a hash of the node identity. Loom
allocates each bill using `dominant_requested_resource_time_v1`: the dominant
requested CPU/memory/storage share multiplied by actual overlap with the billed
node interval. The provider bill remains the cost authority; Pod lifetime alone
never creates cost. Any amount not attributed to attempts remains visible as
`idle_system_fragmentation_microusd`. Settlement requires gap-free persisted
bill intervals. Provider bill records must be split at UTC day boundaries so
daily hard-spend attribution is unambiguous. Settlement replaces the attempt
estimate with the allocation and retains both figures plus the exact price
snapshot. Loom rejects an interval that overlaps a lease without a persisted Pod
termination timestamp, preventing immutable evidence from being prematurely
classified as overhead. Daily/monthly hard-spend counters use the full
provider-billed node amount, including unattributed overhead.

Operators manage and inspect this state through the authenticated
`execution-price-snapshots`, `execution-target-price-bindings`,
`execution-budget-policies`, `execution-node-cost-records`, cost-settlement,
and `execution-finance/status` admin APIs. `loom admin worker-pools
finance-status` exposes the same budgets, reservations, billed allocations,
and overhead. Every mutation writes `admin_audit_events`; none of these
repository surfaces calls Nebius or changes live routing.

Nebius Job creation has a third independent boundary for provisioning and
provider quota. An enabled target policy fixes the maximum node, vCPU, memory,
and storage footprint, the accepted node shape, outstanding Pending,
Unschedulable, and image-pull-backoff counts, create rate, and maximum
observation age. Immutable `execution_capacity_observations` retain provider
quota and usage, physical-capacity state, Kubernetes provisioned/allocatable/
requested resources, Pending reasons, autoscaler state, source/version, and a
canonical digest. Configuration is never upgraded into capacity evidence.

Immediately before a Nebius actuator calls Kubernetes `create`, it locks the
target policy and requires the latest observation to be fresh. Admission adds
the Pod request from the lease-bound finance envelope to every authorization
created after that observation, accounts for currently free allocatable
resources, and conservatively calculates any additional nodes from the accepted
node shape. Both operator maxima and observed provider quota must admit the
projected nodes, vCPU, memory, and storage. Scaling is refused when physical
capacity is insufficient or unknown, or when the autoscaler is stalled or
unknown. Existing fresh allocatable capacity does not require theoretical
provider scale headroom.

Each successful decision is an immutable, lease-bound
`execution_provisioning_authorizations` row. Database transitions retain
whether it is authorized, Pending, Unschedulable, image-pull blocked, running,
or released. Concurrent actuators serialize on the target policy, so create
rate and outstanding-Pending limits cannot both admit the same remaining slot.
Capacity blockers defer the durable create command for bounded retry rather
than calling Kubernetes or corrupting the lease. The authenticated
`execution-capacity-policies`, `execution-capacity-observations`, and
`execution-capacity/status` admin APIs plus `loom admin worker-pools
provisioning-status` expose quota, allocatable/requested state, Pending reasons,
autoscaler state, command backlog, authorization counts, and distinct refusal
reasons. These repository surfaces accept persisted observation evidence; they
do not query or mutate Nebius themselves.

The independently runnable `loom_execution_capacity_collector` is the only
repository component that combines live provider and cluster readback into
that API. Each pass first reads the enabled target policy, then captures the
four exact region-scoped Nebius quota allowances, the bound Managed
Kubernetes node group, all nodes selected for the target, and all non-terminal
Pods consuming those nodes. It publishes nothing unless every source returns a
resource version, the quota service/region/unit bindings match, node-group and
target identities match, and the control-plane receipt repeats the exact
source identity and observation time. Transport retries reuse the same
immutable payload. Usage above quota remains valid evidence and yields zero
headroom instead of making the observation disappear.

Kubernetes requested-resource accounting follows scheduler semantics: normal
containers are summed, restartable init sidecars accumulate, ordinary init
containers contribute their peak, and Pod overhead is added. Ready,
schedulable selected nodes contribute allocatable resources; all selected-node
Pods contribute requests; an unscheduled Pending Pod for the exact Loom target
also contributes demand. Pending, Unschedulable, and image-pull states retain
bounded reason codes, never Pod messages, node names, provider credentials, or
raw API responses. A target Pod found outside the configured node selector is
identity drift and prevents publication.

`deploy/k8s/nebius-capacity-collector.yaml` packages the one-shot collector as
a one-minute `CronJob` with concurrency forbidden, a read-only `get/list`
ClusterRole for Nodes and Pods, a non-root/read-only-root filesystem, and
owner-only copied credential files. It is checked in with `suspend=true` and
depends on a separately provisioned ConfigMap containing the exact target,
pool, namespace, node selector, project, node-group, region, control-plane URL,
and quota name/unit bindings plus two separately provisioned Secrets for the
Nebius service-account credential file and a Loom bearer carrying only
`execution:capacity:observe`. Operators mint that credential with
`loom admin tokens worker mint --kind execution-capacity-collector`; it can
read only the collector policy projection and publish capacity observations,
and cannot mutate policy or use ordinary worker authority. Repository
merge neither creates those credentials nor unsuspends the collector. The
collector uses the official Nebius Python SDK only for quota `list` and node
group `get`; it contains no Nebius create, update, delete, or operation-wait
path.

Resource sizing and capacity forecasts are a fourth independent evidence
boundary. Migration `0120` adds immutable
`execution_resource_calibrations` and versioned target bindings. A calibration
is computed only from persisted #1503 rows for one exact source pool,
architecture, resource-profile identity, candidate SHA, closed time window,
and source version. The query is bounded to 200,000 usage records and its exact
row identities, observation sequences, and update timestamps are reduced to a
canonical digest; replay with changed evidence under the same source version
is rejected.

Each trial-attempt conservatively sums cumulative CPU and I/O counters and
sums per-container memory/PID peaks as upper bounds. CPU utilization is the
attempt-wide mean from cumulative CPU time divided by the complete observation
interval. The immutable snapshot records nearest-rank P50/P95/P99/P99.5,
telemetry completeness, distinct tasks, evidence duration, exact per-batch
peak overlap, throttling, memory-limit, and OOM counts. Recommendations apply
explicit margins and rounding. CPU is the greater of the complete persisted
configured limit and P99.5 mean plus 25%, rounded to 100m, because cumulative
CPU time cannot prove a safe lower burst ceiling. Memory uses the summed peak
upper bound plus 20%, rounded to 64 MiB. Ephemeral storage is the greater of
the immutable Task contract and cumulative-write upper bound plus 25%, rounded
to 256 MiB, because I/O counters alone cannot prove the base image/input
footprint. PID sizing uses the summed peak plus 20%, rounded to eight
processes. These methods and their limitations remain in the evidence payload;
they are not presented as provider measurements.

An enabled binding is rejected unless the snapshot contains at least 1,000
complete trial-attempts across at least 14 days, includes one
batch with at least 100 actually overlapping attempts, and contains no
incomplete telemetry, CPU throttling, memory-limit, or OOM evidence. The exact
task-id set is digested into the snapshot, and enabling a binding requires an
audited non-empty acceptance reason so representativeness remains an explicit
operator decision rather than an invented task-count threshold. This pins
the repository gate to #1503 acceptance rather than the historical
`2 vCPU / 11,500 MiB` division. An ineligible snapshot is still durable and
operator-visible with exact blockers, but it cannot become the active forecast
profile and changes no Task, runtime plan, admission limit, routing weight, or
live target.

`GET /admin/execution-resource-profile/status` and `loom admin worker-pools
resource-profile status` combine an enabled eligible binding with the latest
capacity observation. `immediate_executable_slots` is nonzero only with a
fresh observation, healthy active target, enabled capacity policy, and no
profile blocker. `configured_scale_headroom_slots` remains a separate fit
projection over the minimum policy/provider node, CPU, memory, and storage
headroom; it is never labeled executable capacity. Operators create and bind
snapshots through the authenticated `execution-resource-calibrations` and
`execution-resource-profile-bindings` APIs or the matching `calibrate` and
`bind` CLI commands. Every mutation is audited and none performs a provider,
Kubernetes, routing, profile, or traffic mutation.

`config/service-execution-topology.json` is the machine-validated target
topology for the `nebius-cpu` adapter:

| Environment | Target | Role | Residency |
| --- | --- | --- | --- |
| development | `nebius-eu-north1-development` | primary | EU |
| staging | `nebius-eu-north1-staging` | primary | EU |
| production | `nebius-eu-north1-production` | primary | EU |
| production | `nebius-eu-west1-production` | secondary | EU |

Development, staging, and production cannot share a target identity or health
observation. Every target is probed independently; a target becomes ineligible
when its observation is older than its declared stale threshold. Nebius target
placement is environment-local and health-first. Production prefers
`eu-north1` and may
fail over to `eu-west1` only when the secondary target is independently healthy
and the durable lease policy permits the transition. Queued work does not
cross environments or leave EU residency to recover capacity.

These target records are desired logical bindings, not evidence that any
Nebius project, cluster, node group, runtime class, or capacity exists.

## Durable execution authority

Migrations `0113` through `0120` persist the complete provider-neutral
desired/observed state without making a Nebius or Kubernetes call:

- immutable `execution_classes` and environment/regional `execution_targets`;
- one canonical routing decision and monotonically increasing routing
  generation on each Trial, with the selected pool/reason/digest frozen into
  every Kubernetes lease and its history;
- one attempt `execution_leases` identity per `(trial, attempt)` and, when
  required, one parent-bound verifier identity for the same attempt; each has
  a generation that can advance only by one and can never regain authority
  after revocation;
- at-least-once `execution_commands`, atomically required by a deferred
  database constraint whenever desired state changes;
- idempotent `execution_events` and database-generated
  `execution_lease_history` snapshots.

The reservation transaction locks a queued Trial, creates or verifies its
Kubernetes route, increments its attempt, creates the lease, and appends the
`create` command. A crash before commit leaves every effect absent. A Trial
already routed to a legacy worker pool cannot also reserve a Kubernetes lease.
Command consumers use bounded delivery leases;
an expired claim redelivers the same command and idempotency key. Exact event
and acknowledgement replay is accepted, while changed replay is rejected.
Event arrival may be out of order: all valid events are retained, but only a
higher event ordinal may advance the current projection.

Lease identity includes deterministic provider scope, namespace, Job, and
execution-unit keys. `create`, `start`, `cancel`, `timeout`, `retry`,
`finalize`, `delete_pending`, and `deleted` are explicit desired transitions.
Cancel, timeout, retry, and delete revoke the prior generation before the
command can be observed and persist a five-minute cleanup deadline for the
execution unit/seat release. Missing that deadline remains cleanup debt and
cannot make a new generation authoritative. The same lease/generation fence is checked by step
token minting, Gateway dispatch, worker heartbeat, artifacts, trajectory,
resource usage, and final-result writeback. Legacy Trials with no execution
lease retain their existing path; once any lease exists, missing fence headers
fail closed.

`0114` additionally freezes the canonical Pod-native runtime plan and digest on
the lease. The plan binds candidate, task revision, command identity, execution
role, task/runtime image digests, resources, process phases, sidecars, probes,
volume/output bounds, and verifier topology. Reservation rejects any drift
between this plan, the workload requirements, and the persisted execution
class. The create outbox and history projection retain the same immutable
identity; an actuator refuses legacy or malformed leases that have no valid
runtime plan.

`0115` adds the observed Pod IP and one generation-bound service-execution
Artifact commit ledger. Gateway maps the direct peer to the immutable Pod UID,
resource generation, role, target health, and frozen runtime identity before it
mints a short-lived step JWT. The token is returned only to PID 1, removed from
child environments, protected from same-UID `/proc` reads, and injected by a
loopback refresh proxy. Output is uploaded to Gateway, not directly to object
storage. The common multipart protocol verifies per-part and whole-object
digests, parses and rebinds `result.json`, and writes immutable manifest/marker
evidence before the lease becomes `committed`.

Event and command payloads are database-bounded at 64 KiB. An execution lease
accepts at most 10,000 event ordinals and 20,000 projected history transitions;
operator projections also return at most 500 event and 500 history rows. These
limits are contract errors, not invitations to discard older authority.
Prometheus service-execution metrics aggregate by command type or surface and
never use trial, lease, Job, namespace, or team identifiers as labels.

The schema downgrade is permitted only when every execution class, target, and
lease row has been deliberately removed. An image rollback is forward-schema
compatible; schema rollback requires the protected backup/restore process.
Lifecycle deletion first removes `execution_leases`, which cascades commands,
events, and history, and only then deletes usage/events/calls/artifacts, Trial,
and Batch metadata after object deletion has been verified. Operators must not
manually delete an outbox or provider object to force convergence. See the
[operator runbook](../runbooks/operator-runbook.md#service-execution-recovery-and-retention).

## Hostile-code isolation

The accepted class requires `sandboxed_runtime`. A normal shared-kernel Pod,
Pod Security admission, NetworkPolicy, a namespace, or a dedicated node group
alone is not an accepted hostile-code boundary. The runtime-validation issue
#1551 must prove an available Nebius Kubernetes `RuntimeClass` based on Kata,
gVisor, or another reviewed sandbox implementation, including escape,
network, secret, cleanup, and performance tests. No ordinary-Pod fallback is
allowed when that runtime is missing or unhealthy.

One-attempt-per-node may be proposed later as a distinct
`dedicated_ephemeral_node` class. It needs its own lifecycle, wipe, fencing,
capacity, and cost acceptance and is not silently equivalent to
`sandboxed_runtime`.

Nebius documentation currently confirms managed Kubernetes node groups,
autoscaling, taints, security groups, and CPU-d3 availability in the selected
EU regions. As of 2026-08-25, the official documentation review did not find a
documented supported Kata/gVisor/custom RuntimeClass contract. That absence is
why runtime support remains fail-closed empirical work under #1551 rather than
an architecture assumption. References:

- <https://docs.nebius.com/kubernetes/node-groups/manage>
- <https://docs.nebius.com/kubernetes/node-groups/node-group-autoscaling>
- <https://docs.nebius.com/compute/virtual-machines/types>
- <https://docs.nebius.com/overview/quotas>
- <https://github.com/nebius/pysdk>
- <https://docs.nebius.com/vpc/security-groups>

## Kubernetes execution units

The #1549 actuator consumes the durable command outbox through database-backed
delivery leases. It renders one deterministic `batch/v1` Job per execution
unit, requires an immutable image digest, exact CPU/RAM/ephemeral-storage
requests and limits, a non-root restricted security context, an explicit
sandbox `RuntimeClass`, and an attempt service account with token automount
disabled. It never receives Nebius credentials and its Kubernetes service
account has namespace-only Job create/get/list/watch/delete plus Pod
get/list/watch permissions. There is no Secret read, Pod exec, log, node,
namespace, CRD, or cluster-wide permission.

Job labels bind the immutable resource generation; annotations bind the full
target and execution-unit identities. Create timeouts and HTTP 409 are resolved
only by exact name/identity readback. Deletes carry a Job UID precondition;
404 means cleanup is already converged, while UID reuse or any scope mismatch
is dead-lettered and never deleted. Watch observations are backed by periodic
full lists, and an expired resourceVersion resets the watch cursor. Multiple
replicas are safe because only one holds a durable command delivery lease and
all observations are idempotent by resourceVersion, UID, state, lease, and
generation.

The observed projection records Job/Pod UIDs, Kubernetes resourceVersion,
node, scheduling/start/termination timestamps, last reconciliation, and
bounded normalized failure detail. Pending, unschedulable, image-pull backoff,
running, succeeded, failed, OOM-killed, evicted, node-lost, active-deadline,
terminating, missing, and deleted states have explicit mappings. A stuck Job
remains visible as observed failure/debt; the actuator never fabricates a Loom
success or changes retry policy outside the fenced control-plane transition.

The #1550 renderer consumes only the lease-frozen
`loom.execution-runtime-plan.v1`. For the supported `init_payload` composition
it creates a digest-pinned runtime materializer, verifies the static runtime
binary digest, writes the plan and binary into a bounded `emptyDir`, and starts
the task image with that runtime as PID 1 through a read-only runtime mount.
Workspace, runtime, output, termination grace, log, artifact, and
ephemeral-storage bounds are explicit.
Declared sidecars render as ordered Kubernetes native sidecar init containers
(`restartPolicy: Always`) with digest-pinned images, resources,
startup/readiness probes, dropped capabilities, and no service-account token.
Unsupported compositions fail closed.

The static runtime emits bounded per-phase stdout/stderr evidence and an
atomically renamed `loom.execution-runtime-result.v1` manifest. It distinguishes
setup, task, verifier, timeout, cancellation, and runtime failures, preserves
signal/exit/timestamp/truncation evidence, and repeats the exact lease-bound
runtime identity. The control plane validates `result_reported` against the
frozen contract, expected phase order, container roles, and log bounds before
accepting the idempotent event. A changed candidate, command, image, role,
phase, or digest is rejected.

After the full result file is committed, the runtime writes a separate bounded
termination summary to kubelet's termination-message file. The actuator reads
that summary through ordinary Pod status (never Pod exec or log RBAC), checks
its runtime/command/role identity against Job annotations, and retains it in
the Kubernetes observation event. A completed Job with a missing, malformed,
or mismatched summary is normalized as failure rather than success.

The runtime result file alone is not durable object-storage acceptance. A
successful Job requires the matching committed upload session, manifest digest,
and marker digest in both the lease and termination summary. Cancellation and
retry fence model calls immediately while allowing only the old resource
generation to flush output until the cleanup deadline. The actuator defers Job
deletion during that window; at expiry it records explicit output
unavailability before UID-preconditioned deletion. Live target evidence for
this repository path remains part of #1551 acceptance.

The #1551 repository security controls and exact remaining live gates are in
[Nebius hostile-workload security](nebius-execution-security.md). Operators
must use the corresponding
[security acceptance and incident runbook](../runbooks/nebius-execution-security.md);
merge or manifest rendering is not target acceptance.

`deploy/k8s/nebius-execution-actuator.yaml` is deliberately inert at zero
replicas. Repository merge cannot scale it or create its referenced database
secret, namespace in a live target, runtime class, cluster, or cloud resource.
The disposable k3s conformance test validates the real Kubernetes API seam
with a suspended Job and makes no Nebius call.

Each attempt owns one task-scoped Kubernetes object bearing the Loom trial ID,
attempt generation, lease generation, requirements digest, and image digest.
The primary task runs in one Pod/Job. Declared sidecars are containers in that
Pod when they share the workspace and lifecycle. A dependency requiring a
separate network identity becomes a separately owned Pod and ClusterIP Service
with explicit readiness, resource, network, and image contracts. Undeclared
service discovery and cross-trial sharing are forbidden.

An in-attempt verifier runs after the agent inside the same sandbox and
workspace. A verifier requiring stronger separation runs as a second,
parent-bound `execution_role=verifier` lease and Job for the same trial attempt,
with a fresh writable workspace and read-only immutable input artifacts.
The task Job publishes artifact digests; the verifier consumes only those
digests and publishes a signed result reference. It does not attach to the
task container, mount another trial's volume, or receive agent/provider
credentials. The trial becomes terminal only after Loom records both execution
conditions and the verifier result under the same attempt generation.

GPU, ARM64-only, desktop/GUI, privileged, hostPath, host-network,
nested-container, host-device, and host-specialized workloads are rejected by
the CPU class. They must be converted to the exact contract or retained in a
separately accepted product; compatibility is never obtained by relaxing this
class.

## Compatibility inventory

`config/service-execution-compatibility.toml` assigns every repo-known
benchmark entry point, dynamic service workload class, and pipeline resource
profile an independent `nebius-cpu`, `oldlab`, and `gb10` disposition, owner,
reason, and required action. Generate the deterministic report and schemas with:

```bash
python scripts/ops/generate_execution_contract_artifacts.py
python scripts/ops/generate_execution_contract_artifacts.py --check
```

The generator fails on missing/overlapping rules, missing accepted-pool
identities, and duplicate workload identities. At this decision point no
workload is statically supported on Nebius: 66 require conversion and OSWorld
plus the two GPU/host-specialized Behavior profiles are unsupported there.
Every catalog row has `runtime_admission_required` for OLDLAB and GB10. That
disposition preserves both accepted service paths while requiring the exact
materialized task capability and fresh worker evidence; it is not a claim that
either pool can currently execute every row.

The report covers the repository's current static catalog. Operator-local,
remapped, user-supplied, and live database TaskSets are unbounded classes, not
a finite checked-in list. At rollout, admission must freeze every materialized
`TaskConfig` into `WorkloadRequirementsV1` and emit a per-task disposition
before fan-out. A static report does not claim coverage of mutable live rows.

## Terminology and deprecation

The following migration applies only to service scheduling. Unrelated uses
such as storage backend, state backend, LLM provider backend, and Ingress
backend keep their domain-specific names.

| Current surface | Transitional read | Terminal service model |
| --- | --- | --- |
| `POST /api/v1/batches.backend`, `Batch.backend`, clone/rerun payloads | Server ignores no value silently; it derives and returns `execution_class_id`, while legacy value is read-only migration evidence. | Request field and column removed; admission resolves the execution class. |
| `GET /api/v1/backends`, overview/monitor `available_backends` | Replaced by an execution-class catalog with admissibility and target-health summaries. | Backend catalog route removed. |
| NewBatch backend picker and BatchDetail/Run Library backend labels | Display resolved execution class and compatibility reasons; no provider/target selector. | Backend picker removed. |
| `Trial.requires_caps` | Versioned frozen `WorkloadRequirementsV1` projection is stored alongside legacy JSON during bounded migration. | Legacy unversioned caps removed. |
| Worker `capabilities[].backend` and `Worker.pool_name` | Observed adapter capability and pool identity; never normal user submission identity. | Retained for accepted OLDLAB/GB10 worker claims and joined with provider-neutral route observability. |
| `required_worker_pools` / `required_worker_pool` | Operator-only smoke/pin evidence; user batches remain forbidden from setting it. | Operator control remains distinct from normal provider-neutral scheduling. |
| `autoscaler_pool_name`, physical pool policies, slot counts | Legacy-adapter assignment must match the versioned Trial route; configured and fresh capacity are reported separately. | Retained while OLDLAB/GB10 operate, alongside target/Pod evidence for Nebius. |
| `loom run --backend` | Unchanged local-only driver selection. | Docker, Modal, and fake may remain CLI-only and never appear in service admission or capacity. |

Hybrid Nebius plus OLDLAB/GB10 operation is an accepted terminal state. Adapter
specific fields have an owner and telemetry, but their existence is not a
deprecation or retirement schedule.

## Migration and authority gates

The accepted repository and rollout boundaries are:

1. Merge this contract, generated schemas/inventory, and terminology map.
2. Add immutable requirements, route, target, lease, attempt-generation, and
   reconciliation persistence without changing live traffic.
3. Keep existing OLDLAB/GB10 routes operational; test route selection and
   duplicate-authority fences in the repository and an authorized environment.
4. Implement the fail-closed Nebius actuator and observed-state reconciler.
5. Provision development infrastructure, then prove sandbox/runtime and
   workload conversion; provisioning authority is separate from repository
   merge authority.
6. Provision isolated staging, run bounded canaries, and record target health,
   node-backed capacity, Pod outcomes, artifact hashes, cleanup, and cost.
7. Provision and accept both production regions before any production route
   can use them.
8. Change pool weights, enable a Nebius route, or independently drain one pool
   only through protected rollout authority with explicit candidate, route,
   health, capacity, rollback, and operator approval evidence.
9. Retire a pool only under a future explicit owner decision; this architecture
   neither requires nor authorizes OLDLAB/GB10 retirement.

Passing repository CI authorizes merge only. Merge does not authorize or prove
infrastructure. Provisioned infrastructure does not authorize or prove a live
canary. A successful canary does not authorize pool-weight changes, a drain,
traffic routing, or retirement.

## Reconciliation invariants

- Missing or stale candidate health makes that candidate ineligible; it never
  causes cross-target or cross-pool guessing.
- One route and attempt generation owns at most one worker claim or Kubernetes
  execution unit, never both.
- Observations from a stale lease/attempt generation cannot advance state.
- Unknown provider/Kubernetes objects are quarantined, not adopted by name.
- Cancel stops and cleans the active generation but is not a data rollback.
- Provider quota, configured autoscaler maximum, registered node, healthy API,
  or free-looking slot is not executable capacity. Capacity is observed only
  from fresh healthy target, node, and admitted execution evidence.
- Configured quota/slots and fresh executable capacity are separate fields for
  every pool and any aggregate view.
- Repository merge, provisioning, canary, routing-policy change, drain, and
  retirement each require their own authority and evidence.

## Follow-on ownership

- #1540: durable execution state and provider-neutral lease schema; implemented
  and held traffic-disabled in this change.
- #1549: namespace-scoped Kubernetes Job actuator and observed-state
  reconciliation; implemented and held at zero replicas in this change.
- #1543: Nebius projects, networking, clusters, registries, node groups, and
  regional infrastructure.
- #1551: sandbox runtime and hostile-workload empirical acceptance.
- Later #1536 children: infrastructure, workload conversion, canary, and
  independently authorized production routing. No child implicitly owns
  OLDLAB/GB10 retirement.
