# Nebius service execution contract

Status: accepted target architecture for issue #1548, with the provider-neutral
durable execution control plane from #1540 and namespace-scoped Kubernetes Job
actuator from #1549 implemented but not traffic-enabled. Infrastructure,
sandbox-runtime acceptance, live canaries, cutover, and retirement remain
separately authorized work.

## Decision

Loom service mode converges on one execution architecture: hostile CPU tasks
run as Kubernetes execution units on the logical `nebius-cpu` pool. A trial
declares provider-neutral `WorkloadRequirementsV1`; admission resolves an
`ExecutionClassV1`; a later durable lease binds that class to one healthy
regional `ExecutionTargetV1`. Trials never select a provider, region, cluster,
worker name, or reusable slot directly.

The checked contracts live in
`src/loom/execution_contract.py`. Their generated JSON schemas and the complete
repo-known compatibility report live under `docs/evidence/`. Unknown fields,
unknown schema versions, implicit resource limits, mutable images, or a
capability that would be silently weakened fail admission.

This decision does not make current Docker workers or a configured Nebius quota
equivalent to executable Kubernetes capacity.

## Authority topology

| State | Authority | Rule |
| --- | --- | --- |
| Task, batch, trial, immutable workload requirements | Loom/Postgres | Written before fan-out; user input cannot write derived admission fields. |
| Execution class, target catalog, desired lease, attempt generation | Loom/Postgres | Versioned desired state and fencing authority. The lease work belongs to #1540. |
| Provider project, Kubernetes cluster, node group, Pod/Job, RuntimeClass | Nebius/Kubernetes | Observed resource state only; never the source of trial intent. |
| Reconciliation cursor, observation, condition, retry | Loom/Postgres | The reconciler records every comparison and action outcome. |
| Trial artifacts and immutable image identity | Object store/registry plus Postgres digest references | A mutable tag or local-only build is not executable service identity. |

The Nebius actuator is the only component allowed to translate desired targets
and leases into provider or Kubernetes operations. API handlers persist intent;
they do not call Nebius or create Pods inline. The reconciler reads one fenced
desired generation, observes the provider and Kubernetes objects, performs an
idempotent action, and records the resulting observation. A stale generation,
unknown object owner, target-health failure, or ambiguous action outcome stops
progress and never broadens placement.

The identity chain is:

`task revision -> workload requirements digest -> execution class version ->`
`lease generation -> execution target -> Kubernetes owner UID -> execution`
`unit UID -> attempt generation -> artifact digests`.

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

`config/service-execution-topology.json` is the machine-validated target
topology:

| Environment | Target | Role | Residency |
| --- | --- | --- | --- |
| development | `nebius-eu-north1-development` | primary | EU |
| staging | `nebius-eu-north1-staging` | primary | EU |
| production | `nebius-eu-north1-production` | primary | EU |
| production | `nebius-eu-west1-production` | secondary | EU |

Development, staging, and production cannot share a target identity or health
observation. Every target is probed independently; a target becomes ineligible
when its observation is older than its declared stale threshold. Placement is
environment-local and health-first. Production prefers `eu-north1` and may
fail over to `eu-west1` only when the secondary target is independently healthy
and the durable lease policy permits the transition. Queued work does not
cross environments or leave EU residency to recover capacity.

These target records are desired logical bindings, not evidence that any
Nebius project, cluster, node group, runtime class, or capacity exists.

## Durable execution authority

Migrations `0113` and `0114` persist the complete provider-neutral
desired/observed state without making a Nebius or Kubernetes call:

- immutable `execution_classes` and environment/regional `execution_targets`;
- one attempt `execution_leases` identity per `(trial, attempt)` and, when
  required, one parent-bound verifier identity for the same attempt; each has
  a generation that can advance only by one and can never regain authority
  after revocation;
- at-least-once `execution_commands`, atomically required by a deferred
  database constraint whenever desired state changes;
- idempotent `execution_events` and database-generated
  `execution_lease_history` snapshots.

The reservation transaction locks a queued Trial, increments its attempt,
creates the lease, and appends the `create` command. A crash before commit
leaves all four effects absent. Command consumers use bounded delivery leases;
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

The runtime result file alone is not durable object-storage acceptance. Until
#1551 supplies the reviewed credential-free workload identity and #1550 wires
the bounded uploader/commit protocol, normal Job deletion must not be treated
as successful artifact, trajectory, usage, diagnostic, or result retention.

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
profile an owner, disposition, reason, and required changes. Generate the
deterministic report and schemas with:

```bash
python scripts/ops/generate_execution_contract_artifacts.py
python scripts/ops/generate_execution_contract_artifacts.py --check
```

The generator fails on missing/overlapping rules and duplicate identities. At
this decision point no benchmark is marked supported: all current first-party
benchmark images still require immutable image materialization and explicit
bounded resources/network policy. OSWorld and GPU/host-specialized Behavior
profiles are intentionally unsupported; the remaining workloads require
conversion.

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
| Worker `capabilities[].backend` and `Worker.pool_name` | Observed runtime/target telemetry only; never submission identity. | Kubernetes target/Pod observations replace reusable service-worker slots. |
| `required_worker_pools` / `required_worker_pool` | Release-smoke-only evidence until equivalent target-bound canaries exist. User batches remain forbidden from setting it. | Removed after legacy release evidence retires. |
| `autoscaler_pool_name`, physical pool policies, slot counts | Read-only legacy drain evidence; cannot satisfy a lease. | Replaced by target health, desired executions, and observed Pods/nodes. |
| `loom run --backend` | Unchanged local-only driver selection. | Docker, Daytona, Modal, and fake may remain CLI-only and never appear in service admission or capacity. |

There is no permanent dual path. Compatibility shims have an owner, telemetry,
and removal gate; they cannot create new legacy rows after the service cutover.

## Migration and authority gates

The only accepted order is:

1. Merge this contract, generated schemas/inventory, and terminology map.
2. Add immutable requirements, target, lease, attempt-generation, and
   reconciliation persistence under #1540 without changing traffic. Complete
   in repository; traffic remains disabled pending later gates.
3. Implement the fail-closed Nebius actuator and observed-state reconciler.
4. Provision development infrastructure, then prove sandbox/runtime and
   workload conversion; provisioning authority is separate from repository
   merge authority.
5. Provision isolated staging, run bounded canaries, and record target health,
   node-backed capacity, Pod outcomes, artifact hashes, cleanup, and cost.
6. Provision and accept both production regions before any production route
   can use them.
7. Cut over traffic only through the protected rollout authority with explicit
   candidate, lease, health, rollback, and operator approval evidence.
8. Disable new legacy service submissions, drain legacy attempts, verify no
   durable references or active work remain, then remove backend/pool/worker
   service scheduling and its compatibility shims.

Passing repository CI authorizes merge only. Merge does not authorize or prove
infrastructure. Provisioned infrastructure does not authorize or prove a live
canary. A successful canary does not authorize traffic cutover. Cutover does
not authorize legacy retirement until drain and rollback-retention gates pass.

## Reconciliation invariants

- Missing or stale target health queues work; it never causes cross-target
  guessing.
- One active lease generation owns at most one active Kubernetes execution
  unit for an attempt.
- Observations from a stale lease/attempt generation cannot advance state.
- Unknown provider/Kubernetes objects are quarantined, not adopted by name.
- Cancel stops and cleans the active generation but is not a data rollback.
- Provider quota, configured autoscaler maximum, registered node, healthy API,
  or free-looking slot is not executable capacity. Capacity is observed only
  from fresh healthy target, node, and admitted execution evidence.
- Repository merge, provisioning, canary, cutover, and retirement each require
  their own authority and evidence.

## Follow-on ownership

- #1540: durable execution state and provider-neutral lease schema; implemented
  and held traffic-disabled in this change.
- #1549: namespace-scoped Kubernetes Job actuator and observed-state
  reconciliation; implemented and held at zero replicas in this change.
- #1543: Nebius projects, networking, clusters, registries, node groups, and
  regional infrastructure.
- #1551: sandbox runtime and hostile-workload empirical acceptance.
- Later #1536 children: actuator, workload conversion, canary, production
  cutover, and legacy retirement in the order above.
