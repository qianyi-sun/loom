# TerminalGen authoring implementation plan

Issue: [#1432](https://github.com/qianyi-sun/loom/issues/1432)

This plan describes a complete, durable authoring path. It is not a statement
that the path is currently executable. The official recipe must remain absent
from the recipe catalog and every deployment capability must remain disabled
until all workstreams and the fixed-candidate acceptance gate are complete.

## Non-negotiable boundary

The August 17 delivery is an input to provenance review, not source that Loom
may silently vendor. An official run requires all of the following immutable
inputs:

- a canonical Git repository and exact commit;
- repository tree and delivery-snapshot digests;
- an asserted code license and derivative-data authority;
- a dependency lock and SBOM digest;
- digest-pinned planner, generator, validator, task-base, dependency-resolver,
  and packager images;
- an approved 18-card catalog artifact whose bytes match its recorded digest.

Missing or conflicting authority is a submission failure. Historical call
logs, benchmark snapshots, bulk JSONL, generated workspaces, trajectories, and
corpora remain outside the repository. Loom must never use them as implicit
defaults.

## Durable graph

The graph uses explicit card partitions because the current graph contract has
no partition-scoped group-by fan-in and StageRun shard keys are unique per
run/node. At 500 slots per card, the production graph is approximately 102
nodes and 27,000 StageRuns, below the 128-node and 50,000-StageRun bounds.

1. `plan_batch` validates source authority, the catalog, and parameters.
2. Eighteen `plan_card_NN` nodes commit exactly 500 stable slot records each.
3. `plan_audit`, an outcome gate, and a reject renderer close the complete
   9,000-slot plan before provider-authorized work becomes ready.
4. Eighteen `generate_card_NN` nodes fan out to 500 independent StageRuns.
5. Each generation partition has an accepted gate and reject renderer.
6. Eighteen `validate_card_NN` nodes fan out accepted task bundles. Static and
   dynamic validation belong to the same durable slot lineage; the worker owns
   the dynamic sandbox, not the task container.
7. Eighteen singleton `finalize_card_NN` nodes prove per-card quota and
   validation coverage.
8. Global finalize, gate, and reject nodes prove exact full-batch closure.
9. Restricted-authoring and solution-free-runtime package nodes commit
   separate access classes.
10. A server-owned publisher validates the final audit and atomically creates
    an immutable corpus version before switching a catalog alias.

No node runs an internal task ThreadPoolExecutor. A local worker-count
parameter is not part of the request schema.

## Identity and replay

Before the first model call, the plan fixes:

- recipe, graph, source, catalog, parameter, image, and policy digests;
- card, partition, slot ordinal, capability, source task, variant bucket and
  index, difficulty, seed, domain scope, and `template_family_id`;
- a bytewise-canonical plan digest and an explicit persisted-byte bound.

The authoritative slot identity is `(PipelineRun, node_key, shard_key)`. Every
attempt uses the existing fenced claim and artifact-commit protocol. No
filesystem directory, process-local counter, CLI exit status, or mutable
output name is authoritative.

The provider request key is deterministic from attempt, logical call ordinal,
and request digest. Replaying the same key and digest returns the durable
result or ledger state. Reusing the key with different bytes fails with 409.
An ambiguous upstream outcome is conservatively settled and cannot be retried
as unaccounted spend.

## Typed artifacts

The recipe owns closed validators for:

- licensed source lock and 18-card catalog;
- batch and partition plans;
- generation request and terminal slot record;
- canonical task bundle and tree manifest;
- static and repeated dynamic validation evidence;
- per-card and global final audits;
- restricted authoring-corpus manifest;
- solution-free runtime-corpus manifest;
- publication receipt and immutable alias transition.

All reject unknown fields and secret-looking literals. Every published task is
hash-bound to successful validation evidence. Accepted, rejected, exhausted,
cancelled, and cleanup-failed outcomes use a closed reason taxonomy. Partial
quota can be inspected and exported for authorized diagnosis but cannot be
represented as a successful full corpus.

## Runtime and capacity

The existing Stage-1 BEHAVIOR adapter remains closed. Ordinary execution gains
a separate code-owned eligibility registry keyed by recipe digest, node key,
resource-profile digest, runtime image feature, and optional validation-grant
digest. It must not become an arbitrary-container executor.

Profiles separately admit planning, generation with Gateway access, static
validation, dynamic validation, and packaging. Each profile fixes CPU, memory,
scratch, PID, timeout, network, concurrency, and image-feature bounds.

Dynamic validation uses a worker-owned rootless BuildKit/OCI backend with a
closed validation grant containing bundle, base, validator, dependency,
policy, repetition, resource, and network digests. Authoring containers never
receive Docker/containerd sockets, privileged options, host environment, or
raw runtime controls.

For Gateway nodes, the worker creates an attempt-private read-only tmpfs,
mints and rotates only the bound step token, and passes its directory to the
container. Terminal and cancellation cleanup stops token rotation and removes
secret, network, input, output, upload, and scratch authorities before durable
acknowledgement. Cleanup failure is terminal and blocks retry.

## Provider accounting

The frozen provider binding is generalized beyond BEHAVIOR while remaining
server-owned. It pins provider, model, wire protocol, connection, team
allowlist, runner/image contract, per-attempt request/cost/time bounds, recipe,
and node.

The Gateway reserves request count and worst-case cost transactionally before
dispatch. A durable provider-dispatch record correlates binding and request
digests, reservation, dispatch state, upstream ambiguity, `LlmCall`, and
settlement. The step token includes the frozen binding digest, attempt lease
generation, and token ID; the Gateway rechecks active attempt, cancellation,
binding, connection, model, and wire before each dispatch.

Normal logs contain hashes, bounded usage/cost/timing, and closed reason codes,
not raw prompts, responses, errors, workspaces, stdout, or stderr. Authorized
raw authoring evidence, when policy permits it, is a separate restricted,
scanned, expiring artifact.

## Access and publication

Artifacts receive an immutable access class in both the frozen output
declaration and Artifact row. At minimum:

- `team_runtime` for solution-free runtime artifacts;
- `authoring_restricted` for solutions, raw authoring evidence, and provenance;
- `sanitized_audit` for bounded diagnostics.

Download and lineage resolution enforce access class, team, role, scan state,
and retention. Restricted artifacts are visible only to the run creator, team
owner, or platform administrator and otherwise return 404. Ordinary runtime
consumers cannot resolve a restricted artifact as input.

The publisher is server owned and fenced. It validates the successful terminal
run, final audit, exact 9,000-task quota, licenses, canonical task identity,
verifier bridge, absence of solutions, validation lineage, and tree hashes.
It writes a content-addressed physical corpus version and only then switches a
logical alias after readback. A deterministic tar smoke bundle of at most 500
tasks may be projected to TaskSet; the full corpus does not depend on TaskSet.

## API, CLI, and Web

Generic run detail must not load every StageRun or artifact for a 9,000-slot
run. Add server-side progress aggregation plus cursor-paginated card, slot,
stage, attempt, usage, sanitized-log, and artifact views. The recipe catalog
publishes display metadata, input descriptions, and a strict parameter schema.

The authenticated API, CLI, and Web support idempotent submit, list, show,
watch, cancel, policy-authorized repair-run creation, progress and quota
inspection, sanitized diagnostics, budgets, and authorized downloads. A stage
retry is presented as a lineage-bound repair run, never an in-place mutation.

## Delivery sequence

Each change lands behind an absent catalog entry or a disabled capability. No
sequence item is deployment authorization.

1. Land Loom-owned provenance, catalog, plan, outcome, validation, audit, and
   corpus contracts plus deterministic 18-by-500 property tests.
2. Close ordinary orchestrator fan-out, readiness, dependency, terminal
   snapshot, failure-propagation, and final-result reconciliation.
3. Add code-owned CPU profiles, ordinary runtime eligibility, recipe-neutral
   attempt completion, Gateway secret lifecycle, cancellation, and cleanup.
4. Add provider binding generalization and the provider-dispatch ledger with
   pre-dispatch budget authority and ambiguity tests.
5. Add the rootless validation grant/backend and security tests.
6. Add artifact access-class migration, authorization, retention, and
   solution-boundary tests.
7. Add progress projections, pagination, catalog metadata, API, CLI, and Web.
8. Add corpus publisher, canonical task/verifier projection, and TaskSet smoke.
9. Build and scan runtime images, record exact digests, register the complete
   official recipe, and keep deployment disabled.
10. Run migration upgrade/rollback, multi-worker, failure-injection, security,
    and fixed-candidate end-to-end acceptance. Enablement requires separate
    operator authorization and live readback.

## Acceptance gate

The program is incomplete until all #1432 acceptance checkboxes are supported
by exact-head CI and a fixed-candidate run. In particular, unit success, a
registered recipe, a healthy controller, or a published image does not prove
multi-worker execution, provider accounting, cancellation cleanup, restricted
artifact safety, exact-quota publication, or deployment acceptance.
