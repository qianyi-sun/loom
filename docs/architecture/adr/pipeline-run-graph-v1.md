# Pipeline Run Graph V1

Status: accepted

Date: 2026-08-10

Tracking:

- Epic: [qianyi-sun/loom#1210](https://github.com/qianyi-sun/loom/issues/1210)
- Normative schema and persistence contract:
  [qianyi-sun/loom#1211](https://github.com/qianyi-sun/loom/issues/1211)
- Orchestration, fencing, budget, retry, and terminal projection:
  [qianyi-sun/loom#1212](https://github.com/qianyi-sun/loom/issues/1212)

## Context

Loom's `Batch -> Trial` model remains the execution ledger for flat benchmark
evaluation. Production data and recovery workflows also need immutable staged
execution, typed dependencies, outcome-based routing, bounded fan-out, retry
attempts, and end-to-end provenance. Modeling every workflow as a bespoke
service, accepting caller-authored graphs, or manufacturing synthetic Trials
for non-evaluation stages would weaken authorization, reproducibility, and the
meaning of existing evaluation records.

The Pipeline contract therefore needs a narrow, generic graph vocabulary while
keeping research policy in reviewed, repository-owned recipes. It must also
separate the repository delivery boundary from live deployment authority.

## Decision

### Official Recipes are the only graph authority

Every runnable v1 graph is resolved from a repository-owned, versioned official
Recipe plus declared parameters and committed typed Artifact inputs. Resolution
produces one immutable `RunGraphSpecV1` snapshot. Recipe identity and submission
policy are digest-bound catalog metadata.

Ordinary callers can select an allowed Recipe and its declared inputs; they
cannot submit graph JSON or override image, argv, resource profile, network
profile, mounts, or secret bindings. There is no public raw-graph endpoint,
editable Recipe table, arbitrary expression language, or runtime graph
mutation. The canonical public product surface is Pipeline, with REST rooted at
`/api/v1/pipeline-runs` and CLI rooted at `loom pipeline`; #1216 owns delivery
of those submission and control surfaces. `/api/v1/run-graphs` is not an alias.

The sole v1 `acceptance_authorization_only` Recipe is the fixed
`behavior-recovery-acceptance-preflight@1`. It is available only through the
internal, authorization-injected acceptance controller for its declared matrix
preflight. It is not an ordinary submit or retry option and does not create a
site-admin raw-graph path.

### V1 has a closed graph vocabulary

`RunGraphSpecV1` is an acyclic graph with exactly two node kinds:

- `container`: an immutable digest-pinned container execution contract with
  typed inputs, typed outputs, fixed resource and network profiles, bounded
  attempts, and an optional manifest fan-out contract.
- `gate` with `gate_kind=outcome`: an automatic controller decision over a
  subject stage's closed `domain_outcome` contract. It creates no worker
  attempt and cannot wait for a person.

V1 has no cycles, loops, recursion, manual approval, external-publish gate,
arbitrary expression, caller-provided template, or container-authored routing
decision. A process exit code represents platform execution, not the domain
verdict; an rc=0 container must emit canonical `loom.stage-result.v1` with a
non-null `domain_outcome`.

Fan-out is manifest-driven and bounded. A consumed
`loom.fanout-manifest.v1` is canonical, sorted, typed, size-limited, and
authorization-revalidated before expansion. For dynamic outputs, a container
writes a bounded `loom.platform-fanout-index.v1`; the platform allocates
Artifact identities and atomically synthesizes the fan-out manifest. A
container never supplies future Artifact IDs or writes the platform manifest.

### Pipeline execution is not Trial execution

The persistent execution hierarchy is:

```text
PipelineRun
  -> PipelineStageRun (container or outcome gate, singleton or shard)
       -> ExecutionAttempt (container stages only, at most three)
```

`PipelineRun` stores the immutable official-Recipe graph, resolved inputs,
budgets, identity, and lifecycle. A container `PipelineStageRun` freezes its
resolved bindings and `ExecutionSpecSnapshotV1` at the readiness transition;
all retries reuse those bytes and digests. `ExecutionAttempt` records each
claim and execution try. Outcome-gate StageRuns never create attempts.

Pipeline stages and attempts share Loom's controlled worker, capacity,
DockerDriver, Gateway, cancellation, Artifact, and observability primitives,
but they do not create synthetic Task, Batch, or Trial rows. Existing Batch and
Trial semantics remain unchanged.

### Canonical bytes define immutable identity

Strict schemas reject extra fields. Persisted canonical documents use RFC 8785
JCS followed by exactly one ASCII LF, and document digests cover those exact
bytes including the LF. Identity and idempotency preimages use raw JCS without
the LF. Implementations must not substitute `json.dumps(sort_keys=True)`,
delimiter concatenation, or a second canonicalizer.

### Merge and deployment remain separate authorities

#1211 owns schema models, state machines, identifiers, official Recipe
registration, persistence, and migration compatibility. #1212 owns the
standalone replay-safe controller, lease fencing, readiness freeze, fan-out,
hard budgets, retry, cancellation, and final projection. Neither issue owns
worker execution, public API, browser, live environment mutation, or production
activation. Those capabilities remain with their explicit dependent issues and
rollout gates.

A merged PR and green CI prove only the repository slice that they exercised.
They do not prove deployment, candidate installation, live data-path health,
staging acceptance, production activation, or #1232's fixed-candidate
acceptance.

## Alternatives considered

### Public raw graphs or an editable Recipe database

Rejected. Caller-controlled topology and execution fields would bypass reviewed
code, make digests insufficient as a trust boundary, and turn authorization
into field-by-field filtering. Versioned code-backed Recipes give one auditable
source for graph, renderer, parameter, and submission-policy behavior.

### Reuse Trial rows for every stage attempt

Rejected. Many Pipeline stages are transforms, gates, or data products rather
than benchmark tasks. Synthetic Trials would corrupt evaluation meaning and
couple Pipeline lifecycle changes to stable Batch/Trial semantics.

### Add specialized node kinds for each workflow

Rejected for v1. Custom stage behavior belongs in controlled containers and
automatic routing belongs in the closed outcome gate. Approval, publish,
selection, model-call, and algorithm-specific node types would prematurely
expand both the security boundary and the state machine.

### Let containers author fan-out manifests and Artifact IDs

Rejected. Containers cannot safely predict database identities or atomically
bind object commits, lineage, and expansion. The platform index-to-manifest
commit keeps identity assignment and commit authority in Loom.

## Consequences

- Pipeline APIs, controllers, workers, monitoring, and official Recipes must
  consume this single graph, state, binding, result, and digest vocabulary.
- New runnable v1 behavior requires a reviewed official Recipe registration;
  it cannot be introduced through request JSON or mutable catalog data.
- Pipeline capacity can reuse existing infrastructure while Pipeline records
  stay semantically separate from Trial records.
- Controller reconciliation, hard-budget accounting, container execution,
  Artifact commit, input materialization, cancellation/checkpointing, public
  API/CLI, UI, and live acceptance remain separately reviewable follow-on
  boundaries.
- A future need for another node or submission mode requires a new contract
  version or a superseding ADR, including compatibility and migration analysis.
