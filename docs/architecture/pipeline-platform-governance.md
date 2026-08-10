# Pipeline Platform Governance

Status: active Pipeline architecture guidance

Date: 2026-08-10

Current authority:

- Pipeline productionization Epic:
  [qianyi-sun/loom#1210](https://github.com/qianyi-sun/loom/issues/1210)
- Pipeline RunGraph v1 schema and persistence:
  [qianyi-sun/loom#1211](https://github.com/qianyi-sun/loom/issues/1211)
- Accepted architecture decision:
  [Pipeline Run Graph V1](adr/pipeline-run-graph-v1.md)

Tracking (pre-migration archive on the carinrc/loom tracker):

- Umbrella: carinrc#566
- Skill artifact and injection ADR: carinrc#567
- Typed artifact, lineage, and sharing ADR: carinrc#568
- TaskSet and data-production ADR: carinrc#569
- RunGraph scheduler ADR: carinrc#570
- Custom container security ADR: carinrc#571
- Workflow, recipe, and plugin ADR: carinrc#572

Implementation epics (historical archive):

- carinrc#573 — SkillMarkdown artifact and trial injection MVP
- carinrc#574 — typed artifact registry and lineage MVP
- carinrc#575 — user TaskSet registration MVP
- carinrc#576 — trajectory/completion export MVP
- carinrc#577 — RunGraph MVP
- carinrc#578 — custom container step MVP
- carinrc#579 — official SkillOpt recipe

## Scope

This document is the governance baseline for evolving Loom from an evaluation
platform into a reusable pipeline platform. It records the review standard for
Pipeline, data-production, workflow, Recipe, plugin, and Artifact proposals and
incorporates the accepted Pipeline RunGraph v1 direction from #1211.

Here, "v1" names the Pipeline graph contract version. It is not evidence that a
repository change has been deployed or accepted in a live environment. Existing
v1.0 evaluation release gates also remain unchanged unless their own accepted
issues explicitly amend them.

## Executive Summary

Loom should not integrate every research pipeline as a bespoke service feature.
Research teams will keep producing loops such as SkillOpt, SkillGrad, Best-of-N,
MCTS, self-play, hard-case mining, verifier calibration, PRM/RM trace
collection, curriculum generation, active evaluation, and regression gating.
Encoding each loop directly into service routes, database tables, and top-level
UI pages would create platform sprawl.

The long-term architecture uses a narrow waist:

```text
Research-specific pipelines
  SkillOpt / SkillGrad / MCTS / Best-of-N / self-play / PRM data / RL rollouts
        above
Workflow SDK + plugins + official Recipes
        above
Stable Loom primitives
  Task / Trial / Batch / PipelineRun / PipelineStageRun / ExecutionAttempt
  RunGraphSpec / Outcome gate / Trajectory / Metric / Artifact
  Model call / Sandbox / Lineage / SecretRef / ResourceProfile
        above
Infrastructure
  Queue / Worker / Postgres / MinIO-S3 / Gateway / Auth / Secrets / API / UI
```

The rule is:

> Platform owns execution, observability, lineage, artifacts, security, resource
> governance, scheduling, provider routing, and reproducibility. Research teams
> own algorithmic policy.

Evaluation and data production should share the same execution substrate:
registered tasks, model or agent inference, trajectories, metrics, artifacts,
lineage, and governance. Evaluation primarily emits scores and reports; data
production primarily emits reusable datasets, traces, labels, demonstrations,
synthetic examples, or other training artifacts.

## Ownership Boundary

| Loom core owns | Research pipeline, recipe, or plugin owns |
|---|---|
| Task, Trial, Batch, PipelineRun, StageRun, and Attempt lifecycles | Optimizer algorithm |
| Worker scheduling and resource controls | Search or sampling policy |
| Sandbox lifecycle and network policy | Candidate generation strategy |
| Provider routing through the LLM Gateway | Reflection or critique prompts |
| Secrets, credentials, and token boundaries | Paper-specific state updates |
| Trajectory, ATIF, metrics, and usage capture | Custom evidence summarization |
| Artifact storage, content hashes, and lineage | Data-selection heuristics |
| Redaction, safety state, sharing, and retention policy | Domain-specific interpretation |
| Generic Monitor, Run Library, and Pipeline surfaces | Optional plugin panels |

New research pipelines should first be expressed as recipes, plugins, or custom
container steps. Loom core changes are justified only when a reusable primitive
is missing or when security, lineage, scheduling, artifacts, or observability
would otherwise be inconsistent.

## What Belongs In Core

A capability belongs in Loom core only when most answers are yes:

| Question | Required answer for core admission |
|---|---|
| Is this needed by multiple independent pipeline families? | Yes |
| Does it affect execution correctness or reproducibility? | Yes |
| Does it affect security, secrets, permissions, or sharing? | Yes |
| Does it affect scheduling, quotas, resource limits, or cost attribution? | Yes |
| Does it define a stable platform abstraction rather than one algorithm? | Yes |
| Would duplicating it in plugins create inconsistent behavior or audit gaps? | Yes |
| Is the interface likely to remain stable for at least one major release? | Yes |

Examples that should be core:

- Trial execution, retry, cancellation, and result projection semantics.
- Batch fan-out and aggregation.
- Worker scheduling and resource allocation.
- Sandbox lifecycle, image/build controls, and network policy.
- Model provider routing, usage accounting, and rate-card attribution.
- Event-sourced trajectory format and ATIF projection.
- Typed artifact store, content hashes, lineage, and sharing policy.
- TaskSet, TaskSplit, immutable RunGraph specs, and Pipeline state machines.
- Custom container step execution boundary.
- Secret injection, redaction, and blocked-artifact enforcement.
- Generic Monitor, Run Library, Pipeline, Artifact detail, and comparison views.

Examples that should not be core:

- A specific SkillOpt reflection prompt.
- A SkillGrad momentum-memory update prompt.
- A particular MCTS node expansion strategy.
- A team-specific hard-case clustering method.
- A private benchmark preprocessing script.
- A custom reward shaping heuristic.
- A top-level UI page for one research group or one paper.

These should live in recipes, plugins, or custom container steps unless their
contracts generalize into the primitives above.

## Review Standard For Pipeline Proposals

Every proposal for a platform-level pipeline capability must answer these
questions before implementation begins:

1. Which pipeline families require this capability?
2. Is this a reusable primitive, an official recipe, a plugin, or a paper-specific algorithm?
3. What typed artifacts does it consume and produce?
4. What lineage must be recorded for clone, reuse, debug, and audit?
5. What security boundary does it affect: secrets, network, artifact exposure, or cross-team access?
6. What resource controls are required: CPU, memory, GPU, timeout, concurrency, and max cost?
7. Can it run as a custom container step with typed inputs and outputs?
8. Which UI can remain generic, and what optional plugin panel is actually needed?
9. What compatibility guarantees are required for schemas, recipes, and plugins?
10. What tests prove the primitive works independently of one recipe?

Core changes must include documentation under `docs/architecture/`, unit tests,
contract tests for public APIs, migration tests when schema-backed, CLI and
service-mode coverage when user-facing, security review when secrets or sharing
change, and operator notes when resource or deployment assumptions change.

Recipe changes must include a workflow spec version, example config, tiny smoke
fixture, declared input/output artifacts, expected metrics, compatibility
declaration, and owner.

Plugin changes must include a manifest, version, pinned image digest or package
hash, input/output artifact schema validation, resource profile, required
permissions, test fixture, and documented failure modes.

## Architecture Decision Status

The current Pipeline v1 decision is accepted in
[Pipeline Run Graph V1](adr/pipeline-run-graph-v1.md) and implemented under
#1211. It supersedes the historical raw-route and open-ended node-family
direction below. The remaining historical records continue to explain how the
broader platform program was decomposed.

Historical archive issue numbers below refer to the pre-migration
carinrc/loom tracker.

| Issue | Decision area | Blocks |
|---|---|---|
| qianyi-sun#1211 | official-Recipe-only Pipeline RunGraph v1, persistence, and state vocabulary | #1212, #8, #1214, #1216 |
| carinrc#567 | `SkillMarkdown` artifact and generic trial-time skill injection | carinrc#573, carinrc#579 |
| carinrc#568 | typed artifact base schema, lineage, sharing, redaction, retention | carinrc#573, carinrc#574, carinrc#576 |
| carinrc#569 | user TaskSet registration and data-production model | carinrc#575, carinrc#576 |
| carinrc#570 | RunGraph execution ownership and scheduler boundary | carinrc#577, carinrc#578, carinrc#579 |
| carinrc#571 | custom container step security model | carinrc#578 |
| carinrc#572 | workflow specs, recipes, and plugin versioning | carinrc#579 |

## Accepted Pipeline RunGraph V1

`Batch -> Trial` is sufficient for flat evaluation. Iterative research loops
require graph-level orchestration:

```text
rollout -> aggregate evidence -> produce candidate artifact -> validate
  -> accept or reject -> repeat or export
```

The v1 product surface is Pipeline. A run is a `PipelineRun` whose immutable
`RunGraphSpecV1` is resolved from a repository-owned, versioned official Recipe,
declared parameters, and committed Artifact inputs. The public REST namespace is
`/api/v1/pipeline-runs` and the CLI namespace is `loom pipeline`; #1216 owns
those endpoints. There is no `/api/v1/run-graphs` alias and no public raw-graph
submission.

The graph is acyclic and has exactly two node kinds:

- a digest-pinned `container` node with typed inputs and outputs, bounded
  attempts, fixed resource/network contracts, and optional manifest fan-out;
- an automatic `outcome` gate that routes only on a container's closed
  `domain_outcome` vocabulary and creates no worker Attempt.

V1 does not include cycles, loops, recursion, manual approval, external publish,
arbitrary expressions, caller-supplied templates, or runtime graph mutation.
Containers cannot choose images, workers, routes, secrets, Artifact IDs, or
object-store locations from request data.

Manifest fan-out keeps expansion and commit authority in Loom. A container may
write a bounded platform-fanout index and dynamic item directories; Loom
preallocates Artifact identities, validates the complete set, and atomically
synthesizes the typed fan-out manifest. Consumers bind committed typed Artifacts
and never list object-store prefixes.

The execution ledger is `PipelineRun -> PipelineStageRun ->
ExecutionAttempt`. A gate StageRun has no Attempt. Container StageRuns freeze
their resolved execution snapshot at readiness, and retries reuse it. Pipeline
execution shares controlled worker, capacity, DockerDriver, Gateway,
cancellation, Artifact, and observability infrastructure with evaluation, but
StageRuns and ExecutionAttempts do not manufacture Task, Batch, or Trial rows.
Existing Batch/Trial semantics are unchanged.

The full immutable model, canonical-byte rules, persistence constraints, and
rejected alternatives are recorded in
[Pipeline Run Graph V1](adr/pipeline-run-graph-v1.md).

## Typed Artifact Direction

Pipelines must not pass only file paths or ad hoc JSON. Important outputs should
be typed artifacts with content hash, schema version, lineage, storage pointer,
safety state, redaction state, sharing policy, and retention metadata.

The base contract and migration requirements are decided in carinrc#568
(historical archive) and documented in
[adr-typed-artifacts-lineage-sharing.md](adr/typed-artifacts-lineage-sharing.md).
Initial artifact families should cover trajectory bundles, completion sets,
TaskSets, TaskSplits, skill markdown, workflow specs, verifier replay bundles,
debug bundles, metrics, evidence bundles, and exports.

## Skill Artifact Direction

SkillOpt and SkillGrad should not introduce `skillopt_runs` or
`skillgrad_runs` tables before the generic artifact and RunGraph layers prove
insufficient. The first reusable primitive is a versioned skill artifact that a
trial can reference, authorize, materialize, inject, and record in output
provenance.

The skill contract is decided in carinrc#567 (historical archive) and
documented in [adr-skill-artifact-injection.md](adr/skill-artifact-injection.md).
The first implementation epic is carinrc#573 (historical archive).

## Data Production Direction

Data production is a sibling of evaluation, not a separate engine. Both can be
represented as:

```text
TaskSet or TaskSplit
  + provider/model/agent config
  + optional prompt, skill, or tooling artifacts
  + optional verifier, judge, or filter
  -> Batch or official-Recipe PipelineRun
  -> trajectories, metrics, model calls, and typed artifacts
```

Human review may remain an external product workflow, but it is not a v1
RunGraph node or gate. BEHAVIOR Pipeline v1 uses automatic outcome gates only.

Evaluation usually emits `TrajectoryBundle`, `MetricTable`,
`EvaluationReport`, `FailureSet`, and gate decisions. Data production usually
emits `CompletionTable`, `TrajectoryDataset`, `DemonstrationDataset`,
`PreferencePairDataset`, `ReasoningTraceDataset`, `VerifierLabelDataset`,
`DataQualityReport`, and `TrainingDataExport`.

TaskSet registration and the current trajectory/data-production export substrate
are implemented under #11 and #10. The earlier carinrc#569, carinrc#575, and
carinrc#576 tickets are historical design and migration references, not pending
implementation carriers. User TaskSets follow the current `internal_trusted`
boundary: transform declarations fail closed and do not imply arbitrary
untrusted-code isolation.

## Anti-patterns

Avoid paper-specific service routes:

```text
POST /api/v1/skillopt/iterate
POST /api/v1/skillgrad/momentum-update
```

Prefer the accepted generic Pipeline and Artifact routes:

```text
POST /api/v1/pipeline-runs
POST /api/v1/pipeline-stage-runs/{stage_run_id}/retry
GET /api/v1/artifacts/{id}/lineage
```

The Pipeline submit body identifies an official Recipe and its declared
parameters and Artifact inputs. Treating the body as a raw graph, adding a
`/run-graphs` alias, or accepting execution overrides is an anti-pattern.

Avoid early pipeline-specific tables:

```text
skillopt_rejected_edits
skillgrad_momentum_memories
mcts_expansion_nodes
```

Prefer typed artifacts first:

```text
OptimizerState
PatchSet
MomentumMemory
SearchTree
```

Avoid hidden external state, unmanaged provider calls, UI forking, and silent
fallbacks that run without a requested skill, verifier, model, or artifact.
Preflight should fail with actionable diagnostics.

## Delivery And Authority Guardrails

Current implementation follows #1210's dependency ledger while treating
completed foundations as existing platform contracts:

1. Decide skill injection and typed artifact contracts (carinrc#567, carinrc#568).
2. Implement `SkillMarkdown` injection and typed artifact registry MVP (carinrc#573, carinrc#574).
3. Maintain the implemented TaskSet registration and trajectory/data-production
   export contracts (#11 and #10; historical carinrc#569, carinrc#575, and
   carinrc#576).
4. Keep #1211 as the single v1 graph, state, binding, result, canonicalization,
   and persistence authority; dependent issues may reference it but not invent
   parallel vocabularies.
5. Add controller reconciliation and budgets (#1212), controlled container
   execution (#8), Artifact commit (#1214), materialization (#1240),
   checkpoint/cancellation (#1215), API/CLI (#1216), and monitor UI (#1217) at
   their explicit boundaries.
6. Deliver official BEHAVIOR Recipes and fixed-candidate acceptance only after
   their declared dependencies merge and pass their own acceptance.

Each step should preserve the narrow-waist rule: Loom core gains reusable
execution, artifact, lineage, security, and observability primitives; research
policy stays in recipes, plugins, or controlled custom steps.

Repository merge authority and live-environment authority are separate. A
merged Pipeline PR or green CI does not authorize installation, deployment,
staging mutation, production activation, or #1232 live acceptance. Those claims
require the exact candidate, action authorization, rollout procedure, and live
read-back required by the owning issue.
