# Pipeline Platform Governance

Status: post-v1 architecture guidance

Date: 2026-06-26

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

This document is a post-v1 governance baseline for turning Loom from an
evaluation platform into a reusable pipeline platform. It records the review
standard for pipeline, data-production, workflow, recipe, plugin, and artifact
proposals.

This is not part of the v1.0 release gate. It must not expand the v1.0 scope
(historical: carinrc#467, carinrc#82). v1.0 remains focused on
AI/API-first evaluation, supported benchmark execution, user-visible debug
evidence, score credibility, and release-critical monitoring surfaces.

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
Workflow SDK + plugins + official recipes
        above
Stable Loom primitives
  Task / Trial / Batch / RunGraph / Step / Trajectory / Metric / Artifact
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
| Task, trial, batch, and future RunGraph execution | Optimizer algorithm |
| Worker scheduling and resource controls | Search or sampling policy |
| Sandbox lifecycle and network policy | Candidate generation strategy |
| Provider routing through the LLM Gateway | Reflection or critique prompts |
| Secrets, credentials, and token boundaries | Paper-specific state updates |
| Trajectory, ATIF, metrics, and usage capture | Custom evidence summarization |
| Artifact storage, content hashes, and lineage | Data-selection heuristics |
| Redaction, safety state, sharing, and retention policy | Domain-specific interpretation |
| Generic Monitor, Run Library, and RunGraph surfaces | Optional plugin panels |

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
- TaskSet, TaskSplit, workflow spec, and RunGraph state machines after ADRs.
- Custom container step execution boundary.
- Secret injection, redaction, and blocked-artifact enforcement.
- Generic Monitor, Run Library, RunGraph, artifact detail, and comparison views.

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

## Required Post-v1 ADRs

The following decisions must be settled before implementation epics move from
planning to build:

Historical archive issue numbers below refer to the pre-migration
carinrc/loom tracker.

| Issue | Decision area | Blocks |
|---|---|---|
| carinrc#567 | `SkillMarkdown` artifact and generic trial-time skill injection | carinrc#573, carinrc#579 |
| carinrc#568 | typed artifact base schema, lineage, sharing, redaction, retention | carinrc#573, carinrc#574, carinrc#576 |
| carinrc#569 | user TaskSet registration and data-production model | carinrc#575, carinrc#576 |
| carinrc#570 | RunGraph execution ownership and scheduler boundary | carinrc#577, carinrc#578, carinrc#579 |
| carinrc#571 | custom container step security model | carinrc#578 |
| carinrc#572 | workflow specs, recipes, and plugin versioning | carinrc#579 |

## RunGraph Direction

`Batch -> Trial` is sufficient for flat evaluation. Iterative research loops
require graph-level orchestration:

```text
rollout -> aggregate evidence -> produce candidate artifact -> validate
  -> accept or reject -> repeat or export
```

A future RunGraph should own generic step status, dependencies, retries,
artifacts, metrics, logs, costs, and provenance. Node execution should map onto
existing trial, batch, artifact, and worker boundaries instead of creating a
parallel lifecycle. The MVP decision is tracked by carinrc#570 (historical
archive).

Required generic step families are expected to include:

- `rollout_batch`: run Loom trials over a TaskSplit with agent/model/provider config.
- `aggregate_metrics`: aggregate trial metrics into summaries.
- `artifact_transform`: convert typed artifacts without changing platform state directly.
- `custom_container`: run controlled research code with typed inputs and outputs.
- `gate`: produce a structured decision from metrics or artifacts.
- `select`: choose one artifact based on a gate decision.
- `model_call_batch`: run controlled non-agent model calls through the Gateway.

## Typed Artifact Direction

Pipelines must not pass only file paths or ad hoc JSON. Important outputs should
be typed artifacts with content hash, schema version, lineage, storage pointer,
safety state, redaction state, sharing policy, and retention metadata.

The base contract and migration requirements are decided in carinrc#568
(historical archive) and documented in
[adr-typed-artifacts-lineage-sharing.md](adr-typed-artifacts-lineage-sharing.md).
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
documented in [adr-skill-artifact-injection.md](adr-skill-artifact-injection.md).
The first implementation epic is carinrc#573 (historical archive).

## Data Production Direction

Data production is a sibling of evaluation, not a separate engine. Both can be
represented as:

```text
TaskSet or TaskSplit
  + provider/model/agent config
  + optional prompt, skill, or tooling artifacts
  + optional verifier, judge, filter, or human review
  -> Batch or RunGraph
  -> trajectories, metrics, model calls, and typed artifacts
```

Evaluation usually emits `TrajectoryBundle`, `MetricTable`,
`EvaluationReport`, `FailureSet`, and gate decisions. Data production usually
emits `CompletionTable`, `TrajectoryDataset`, `DemonstrationDataset`,
`PreferencePairDataset`, `ReasoningTraceDataset`, `VerifierLabelDataset`,
`DataQualityReport`, and `TrainingDataExport`.

TaskSet registration and data-production behavior are deferred to the
post-v1 epics carinrc#569, carinrc#575, and carinrc#576 (historical archive).

## Anti-patterns

Avoid paper-specific service routes:

```text
POST /api/v1/skillopt/iterate
POST /api/v1/skillgrad/momentum-update
```

Prefer generic routes after the relevant ADRs are accepted:

```text
POST /api/v1/run-graphs
POST /api/v1/run-steps/{id}/retry
GET /api/v1/artifacts/{id}/lineage
```

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

## Roadmap Guardrails

Post-v1 implementation should proceed in dependency order:

1. Decide skill injection and typed artifact contracts (carinrc#567, carinrc#568).
2. Implement `SkillMarkdown` injection and typed artifact registry MVP (carinrc#573, carinrc#574).
3. Decide and implement TaskSet/data-production registration (carinrc#569, carinrc#575, carinrc#576).
4. Decide and implement RunGraph and custom container boundaries (carinrc#570, carinrc#571, carinrc#577, carinrc#578).
5. Decide recipe/plugin versioning and implement the first official SkillOpt recipe (carinrc#572, carinrc#579).

Each step should preserve the narrow-waist rule: Loom core gains reusable
execution, artifact, lineage, security, and observability primitives; research
policy stays in recipes, plugins, or controlled custom steps.
