# ADR: Skill Artifact and Injection Contract

Status: accepted for post-v1 planning

Date: 2026-06-26

Tracking (pre-migration archive on the carinrc/loom tracker): parent
carinrc#566; decision issue carinrc#567; depends on typed artifact policy
carinrc#568; implementation epic carinrc#573; official SkillOpt recipe epic
carinrc#579.

## Context

SkillFlow and SkillLearnBench already require task-local skill materialization
for v1.0 benchmark execution, but learned skills are not yet a durable Loom
artifact that users can create, share, select, inject, and audit. SkillOpt and
SkillGrad need skill version graphs, validation metrics, source rollouts,
redaction state, and reusable trial inputs.

The platform must support skill reuse without adding SkillOpt-specific service
routes, benchmark-specific skill tables, or one-off agent code paths. Skill
inputs should be a generic trial/run input that agent adapters can consume
through declared runtime contracts.

## Decision

Use `skill_markdown` as the first durable learned-skill artifact type. It is a
typed artifact that references a single immutable `SKILL.md` file plus metadata.
Future `skill_package` support can extend the same contract for multi-file
skills, resources, and structured metadata.

A trial or RunGraph rollout step may reference one or more safe skill artifacts.
The platform resolves and authorizes those references during preflight,
materializes them into the sandbox under a stable directory, and injects them
through the selected agent's declared skill injection mode.

## SkillMarkdown Artifact Extension

`skill_markdown` uses the base typed artifact schema from
[adr-typed-artifacts-lineage-sharing.md](typed-artifacts-lineage-sharing.md).
Its type-specific metadata must include:

```json
{
  "artifact_type": "skill_markdown",
  "artifact_schema_version": "1.0",
  "metadata": {
    "skill": {
      "name": "coding-debug-skill",
      "skill_version": "v7",
      "entrypoint": "SKILL.md",
      "format": "markdown",
      "source_runs": [
        {
          "run_graph_id": "run_graph_...",
          "batch_id": "batch_...",
          "trial_ids": ["trial_..."]
        }
      ],
      "generating_recipe": {
        "name": "skillopt",
        "version": "0.1.0",
        "spec_hash": "sha256:..."
      },
      "task_scope": {
        "benchmark_ids": ["swe-bench-verified"],
        "task_set_artifact_ids": [],
        "task_split_artifact_ids": [],
        "tags": ["coding"]
      },
      "author": {
        "team_id": "team_...",
        "user_id": "user_..."
      },
      "evaluator_provenance": {
        "metric_name": "resolved",
        "score": 0.47,
        "validation_batch_id": "batch_...",
        "validation_task_split_artifact_id": "artifact_..."
      },
      "compatibility": {
        "agent_names": ["codex", "claude-code", "litellm"],
        "injection_modes": ["skills_directory", "instruction_prefix"],
        "min_loom_version": "1.1.0",
        "max_loom_version": "<2.0.0"
      },
      "safety_classification": "safe|unsafe|needs_review",
      "redaction_notes": null
    }
  }
}
```

The artifact body is the immutable `SKILL.md` content. `content_hash` covers the
exact bytes that will be injected.

## Lifecycle States

The artifact base fields determine reuse:

| Lifecycle | Base field state |
|---|---|
| Imported or generated | `share_status=pending_scan`, `safety_state=unknown` |
| Owner-team usable after local validation | `visibility=team`, `safety_state=safe`, `redaction_state=not_required` or `redacted` |
| Org-reusable through Run Library | `visibility=org`, `share_status=shared`, `safety_state=safe` |
| Blocked | `share_status=blocked` or `safety_state=unsafe` |
| Deprecated | `metadata.skill.deprecated=true` with replacement provenance |
| Archived | Retention policy prevents new reuse but preserves audit metadata |

Blocked or unsafe skills must not be injected into cross-team trials. If a skill
was requested and cannot be authorized, scanned, fetched, or materialized, the
trial fails preflight instead of silently running without the skill.

## Trial Request Shape

Trial and batch creation should accept skill references as generic trial config,
not benchmark-specific fields:

```json
{
  "trial_config": {
    "agent_name": "codex",
    "agent_model": {
      "provider_connection_id": "provider_...",
      "provider_model_id": "claude-sonnet-4-6"
    },
    "skill_artifacts": [
      {
        "artifact_id": "artifact_skill_v7",
        "alias": "coding-debug-skill",
        "required": true,
        "injection_mode": "agent_default"
      }
    ]
  }
}
```

Rules:

- `artifact_id` is required.
- `alias` becomes the materialized directory name and must be stable,
  filesystem-safe, and unique within the trial.
- `required=true` means preflight fails if the skill cannot be injected.
- `injection_mode=agent_default` lets the adapter choose from its declared
  modes; callers may request a specific supported mode for reproducibility.
- The service resolves cross-team Run Library access before a trial is queued.
- The worker verifies the resolved content hash before materialization.

## Materialization Contract

Resolved skill artifacts are materialized read-only under:

```text
/workspace/.loom/skills/<alias>/
  SKILL.md
  manifest.json
```

`manifest.json` records the artifact id, alias, content hash, schema version,
owner team label, source provenance summary, injection mode, and redaction
state. It must not contain raw provider credentials, team tokens, signed URLs,
or internal object-store URLs.

The trial context passes the concrete skills directory to the agent execution
path. Existing task, batch, and trial code remains generic: it resolves typed
artifact inputs, materializes them, and records references. It does not branch
on SkillOpt, SkillGrad, SkillFlow, SkillLearnBench, or any benchmark name.

## Agent Injection Modes

Agent adapters declare supported skill modes:

| Mode | Behavior |
|---|---|
| `none` | Agent cannot consume skills; preflight fails when a required skill is requested |
| `instruction_prefix` | Platform prepends or appends selected skill text to the agent instruction |
| `skills_directory` | Platform exposes `/workspace/.loom/skills` and tells the agent where to read it |
| `project_instruction_file` | Platform writes or references a project-level instruction file derived from the skill |
| `agent_native` | Adapter maps the skill directory into the agent's native skill mechanism |
| `custom_adapter` | Adapter owns a documented, tested conversion from skill artifact to invocation |

Direct chat or LiteLLM-style agents can use `instruction_prefix`. CLI coding
agents such as `codex` or `claude-code` should prefer `skills_directory`,
`project_instruction_file`, or `agent_native` when their runtime supports it.
Agents that do not use a model, such as `oracle`, normally declare `none`.

## Output Provenance

Every trial that uses skills must record:

- skill artifact ids;
- aliases;
- content hashes;
- schema versions;
- injection modes;
- authorization source: owner-team or Run Library shared artifact;
- materialization path;
- adapter-declared compatibility mode;
- any preflight failure reason.

ATIF should include skill references in the agent or run-context section. Run
Library detail should show the same references so later users can understand
which skill influenced the run.

## Sharing, Redaction, And Cloning

Skill sharing follows the typed artifact policy:

- Owner teams can inspect their own pending or blocked skills through owner-team
  routes when policy permits.
- Other teams can discover and reuse only `visibility=org`,
  `share_status=shared`, `safety_state=safe` skill artifacts.
- Blocked skills can show a safe blocked reason cross-team but cannot be
  downloaded, cloned, or injected.
- Clone/reuse preserves source artifact id, source owner-team label, content
  hash, schema version, and validation provenance.
- Destination teams must choose their own provider/model/agent configuration.
  Source provider credentials or API tokens are never copied.

If a copied or derived skill artifact is edited, the new artifact receives a new
content hash and a `patched_from` or `cloned_from` parent edge.

## SkillOpt-style Example

An official SkillOpt recipe can stay outside core execution policy:

```text
initial SkillMarkdown
  -> rollout_batch on train sample
  -> TrajectoryBundle
  -> EvidenceBundle transform
  -> custom_container optimizer plugin
  -> candidate SkillMarkdown + PatchSet
  -> validation rollout_batch on selection split
  -> MetricTable
  -> gate strict_improvement
  -> select accepted candidate or previous best
  -> final rollout_batch
  -> reusable best SkillMarkdown + EvaluationReport
```

Core owns the rollouts, artifacts, lineage, safety state, Gateway usage, and
RunGraph visibility. The SkillOpt plugin owns the reflection prompt, edit
policy, rejected-edit buffer, and optimizer state update.

## Non-SkillOpt Example

A team imports a hand-authored "repo conventions" `SKILL.md` for coding-agent
experiments:

1. The user creates a `skill_markdown` artifact through CLI/API.
2. Loom stores the content hash and marks it `pending_scan`.
3. After scan, the owner marks it `visibility=org`, `share_status=shared`.
4. Another team selects the safe skill from Run Library and submits a
   `codex` SWE-Bench evaluation batch with `skill_artifacts`.
5. The worker materializes the skill under `/workspace/.loom/skills/repo-conventions/`.
6. The `codex` adapter injects it through its declared mode.
7. The completed trials record skill ids and hashes in ATIF and Run Library.

No benchmark-specific skill table or special service route is required.

## Non-goals

- No SkillOpt optimizer implementation in this ADR.
- No benchmark-specific skill tables.
- No UI-first workflow before API and CLI contracts are clear.
- No promise that every displayed agent can consume skills. Unsupported agents
  must fail preflight when skills are required.

## Consequences

This contract makes skill reuse a normal artifact input. It adds preflight and
adapter contract requirements, but avoids special-case SkillOpt infrastructure.
It also creates a safe path for human-authored skills, learned skills, and
future structured skill packages to share the same lineage and Run Library
policy.
