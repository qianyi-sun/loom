# ADR: Typed Artifact Schema, Lineage, and Sharing

> Archived decision record. Current artifact behavior is documented in
> `docs/architecture/run-library.md` and the pipeline reference.

Status: accepted and implemented for the base registry, lineage, safety, and reuse contract

Date: 2026-06-26

Tracking (pre-migration archive on the carinrc/loom tracker): parent
carinrc#566; decision issue carinrc#568; related implementation carinrc#574;
related skill artifact work carinrc#567 and carinrc#573; related
data-production work carinrc#569 and carinrc#576.

Implementation note (2026-08-01): #12 completed the typed artifact registry and
Run Library policy, and #10 completed the supported trajectory/ATIF delivery
export surface. Additional dataset artifact types remain extensions of this
contract rather than missing base-registry acceptance.

## Context

Current Loom runs already produce trajectories, ATIF projections, collected
artifacts, verifier output, diagnosis, debug evidence, and Run Library metadata.
Those outputs are indexed enough for v1.0 evaluation and release debugging, but
post-v1 pipelines need stronger contracts:

- artifacts must be reusable as inputs to later trials, batches, and RunGraphs;
- clone and reuse must preserve provenance without granting access to secrets;
- unsafe content must not become cross-team downloadable just because the parent
  run is visible in the Run Library;
- data-production outputs must carry schema, source, model, prompt/skill, and
  retention metadata;
- plugins and recipes need type checks before they consume or emit artifacts.

## Decision

Introduce a base typed artifact contract before building post-v1 RunGraph,
SkillMarkdown, TaskSet, or data-production implementations.

The base artifact is a metadata record plus a storage pointer. It does not force
one object-store backend; storage remains behind Loom's object-store interface.
All important reusable outputs should become typed artifacts. Existing run and
trial artifacts can be migrated into this contract incrementally.

## Initial Artifact Types

Use lower-snake-case names in API/storage metadata and human names in UI:

| Type | Purpose |
|---|---|
| `trajectory` | One trial's event-sourced JSONL trajectory |
| `atif_projection` | One trial's ATIF projection |
| `trajectory_bundle` | Manifest of many trajectories and ATIF projections |
| `completion_set` | Model outputs over tasks before export or filtering |
| `task_set` | Registered user or platform task collection |
| `task_split` | Reproducible subset or partition derived from a TaskSet |
| `skill_markdown` | Single-file learned or hand-authored skill |
| `workflow_spec` | Versioned workflow or recipe spec |
| `verifier_replay` | Inputs/outputs needed to replay scorer behavior |
| `debug_bundle` | Redacted diagnosis/debug evidence for a run, batch, or trial |
| `metric_table` | Trial-level and aggregate metrics |
| `evidence_bundle` | Optimizer-friendly projection of trajectories and failures |
| `training_data_export` | Final export bundle such as JSONL, Parquet, or HF layout |

Additional types must follow the extension pattern below. They should not add
new tables unless query or scale requirements prove the generic registry is
insufficient.

## Base Schema

Every artifact record must include these base fields:

```json
{
  "id": "artifact_...",
  "artifact_type": "skill_markdown",
  "artifact_schema_version": "1.0",
  "name": "coding-debug-skill",
  "team_id": "team_...",
  "project_id": null,
  "created_by": {
    "kind": "trial|batch|run_graph_step|manual_import|system_backfill",
    "run_graph_id": null,
    "run_step_id": null,
    "batch_id": null,
    "trial_id": null,
    "user_id": "user_..."
  },
  "parents": [
    {
      "artifact_id": "artifact_parent",
      "relation": "produced_from"
    }
  ],
  "content_hash": "sha256:...",
  "storage": {
    "backend": "object_store",
    "bucket": "artifacts",
    "key": "team-id/artifact-id/object",
    "media_type": "application/json",
    "size_bytes": 12345
  },
  "visibility": "private|team|org",
  "share_status": "pending_scan|shared|blocked|owner_only",
  "redaction_state": "not_required|pending|redacted|blocked",
  "safety_state": "unknown|safe|unsafe|policy_blocked",
  "blocked_reason": null,
  "retention": {
    "class": "temporary_intermediate|owner_only_debug|shared_reusable|release_evidence|training_data_export",
    "expires_at": null
  },
  "provenance": {
    "loom_commit": "367d17c9",
    "recipe": null,
    "plugin": null,
    "provider_connection_id": null,
    "provider_type": null,
    "model": null,
    "agent": null,
    "task_set_artifact_id": null,
    "task_split_artifact_id": null,
    "source_trial_ids": []
  },
  "metadata": {},
  "created_at": "2026-06-26T00:00:00Z"
}
```

Mandatory fields for all artifact types:

- `id`, `artifact_type`, and `artifact_schema_version`;
- `team_id` as the owner boundary;
- `created_by` with the best available run, trial, batch, step, user, or
  backfill source;
- `content_hash`;
- `storage` pointer;
- `visibility`, `share_status`, `redaction_state`, and `safety_state`;
- `retention`;
- `provenance`;
- `created_at`.

## Per-type Extension Pattern

Type-specific fields live under `metadata` or a typed API response extension.
The base schema remains stable and indexable; type schemas evolve
additively within a major version.

Example `skill_markdown` extension:

```json
{
  "metadata": {
    "skill": {
      "entrypoint": "SKILL.md",
      "skill_version": "v7",
      "task_scope": {
        "benchmark_ids": ["swe-bench-verified"],
        "task_set_artifact_ids": []
      },
      "validation": {
        "metric_name": "resolved",
        "score": 0.47,
        "validation_run_graph_id": "run_graph_..."
      }
    }
  }
}
```

Example `trajectory_bundle` extension:

```json
{
  "metadata": {
    "trials": [
      {
        "trial_id": "trial_...",
        "task_id": "humaneval/HumanEval/0",
        "trajectory_artifact_id": "artifact_...",
        "atif_artifact_id": "artifact_...",
        "state": "succeeded",
        "aggregate_reward": 1.0
      }
    ],
    "score_semantics": "mean_task_reward"
  }
}
```

## Lineage Model

Lineage must support clone, reuse, audit, and debugging. The artifact registry
must record both direct parent edges and execution provenance:

- Parent edge relations: `produced_from`, `selected_from`, `patched_from`,
  `validated_by`, `summarized_from`, `exported_from`, `cloned_from`, and
  `reused_as_input`.
- Execution source: run graph, run step, batch, trial, or manual import.
- Content source: object hash, schema version, and storage pointer.
- Model source: provider type, provider connection id as metadata only, model
  id, agent, prompt/skill artifacts, and gateway usage references when present.
- Task source: TaskSet, TaskSplit, benchmark id, task ids, and sampling seed
  when applicable.
- Recipe/plugin source: name, version, digest, config hash, and compatibility
  declaration when applicable.

Clone/reuse creates destination-team records that point back to source
artifacts. It must not copy provider secrets, API tokens, team membership,
private run state, or raw unsafe object bodies.

## Sharing And Access Rules

Team remains the boundary for execution, cost, credentials, API tokens, and
membership. The org-wide Run Library is the only cross-team sharing surface for
completed work.

Artifact metadata can appear in another team's Run Library view only when:

- the parent run, batch, trial, or future RunGraph is visible to the org;
- the artifact `visibility` is `org`;
- the artifact `share_status` is `shared`;
- the artifact `safety_state` is `safe`;
- the artifact `redaction_state` is `not_required` or `redacted`;
- the parent execution is terminal enough to inspect.

Artifact content can be downloaded or reused across teams only when all metadata
conditions are true and the access happens through authenticated Loom service
routes. Raw object-store URLs, signed URLs, internal service URLs, provider
secrets, and team tokens must not be exposed.

Unsafe or blocked artifacts:

- remain visible to the owner team through normal diagnostic routes when policy
  permits;
- may appear cross-team only as redacted metadata with a safe blocked reason;
- cannot be downloaded, cloned, or reused across teams;
- cannot be mounted into a new trial or RunGraph step outside the owner team;
- must preserve enough provenance for audit without exposing raw unsafe content.

Platform admins may inspect metadata for operations and incident response, but
admin access is not a reason to weaken ordinary Run Library policy.

## Clone And Reuse Semantics

Clone/reuse always creates a new destination-team execution request or record.
The destination record stores:

- source artifact id;
- source owner-team label;
- source content hash and schema version;
- source visibility/share/safety state at the time of clone;
- relation edge such as `cloned_from` or `reused_as_input`;
- destination provider/model/agent configuration chosen by the destination team.

The destination team must supply its own provider connection and credentials.
Provider connection ids from the source can be shown as redacted metadata for
compatibility decisions, but they are never copied or used to mint credentials.

If a source artifact is later blocked, future clone/reuse attempts must fail.
Existing destination executions keep their recorded provenance and should be
handled by retention or incident policy.

## Migration Requirements

The first typed-artifact implementation should migrate current outputs without
breaking v1.0 APIs:

- Backfill one typed record for each indexed trajectory and ATIF object in
  `trials.trajectory_index`.
- Backfill collected trial artifacts from the current artifact list with their
  object key, size, share status, and safe blocked reason.
- Compute or store content hashes when the object body is available; mark
  unavailable legacy content as `content_hash` pending rather than fabricating
  a hash.
- Default legacy artifact `share_status` to existing values where present and
  otherwise `pending_scan`.
- Keep owner-team routes working while adding Run Library typed-artifact views.
- Preserve existing batch/trial `source_provenance` and convert it into artifact
  parent edges where source ids are known.
- Add tests proving unsafe legacy artifacts do not become cross-team
  downloadable after migration.

## Non-goals

- No quota or billing enforcement in this ADR.
- No hard dependency on MinIO, S3, SeaweedFS, or any single storage backend.
- No broad UI redesign. UI changes should follow the contract, but the ADR is
  about metadata, lineage, and access policy.
- No recipe- or pipeline-specific database tables.

## Consequences

This ADR makes typed artifacts the durable interchange format for post-v1
pipelines. It increases metadata requirements before artifacts become reusable,
but it avoids later security and provenance gaps. Recipes and plugins can move
faster because they validate input/output types without owning storage,
authorization, redaction, retention, or lineage.
