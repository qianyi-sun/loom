# User-brought TaskSets

Status: partially implemented. TaskSet intake/list/status/materialization
fixtures exist, and run creation now accepts team-visible TaskSets through
`task_filter.task_set_id` / `task_filter.task_set_ids` and
`loom eval batch create --task-set`. Row-oriented manifests and uploaded
bundle archives are supported by the materializer; full TaskSet management UI
and live staging validation are still tracked separately. This updates the
earlier user-brought-benchmarks design by separating the
user-facing object from the platform benchmark catalog. It complements
[`benchmark-adapter.md`](benchmark-adapter.md) for native platform benchmarks
and [`sandbox-isolation.md`](sandbox-isolation.md) for verifier and transform
trust boundaries. The release-level rule is recorded in
[`adr/v1-workload-trust-contract.md`](adr/v1-workload-trust-contract.md).

## Goal

Let a Loom team submit its own task collection without writing a Python
`BenchmarkAdapter` subclass, without operator file-system access, and without a
separate publish-and-register dance.

The user-facing object is a **TaskSet**: a team-owned collection of task
bundles that can be used for trajectory/data-production runs, evaluation runs,
or both. Native platform benchmarks remain first-class and keep their existing
benchmark semantics. A user TaskSet becomes evaluation-ready only when it
declares verifier/scoring configuration.

v1 is **team-private**: the owning team is the only consumer. Loom's existing
auth unit is the team (tokens scope to `team_id`, batches and runs are
team-scoped); user-brought TaskSets follow the same pattern. Cluster-wide
sharing and per-user-or-team ACLs are explicitly deferred to a follow-up; the
data model leaves the slot for that future without committing to a UI for it
now.

## Terminology

- **Benchmark**: a platform-native evaluation collection with scoring semantics.
  Existing first-party/system benchmarks, `/api/v1/benchmarks`,
  `loom benchmarks ...`, and the Benchmarks UI remain for this concept.
- **TaskSet**: a team-owned collection of materialized task bundles. A TaskSet
  can be trajectory-only, evaluation-ready, or both.
- **Evaluation-ready TaskSet**: a TaskSet with verifier/scoring configuration.
  It can be offered in evaluation creation flows, but it is still owned and
  managed through TaskSet APIs and UI.
- **Data-production run**: a run that executes tasks to produce trajectories,
  artifacts, and completions for training or analysis. It does not require a
  verifier or benchmark score.

## Non-goals (v1)

- Replacing native platform benchmarks or changing first-party benchmark
  adapter intake. First-party intake remains the entry-point + catalog flow in
  [`benchmark-adapter.md`](benchmark-adapter.md).
- Cross-team sharing of any kind (team-private only).
- Per-individual-user scoping inside a team. Within an owning team, every
  member can see and run the TaskSet, the same way teams see batches and runs
  today.
- Browser zip upload as the normal path for first-party benchmarks. User
  TaskSet upload is a separate user-owned intake surface.
- Letting users ship a full `BenchmarkAdapter` Python package. Conversion is
  declarative in v1. User-provided `transform()` execution is unavailable in
  the v1 `internal_trusted` workload mode.
- VM-backed task shapes (OSWorld, WebArena). These remain first-party-only
  until the cluster has a provisioned VM substrate.
- A curation / review pipeline. Owner-private removes the need.

## Design principles

- **User language is TaskSet-first.** Users submit, list, rebuild, delete, and
  run TaskSets. "Benchmark" appears only when the user is selecting a
  platform-native evaluation benchmark or an evaluation-ready TaskSet.
- **Benchmarks remain intact.** Existing benchmark APIs, CLI commands, system
  catalog rows, and first-party adapter behavior are preserved. User TaskSets do
  not require renaming native benchmarks or weakening score semantics.
- **Divergence stops at the task bundle.** User TaskSets materialize into the
  canonical `task.toml` bundle layout. The worker execution path can consume the
  resulting tasks without knowing whether they came from a system benchmark or a
  user TaskSet.
- **Evaluation is a capability, not the object identity.** A TaskSet with a
  verifier is evaluation-ready; a TaskSet without one is still valid for
  trajectory/data-production runs.
- **Trust boundary = existing trial sandbox.** Verifier scripts ride the same
  isolation primitives first-party verifiers already use. v1 has no trusted
  transform-execution boundary: any manifest that declares `transform` fails
  materialization with `transform_unavailable_in_internal_trusted` before any
  transform, verifier, or source blob is fetched.
- **Forward-compat for sharing without building it.** A `visibility` column
  ships in v1 with the single value `private`. Promoting a TaskSet later is a
  metadata change plus the filter rule already in place.

## Data model

Four concepts are involved. Exact table names should be reconciled with the
actual schema during implementation planning, but the ownership boundary is not
optional: user-owned metadata belongs to TaskSet tables, not to native
benchmark rows.

### `task_sets` - new source of truth for user uploads

| Column | Type | Notes |
|---|---|---|
| `id` | text PK | `ts/<owning_team_id>/<slug>`; user-facing stable id |
| `owning_team_id` | uuid NOT NULL FK -> `teams(id)` | Owner team. |
| `slug` | text NOT NULL | Team-local slug, path traversal rejected. |
| `display_name` | text NOT NULL | User-facing name. |
| `visibility` | text NOT NULL DEFAULT `'private'` | v1 only supports `private`; `cluster`/`public` reserved. |
| `status` | text NOT NULL | `materializing`, `ready`, `partial`, `failed`, or `deleted`. |
| `status_reason` | text NULL | Human-readable failure detail. |
| `intents` | text[] NOT NULL | Contains `trajectory_generation`, `evaluation`, or both. |
| `evaluation_ready` | bool NOT NULL DEFAULT false | True only after verifier/scoring config validates. |
| `manifest_blob_uri` | text NOT NULL | Object-store URI of the submitted manifest. |
| `task_count` | int NOT NULL DEFAULT 0 | Materialized runnable tasks. |
| `soft_deleted_at` | timestamptz NULL | Populated by delete; GC purges blobs later. |
| `created_at`, `updated_at` | timestamptz | Standard audit columns. |

### `task_set_manifests` - new sidecar

| Column | Type | Notes |
|---|---|---|
| `task_set_id` | text PK FK -> `task_sets(id)` | One current manifest per TaskSet in v1. |
| `schema_version` | int NOT NULL | Manifest schema version. |
| `manifest` | jsonb NOT NULL | Parsed manifest. |
| `verifier_blob_uri` | text NULL | Present only for evaluation-ready TaskSets. |
| `transform_blob_uri` | text NULL | Compatibility storage for an uploaded `transform.py`; v1 never fetches or executes it. |
| `created_at`, `updated_at` | timestamptz | Standard audit columns. |

Stored separately so TaskSet rows stay queryable and manifest revisions can be
added later without re-shaping the public TaskSet list.

### `benchmarks` - preserved native benchmark catalog

Native platform benchmarks remain in `benchmarks` with their existing meaning.
The current implementation does not project TaskSets into `benchmarks`; batch
creation uses explicit TaskSet selectors in `task_filter` instead. If a future
projection is added, it must remain internal compatibility, not the user-owned
source of truth.

Any future projection must obey these rules:

- System benchmark ids remain bare (`humaneval`, `skilllearnbench`, etc.).
- User TaskSet projection ids are derived from `task_sets.id` and are never the
  canonical management id.
- `/api/v1/benchmarks` and `loom benchmarks list` keep working for native
  benchmarks. Any inclusion of evaluation-ready TaskSets must be explicit in
  evaluation flows and labeled as TaskSets.

### `tasks`

User TaskSets materialize to canonical task bundles. The implementation can add
`task_set_id` to `tasks` or maintain a compatibility `benchmark_id` projection,
but every user-owned task row must remain traceable to its owning TaskSet and
team. The current implementation links materialized user-owned task rows through
`tasks.task_set_id`.

### Visibility helper

A single helper enforces team visibility for user-owned TaskSets:

```python
def visible_task_sets(*, team_id: UUID | None) -> Select:
    if team_id is None:
        return select(TaskSet).where(false())
    return select(TaskSet).where(
        and_(
            TaskSet.owning_team_id == team_id,
            TaskSet.visibility == "private",
            TaskSet.soft_deleted_at.is_(None),
        )
    )
```

Benchmark read sites keep their existing native benchmark behavior. Evaluation
selection code that wants to offer evaluation-ready TaskSets should compose:

```python
system_benchmarks = visible_benchmarks(team_id=team_id)
owned_eval_task_sets = visible_task_sets(team_id=team_id).where(
    TaskSet.evaluation_ready.is_(True)
)
```

Run creation must pass the caller team into task-filter resolution. Explicit
TaskSet selectors are rejected unless the TaskSet is visible to that team and
has status `ready` or `partial`. Even explicit `task_ids` are filtered so a
team cannot run another team's TaskSet task by guessing an id.

A repo-level lint test should reject bare `select(TaskSet)` outside the TaskSet
visibility helper module. If an implementation adds benchmark-compatible
TaskSet projections, it should also guard projection read sites so TaskSet rows
cannot leak across teams.

### Object-storage layout

```
benchmarks/system/<slug>/...                                  # unchanged
tasksets/user/<owning_team_id>/<slug>/manifest.yaml
tasksets/user/<owning_team_id>/<slug>/<source.locator>.tar.gz # bundle-upload source
tasksets/user/<owning_team_id>/<slug>/verifier.{py,sh}         # optional
tasksets/user/<owning_team_id>/<slug>/transform.py             # retained blob; v1 never executes it
tasksets/user/<owning_team_id>/<slug>/tasks/<task_id>/...
```

Per-team prefix means a future bucket policy can enforce team isolation at the
storage layer; v1 enforces it at the application layer via the helper above.

## Manifest schema (`loom.taskset/v1`)

```yaml
apiVersion: loom.taskset/v1
kind: UserTaskSet

metadata:
  name: my-coding-tasks              # slug; server stores as ts/<team_id>/my-coding-tasks
  display_name: My Coding Tasks

intents:
  - trajectory_generation             # always allowed
  - evaluation                        # row sources require verifier below;
                                      # bundle-upload uses per-task verifiers

source:
  type: hf                            # hf | git | https | jsonl-inline | bundle-upload
  locator: namespace/dataset
  revision: 1.2.3                     # optional
  subset: default                     # optional
  split: test                         # optional

instance_mapping:                     # how to derive per-instance fields from a raw row
  prompt: row.question
  answer: row.solution
  task_id: row.id

task_template:                        # rendered per instance with {{ instance.* }}
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }} - {{ instance.task_id }}"
  environment:
    os: linux
    docker_image: ghcr.io/example/coding-task:1.0
  agent:
    name: default
  steps:
    - artifacts: [solution.py]

verifier:                             # optional; required for row-source evaluation
  type: pytest                        # pytest | script
  file: verifier/test_solution.py     # path inside the upload bundle

# `transform` is intentionally unavailable in v1 internal_trusted mode.
# If present for compatibility with an older manifest, materialization fails
# with `transform_unavailable_in_internal_trusted` before any blob fetch.

limits:
  max_instances: 500
  timeout_per_task_s: 300
```

Validated by a pydantic model `UserTaskSetManifest` with `extra="forbid"`.
Top-level `apiVersion` carries the schema version, decoupled from
`schema_version` in storage so the on-disk row can outlive a manifest format
change.

Validation rules:

- `trajectory_generation` is allowed with or without `verifier`.
- `evaluation` on row-oriented sources requires a manifest-level `pytest` or
  `script` verifier.
- `evaluation` on `bundle-upload` sources uses the verifier declared by each
  uploaded task bundle and does not require a shared manifest-level verifier.
- A manifest without `intents` defaults to `["trajectory_generation"]`.
- A manifest with `verifier` but no explicit `evaluation` intent may be accepted
  as evaluation-ready, but the API response should make the inferred capability
  explicit.

Bundle-upload TaskSets use the same top-level manifest shape, but the source
locator points at a tar archive shipped in the same submit directory. The archive
contains complete per-task directories and is safely unpacked by the
materializer; absolute paths, traversal entries, symlinks, hardlinks, and device
entries are rejected before any task row is created.

```yaml
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: source-useful-5003-slice
  display_name: Source Useful 5003 Slice
intents:
  - evaluation
source:
  type: bundle-upload
  locator: bundle.tar.gz
  subset: tasks
limits:
  max_instances: 100
```

Expected archive layout:

```
bundle.tar.gz
└── tasks/
    ├── task-a/
    │   ├── task.toml
    │   ├── instruction.md
    │   ├── environment/Dockerfile
    │   └── verifier/...
    └── task-b/
        └── ...
```

## Components

### Intake API

| Endpoint | Behavior |
|---|---|
| `POST /api/v1/tasksets` | multipart: `manifest.yaml` + optional `verifier.*`; `bundle-upload` manifests also require a `bundle` part matching `source.locator`. A compatibility `transform.py` part may be stored, but a manifest declaring `transform` deterministically fails materialization in v1 with `transform_unavailable_in_internal_trusted`. |
| `GET /api/v1/tasksets/{id}` | materialization status, capabilities, task count, and per-instance error summary. |
| `POST /api/v1/tasksets/{id}/rebuild` | re-enqueues materialization; manifest re-fetched from storage. |
| `DELETE /api/v1/tasksets/{id}` | soft-delete; blobs purged by GC after 7 days. |

Native benchmark list/detail continues through `/api/v1/benchmarks`. Evaluation
creation APIs can offer a combined selector of native benchmarks plus
evaluation-ready TaskSets, but TaskSet management remains under
`/api/v1/tasksets`.

Batch creation accepts TaskSets through the existing batch API without
renaming them as benchmarks:

```json
{
  "task_filter": {
    "task_set_id": "ts/<team_id>/<slug>",
    "subset_kind": "all"
  }
}
```

For mixed source runs, `benchmark_id(s)` and `task_set_id(s)` are unioned as
task sources before `task_ids`, tag filters, and subset filters are applied.

### CLI

```
loom tasksets submit ./my-taskset/        # manifest.yaml, [verifier.*], [bundle.tar.gz]
loom tasksets status <id>
loom tasksets rebuild <id>
loom tasksets delete <id>
loom tasksets list
```

Evaluation/data-production batch creation can target one TaskSet directly:

```
loom eval batch create \
  --task-set ts/<team_id>/<slug> \
  --agent <agent> \
  --provider <provider-connection> \
  --model <model>
```

`--benchmark`, `--task-set`, and `--task-filter` are mutually exclusive CLI
shortcuts. Use `--task-filter` when a run needs mixed native benchmark and
TaskSet sources.

`loom benchmarks list` remains the native benchmark listing. If a future
evaluation CLI wants to show evaluation-ready TaskSets alongside native
benchmarks, it should label them as TaskSets rather than calling them user
benchmarks.

### Materialization worker

A background job-queue consumer:

1. Load manifest from `task_set_manifests`.
2. `fetch_upstream(source)` - reuses
   `packages/loom-benchmarks/loom_benchmarks/fetch.py` unchanged where
   possible for row-oriented sources. For `bundle-upload`, fetch the uploaded
   tar archive from the TaskSet prefix instead.
3. If the manifest declares `transform`, fail the job with
   `transform_unavailable_in_internal_trusted` before fetching any blob or
   iterating any rows.
4. Iterate rows up to `limits.max_instances`.
5. Render `task.toml` from `task_template` using the raw mapped row plus
   `instance_mapping`.
6. Validate via `TaskConfig` (`extra="forbid"`) - per-row failure is a skip,
   not an abort.
7. Run task-bundle compatibility preflight on the rendered local bundle before
   upload. Hard failures are recorded in `error_summary` with structured fields
   (`code`, `severity`, `path`, `line`, `phase`, `message`, `hint`,
   `evidence`) and no runnable task row is inserted. The platform reports
   issues such as DNS/NSS mutation before agent setup or Dockerfile/build
   context path drift; it does not flatten or silently repair user bundles.
8. Stage generated bundles (`task.toml`, `instruction.md`, optional
   `verifier/<name>`, declared artifacts) under
   `tasksets/user/<team>/<slug>/materializations/<job-id>/<lease-epoch>/tasks/`.
   For `bundle-upload`, preserve and upload the complete per-task directory,
   including verifier/tests/data assets; the DB `config` stores a validated Loom
   `TaskConfig`.
9. In one lease-fenced transaction, replace the current `tasks` rows linked to
   the TaskSet and owning team. Those `Task.source` values are the publication
   pointer for a generation; an uploaded object key or a job audit field is not
   a published result.
10. On completion, update `task_sets.status` to `ready`, `partial`, or
    `failed`; set `evaluation_ready=true` only when verifier/scoring config is
    valid and at least one task materialized.

### Generation storage and cleanup

Stable intake inputs stay directly below
`tasksets/user/<team>/<slug>/`: the manifest, verifier/transform blobs, and an
uploaded bundle archive are durable inputs. Generated task bundles are staged
separately by the canonical `(job UUID, lease epoch)` generation namespace.
This lets a replacement generation coexist with the generation still referenced
by current `Task.source` rows until a lease-fenced publication commits.

The materializer charges a staged rebuild against physical bytes for the whole
team TaskSet prefix. It therefore includes stable inputs, the published
generation, and any abandoned partial generation until cleanup completes;
rebuilds need temporary coexistence headroom and do not subtract their own root
or bypass quota.

Two cleanup contracts remain deliberately separate:

- The live bounded reconciler selects only non-deleted TaskSets and their
  materialization jobs from the database. It can delete only an exact
  DB-derived `materializations/<job-id>/<epoch>/` prefix after preserving the
  current active epoch and every generation referenced by a current
  `Task.source`. Unknown jobs, malformed/future epochs, legacy `tasks/`, and
  all stable inputs are never live-GC targets.
- Retention-delayed soft-delete GC is the only path that removes an entire
  TaskSet root. After `soft_deleted_at + retention`, it deletes the
  delimiter-terminated `<root>/` and then hard-deletes the TaskSet rows. It is
  allowed to remove durable inputs and unknown/legacy objects because the
  TaskSet has already completed its user-visible deletion retention window.

### Sandbox

v1 has one executable user-authored surface:

- **Verifier** runs only for evaluation-ready TaskSets and native benchmarks. It
  runs at trial time in the existing trial sandbox container. The user's
  `verifier.*` is mounted as `verifier/<name>`, exactly like a first-party
  verifier. No new verifier isolation infrastructure.
- **Transform** is not executable in v1. The legacy constrained-subprocess
  helper remains dormant, but `os.unshare`, resource limits, and legacy flags
  are not authorization semantics. A manifest declaring `transform` fails
  before source, verifier, or transform blob fetches and before a subprocess
  can run. Untrusted transform isolation is a post-v1 workload-mode capability.

### UI

Add a TaskSets surface instead of folding user uploads into the Benchmarks page:

- "Task Sets" or "My Task Sets" page for team-owned TaskSets.
- Status indicator (`materializing` / `ready` / `partial` / `failed` /
  `deleted`) with a detail panel showing the first 50 per-instance errors.
- Capability badge: `trajectory-only`, `evaluation-ready`, or `both`.
- "Submit TaskSet" CTA: drag-and-drop directory upload, or paste a manifest
  with separate file pickers for verifier and bundle archive when
  `source.type=bundle-upload`. The UI must not offer a transform picker in v1.
- Data-production run creation selects TaskSets.
- Evaluation run creation selects native Benchmarks and clearly labeled
  evaluation-ready TaskSets.

The native Benchmarks page remains focused on system/platform benchmarks. It
may link to evaluation-ready TaskSets in creation flows, but it should not
present every user upload as a benchmark.

## Data flow

### Submit

```
CLI/UI -> build multipart (manifest + [verifier])
       -> POST /api/v1/tasksets
API    -> validate manifest (pydantic, extra=forbid)
       -> upload blobs to tasksets/user/<owning_team_id>/<slug>/
       -> INSERT task_sets (status=materializing, owning_team_id=caller_team)
       -> INSERT task_set_manifests
       -> enqueue materialize(task_set_id)
       -> 202 + task_set_id
```

For `bundle-upload`, the multipart form is `manifest + bundle + [verifier]`.
Any manifest that declares `transform` fails its asynchronous materialization
with `transform_unavailable_in_internal_trusted`; it must not be used as a
successful submission path.

### Materialize (worker)

```
load manifest
if manifest.transform:
    fail task set and job with transform_unavailable_in_internal_trusted
    # Do not fetch source, verifier, transform, or bundle blobs.
if source.type == bundle-upload:
    fetch archive from tasksets/user/<team>/<slug>/<source.locator>
    safely extract archive; reject traversal/link/device entries
    foreach task.toml under source.subset or tasks/ up to limits.max_instances:
        normalize Terminal-Bench-shaped task.toml when needed
        validate TaskConfig (extra=forbid)
        preflight task bundle compatibility
        stage complete per-task directory beneath materializations/<job>/<epoch>/tasks/
        prepare Task row linked to task_set_id
else:
    fetch_upstream(source)
    foreach row up to limits.max_instances:
        render task.toml from task_template using row + instance_mapping
        validate TaskConfig (extra=forbid)
        stage bundle beneath materializations/<job>/<epoch>/tasks/
        prepare Task row linked to task_set_id
lease-fenced transaction -> replace current Task rows and publish their sources
UPDATE task_sets.status = ready | partial | failed
UPDATE task_sets.evaluation_ready based on shared or per-task verifier validity
```

### Data-production run

```
user picks TaskSet
API authz: visible_task_sets(user) must include it
POST /api/v1/batches task_filter={"task_set_id":"ts/<team>/<slug>", ...}
worker pulls task bundle
agent/model runs -> trajectories, completions, and artifacts are persisted
no verifier or benchmark score is required
```

### Evaluation run

```
user picks native Benchmark or evaluation-ready TaskSet
API authz: visible_benchmarks(user) or visible_task_sets(user) must include it
POST /api/v1/batches task_filter={"benchmark_id":"humaneval"} or {"task_set_id":"ts/<team>/<slug>"}
worker pulls task bundle
agent/model runs -> verifier runs in trial sandbox -> score/reward persisted
```

## Error handling

| Condition | Behavior |
|---|---|
| Manifest schema invalid | API 400 with field-level errors at intake. |
| `evaluation` intent without verifier on row-oriented sources | API 400 with `verifier_required_for_evaluation`. |
| `bundle-upload` missing multipart bundle | API 400 with `bundle file required when manifest source is bundle-upload`. |
| Unsafe uploaded archive entry | Materialization fails with `bundle_extract_unsafe`. |
| Uploaded archive contains no task directories | Materialization fails with `bundle_no_tasks`. |
| Source unreachable | 3 retries with exponential backoff, then `status=failed` (`source_unreachable`). |
| Manifest declares `transform` | Materialization fails before any blob fetch or runner call with `transform_unavailable_in_internal_trusted`; no task rows are created. |
| Rendered `task.toml` fails validation | Per-row skip, recorded. |
| 0 rows skipped | TaskSet goes `ready`. |
| 1 row skipped up to and including 50% | TaskSet goes `partial` with skip count surfaced. |
| More than 50% of rows skipped | TaskSet goes `failed` (`majority_skipped`). |
| Verifier crash at evaluation time | Existing `verifier_error` path; data-production runs are unaffected. |
| Bundle storage exceeds 5 GiB | Abort, `failed` (`size_exceeded`). |
| Team over quota (50 TaskSets or 20 GiB) | API 429 at submit; materialization aborts with `failed` (`size_exceeded`) before an over-limit put. Rebuild accounting includes the full team TaskSet total, including prior generated bytes and stable inputs. |
| Stalled or stale generation | Current Task sources and an active job epoch remain protected. A bounded reconciler later removes only unreferenced DB-derived generation prefixes; it never removes the TaskSet root or durable inputs. |
| Delete | Soft-delete row; blobs retained 7 days for undo, then delimiter-safe root GC purges the complete root. Run history continues to reference the soft-deleted TaskSet. |

Defaults (5 GiB / 50 TaskSets / 20 GiB / 500 instances) are
operator-configurable via `config/tasksets.toml`; defaults aim for "reasonable
individual user" not "team."

## Migration

Single foundation slice:

1. Create `task_sets`.
2. Create `task_set_manifests`.
3. Add CHECK constraints for TaskSet namespace, intent values, status values,
   and visibility values.
4. Add indices on `(owning_team_id, visibility, status)`, `slug`, and
   `evaluation_ready`.
5. Add a nullable `task_set_id` relationship to user-materialized tasks if the
   current `tasks` schema requires it; otherwise document the compatibility
   projection used to link tasks to TaskSets.

The migration must not mutate native system benchmark rows. An integration test
asserts row-for-row equality on a snapshot of the pre-migration `benchmarks`
table.

## Testing

### Unit

- Manifest schema: positive cases per source type; negative cases per required
  field; `extra="forbid"` rejection.
- Intent/capability validation: trajectory-only accepted without verifier;
  row-source evaluation rejected without verifier; bundle-upload evaluation
  accepted with per-task verifiers; verifier makes row-source evaluation
  readiness explicit.
- Template renderer: placeholder coverage and missing-field error messages.
- Instance-mapping DSL: dotted paths, defaults, type coercion.
- `visible_task_sets` helper: every (owning_team, visibility, viewer_team)
  combination.
- Namespace enforcement: TaskSet id must use `ts/<team_id>/<slug>`, slug path
  traversal rejected, and owner team must be present.
- Quota and size enforcement at the helper layer.
- Canonical generation-prefix validation, bounded live-GC deletion, current
  Task-source/active-lease protection, and delimiter-safe soft-delete root GC.

### Integration (testcontainers Postgres + MinIO + worker)

- End-to-end submit -> materialize -> list tasks -> start a data-production run
  against a stub agent, with trajectories/artifacts persisted and no score
  required.
- End-to-end submit with verifier -> materialize -> start an evaluation run
  against a stub agent and verifier.
- Native `/api/v1/benchmarks` and `loom benchmarks list` continue to show native
  platform benchmarks without requiring user TaskSet setup.
- One real fetch per source type (`hf`, `git`, `https`, `jsonl-inline`); the
  rest mocked.
- Bundle-upload materialization preserves complete per-task assets, normalizes
  Terminal-Bench-shaped `task.toml` into DB config, and rejects traversal/link
  archive entries.
- Cross-team isolation: team A cannot list, get, rebuild, delete, data-run, or
  evaluation-run team B's TaskSet.
- TaskSet run selection: owned `task_set_id` succeeds, native
  `benchmark_id(s)` plus `task_set_id(s)` are source-unioned, cross-team
  `task_set_id` is rejected, and guessed cross-team TaskSet `task_ids` do not
  create runs.
- Partial-failure materialization: a source with 30% malformed rows produces
  `partial` with the expected skip count.
- Soft-delete + GC: 7-day timer respected; delete during GC window is
  reversible; afterwards is not.
- Rebuilds preserve the old current generation until a lease-fenced winner
  publishes, and live cleanup never widens from a database generation prefix to
  stable inputs, legacy paths, malformed epochs, or another TaskSet root.

### Security

- Malicious verifier (network call, file escape, fork bomb) is contained /
  killed by the trial sandbox; evaluation trial reports `verifier_error`.
- Malicious transform is never executed in v1. Its manifest fails closed with
  `transform_unavailable_in_internal_trusted`, independent of legacy sandbox
  flags or best-effort `os.unshare` behavior.
- Path traversal in slug rejected (`ts/<team>/../../etc/passwd`).
- Manifest size cap enforced before parsing.
- Authz check present on every TaskSet read/write/run endpoint asserted via
  parameterized test, not per-route one-offs.

### CI gates

- Existing first-party benchmark suite remains green.
- Existing benchmark APIs/CLI still pass without user TaskSet rows.
- New TaskSet suite green.
- Schema migration applied against a snapshot with first-party benchmark rows;
  assert no native benchmark row mutated.

## Open questions for the plan stage

These do not block design approval; they require code-level reconciliation when
the implementation plan is drafted.

- Whether evaluation-ready TaskSets appear in `/api/v1/benchmarks` only behind
  an explicit flag, or only through run-creation selector endpoints.
- For a post-v1 untrusted-workload mode, whether transform isolation should use
  a gVisor/Kata-capable executor rather than the dormant worker-side subprocess
  primitive.
- The choice of job queue for materialization jobs (reuse the trial queue with a
  distinct kind, or stand up a sibling queue).
- Whether to add a human-readable `team_handle` column to `teams`. Until then,
  TaskSet ids use the raw team UUID for unambiguous uniqueness.

## Future shapes (informational)

Beyond v1, the same `apiVersion: loom.taskset/v1` manifest can grow these
without breaking older entries:

- `cluster` and `public` visibility, plus a sharing UI.
- `tool-trace` grading for function-call benchmarks (BFCL / tau2-style).
- `container-task` grading for SWE-Bench-style repo-checkout-and-patch tasks.
- `vm-task` grading once a cluster-side VM provisioner exists (OSWorld /
  WebArena).
- Recipe/data-production templates that run TaskSets for trajectory generation
  without any evaluation score.

Each is a strict extension: new optional fields, new verifier or execution
capabilities, no breaking change to the v1 schema.
