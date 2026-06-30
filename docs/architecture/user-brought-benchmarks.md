# User-brought benchmarks

Status: design (no implementation). Extends [`benchmark-adapter.md`](benchmark-adapter.md) with an end-user intake surface; complements [`sandbox-isolation.md`](sandbox-isolation.md) for the verifier and transform trust boundaries.

## Goal

Let an authenticated Loom user submit their own benchmark — without writing a Python `BenchmarkAdapter` subclass, without operator file-system access, and without a separate publish-and-register dance — and run trials against it the same way they would against any first-party benchmark.

v1 is **owner-private**: the submitter is the only consumer. Cluster-wide and per-user-list sharing are explicitly deferred to a follow-up; the data model leaves the slot for that future without committing to a UI for it now.

## Non-goals (v1)

- Cross-user sharing of any kind (private only).
- Browser zip-upload as the normal evaluation path for first-party benchmarks — first-party intake remains the entry-point + catalog flow in [`benchmark-adapter.md`](benchmark-adapter.md).
- Letting users ship a full `BenchmarkAdapter` Python package. Conversion is declarative + an optional sandboxed `transform()`.
- VM-backed task shapes (OSWorld, WebArena). These remain first-party-only until the cluster has a provisioned VM substrate.
- A curation / review pipeline. Owner-private removes the need.

## Design principles

- **Divergence stops at the catalog.** A user benchmark must materialise into the same `task.toml` bundle layout as first-party. The trial worker has no idea — and no reason to care — whether a bundle came from a user or a first-party adapter.
- **One consumption surface.** First-party and user benchmarks live in the same `benchmarks` table, are listed by the same API, and render on the same SPA page. The distinguisher is an `owner_id` column plus an `u/<owner>/<slug>` name namespace, not a parallel system.
- **Trust boundary = the existing trial sandbox.** Verifier scripts ride the same isolation primitives first-party verifiers already use. The only new sandboxed surface is the optional `transform()`, executed during materialisation.
- **Forward-compat for sharing without building it.** A `visibility` column ships in v1 with the single value `private`. Promoting a benchmark later is `UPDATE visibility = 'cluster'` plus the filter rule already in place.

## Data model

Three tables; names are illustrative and will be reconciled with the actual Loom schema during plan writing.

### `benchmarks` — extended in place

| Column | Type | Notes |
|---|---|---|
| `id` | text PK | unchanged |
| `name` | text | unchanged; first-party is bare (`humaneval`), user-brought is `u/<owner_handle>/<slug>` |
| `series` | text | unchanged; user benchmarks default to `user` |
| existing columns | | unchanged |
| `owner_id` | uuid NULL | NEW. `NULL` = first-party / system. NOT NULL = user-brought. |
| `kind` | text NOT NULL DEFAULT `'system'` | NEW. `'system'` or `'user'`. Redundant with `owner_id IS NULL` but explicit for indexed filters, badges, and audit. |
| `visibility` | text NOT NULL DEFAULT `'private'` | NEW. v1 ships only `'private'`. `'cluster'` and `'public'` reserved. CHECK constraint enforces the allowed set. |
| `manifest_blob_uri` | text NULL | NEW. Object-store URI of the user manifest (NULL for first-party). |
| `status` | text NOT NULL | NEW. `materialising` \| `ready` \| `partial` \| `failed`. First-party rows are seeded as `ready`. |
| `status_reason` | text NULL | NEW. Human-readable failure detail. |

**Namespace rule** (DB CHECK constraint, mirrored in the application layer):

- `kind = 'system'` ⇒ `name NOT LIKE 'u/%'` AND `owner_id IS NULL`
- `kind = 'user'` ⇒ `name LIKE 'u/%'` AND `owner_id IS NOT NULL`

The namespace gives free visual distinction in every CLI list, log line, and search box without any UI work.

### `benchmark_manifests` — new

| Column | Type | Notes |
|---|---|---|
| `benchmark_id` | text PK FK → `benchmarks(id)` | one row per user benchmark |
| `schema_version` | int NOT NULL | manifest schema version, for forward-compat |
| `manifest` | jsonb NOT NULL | parsed manifest |
| `verifier_blob_uri` | text NOT NULL | grader script in object storage |
| `transform_blob_uri` | text NULL | optional `transform.py` in object storage |
| `created_at`, `updated_at` | timestamptz | |

Stored separately so first-party rows do not carry nullable columns, and so manifest revisions can be added later without re-shaping `benchmarks`.

### `tasks` — unchanged

User benchmarks materialise to the canonical bundle format. The trial worker code path is unchanged.

### Visibility helper

A single helper enforces who can see what:

```python
def visible_benchmarks(user: User) -> Query:
    return (
        Benchmark.query
        .where(or_(
            Benchmark.owner_id.is_(None),
            and_(Benchmark.owner_id == user.id, Benchmark.visibility == 'private'),
        ))
    )
```

Every read site (REST, CLI, SPA backend) routes through this helper. A repo-level lint rule rejects bare `select … from benchmarks` outside the helper module, to prevent accidental leakage when adding new endpoints.

### Object-storage layout

```
benchmarks/system/<slug>/...                              # unchanged
benchmarks/user/<owner_id>/<slug>/manifest.json
benchmarks/user/<owner_id>/<slug>/verifier.{py,sh}
benchmarks/user/<owner_id>/<slug>/transform.py           # optional
benchmarks/user/<owner_id>/<slug>/tasks/<task_id>/...    # materialised bundles
```

Per-owner prefix means a future bucket policy can enforce owner isolation at the storage layer; v1 enforces it at the application layer via the helper above.

## Manifest schema (`loom.benchmark/v1`)

```yaml
apiVersion: loom.benchmark/v1
kind: UserBenchmark

metadata:
  name: my-coding-eval               # slug; server stores as u/<owner>/my-coding-eval
  display_name: My Coding Eval

source:
  type: hf                            # hf | git | https | jsonl-inline
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
    name: "{{ metadata.display_name }} — {{ instance.task_id }}"
  environment:
    os: linux
    docker_image: ghcr.io/example/coding-eval:1.0
  agent:
    name: default
  verifier:
    name: pytest
  steps:
    - artifacts: [solution.py]

verifier:                             # one verifier per benchmark
  type: pytest                        # pytest | script | exact-match | regex | llm-judge
  file: verifier/test_solution.py     # path inside the upload bundle

transform:                            # optional
  file: transform.py                  # must export `def transform(row: dict) -> dict`

limits:
  max_instances: 500
  timeout_per_task_s: 300
```

Validated by a pydantic model `UserBenchmarkManifest` with `extra="forbid"`. Top-level `apiVersion` carries the schema version, decoupled from `schema_version` in storage so the on-disk row can outlive a manifest format change.

## Components

### Intake API

| Endpoint | Behaviour |
|---|---|
| `POST /api/v1/user-benchmarks` | multipart: `manifest.yaml` + `verifier.*` + optional `transform.py`. Validates, uploads blobs, inserts rows, enqueues materialisation. Returns 202 + `benchmark_id`. |
| `GET /api/v1/user-benchmarks/{id}` | materialisation status + per-instance error summary |
| `POST /api/v1/user-benchmarks/{id}/rebuild` | re-enqueues materialisation; manifest re-fetched from storage |
| `DELETE /api/v1/user-benchmarks/{id}` | soft-delete; blobs purged by GC after 7 days |

List and detail of any benchmark continue to go through `/api/v1/benchmarks` — the `visible_benchmarks` helper applies there.

### CLI

```
loom benchmarks submit ./my-benchmark/        # directory with manifest.yaml, verifier.*, [transform.py]
loom benchmarks status <id>
loom benchmarks rebuild <id>
loom benchmarks delete <id>
```

`loom benchmarks list` is the existing first-party listing, extended to render owned user benchmarks alongside system ones with a column distinguishing kind.

### Materialisation worker

A background job-queue consumer:

1. Load manifest from `benchmark_manifests`.
2. `fetch_upstream(source)` — reuses `packages/loom-benchmarks/loom_benchmarks/fetch.py` unchanged.
3. Iterate rows up to `limits.max_instances`.
4. For each row: optionally run `transform(row)` in a constrained subprocess (see Sandbox).
5. Render `task.toml` from `task_template` using the (transformed) row + `instance_mapping`.
6. Validate via `TaskConfig` (`extra="forbid"`) — per-row failure is a skip, not an abort.
7. Write the bundle (`task.toml`, `instruction.md`, `verifier/<name>`, declared artefacts) to object storage under the user prefix.
8. Insert `tasks` row.
9. On completion, `UPDATE benchmarks.status` to `ready` / `partial` / `failed`.

### Sandbox

Two surfaces:

- **Verifier** runs at trial time in the existing trial sandbox container. The user's `verifier.*` is mounted in as `verifier/<name>`, exactly like a first-party verifier. No new isolation infrastructure.
- **Transform** runs at materialisation time, in a constrained subprocess on the worker:
  - `RLIMIT_CPU`, `RLIMIT_AS`, `RLIMIT_NOFILE`, wall-clock timeout
  - empty parent environment
  - ephemeral working directory
  - no network (network namespace or seccomp filter, whichever the worker host already supports — to be reconciled with [`sandbox-isolation.md`](sandbox-isolation.md) during plan writing)
  - stdout/stderr captured to error log; structured return is the only result channel

If the platform cannot guarantee the no-network constraint for in-process subprocesses on a given worker, that worker MUST refuse to schedule materialisation jobs that include a `transform`. Better to surface "transforms unsupported on this cluster" than to silently grant network.

### UI

Extend the existing SPA Benchmarks page:

- Render `u/<owner>/<slug>` benchmarks alongside system ones, with an "owned by you" badge derived from `owner_id == me`.
- Add a "My benchmarks" filter toggle.
- Status indicator (`materialising` / `ready` / `partial` / `failed`) with a detail panel showing the first 50 per-instance errors.
- "Submit benchmark" CTA: drag-and-drop directory upload, or paste a manifest with separate file pickers for verifier and transform.

## Data flow

### Submit

```
CLI/UI → build multipart (manifest + verifier + [transform])
       → POST /api/v1/user-benchmarks
API    → validate manifest (pydantic, extra=forbid)
       → upload blobs to benchmarks/user/<owner>/<slug>/
       → INSERT benchmarks (kind=user, status=materialising, owner_id=me, name=u/<owner>/<slug>)
       → INSERT benchmark_manifests
       → enqueue materialise(benchmark_id)
       → 202 + benchmark_id
```

### Materialise (worker)

```
load manifest → fetch_upstream(source)
foreach row up to limits.max_instances:
    [if transform: row = sandboxed transform(row)]
    render task.toml from task_template using row + instance_mapping
    validate TaskConfig (extra=forbid)
    write bundle to obj store
    INSERT tasks row
UPDATE benchmarks.status = ready | partial | failed
```

### Trial run (unchanged)

```
user picks benchmark
API authz: visible_benchmarks(user) must include it
trial worker pulls task bundle (same path as first-party)
agent runs → verifier runs in trial sandbox
```

## Error handling

| Condition | Behaviour |
|---|---|
| Manifest schema invalid | API 400 with field-level errors at intake |
| Source unreachable | 3 retries with exponential backoff, then `status=failed` (`source_unreachable`) |
| `transform()` crash on a row | capture stderr, skip instance, record error; first 50 errors retained on the benchmark detail |
| `transform()` exceeds resource limits | killed, instance skipped with `transform_limit_exceeded` |
| Rendered `task.toml` fails validation | per-row skip, recorded |
| 0 rows skipped | benchmark goes `ready` |
| 1 row skipped up to and including 50% | benchmark goes `partial` with skip count surfaced |
| More than 50% of rows skipped | benchmark goes `failed` (`majority_skipped`) |
| Verifier crash at trial time | existing `verifier_error` path; no new code |
| Bundle storage exceeds 5 GiB | abort, `failed` (`size_exceeded`) |
| Owner over quota (50 benchmarks or 20 GiB) | API 429 at submit |
| Delete | soft-delete row; blobs retained 7 days for undo, GC purges thereafter; trial history continues to reference the soft-deleted row |

Defaults (5 GiB / 50 benchmarks / 20 GiB / 500 instances) are operator-configurable via `config/byob.toml`; defaults aim for "reasonable individual user" not "team."

## Migration

Single Alembic revision:

1. Add `owner_id`, `kind`, `visibility`, `manifest_blob_uri`, `status`, `status_reason` to `benchmarks`.
2. Backfill `kind='system'`, `visibility='private'`, `status='ready'` for all existing rows. `owner_id` stays NULL.
3. Add CHECK constraints for the namespace rule and the `visibility` allowed set.
4. Create `benchmark_manifests`.
5. Create indices on `(owner_id, kind)` and on `name`.

The migration is idempotent against the seeded first-party rows: no `system` row should mutate beyond column defaults. An integration test asserts row-for-row equality on a snapshot of the pre-migration first-party table.

## Testing

### Unit

- Manifest schema: positive cases per source type; negative cases per required field; `extra="forbid"` rejection.
- Template renderer: placeholder coverage, missing-field error messages.
- Instance-mapping DSL: dotted paths, defaults, type coercion.
- `visible_benchmarks` helper: every (owner, visibility, viewer) combination.
- Namespace enforcement: insert system row with `u/` rejected; insert user row without `u/` rejected; insert user row with NULL `owner_id` rejected.
- Quota and size enforcement at the helper layer.

### Integration (testcontainers Postgres + MinIO + worker)

- End-to-end submit → materialise → list tasks → run a single trial against a stub agent and verifier.
- One real fetch per source type (`hf`, `git`, `https`, `jsonl-inline`); the rest mocked.
- Cross-user isolation: user A cannot list, get, rebuild, delete, or trial-start user B's benchmark.
- Partial-failure materialisation: a source with 30% malformed rows produces `partial` with the expected skip count.
- Soft-delete + GC: 7-day timer respected; delete during GC window is reversible; afterwards is not.

### Security

- Malicious verifier (network call, file escape, fork bomb) is contained / killed by the trial sandbox; trial reports `verifier_error`.
- Malicious transform (same vectors) is contained / killed by the materialisation sandbox; instance reports `transform_limit_exceeded` or `transform_error`.
- Path traversal in slug rejected (`u/../../etc/passwd`).
- Manifest size cap enforced before parsing.
- Authz check present on every read endpoint asserted via parameterised test, not per-route one-offs.

### CI gates

- Existing first-party suite remains green (no regression in the unchanged path).
- New `byob` suite green.
- Schema migration applied against a snapshot with first-party rows; assert no `system` row mutated beyond column defaults.

## Open questions for the plan stage

These do not block design approval; they require code-level reconciliation when the implementation plan is drafted.

- Exact reconciliation of the namespace CHECK constraint with the actual `benchmarks` table column names (this spec uses placeholder names where the schema is not yet read).
- Whether the `transform` sandbox should reuse an existing worker-side primitive from [`sandbox-isolation.md`](sandbox-isolation.md) or introduce a new helper.
- The choice of job queue for materialisation jobs (reuse the trial queue with a distinct kind, or stand up a sibling queue) — both work; the call belongs in the plan.
- Whether `loom benchmarks list` should default to a kind-filter or show both kinds with a column; ditto the SPA. Default chosen here is "show both with column / badge"; revisit during plan if the kind mixture is noisy.

## Future shapes (informational)

Beyond v1, the same `apiVersion: loom.benchmark/v1` manifest is intended to grow these without breaking older entries:

- `cluster` and `public` visibility, plus a sharing UI.
- `tool-trace` grading for function-call benchmarks (BFCL / tau2-style).
- `container-task` grading for SWE-Bench-style repo-checkout-and-patch benchmarks.
- `vm-task` grading once a cluster-side VM provisioner exists (OSWorld / WebArena).

Each is a strict extension: new optional fields, new `verifier.type` values, no breaking change to the v1 schema.
