# Workflows

> **Status**: shipped (PR C). Implementation in
> `src/loom_service/routes/workflows.py` + ORM `Workflow` model in
> `src/loom/db/schema.py` + migration `0009_workflows.py` + SPA pages
> under `web/src/pages/Workflows.tsx` / `WorkflowDetail.tsx` /
> `NewWorkflow.tsx`.

A **Workflow** is a global, admin-managed saved recipe that pins every
field needed to reproduce a benchmark run: which benchmark, which
agent (name + version), which model, which backend, concurrency, the
task filter, and the trial config. Launching a Workflow creates a
`Campaign` whose `task_filter` + `trial_config` are deep-copied from
the workflow at submit time. Subsequent edits to the workflow do NOT
retroactively change historical runs.

## Why this exists

Before workflows, every Campaign was a snowflake: users hand-wrote
`task_filter` + `trial_config` from scratch each time. There was no
way to say "the same run we did last week" except by reading old
campaigns and copy-pasting. The CLI's `loom run --workflow X`
analogue doesn't exist either — agent/model choices were ad-hoc.

Workflows sit between **Benchmarks** (ship-installed entry-point
adapters, not user-mutable) and **Campaigns** (one execution
instance). They give a stable, named handle that admins curate and
teams launch.

## Domain mapping

| Concept                | Owned by       | Mutable? | Notes |
|------------------------|----------------|----------|-------|
| Benchmark              | entry-points   | no       | Installed via `pip`; row in `benchmarks` table populated by seed script. |
| Workflow               | admin          | yes      | Saved recipe pinned across every field. Global — no `team_id`. |
| Campaign               | submitting team| yes (cancel only) | One execution. Can be created directly (`POST /campaigns`) or via `POST /workflows/{id}/launch`. |
| Trial                  | worker         | state machine | One task × one agent × one model. |

## What "fully pinned" means

Each workflow row carries:

| Field             | Why pinned                                            |
|-------------------|-------------------------------------------------------|
| `benchmark_id`    | Foreign key to `benchmarks.id` — adapter is fixed.    |
| `agent_name`      | Which CLI adapter (e.g. `claude-code`).               |
| `agent_version`   | Pinned semver — no `latest`. Avoids version skew between launches of the "same" workflow. |
| `model_provider`  | `anthropic` / `openai` / `google` / `local:vllm` / etc. |
| `model_name`      | e.g. `claude-opus-4-7` — exact identifier.            |
| `backend`         | `docker` / `fake` / `daytona` / `modal`.              |
| `concurrency`     | Bounded `[1, 64]` — explicit, not auto-tuned.         |
| `task_filter`     | JSONB — typically `{benchmark_id: …}` or a `task_ids` list. |
| `trial_config`    | JSONB — forwarded into `TrialConfig` on each child trial. |

Future additions (tracked separately):
- `image_digest` — the agent runtime image SHA256 for true binary
  reproducibility.
- `seed` — for stochastic agents.
- `budget` — per-trial dollar cap.

## Authorization

```
GET    /api/v1/workflows         → any human/admin (open read)
GET    /api/v1/workflows/{id}    → any human/admin
POST   /api/v1/workflows         → admin:workflows scope
PATCH  /api/v1/workflows/{id}    → admin:workflows scope
DELETE /api/v1/workflows/{id}    → admin:workflows scope (soft delete)
POST   /api/v1/workflows/{id}/launch  → submit scope + non-null team_id
```

The split mirrors rate cards from PR B (`docs/architecture/cost-and-rate-cards.md`): reads open to every team, mutations gated behind a named admin scope. The `admin:workflows` scope is included in the dev-only admin token seeded by `seed_test_data.py` (issue #295 tracks the long-term auth replacement).

`POST /launch` deliberately requires a team token even for admins: every Campaign is attributable to a team, so an admin without `team_id` is asked to use a team token for the actual run. The error message is explicit.

## Launch semantics

```
                       ┌─────────────────────────────┐
        team token →   │ POST /workflows/{id}/launch │
                       │   body: { name? }           │
                       └─────────────┬───────────────┘
                                     │
                  ┌──────────────────▼──────────────────┐
                  │ resolve task_filter against current │
                  │ benchmarks/tasks slate              │
                  └──────────────────┬──────────────────┘
                                     │
                ┌────────────────────▼────────────────────┐
                │ if zero tasks materialize → 400         │
                │ (refuse to create an empty Campaign)    │
                └────────────────────┬────────────────────┘
                                     │
                ┌────────────────────▼─────────────────────┐
                │ Campaign(                                │
                │   team_id  = ctx.team_id,                │
                │   workflow_id = w.id,        ◀── back-ref│
                │   task_filter = deepcopy(w.task_filter), │
                │   trial_config= deepcopy(w.trial_config),│
                │   expected_trial_count = len(task_ids),  │
                │   name = body.name or f"{w.name} — <now>"│
                │ )                                        │
                └────────────────────┬─────────────────────┘
                                     │
                          existing campaign_runner
                          fans out N trials as today
```

The frozen-config snapshot is what gives reproducibility: even if an admin edits the workflow tomorrow, the Campaign you launched today has its own copy. The `workflow_id` column on `campaigns` is a traceability back-pointer (so you can answer "which campaigns came from workflow X?"); it is NOT used for lookups during the run.

## Schema

```sql
CREATE TABLE workflows (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name                     TEXT NOT NULL,
  description              TEXT,
  benchmark_id             TEXT NOT NULL REFERENCES benchmarks(id),
  agent_name               TEXT NOT NULL,
  agent_version            TEXT NOT NULL,
  model_provider           TEXT NOT NULL,
  model_name               TEXT NOT NULL,
  backend                  TEXT NOT NULL DEFAULT 'docker',
  concurrency              INTEGER NOT NULL DEFAULT 1,
  task_filter              JSONB NOT NULL DEFAULT '{}'::jsonb,
  trial_config             JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by_token_prefix  TEXT NOT NULL,
  deleted_at               TIMESTAMPTZ,
  CONSTRAINT workflows_name_unique
    UNIQUE (name) WHERE deleted_at IS NULL
);

-- Active-name lookups (the dominant query: "list active workflows").
CREATE INDEX workflows_name_active_idx
  ON workflows (name) WHERE deleted_at IS NULL;

-- Back-reference on campaigns.
ALTER TABLE campaigns ADD COLUMN workflow_id UUID REFERENCES workflows(id);
CREATE INDEX campaigns_workflow_id_idx
  ON campaigns (workflow_id) WHERE workflow_id IS NOT NULL;
```

The partial unique index on `name WHERE deleted_at IS NULL` lets soft-deleted names be re-used; the index is sufficient for the unique-active-name query.

## What this is NOT

- **Not a per-team override surface.** Every workflow is global. Teams can't fork a workflow into their own customised copy. The closest analogue is launching with a different team token and tweaking the resulting Campaign via the existing cancel/inspect surface. A "team-scoped workflows" extension is tracked as a follow-up if real teams ask.
- **Not a versioning system.** A workflow has a single live row; edits update in place (with `updated_at`). There's no "workflow vN" history. Historical reproducibility is guaranteed by the Campaign-side frozen snapshot — if you launched workflow X yesterday, the Campaign carries the v-yesterday config regardless of today's edits.
- **Not a scheduler.** Launches are explicit user actions. Recurring/cron-style automation is a separate concern; if needed, build on top of `POST /workflows/{id}/launch`.
- **Not a CLI surface yet.** `loom run --workflow <id>` is a natural follow-up but doesn't ship in PR C. The CLI continues to take `--agent` / `--model` / `--backend` flags directly.

## See also

- [`cli-mode.md`](cli-mode.md) — how `loom run` dispatches today (no Workflow integration yet).
- [`benchmark-adapter.md`](benchmark-adapter.md) — the entry-point protocol that backs `benchmark_id`.
- [`cost-and-rate-cards.md`](cost-and-rate-cards.md) — sibling admin-mutation / open-read pattern.
- Issue #295 — long-term auth/admin model that will replace the current dev-only admin token.
