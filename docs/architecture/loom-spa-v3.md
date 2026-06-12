# Loom SPA v3 — trial-centric simplification

**Status:** shipped 2026-06-12.

**Date:** 2026-06-11

## Background

The current SPA mirrors the data model 1:1 — Tasks, Workflows,
Campaigns, Trials all get their own list + detail pages. User
feedback over the past week converged on a single critique:

> "I think we don't have the ability to manage on campaign level,
> the campaign page will be cancelled, also workflows, and tasks
> (the current 'browse registered tasks' purpose). The platform
> will be a place to create and monitor the trials."

Translation: most of the SPA's surfaces are noise. The two things
users actually do are (a) submit a batch of trials with a chosen
config, and (b) watch them run. Everything else (browsing the task
catalog, managing saved Workflow recipes, inspecting Campaign rows
as a separate concept) is friction that confuses without
substantial payoff at this stage.

This doc specs the resulting redesign.

## Naming — align with Harbor

Harbor's vocabulary is **Task / Trial / Benchmark / Step**. Harbor
does NOT have a "Campaign" concept (the loom-vs-harbor doc
confirms: "Loom's SPA shows trials + campaigns + usage" — contrast
implied). Loom added Campaign + Workflow to handle
batch-submission and saved-recipe concerns Harbor doesn't model.

User direction (after audit): "I don't want to confuse users or us
— if Campaign + Workflow are useful, name them clearly; if not,
drop them."

**Resolution:**

- **Campaign earns its keep** — it's the batch concept that powers
  atomic submit, cancel-all-in-batch, progress aggregation,
  idempotent runner re-ticks, and "show me the trials I submitted
  together" grouping. **But the name "Campaign" itself is the
  confusion.** Rename it to **Batch** everywhere — DB table, API
  path, code, docs, SPA. One word, one meaning, used identically
  across every surface.
- **Workflow does not earn its keep** — landed recently, has no
  curl-driven users yet, and the saved-recipe value prop is
  unvalidated. **Drop entirely:** table, route, code, tests, SPA
  pages. Net negative LOC. If saved recipes return later they get
  a cleaner design.

### Authoritative glossary

| Name | Definition | Where used |
|---|---|---|
| **Task** | One benchmark instance (e.g. `HumanEval/0`). Has a `TaskConfig` describing env, agent default, verifier, steps. | DB table, SPA, docs, CLI, API |
| **Trial** | ONE execution of a Task by one (agent, model, backend), running through that task's steps and producing a trajectory + result. The atomic unit of work. | DB table, SPA, docs, CLI, API |
| **Step** | One stage inside a Trial. Single-step Tasks have a one-element step list; multi-step have many. | TaskConfig, TrialResult, ATIF |
| **Batch** | A group of Trials submitted together. A Batch picks a task slate + a backend + one-or-more Combinations. The SPA's submit form creates one Batch + N Trials atomically. Used for cancel-all, progress aggregation, runner idempotency. | DB table (renamed from `campaigns`), API path (`/api/v1/batches`), SPA, docs |
| **Combination** | ONE (agent, model, n_per_task) tuple within a Batch. Harbor calls this `组合` ("AgentModelCombination"). A Batch may have multiple Combinations to run the same task slate against several (agent, model) pairs in one submission. Each Combination × Task × sample produces one Trial. | DB (jsonb on `batches.combinations`), SPA, docs |
| **Benchmark** | A collection of Tasks shipped together (e.g. `humaneval`, `mbpp`). One row in `benchmarks` table. | DB, SPA, docs |
| **Agent** | The runtime that drives the Trial (oracle, claude-code-inbox, a `loom-launcher` adapter). | Catalog, TaskConfig.agent, Combination.agent_name |
| **Model** | An LLM the Agent calls (anthropic/claude-opus-4-7). Catalog from rate_cards. | Catalog, Combination.agent_model |
| **Backend** | The sandbox provider (docker / daytona / fake / future modal). Picked at submit time, applied per-Batch (Harbor scopes it the same way). | Catalog (new), Batch.backend |

Words that **do NOT appear** anywhere user-facing or in code:

- ~~Campaign~~ — RENAMED to Batch. The string "campaign" should
  not appear anywhere in the codebase except in the migration that
  performs the rename and in this superseding paragraph.
- ~~Workflow~~ — DROPPED. The string "workflow" should not appear
  anywhere in the codebase except in the migration that drops the
  table and in this paragraph.
- ~~Submission~~ — used in the v3 first draft as a UI label.
  Drop it; the v3 final draft uses "Batch" everywhere (SPA shows
  "Submit batch" / "Batches" list, not "Submit trials" /
  "Submissions"). One word per concept.
- ~~Run~~ — informal word in some external tools. Don't use it
  anywhere; "Trial" covers the same meaning unambiguously.

Translation when the SPA calls into the API (post-rename):

| SPA action | API path it hits |
|---|---|
| "Submit batch" form → POST | `POST /api/v1/batches` |
| "Batches" list page → GET | `GET /api/v1/batches` |
| Batch detail | `GET /api/v1/batches/:id` |
| Trials within a batch | `GET /api/v1/trials?batch_id=:id` |
| Trial detail | `GET /api/v1/trials/:id` |

### Distinguishing the names in writing

Every doc page touching trials/batches SHOULD include this short
glossary box (or link to this one) so the easy-to-confuse names
don't drift. Docs that need touch-up:

- `architecture/overview.md` — already uses Trial / Task
  correctly; sweep for any "campaign" mentions.
- `architecture/workflows.md` — DELETE. Workflows are gone.
- `architecture/drf-scheduling.md`,
  `architecture/llm-gateway.md`, `architecture/service-mode.md`,
  `architecture/cost-and-rate-cards.md` — sweep for "campaign"
  mentions; replace with "Batch" where relevant or rewrite the
  section if it described Workflow integration.
- `user-guide.md` — verify no informal "run" usage. Add the new
  "Pasting task ids" section (see parser docs below).
- `authoring-a-task.md` — verify no informal "run" usage.
- `loom-vs-harbor.md` — update the comparison row that mentions
  "campaigns".

PR-1 of the rollout includes this docs sweep.

## Patterns inherited from the reference frontend

The user direction was to learn from the reference frontend's trial
monitoring and submission flows. A read of the reference
`frontend/src/pages/` yielded concrete patterns we adopt:

### Monitor patterns

- **Single route with a Segmented toggle** between two views.
  Harbor: `/trajectory-runs?view=batches|agents` switches
  `运行批次` ↔ `单轨迹详情` on the same page. Loom:
  `/batches?view=batches|trials` switches **Batches** ↔
  **Trials**. The filter bar layout is shared between views;
  view-specific columns appear/disappear. Saves a route, avoids
  losing filter state on switch.
- **Adaptive polling** via `useAdaptivePolling`. Harbor uses
  base 4s / min 3s / max 60s on the list, base 10s on the
  detail, terminal-state throttle to 60s+. Pause on hidden tab,
  slow on blurred tab. Combined with a `query.state.data`
  inspector that returns the fast interval when any row is in
  an active status. Loom already has this hook; PR-3 wires it
  to the new pages with the same cadence numbers.
- **Lazy detail sections.** Detail-page subsections like "Agent
  Runs", "Artifacts", "Manifest preview" sit behind a
  `加载 X` button that flips a `showX` state which gates the
  query's `enabled`. Saves a lot of bandwidth on auto-poll.
  Loom adopts: on the Batch detail page the Trials list and the
  per-trial trajectory previews are lazy.
- **Skeleton inside the table body**, not a separate spinner:
  `<tr><td colSpan={N}><div className="h-12 animate-pulse rounded-xl bg-slate-100" /></td></tr>` × 5 rows. Preserves
  the table layout, no shift on data arrival.
- **Row-level deep-links that pre-populate the other view's
  filters.** Harbor's "查看轨迹" button on a batch row sets
  `agent_public_id` filter + clears unrelated filters + switches
  to the agents view. Loom equivalent: "View trials" button on
  each Batch row pre-populates the Trials view's `batch_id`
  filter and switches the segmented toggle.
- **Two-status taxonomy** (lifecycle + outcome) split as
  separate fields with separate colour scales.
- **Inline error inside the table cell** with a retry button
  that calls `queryClient.invalidateQueries` — never a global
  modal. Empty states are centered slate-400 text inside the
  same shell.

### Submit patterns

- **Single-step submit** (the user explicitly opted out of
  Harbor's two-step precheck → confirm pattern). Submission
  goes straight to queued.
- **2-column responsive grid** for the submit form. Left =
  identity + task selection; right = combinations + runtime
  knobs (Harbor uses `xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]`,
  we match).
- **Right-panel `bg-slate-50` background** to visually separate
  "how to run it" from "what to run".
- **Repeating Combination rows** with a "+ Add combination"
  button at the top of the right panel. Each row:
  agent dropdown / agent version (optional) / model picker /
  n_per_task input. "Remove combination" hidden when only one
  row exists.
- **Inline confirmation banner above submit.** Computes:
  total trials = matched_tasks × Σ_combinations n_per_task,
  per-combination chip showing `combo / agent / model / n=K`,
  validation hints (missing model, invalid n) rendered in
  amber/red within the same banner.
- **Two-phase upload progress** (when files are involved):
  indigo bar driven by axios `onUploadProgress` events, then
  switches to amber at 100% with "server is importing tasks…"
  copy. Not applicable to Loom's catalog-driven flow but worth
  noting for future task-bundle upload.
- **Errors rendered as a red panel below submit** with
  structured detail (code / stage / first failed items) +
  contextual deep-links. Not a toast.

### Visual language

- **Status chips**: `rounded-md px-2 py-1 text-xs font-medium`,
  `bg-{color}-50` + `text-{color}-700`. NOT pills, NOT dots.
- **Colour scale** consistent across lifecycle status and
  outcome status:
  - **emerald** — completed / succeeded
  - **indigo** — running / in-progress
  - **amber** — queued / awaiting / partial
  - **red** — failed
  - **slate** — cancelled / unknown / default
- **Status label refinement**: Harbor's `agentStatusLabel`
  reads `hold_reason` and refines "accepted" to
  `waiting_image_build` / `waiting_resource` /
  `waiting_dispatch`. Loom's Trial state has the same need —
  surface `failure_reason` or `claim_state` in the chip when
  the lifecycle status alone is ambiguous.
- **`glass-card`** wrapper (Harbor's existing primitive: a
  card with backdrop-blur + slight transparency). Loom's
  Card primitive can stay; just adopt the same `shadow-sm
  rounded-2xl border border-slate-200/80 bg-white` look.

## Surfaces

### Top-level navigation

The SPA reduces from 7 nav entries to 3:

- **New batch** — the submission form. Submit button text
  reflects the resolved fan-out (`Submit 1 trial` if one task
  × one sample × one combination; `Submit 984 trials` for 164
  tasks × 2 combinations × 3 samples).
- **Monitor** — one route with a **Segmented toggle** between
  **Batches** (aggregated view: one row per Batch with
  per-state counters + lifecycle/outcome chips) and **Trials**
  (per-trial view: one row per Trial, filterable by `batch_id`
  to drill into a specific batch's trials). Same filter bar
  layout under both. Harbor's pattern: route is
  `/monitor?view=batches|trials` and switching the toggle
  preserves filters.
- **Settings** — tokens, rate cards, profile (existing pages).

A Batch detail page lives at `/batches/:id` for drill-down from
either monitor view. It surfaces the config snapshot + lazy
trials list. Clicking a trial row navigates to the Trial detail
(`/trials/:trial_id`).

What disappears from nav:

- **Tasks page** — the catalog browser is gone. Tasks are picked
  inside the submit form via the benchmark dropdown + subset rules.
  If a power user needs to inspect a task's TaskConfig, the
  `/api/v1/tasks/{id}` endpoint still works for curl; surfacing it
  in the SPA isn't worth a top-level entry.
- **Workflows page** — admin-pinned recipes aren't a user concept
  yet; punt the SPA surface until the data model supports
  saved-recipe sharing in a way users ask for. The data model
  stays; only the UI is removed.
- **Campaigns page (current)** — folded into "Trials" (see below).
- **Trials page (current)** — also folded into "Trials". The
  individual task-run rows are reachable via the trial-detail
  drill-down, not as a separate top-level list.

### `/batches/new` — Submit form

Renamed from the current "New Campaign". The form structure stays
close to what PR J shipped, with three deliberate changes:

1. **Backend dropdown added** as a top-level required field next to
   agent + model. Currently the SPA quietly defaults to whatever
   the worker pool's default driver is; surfacing it removes an
   invisible choice. Catalog comes from a new
   `GET /api/v1/backends` route that returns the union of what
   live workers report as supported (`docker`, `daytona`, `fake`,
   and any worker-reported extras).

   **Backend is a free runtime choice, not task-locked.** Tasks
   describe their environmental needs in `TaskConfig.environment`
   (docker_image, OS, GPU requirements via `requires_caps`). The
   backend is whatever sandbox provider runs that environment.
   The validation isn't a name-match against TaskConfig — it's a
   capability check:
   - The selected backend must be advertised by at least one live
     worker (catalog membership).
   - For tasks with `requires_caps` (GPU vendor / GPU type / OS),
     at least one live worker advertising the selected backend
     must also satisfy those caps.
   - Conflicts surface as 400s with a specific message:
     `"docker workers in this cluster don't have GPU support; this
     task requires gpu_vendor=nvidia. Pick modal or daytona, or
     ask an admin to add GPU workers."`

2. **Task selection card** gets richer than today's "benchmark
   dropdown + id-substring search". The new options:

   - **All tasks in the benchmark** (default, today's behavior).
   - **First N** — first N when sorted by id ascending. Smoke runs.
   - **Last N** — last N. Sometimes the last ones are the hardest.
   - **Random N (seeded)** — uniform random N without replacement.
     Seed input next to the N input; seed defaults to a fresh
     `Date.now()` value on each page load but is editable for
     reproducibility.
   - **Explicit task ids (paste)** — paste box that handles the
     formats users tend to paste from (see "Smart task-id parser"
     below).

   The selection is a radio between the five modes plus a benchmark
   dropdown above them (the benchmark scopes the pool that random/
   first/last operate on; for "Explicit", the benchmark is implied
   by the ids and the dropdown disables).

3. **Every TrialConfig knob remains visible** in the Advanced
   disclosure (kept from PR J). Backend joins the structured fields
   above; nothing else moves.

### `/batches` — Monitoring list

Renamed from the current "Campaigns" list. Each row represents
one Batch and surfaces:

- Batch name (user-given at submit)
- Agent + Model + Backend badges
- Total / Done / In flight / Failed (4 small counters)
- State (submitted / running / finished / cancelled)
- Created at + completed at timestamps
- Aggregate reward (when finished)
- Estimated cost (rate-card lookup × tokens)

To inspect one trial, click into the Batch and then into the
trial row inside it.

### `/batches/:batch_id` — Batch detail

Three sections, vertically stacked:

1. **Header**: batch name, state, agent + model + backend badges,
   timestamps, link back to /batches.
2. **Config snapshot**: every TrialConfig field as a read-only
   table so users can audit what was actually submitted (catches
   "did I really turn skip_verifier off?" right after submit).
3. **Trials in this batch**: one row per (task_id, sample_idx).
   Columns: task id, sample, state, reward, cost, click → Trial
   detail. Filter chips by state.

### `/trials/:trial_id` — Trial detail

The current "Trial Detail" page. Same content: trajectory viewer,
summary stats, ATIF download. Path stays at `/trials/:id` to match
the DB-level name (Harbor + Loom both call this a Trial).

## Smart task-id parser

The "Explicit task ids" paste box accepts any of the formats below
and normalizes them to a sorted, deduplicated id list. The preview
line below the input shows `Parsed N ids` (or an inline error with
the first offending segment).

**All of these parse to the same 5 ids:**

```
Newline-separated (one per line):
  HumanEval/0
  HumanEval/1
  HumanEval/2
  HumanEval/3
  HumanEval/4

Comma-separated:
  HumanEval/0, HumanEval/1, HumanEval/2, HumanEval/3, HumanEval/4

Space-separated:
  HumanEval/0 HumanEval/1 HumanEval/2 HumanEval/3 HumanEval/4

Tab-separated:
  HumanEval/0	HumanEval/1	HumanEval/2	HumanEval/3	HumanEval/4

Semicolon-separated:
  HumanEval/0; HumanEval/1; HumanEval/2; HumanEval/3; HumanEval/4

Pipe-separated:
  HumanEval/0 | HumanEval/1 | HumanEval/2 | HumanEval/3 | HumanEval/4

JSON array (with single or double quotes):
  ["HumanEval/0", "HumanEval/1", "HumanEval/2", "HumanEval/3", "HumanEval/4"]
  ['HumanEval/0', 'HumanEval/1', 'HumanEval/2', 'HumanEval/3', 'HumanEval/4']

Python-list-literal:
  ['HumanEval/0','HumanEval/1','HumanEval/2','HumanEval/3','HumanEval/4',]

Range shorthand:
  HumanEval/0-4

Comma-list-after-prefix shorthand:
  HumanEval/0,1,2,3,4

Mixed range + list shorthand:
  HumanEval/0-2, HumanEval/3, HumanEval/4

URL paths (strip API/SPA prefix):
  /api/v1/tasks/HumanEval/0
  /api/v1/tasks/HumanEval/1
  /tasks/HumanEval/2

Markdown bullet list:
  - HumanEval/0
  - HumanEval/1
  * HumanEval/2
  • HumanEval/3
  → HumanEval/4

Markdown numbered list:
  1. HumanEval/0
  2. HumanEval/1
  …

Markdown table (1 column):
  | task_id      |
  | ------------ |
  | HumanEval/0  |
  | HumanEval/1  |

CSV with header (first column wins):
  task_id,note
  HumanEval/0,easy
  HumanEval/1,medium
  …

Code-fenced block (strip the ``` fences):
  ```
  HumanEval/0
  HumanEval/1
  …
  ```

Hybrid (whatever the user pastes from a notebook cell):
  # smoke set, picked 2026-06-09
  - HumanEval/0  # easy
  - HumanEval/1, HumanEval/2
  HumanEval/3
  HumanEval/4
```

**Parsing rules** (applied in order):

1. Drop everything inside triple-backtick fences except the
   contents (i.e. `` ``` ``-strip).
2. Strip everything after `#` on a line (treat as a comment).
3. Strip leading `-`, `*`, `•`, `→`, `>`, `1.`, `2.`, etc. on each
   line (markdown bullets / numbered-list noise).
4. Strip pipe-table cell delimiters `|` and the `| --- |` separator
   row from markdown tables.
5. Strip JSON array brackets, quotes (single + double), trailing
   commas, Python's `(`, `)`, `[`, `]`.
6. Drop empty lines / blank-only segments.
7. Detect a CSV header row: if the first line has multiple
   comma-separated fields AND the first field reads like a column
   name (not a task id pattern), discard it and take the first
   column from every subsequent row.
8. Split each remaining line on any of: comma, semicolon, pipe,
   tab, or 2+ spaces. (Single spaces are kept because task ids
   sometimes contain them.)
9. For each segment matching `<prefix>/<a>-<b>` (a,b integers,
   a ≤ b), expand to `<prefix>/a … <prefix>/b`.
10. For each segment matching `<prefix>/<n1>,<n2>,<n3>`
    (numbers-only after the prefix), expand to
    `<prefix>/n1`, `<prefix>/n2`, …
11. Strip `/api/v1/tasks/` or `/tasks/` URL prefixes if present.
12. Final list = `Array.from(new Set(parsed)).sort()`.

The preview updates on every keystroke; the parser is pure (no
network), so cost is negligible.

A "Validate against catalog" button does a single
`GET /api/v1/tasks?task_ids=<ids>&limit=N` roundtrip and surfaces
any unknown ids inline. Submission itself validates server-side
(same code path the campaign route already runs), so the catalog
check is purely a UX prefetch.

### In-frontend documentation

The paste field renders this disclosure right below the textarea
(closed by default, opens to a scrollable panel):

```
▸ Accepted formats (click to expand)
  One id per line, or any of:
    • comma / semicolon / pipe / tab separated
    • JSON array (single or double quotes)
    • range shorthand: HumanEval/0-4
    • prefix shorthand: HumanEval/0,1,2,3
    • markdown bullets, numbered lists, tables
    • CSV with header (first column wins)
    • triple-backtick fences (stripped)
    • # comments (rest-of-line stripped)
  Examples + the full parsing rules:
  → docs/user-guide.md#task-id-paste-formats
```

The link points at `docs/user-guide.md` which gets a new
**"Pasting task ids"** subsection mirroring this list. Both
locations (in-frontend disclosure + user-guide section) ship in
PR-2.

## Backend dropdown — `/api/v1/backends`

New route. Returns the union of backends every currently-active
worker reports as supported, plus a per-entry description so the
SPA can render a tooltip. Output:

```json
{
  "items": [
    {"name": "docker", "description": "Local docker on the worker host."},
    {"name": "daytona", "description": "Cloud sandboxes via the Daytona API."},
    {"name": "fake", "description": "In-memory driver. Tests + smoke only — no real env."}
  ]
}
```

A backend appears in the list iff at least one currently-active
worker advertises support for it. (Workers report capabilities at
registration; the route queries the workers table directly.) This
keeps the dropdown honest about what will actually run rather than
listing things no available worker can handle.

## Data model changes

Three migrations land in order — rename first, then drop, then
extend. Each is reviewable in isolation:

### Migration 0011 — rename Campaign → Batch

```sql
ALTER TABLE campaigns RENAME TO batches;
ALTER TABLE trials RENAME COLUMN campaign_id TO batch_id;
ALTER INDEX campaigns_pkey RENAME TO batches_pkey;
ALTER INDEX trials_campaign_id_idx RENAME TO trials_batch_id_idx;
-- (plus any other campaign-named index / constraint)
```

Pure metadata rename — no data movement, no downtime. Same
migration touches Python: `Campaign` Pydantic model →
`Batch`, `campaign_runner.py` → `batch_runner.py`,
`routes/campaigns.py` → `routes/batches.py`,
`/api/v1/campaigns` → `/api/v1/batches`. Test files renamed
in lockstep.

### Migration 0012 — drop Workflow

```sql
ALTER TABLE batches DROP COLUMN workflow_id;
DROP INDEX IF EXISTS batches_workflow_id_idx;
DROP TABLE workflows;
```

Drops the table, the back-reference column on batches (was on
campaigns under the old name), the partial unique index on
`workflows.name`. Python: delete `src/loom_service/routes/
workflows.py`, the `Workflow` model in
`src/loom/db/schema.py`, the Workflow SPA pages, all workflow
tests.

### Migration 0013 — Batch.backend + Batch.combinations + Trial.combination_idx + result_status

```sql
ALTER TABLE batches
  ADD COLUMN backend       TEXT NOT NULL DEFAULT 'docker',
  ADD COLUMN combinations  JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN result_status TEXT;
ALTER TABLE trials
  ADD COLUMN combination_idx INTEGER NOT NULL DEFAULT 0;
```

Four additions in one migration:

- **`batches.backend`** — see Backend dropdown section above.
- **`batches.combinations`** — list of `{agent_name, agent_model,
  n_per_task, label?}` tuples for multi-(agent, model) batches.
  Default `[]` means single-combination behavior (back-compat
  with the new `trial_config.agent_name` / `agent_model` /
  `n_per_task` path). Same backward-compat pattern as the
  earlier variants spec.
- **`batches.result_status`** — outcome lane separate from
  lifecycle `status`. Values: `null` (in progress / pre-terminal),
  `succeeded` (all trials terminal + all reward > 0),
  `partial_failed` (mix), `all_failed`, `cancelled`. Computed by
  the batch-runner when transitioning the lifecycle status to
  `finished`/`cancelled`. The existing `status` column keeps the
  lifecycle (`submitted` → `running` → `finished` / `cancelled`).
- **`trials.combination_idx`** — which Combination this trial
  belongs to within its parent Batch. 0 for single-combination
  batches. The idempotency key becomes
  `{batch}::{task}::{combination_idx}::{sample_idx}` when
  `combinations` is non-empty; preserves the 3-segment form
  otherwise.

### No-DDL extensions

- Task subset rules (first/last/random/explicit) live in the
  existing `task_filter` JSONB blob with a `subset_kind`
  discriminator. The route's `_resolve_task_filter` gains a
  switch on `subset_kind`. No new column.
- Backend catalog (`GET /api/v1/backends`) is derived from the
  `workers` table's `capabilities` JSONB at request time. No new
  column or table.

## API changes

### POST /api/v1/batches (was /campaigns)

Request body — single-combination form (backward-compat with
today's submit):

```json
{
  "name": "humaneval — claude-opus",
  "backend": "docker",
  "task_filter": {
    "benchmark_id": "humaneval",
    "subset_kind": "random_n",
    "n": 50,
    "seed": 42
  },
  "trial_config": { /* every TrialConfig knob */ },
  "n_per_task": 3
}
```

Multi-combination form (Harbor-style `组合` rows). The SPA always
sends this shape when more than one combination is selected:

```json
{
  "name": "humaneval — claude vs gpt",
  "backend": "docker",
  "task_filter": { "benchmark_id": "humaneval", "subset_kind": "all" },
  "trial_config": { /* shared knobs only — NO agent_name / agent_model / n_per_task */ },
  "combinations": [
    {
      "label": "claude-opus",
      "agent_name": "claude-code-inbox",
      "agent_model": {"provider": "anthropic", "name": "claude-opus-4-7"},
      "n_per_task": 3
    },
    {
      "label": "gpt-4o",
      "agent_name": "claude-code-inbox",
      "agent_model": {"provider": "openai", "name": "gpt-4o"},
      "n_per_task": 3
    }
  ]
}
```

`subset_kind` values: `"all"` (default), `"first_n"`, `"last_n"`,
`"random_n"`, `"explicit"`. When `subset_kind = "explicit"`, the
parser-emitted `task_ids` list goes in `task_ids`; `benchmark_id`
becomes optional (the explicit ids ARE the slate). When
`subset_kind = "random_n"`, `seed` is required for reproducibility.

Server-side validation:
- `n` is positive when subset_kind is first/last/random.
- `seed` is a 32-bit unsigned int.
- `task_ids` is non-empty when subset_kind is explicit.
- `backend` must be advertised by ≥1 live worker AND that worker
  must satisfy any `requires_caps` derived from the matched
  tasks (capability check, not name-match).
- When `combinations` is non-empty: `trial_config.agent_name`,
  `agent_model`, and `n_per_task` MUST be absent (route 400s
  otherwise — one source of truth per Batch).
- Every Combination's `agent_name` must be in the agent catalog.
- Combination `label`s are unique within the Batch (case-sensitive).
- `combinations` cap: 16 entries max.

### GET /api/v1/backends (new)

Described above.

### GET /api/v1/batches/:id (was /campaigns/:id)

Adds `backend` and the resolved `task_filter` (so the Batch
detail can render "ran 50 random tasks (seed 42) on docker"
without re-running the selection).

### DELETE — Workflow routes are gone

`GET /api/v1/workflows`, `POST`, `PATCH`, `DELETE`, the
`workflows/:id/launch` route — all removed in migration 0012.
The `loom-launcher` registry is unrelated and stays.

## Validation summary

Client-side (UI blocks submit):
- Name required.
- Benchmark required (unless subset_kind = explicit and at least
  one parsed id).
- Subset rules: n ≥ 1 for first/last/random; ids ≥ 1 for explicit.
- Random subset: seed is editable; defaults to a fresh value.
- Agent + model: catalog membership (today's checks).
- Backend: in the live-worker catalog.
- Fan-out cap: matched_tasks × n_per_task ≤ 200 unless confirmed.

Server-side (route 400s):
- All of the above.
- `subset_kind = explicit` rejects request if any of the supplied
  ids isn't in the tasks table.
- `backend` is rejected when no live worker advertises it.

## What this design intentionally does NOT do

- **No comparison feature on campaigns.** The user explicitly said
  not to build it (the answer to the comparison question above).
  If two campaigns need comparison later, export CSVs and analyse
  externally.
- **No Workflows admin page.** Workflows stay in the data model;
  the admin UI for creating them is dropped. If teams want pinned
  recipes, they can be created via curl + admin token until
  there's user pressure for a UI.
- **No Tasks catalog page.** Same logic — `/api/v1/tasks` still
  works for power users.
- **No multi-variant submission.** Dropped per the variants spec
  supersede note. To compare two configurations, submit twice.
- **No SPA rename of Trial → Run at the data layer.** The UI uses
  "Task run" for the leaf execution; the URL is `/runs/:id`
  because that reads better. The DB keeps "trials".

## Rollout

Three PRs, in order. PR-1 and PR-2 are mechanical (rename / drop)
and small enough to land same-day; PR-3 is the actual feature work.

### PR-1: Rename Campaign → Batch (everywhere)

- Migration 0011 (table + column + index rename).
- `src/loom_service/campaign_runner.py` → `batch_runner.py`;
  module-level `Campaign` references → `Batch`.
- `src/loom_service/routes/campaigns.py` → `routes/batches.py`;
  route prefix change.
- `src/loom/db/schema.py` — `Campaign` model → `Batch`,
  `Trial.campaign_id` → `Trial.batch_id`.
- Tests: `tests/integration/test_service_campaigns_crud.py` →
  `test_service_batches_crud.py`; assert URLs + JSON keys
  updated. Same for `test_campaign_runner_e2e.py` →
  `test_batch_runner_e2e.py`.
- SPA: rename `web/src/pages/Campaigns.tsx` → `Batches.tsx`,
  `NewCampaign.tsx` → `NewBatch.tsx` (kept "New trial" label
  was wrong in the v3 first draft; the form creates a Batch).
  Route paths in `App.tsx` updated.
- Docs: sweep architecture/* for "campaign" mentions; replace
  with "batch". Update loom-vs-harbor.md.
- Verification: every grep for `\bcampaign\b` in src/, tests/,
  docs/, web/src/ returns zero (except for the migration file
  itself and this doc).

No behavior changes. Pure rename + minimal docs touch-ups.

### PR-2: Drop Workflow

- Migration 0012 (drop column, drop table).
- Delete:
  - `src/loom_service/routes/workflows.py`
  - `Workflow` model in `src/loom/db/schema.py`
  - `Batch.workflow_id` column reference in the Batch model
  - `tests/integration/test_service_workflows.py`
  - `web/src/pages/Workflows.tsx`,
    `web/src/pages/WorkflowDetail.tsx`,
    `web/src/components/SubmitWorkflowModal.tsx` (or wherever
    Workflow components live)
  - `web/src/__tests__/pages/Workflows.test.tsx` etc.
  - Workflow routes in `web/src/App.tsx` + nav entry.
  - `docs/architecture/workflows.md` (deleted file)
- Verification: every grep for `workflow` in src/, tests/, docs/,
  web/src/ returns zero (except the migration file + this doc).
- The `loom-launcher` registry stays (it's unrelated; the name
  "launcher" makes this clear).

Negative LOC. Risk is low because Workflow landed yesterday and
has no production users.

### PR-3: Backend + combinations + two-status + smart subset (backend)

- Migration 0013 — backend column, combinations JSONB,
  result_status, combination_idx (4-column migration as one
  unit since they ship together).
- Pydantic models: `Combination`, batch payload accepts both
  shapes (single-combination + multi-combination); validation
  per the rules above.
- POST /batches accepts `backend` + `subset_kind` / `seed` /
  `task_ids` on task_filter + `combinations`.
- GET /backends route + backend catalog module derived from
  worker capabilities. Capability-check helper invoked at
  submit time.
- batch_runner fan-out walks `(task_id, combination_idx,
  sample_idx)` units; idempotency_key 4-segment format for
  multi-combination batches.
- result_status computation lives in batch_runner's state
  advance — when transitioning to `finished`, derive from
  trial reward rollup; when transitioning to `cancelled`, set
  `result_status = "cancelled"`.
- `_resolve_task_filter` honors `subset_kind`.
- Backend + integration tests for everything.

### PR-4: SPA — segmented Monitor + new submit form

- Monitor route `/monitor` with the Segmented toggle (Batches
  ↔ Trials), shared filter bar, view-conditional columns,
  adaptive polling at Harbor's cadence (base 4s / min 3s /
  max 60s on list, 60s+ when all-terminal).
- Submit form `/batches/new`:
  - 2-column responsive layout matching Harbor
    (`xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]`,
    `bg-slate-50` on the right panel).
  - Identity + task selection on the left (benchmark dropdown
    + subset radio + smart paste parser + live "N tasks
    matched" preview).
  - Combinations on the right: repeating rows with agent /
    model / n_per_task. "+ Add combination" + "Remove" per
    row. Cap at 16.
  - Backend dropdown.
  - Advanced TrialConfig section collapsed by default.
  - Inline confirmation banner above submit: per-combination
    chips + total fan-out + validation hints.
  - Submit button text dynamic: `Submit N trial(s)`.
- Batch detail page (`/batches/:id`) with lazy trials list.
- Two-status chips everywhere (lifecycle + outcome on Batch;
  lifecycle-only on Trial since the state encodes outcome).
- Smart paste parser implementation + 15-format test coverage.
- In-frontend disclosure listing accepted formats.
- `docs/user-guide.md` gains the "Pasting task ids" subsection.
- Skeleton-in-table-body pattern everywhere.
- Drop the top-level "Trials" nav entry; URLs `/trials` and
  `/campaigns` redirect to `/monitor`.

### PR-5: Polish + redirects + docs sweep

- All `/campaigns/*`, `/workflows/*`, `/tasks` SPA routes
  redirect to their `/monitor` equivalents (or a 404 with a
  "moved" hint for the few that don't have a target).
- Sweep all docs for stale "campaign" / "workflow" / "trial
  catalog" references that PR-1/PR-2 missed.
- Empty-state copy passes (every list page has a friendly
  empty state).
- Skeleton states audited.

Estimated sizes: PR-1 ~600 LOC (mostly renames), PR-2 ~800 LOC
deleted, PR-3 ~900 LOC, PR-4 ~1400 LOC, PR-5 ~200 LOC.

## Open questions

1. **Per-trial backend override** — RESOLVED. Backend is a free
   runtime choice (the user is the boss); TaskConfig describes
   environmental needs (image / OS / GPU), the backend is whatever
   sandbox provider runs them. Validation is capability-based, not
   name-locked (see Backend dropdown section above).
2. **Backend catalog freshness** — backends advertised by workers
   stop being valid when those workers drain. The catalog query
   uses the workers table; an idle/drained worker doesn't show up.
   If the LAST worker advertising `daytona` drains mid-flight,
   campaigns already submitted on daytona stall (no claim). The
   campaign-runner already handles this case (claim returns 204,
   campaign stays queued); the catalog route just stops listing the
   backend.
3. **Workflows data-model retention** — even though we drop the
   admin UI, the data model stays. Migrations + tests preserve
   workflows behavior. Should the campaign-runner still surface
   `workflow_id` on campaigns when no admin UI creates them? Yes —
   curl-driven workflows still flow through (zero regression).
