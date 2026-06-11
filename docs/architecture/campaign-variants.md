# Campaign Variants — multi-(agent, model) comparison campaigns

**Status:** SUPERSEDED 2026-06-11 — see
[loom-spa-v3.md](loom-spa-v3.md). The user direction shifted away
from variants in favor of a simpler "submit-one-batch-at-a-time"
SPA. The data-model design below remains useful reference if
multi-variant ever returns, but is not on the v1 path.

**Status when authored:** design (not yet shipped)

**Date:** 2026-06-11

## Motivation

Today a `Campaign` runs **one** (agent, model) configuration across a
task slate. To compare "claude-opus-4-7 vs gpt-4o on HumanEval" the
user has to create two separate campaigns and join the results by
hand.

The NewCampaign form's header copy currently says "multi-agent /
multi-model comparison runs aren't supported by the data model yet" —
this doc fills that gap.

## Goal

One campaign launches `M tasks × V variants × N samples` trials,
where each variant is a distinct (agent, model) pair (optionally with
its own overrides on top of the shared trial_config). Trials carry
their variant index so result aggregation can group by variant.

## Non-goals

- **Comparison UI / charting.** This doc enables the data model; the
  campaign-detail page redesign that shows per-variant rollups +
  comparison charts is a separate follow-up.
- **Per-task (not per-variant) configuration.** The user can say "run
  HumanEval/0 with oracle and HumanEval/1 with claude-code" by
  defining two single-task variants — but the data model does not
  model "this task instance runs with that config" directly. If the
  power-user case grows, that's a future extension.
- **Workflow-level variants.** A Workflow still pins one (agent,
  model). Workflow → Campaign launch passes that single config
  through. Multi-variant Workflows is a future extension that needs
  the same data-model change, so this design accommodates it without
  baking it in.

## In scope (added per user feedback)

User asked: "different retry counts for different variants, and/or
set up what benchmark paired with what agent and what model to use
for each task". The design now treats a variant as the unit of
**full configuration override**:

- **Per-variant task scope.** A variant may carry its own
  `task_filter`. When set, this variant runs only on the tasks that
  filter resolves to — independent of every other variant. When
  absent (the default), the variant inherits the campaign's shared
  `task_filter`.
- **Per-variant sample count.** A variant may carry its own
  `n_per_task`. When set, this variant runs that many samples per
  matched task instead of the campaign default.
- **Per-variant TrialConfig overrides.** A variant may override any
  field of the shared `trial_config` (skip_verifier, retry policy,
  every timeout knob). When absent, the field is inherited.

The variants list thus expresses "the full matrix of
{tasks × agent × model × samples × trial knobs} cells the campaign
will run", with most cells inheriting from a shared baseline so the
common case stays simple.

## Data model

### `Campaign.variants` JSONB column

```sql
ALTER TABLE campaigns ADD COLUMN variants JSONB NOT NULL DEFAULT '[]'::jsonb;
```

Shape (validated by Pydantic, stored as JSONB):

```python
class CampaignVariant(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # Required — every variant identifies an (agent, model) pair.
    agent_name: str = Field(min_length=1)
    agent_model: ModelSpec | None

    # Display label for SPA + ATIF. Falls back to a derived value
    # `f"{agent_name}" + (f"/{provider}/{name}" if agent_model else "")`
    # when None. Must be unique within the campaign.
    label: str | None = None

    # Per-variant task scope. When set, this variant materializes its
    # OWN task list (independent of every other variant). When None,
    # inherits the campaign's shared task_filter. Accepts the same
    # keys the campaign-level filter does — {license, task_ids,
    # benchmark_id} — and is validated identically.
    task_filter: dict[str, Any] | None = None

    # Per-variant sample count. When set, this variant runs this
    # many samples per matched task instead of the campaign's
    # n_per_task. 1..100.
    n_per_task: int | None = Field(default=None, ge=1, le=100)

    # Per-variant overrides on the shared trial_config. Same shape
    # as TrialConfig minus agent_name + agent_model (which live on
    # the variant directly above). Each field is independently
    # overridable; absent fields inherit the shared value.
    overrides: VariantOverrides | None = None


class VariantOverrides(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    skip_verifier: bool | None = None
    verifier_env_mode: VerifierEnvMode | None = None
    override_agent_timeout_sec: float | None = Field(default=None, gt=0)
    override_verifier_timeout_sec: float | None = Field(default=None, gt=0)
    override_env_build_timeout_sec: float | None = Field(default=None, gt=0)
    agent_timeout_multiplier: float | None = Field(default=None, gt=0)
    verifier_timeout_multiplier: float | None = Field(default=None, gt=0)
    env_build_timeout_multiplier: float | None = Field(default=None, gt=0)
    retry: RetryPolicy | None = None
    submit_priority: int | None = Field(default=None, ge=0, le=1000)
```

The fields available for per-variant override are the ones where
"different per variant" is semantically coherent. Fields that apply
campaign-wide (force_build, delete_env, extra_mcp_servers,
extra_skills, baseline_network_policy_override) are NOT overridable
per variant — they live only on the shared trial_config.

### Backwards compat semantics

`Campaign.variants = []` (the migration default) means single-variant
behavior, identical to today:

- Campaign.trial_config carries `agent_name` + `agent_model` as
  today.
- Fan-out logic walks `(task_id, sample_idx)`.

`Campaign.variants` non-empty means multi-variant:

- Campaign.trial_config **must not** carry `agent_name` / `agent_model`
  (server-side validator rejects with 400).
- Fan-out logic walks `(task_id, variant_idx, sample_idx)`.

This split avoids ambiguity about which agent_name "wins" — there's
exactly one source of truth per campaign.

### `Trial.variant_idx` column

```sql
ALTER TABLE trials ADD COLUMN variant_idx INTEGER NOT NULL DEFAULT 0;
```

Mirrors `sample_idx` (migration 0010). `variant_idx = 0` for
single-variant campaigns; `variant_idx = i` for the i-th element of
`Campaign.variants` in multi-variant.

### Idempotency key

Today: `{campaign_id}::{task_id}::{sample_idx}`

After this change:
- Single-variant: unchanged (`{campaign}::{task}::{sample}`) — the
  variant_idx=0 is implicit, no schema break to in-flight campaigns.
- Multi-variant: `{campaign}::{task}::{variant_idx}::{sample_idx}` —
  the extra segment keeps multi-variant campaigns from colliding with
  single-variant historical keys.

Encoded as:

```python
def _idempotency_key(
    campaign_id: UUID,
    task_id: str,
    sample_idx: int,
    variant_idx: int | None = None,  # None => single-variant
) -> str:
    if variant_idx is None:
        return f"{campaign_id}::{task_id}::{sample_idx}"
    return f"{campaign_id}::{task_id}::{variant_idx}::{sample_idx}"
```

### `expected_trial_count`

Today: `len(task_ids) * n_per_task`

After this change: sum across variants of each variant's resolved
fan-out, because per-variant `task_filter` / `n_per_task` may
diverge.

```python
def expected_trial_count(c: Campaign, shared_task_ids: list[str]) -> int:
    if not c.variants:
        return len(shared_task_ids) * c.n_per_task

    total = 0
    for variant in c.variants:
        v_task_ids = (
            resolve(variant.task_filter)
            if variant.task_filter else shared_task_ids
        )
        v_n = variant.n_per_task or c.n_per_task
        total += len(v_task_ids) * v_n
    return total
```

Computed once at campaign create and persisted on the row (same as
today's behavior — the field is materialized, not derived on read).

## Migration

`0011_campaign_variants_and_variant_idx.py`:

- `ALTER TABLE campaigns ADD COLUMN variants JSONB NOT NULL DEFAULT '[]'::jsonb`
- `ALTER TABLE trials ADD COLUMN variant_idx INTEGER NOT NULL DEFAULT 0`

`DEFAULT '[]'` + `DEFAULT 0` on both columns means existing rows
read as single-variant without backfill. No data migration needed.

`Workflow` table is untouched in this migration — Workflows pin a
single (agent, model) today and continue to do so. A future migration
adds `workflows.variants` once Workflows-level variants land.

## API surface

### `POST /api/v1/campaigns`

Request body adds optional `variants`. A real example showing the
shared baseline + per-variant overrides (full flexibility):

```json
{
  "name": "humaneval vs mbpp — agent comparison",
  "task_filter": {"benchmark_id": "humaneval"},
  "n_per_task": 3,
  "trial_config": {
    "force_build": false,
    "submit_priority": 100,
    "skip_verifier": false,
    "agent_timeout_multiplier": 1.0
  },
  "variants": [
    {
      "label": "claude-opus on HumanEval",
      "agent_name": "claude-code-inbox",
      "agent_model": {"provider": "anthropic", "name": "claude-opus-4-7"}
    },
    {
      "label": "gpt-4o on HumanEval (more samples)",
      "agent_name": "claude-code-inbox",
      "agent_model": {"provider": "openai", "name": "gpt-4o"},
      "n_per_task": 10,
      "overrides": {
        "agent_timeout_multiplier": 2.0,
        "retry": {
          "max_attempts": 3,
          "retry_on": ["worker_crash", "env_start_failure"],
          "backoff": {"base_sec": 30, "max_sec": 600, "multiplier": 2, "jitter": 0.2}
        }
      }
    },
    {
      "label": "gemini-pro on MBPP",
      "agent_name": "claude-code-inbox",
      "agent_model": {"provider": "google", "name": "gemini-2.5-pro"},
      "task_filter": {"benchmark_id": "mbpp"},
      "overrides": {"skip_verifier": true}
    }
  ]
}
```

Reading the example: variant 1 inherits everything from the
campaign. Variant 2 overrides sample count, agent timeout, and adds
retry. Variant 3 runs on a *different* benchmark (MBPP) with skip
verifier turned on.

Validation:

- If `variants` is absent or `[]`: existing single-variant behavior.
  `trial_config.agent_name` and `trial_config.agent_model` are
  required (unchanged from today).
- If `variants` is non-empty: every variant validated against the
  agent catalog (`agent_name` must be a known agent). Setting
  `trial_config.agent_name` or `trial_config.agent_model` is
  rejected with 400 ("variants supplied — clear agent_name /
  agent_model from trial_config").
- A campaign with 1 variant is allowed (degenerates to "this is a
  multi-variant campaign that happens to have one variant") so the
  SPA can use the multi-variant code path uniformly.
- Variant labels are unique within a campaign (case-sensitive); the
  route 400s on duplicates so the SPA's "compare by variant" view
  works.
- The 422 / `extra="forbid"` semantics of TrialConfig continue to
  apply per-variant when merging shared `trial_config` + variant
  overrides.

Response:

```json
{
  "campaign_id": "…",
  "expected_trial_count": 18,
  "n_per_task": 3,
  "variant_count": 2,
  "state": "submitted",
  "created_at": "…"
}
```

### `GET /api/v1/campaigns/{id}`

Adds `variants` to the response when non-empty. The shape mirrors the
request body. Single-variant campaigns omit `variants` (or return
`[]`) for backward compat — the existing SPA detail page ignores
unknown fields.

### `GET /api/v1/trials`

Adds `variant_idx` and `variant_label` to each trial row. `variant_label`
comes from joining `Trial.campaign_id` → `Campaign.variants[variant_idx].label`.
Both are null for non-campaign trials (hand-submitted via POST /trials)
and for campaigns with no variants.

### `GET /api/v1/trials/{id}`

Same: surfaces `variant_idx` and `variant_label` when applicable.

### `POST /api/v1/trials`

Unchanged. Hand-submitted trials are inherently single-variant.

## Campaign runner

`campaign_runner.run_once`'s Phase-1 / Phase-2 logic changes to walk
**per-variant** task scopes:

```python
async def _resolve_pending(
    session: AsyncSession, c: Campaign, shared_task_ids: list[str],
) -> list[PendingTrial]:
    """Pending (task_id, variant_idx, sample_idx) units. Each variant
    materialises its own task slate when it carries a task_filter
    override; otherwise it inherits `shared_task_ids`."""

    existing = {
        (row[0], row[1], row[2])
        for row in (await session.execute(
            select(Trial.task_id, Trial.variant_idx, Trial.sample_idx)
              .where(Trial.campaign_id == c.id),
        )).all()
    }

    # Single-variant fast path — preserves today's exact behavior.
    if not c.variants:
        pending: list[PendingTrial] = []
        for t in shared_task_ids:
            for s in range(c.n_per_task):
                if (t, 0, s) not in existing:
                    pending.append(PendingTrial(t, 0, s))
        return pending

    # Multi-variant path — each variant resolves its own scope.
    pending = []
    for v_idx, variant in enumerate(c.variants):
        if variant.task_filter:
            v_task_ids = await _resolve_task_filter(session, variant.task_filter)
        else:
            v_task_ids = shared_task_ids
        v_n_per_task = variant.n_per_task or c.n_per_task
        for t in v_task_ids:
            for s in range(v_n_per_task):
                if (t, v_idx, s) not in existing:
                    pending.append(PendingTrial(t, v_idx, s))
    return pending
```

Phase-2 `_submit_one` accepts `variant_idx`, materializes the
per-trial config by merging shared + variant overrides, and sends the
idempotency key in the new format:

```python
def _materialize_trial_config(
    shared: dict, variant: CampaignVariant | None,
) -> dict:
    if variant is None:
        return shared  # single-variant fast path

    # Start from a deepcopy of the shared config so per-variant
    # mutations don't leak across variants in the same tick.
    out = copy.deepcopy(shared)

    # Per-variant overrides clobber shared values field-by-field.
    overrides = variant.overrides.model_dump(exclude_none=True) if variant.overrides else {}
    out.update(overrides)

    # Agent + model live on the variant itself (never on shared
    # when variants are present — the route enforces this).
    out["agent_name"] = variant.agent_name
    out["agent_model"] = (
        variant.agent_model.model_dump(mode="json")
        if variant.agent_model else None
    )
    return out
```

The CP route persists `sample_idx` and now `variant_idx` (defaulting
to 0 for non-campaign trials).

## SPA changes

### Design principles

These goals come from explicit user feedback ("professional design,
strong features without loss of configure flexibility") and they
shape every decision below:

1. **Single-variant doesn't get more complex.** A campaign with one
   variant looks and behaves exactly like today's NewCampaign. The
   variant UI only "lights up" when the user adds a second variant.
2. **Multi-variant doesn't lose any configuration knob the
   single-variant form has.** Every TrialConfig field reachable today
   must still be reachable — either as a shared (campaign-level)
   config OR as a per-variant override.
3. **Comparison is the point.** The form makes it obvious that the
   user is building a comparison: total-trials preview, per-variant
   cost preview, "what the results table will look like" preview.
4. **Avoid grunt work.** Common patterns (agents × models matrix,
   "same agent with two models", duplicate-and-tweak) are one click,
   not seven inputs filled in by hand.
5. **Field-level provenance.** When a per-variant override is set,
   the field shows it's overriding the shared value. A "reset to
   shared" affordance restores the default. No mystery.

### Information architecture

NewCampaign's structure becomes:

```
1. Identity                          (Card)
2. Which tasks to run                (Card — unchanged)
3. Shared trial config               (Card — see below)
4. Variants                          (Card — the new heart of the form)
5. Samples per task                  (Card)
6. Confirm + Submit                  (Footer)
```

The **Shared trial config** card holds every TrialConfig field that
isn't agent/model: timeouts, retry, skip_verifier, force_build,
delete_env, verifier_env_mode, submit_priority. Today's "Advanced
options" card is renamed and gets a clarifying header: "These apply
to every variant unless a variant overrides them below."

The **Variants** card is the new design surface. See below.

### Variants card — layout

The design principle that drives this layout: **the card surface
always fits the "I just want to swap agent and model" use case in
one glance**, and every advanced knob is one click deeper *inside
the same card*. No modals for power-user features; no rapid card
height changes for the simple case.

**Collapsed variant (defaults — what 90% of users see):**

```
┌─ ⋮⋮ Variant 1                          ⎘ ✕ ┐
│ Label  [claude-opus            ]            │
│ Agent  [claude-code-inbox             ▾ ]   │
│ Model  [anthropic / claude-opus-4-7   ▾ ]   │
│                                             │
│ Tasks      shared (HumanEval, 164)          │
│ Samples    shared (3)                       │
│ ▸ Overrides (0)                             │
│                                             │
│ ◷ ~$2.34   →  3 × 164 = 492 trials          │
└─────────────────────────────────────────────┘
```

**Expanded variant (every override slot visible inline):**

```
┌─ ⋮⋮ Variant 1                          ⎘ ✕ ┐
│ Label  [claude-opus            ]            │
│ Agent  [claude-code-inbox             ▾ ]   │
│ Model  [anthropic / claude-opus-4-7   ▾ ]   │
│                                             │
│ ▼ Tasks                                     │
│   ○ Use campaign default (HumanEval, 164)   │
│   ● Use a different scope for this variant  │
│     Benchmark   [MBPP                  ▾ ]  │
│     Search      [_____________________]     │
│     → 374 tasks match                       │
│                                             │
│ ▼ Samples per task                          │
│   ○ Use campaign default (3)                │
│   ● Override: [10]   (1 – 100)              │
│                                             │
│ ▼ Overrides (3)                             │
│   ▸ Timeouts (1 overridden) →               │
│   ▸ Retry    (max=5, reasons=[crash])  →    │
│   ▸ Verifier (skip)                    →    │
│                                             │
│ ◷ ~$5.20  →  10 × 374 = 3740 trials         │
└─────────────────────────────────────────────┘
```

Three things to notice in the expanded form:

1. The default state for every override field is the **radio
   "Use campaign default"** — picking the override radio reveals
   the input. This makes "inherits" visually distinct from "I set
   this myself", and resetting is one click.
2. The trial-count line lives at the bottom of every variant —
   updates live as the user changes the variant's scope or sample
   count. The product is **this variant's** fan-out, not the
   campaign's, so users can sanity-check per-variant cost before
   submit.
3. The TrialConfig overrides (timeouts, retry, verifier) collapse
   into a single accordion with a count to keep the card vertically
   manageable even with all overrides on.

**The Variants section as a whole:**

```
┌─ Variants ───────────────────────────────────────────────────────┐
│  3 variants • Total: 4232 trials                                  │
│    • claude-opus on HumanEval: 492 (164 × 3)                      │
│    • gpt-4o on HumanEval: 492 (164 × 3)                           │
│    • gemini-pro on MBPP × 10 samples: 3740 (374 × 10)             │
│                                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │
│  │ Variant 1   │  │ Variant 2   │  │ Variant 3   │                │
│  │ (collapsed) │  │ (collapsed) │  │ (expanded)  │                │
│  └─────────────┘  └─────────────┘  └─────────────┘                │
│                                                                   │
│  + Add variant      ◊ Build matrix     ⎘ Duplicate variant 1      │
└───────────────────────────────────────────────────────────────────┘
```

The summary line at the top reflects per-variant scopes — when
variants diverge in tasks or samples, the user sees the breakdown
right away.

**Per-card elements:**

- `⋮⋮` drag handle — drag to reorder. Variant_idx is positional; the
  drag preserves identity (no idempotency-key churn for already-
  submitted variants because the runner derives idempotency from
  the persisted Campaign.variants array, and reorders before submit
  are pre-persistence). Drag is a v2 enhancement; v1 ships up/down
  arrows in the card header.
- `⎘` (duplicate) / `✕` (remove) live in the card header.
- **Label** — auto-derived as `"{model.name}"` when a model is set,
  `"{agent_name}"` otherwise. Editable. Duplicate labels are
  highlighted red with a tooltip; submit is blocked until unique.
- **Agent / Model** — same AgentModelPicker the form already uses.
- **Tasks slot** — collapsed shows `shared (Benchmark, N matched)`.
  Tapping the row expands the radio + per-variant filter inputs.
  When the variant uses a per-variant filter, the live "N tasks
  match" preview runs against THAT filter, with the same
  debounce / count semantics as the campaign-level picker.
- **Samples slot** — same UX: radio defaults to "use campaign
  default"; override radio reveals an integer input (clamped 1..100).
- **Overrides accordion** — collapsed: `▸ Overrides (N)` where N is
  the count of fields THIS variant overrides relative to the shared
  trial_config. Open: three sub-accordions for the families of
  TrialConfig knobs (Timeouts, Retry, Verifier+misc). Each
  sub-accordion shows ONLY the fields actually applicable, with the
  same radio pattern (default vs override). Fields that aren't
  per-variant-overridable per the data model (force_build,
  delete_env, extra_mcp_servers, extra_skills, baseline_network_
  policy_override) are absent — they live only on the shared
  trial_config card up above.
- **Cost estimate `◷ ~$X.XX`** — when a rate card exists for the
  variant's (provider, model). Hidden otherwise; the slot shows a
  small `rate card not imported` muted note.
- **Trial-count line** — `n_per_task × matched_tasks = N trials`,
  with this variant's resolved values (which may differ from the
  campaign's).

**Footer of the Variants card:**

- `+ Add variant` — appends a blank variant. Default values: same
  agent + model as variant 1, no per-variant overrides set.
- `◊ Build matrix` — opens the matrix builder (see below).
- `⎘ Duplicate variant 1` — quick-clones the first variant. Useful
  for "same agent, swap the model" workflows.

### Matrix builder

Opens in a modal. The matrix dimensions cover the **three** axes
that are independently variant-scoped: agents, models, and
benchmarks. Most users only tick the first two; the benchmark axis
is collapsed by default.

```
┌─ Build variants ──────────────────────────────────────────┐
│                                                            │
│  Agents (pick one or more)                                 │
│   ☐ oracle          (no model — appears once in result)    │
│   ☑ claude-code-inbox                                      │
│   ☐ aider                                                  │
│   …                                                        │
│                                                            │
│  Models (pick one or more)                                 │
│   ☑ anthropic / claude-opus-4-7                            │
│   ☑ openai / gpt-4o                                        │
│   ☐ google / gemini-2.5-pro                                │
│   ☐ Custom model…                                          │
│                                                            │
│  ▸ Benchmarks (optional — adds a third axis)              │
│    By default the matrix uses the campaign's shared task   │
│    scope for every variant. Expand to pair each generated  │
│    variant with a specific benchmark.                      │
│                                                            │
│  Per-variant samples per task                              │
│   ○ Use campaign default                                   │
│   ● Set explicitly  [3]                                    │
│                                                            │
│  ─────────────────────────────────────────────────────     │
│  Will create 2 variants:                                   │
│    • claude-code-inbox + anthropic/claude-opus-4-7         │
│    • claude-code-inbox + openai/gpt-4o                     │
│                                                            │
│  ○ Replace current variants                                │
│  ● Append to current variants                              │
│                                                            │
│              [Cancel]   [Add 2 variants]                   │
└────────────────────────────────────────────────────────────┘
```

When the **Benchmarks** axis is expanded, the user can tick one or
more benchmarks AND opt into "Cross-product with the agent×model
axes". Examples:

- `Agents={claude-code-inbox}`, `Models={opus, gpt-4o}`,
  `Benchmarks={HumanEval, MBPP}`, cross-product on
  → 4 variants, each with its own `task_filter` set to the
  benchmark's id.
- Or `Benchmarks={HumanEval, MBPP}` paired in lockstep with
  `Agents={oracle, claude-code-inbox}` (not cross-product) →
  2 variants: HumanEval+oracle, MBPP+claude-code-inbox.

The toggle between lockstep and cross-product is presented as a
small radio when more than one axis has multiple entries.

Behaviour:

- A no-model agent (oracle) included in the agent selection appears
  exactly once in the model cross-product (deduped). Tooltip
  clarifies why.
- "Custom model…" expands inline to provider + name inputs; the
  custom model is included in the cross-product like any other.
- Result-list updates live as boxes are ticked.
- Variants created via the matrix get auto-derived labels:
  `"{agent}/{model.name}[/{benchmark}]"`. User can rename after
  dismissing the modal.
- Cap: matrix > 16 result variants is blocked at the modal level
  ("16 max — split into multiple campaigns").

### Shared trial config card — provenance

The shared card renders the same fields as today's Advanced section
(timeouts, retry, skip_verifier, etc.). When >1 variant exists, each
field carries a subtle indicator: an empty dot `○` if no variant
overrides it, a filled dot `●` if at least one does. Hovering shows
"Overridden by: variant 2 (gpt-4o)". This is the "field-level
provenance" promised in design principle 5.

When a user edits a shared field, a confirmation prompt appears if
any variant currently overrides it: "Two variants override this
field — keep their overrides, or reset them too?"

### CampaignDetail — the "By variant" view

For multi-variant campaigns, the detail page gains a "By variant"
section above the existing trial list:

```
┌─ Comparison ───────────────────────────────────────────────────┐
│                                                                │
│  Variant            Trials   Done   Avg reward   Success   $   │
│  ─────────────────  ──────   ────   ──────────   ───────   ─── │
│  claude-opus-4-7      12     12 ✓   0.72         67%      2.34 │
│  gpt-4o               12     12 ✓   0.55         50%      1.02 │
│  gemini-2.5-pro       12      9 ◷   (pending)    —        —    │
│                                                                │
│  [⇣ Download CSV]   [📊 Reward distribution]                   │
└────────────────────────────────────────────────────────────────┘
```

The trial list below gains a "Variant" column. A variant filter
chip lets users drill into "just gpt-4o trials".

The "Reward distribution" CTA is a follow-up — out of scope for the
data-model PR but reserved here so the design accounts for it.

### TrialDetail / TrialsList

Each trial row + the trial header card gain a variant badge:
`variant: gpt-4o`. The badge links back to the parent campaign
filtered to that variant.

### Submit shape decision (UI ↔ API)

The SPA always sends the same shape regardless of variant count:

```json
{
  "name": "…",
  "task_filter": {…},
  "trial_config": {…shared, no agent_name, no agent_model…},
  "n_per_task": N,
  "variants": [
    {"agent_name": "…", "agent_model": {…}, "label": "…", "overrides": {…}}
  ]
}
```

For single-variant: `variants` is a 1-element array. The backend's
backward-compat path (accepting `trial_config.agent_name` /
`agent_model` with no `variants`) stays — it serves API-direct callers
and existing campaigns — but the SPA doesn't use it. This keeps the
SPA's code path uniform and removes one branch worth of complexity.

### Validation summary

Client-side (UI blocks submit):
- At least one variant.
- ≤16 variants.
- Every variant has agent_name in the catalog.
- Every variant whose agent needs_model has agent_model set
  (catalog OR custom).
- Variant labels unique within the campaign.
- For each variant with a `task_filter` override: the filter must
  resolve to ≥1 task (live "0 tasks match" preview blocks submit
  the same way the campaign-level filter does).
- For each variant with an `n_per_task` override: 1 ≤ n ≤ 100.
- Total fan-out (Σ_variants of len(tasks_v) × n_per_task_v) ≤ the
  configurable fan-out cap (200 default, requires explicit
  confirmation above).
- Backoff max ≥ base in any variant retry override (same as the
  shared validation).
- For each variant with `retry` override: the BOTH-required
  semantics from PR J apply (max_attempts > 1 AND retry_on
  non-empty); a half-config is rejected with an inline error.

Server-side (route 400s):
- All of the above, plus catalog membership re-check on agent_name
  (defense in depth).
- `trial_config.agent_name` / `agent_model` MUST be absent when
  `variants` is non-empty.
- `variants=[]` is canonicalized to absent on write.
- Each variant's `task_filter` (if present) is validated the same
  way the campaign-level filter is — unknown keys rejected, license
  / benchmark_id / task_ids only.
- Each variant's `n_per_task` (if present) bounded 1..100.

### State machine — what happens when a user reduces from N variants to 1

If the user removes variants until only one remains, the UI keeps the
multi-variant payload shape (variants: 1-element array). It does NOT
silently downgrade to the single-variant shape — because that would
require either confirming the user truly intends to discard
override flexibility OR silently moving the variant's agent_* into
`trial_config`. Keeping the payload shape stable across edits avoids
that surprise.

### Out of scope (deferred)

These are reserved by the design but not in the initial PR:

- Per-variant rate-card cost estimates (the "◷ ~$X.XX" tile).
  Requires a model-cost projection function the SPA doesn't have
  today.
- Reward-distribution chart on CampaignDetail.
- Save-as-Workflow from a NewCampaign draft (would need Workflow
  variants support — a separate follow-up).
- Per-variant n_per_task — see open question 1 in the doc above.
- Drag-to-reorder. Implementing without an external dep is doable
  with HTML5 drag-and-drop but adds noise to the initial PR.
  Up/down buttons in the card header are the v1 ship; native drag
  is v2.

## Tests / acceptance criteria

Backend:
- Migration 0011 applies idempotently; existing campaigns continue to
  fan out single-variant.
- `POST /campaigns` with no `variants` → today's behavior unchanged.
- `POST /campaigns` with `variants=[]` → treated as no variants (same
  as single-variant). The route SHOULD treat empty-list as absent
  (canonicalize on write).
- `POST /campaigns` with non-empty `variants` AND
  `trial_config.agent_name` set → 400.
- `POST /campaigns` with duplicate variant labels → 400.
- `POST /campaigns` with a variant whose `agent_name` isn't in the
  catalog → 400 (mirrors today's check).
- Campaign with `variants=[A, B]` and `n_per_task=3` and 2 matched
  tasks produces 12 trials with the right (task, variant_idx,
  sample_idx) tuples after `run_once`.
- Running `run_once` twice on the same multi-variant campaign produces
  12 trials, not 24 (idempotency).
- `expected_trial_count` is materialised correctly at create time.

SPA:
- NewCampaign with one variant POSTs the legacy shape.
- NewCampaign with two variants POSTs the new shape; `trial_config`
  has no `agent_name` / `agent_model`.
- Labels are pre-filled from the picker but editable.
- Adding a duplicate label is rejected client-side before the POST.

## Open questions

1. **DRF quota accounting.** A multi-variant campaign that fans out
   to 100 trials hits the team quota differently from a single-variant
   campaign that fans out to 100. The scheduler doesn't care (DRF
   operates per-trial), but the per-team aggregate views should label
   variant trials so quota reporting is intelligible. Out of scope
   for this PR.
2. **ATIF projection.** Per-trial ATIF already carries the trial's
   agent + model. The campaign-level rollup is a separate
   "comparison ATIF" format (not specced here).
3. **Hyperparameter sweep.** A natural extension of per-variant
   overrides is "sweep agent_timeout_multiplier across {1, 1.5, 2}"
   — automatically expanding to three variants of the same agent+
   model with the parameter swept. This is a power-user feature and
   not in v1; the matrix builder above already covers most of the
   cross-product needs.

## Out of scope (future work)

- Workflow-level variants. Same `variants` column on `workflows`;
  workflow launch copies into Campaign.
- Comparison charting on CampaignDetail.
- Per-variant rate-card cost projection at create time
  ("this campaign will cost approximately $X").
- Re-running a single failed variant without re-running the others.

## Rollout

The frontend redesign is substantial enough to warrant splitting:

**PR-1 (backend + minimal SPA):**
1. Migration 0011.
2. Pydantic models (`CampaignVariant`, `VariantOverrides`).
3. Route validators on `POST /campaigns` and serializers on
   `GET /campaigns/{id}` and trials routes.
4. Campaign runner Phase-1 / Phase-2 changes.
5. CampaignDetail gains a "By variant" rollup table (read-only —
   no UI changes to NewCampaign yet).
6. NewCampaign continues to ship the legacy single-variant shape.
7. Backend + serializer tests.

This PR enables the data model and surfaces results — useful for
API-direct callers and shows immediate value even before the form
redesign.

**PR-2 (NewCampaign multi-variant form):**
1. The new Variants card layout described above.
2. Shared trial config card rename + provenance dots.
3. Matrix builder modal.
4. Per-variant overrides inline expansion.
5. Up/down reorder buttons (drag-to-reorder reserved for a v2).
6. The cost-estimate tile rendered conditionally (only when a rate
   card exists for the variant's (provider, model) — otherwise
   shows "rate card not imported").
7. Full SPA test coverage including the matrix builder, duplicate
   detection, override provenance.

**PR-3 (TrialDetail / TrialsList variant surfacing):**
1. Variant badge on trial rows + detail header.
2. Variant filter chip on TrialsList.
3. Test coverage.

Estimated sizes: PR-1 ~600 LOC, PR-2 ~900 LOC, PR-3 ~150 LOC.
Total ~1650 LOC across three reviewable chunks.
