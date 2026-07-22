# Family runs — ordered, adaptive execution across related trials

**Status: design.** Covers a benchmark-agnostic execution mode for batches whose
trials should run in a deliberate order, with optional cross-trial state
evolution between them. SkillFlow's iterative shared-skills paper is the
motivating case; SkillLearnBench's online variant is the second consumer;
any future benchmark can opt in.

## Motivation

Trials are atomic in Loom. Every trial materialises its bundle, runs its agent,
runs its verifier, and finalises independently. There is no supported mechanism
for a benchmark to say "run these ten tasks in a specific order, and let trial
N read a scratch space that trial N-1 wrote to."

Two benchmarks want exactly that:

- **SkillFlow (iterative shared skills).** Upstream groups tasks into workflow
  families, runs them serially inside a family, and after each trial calls an
  evolver LLM to patch a shared skill directory. Later trials in the family
  read the patched skills.
- **SkillLearnBench (online mode).** Upstream ships static skill sets per
  (baseline × model). An online variant seeds from one of those and evolves
  the same way SkillFlow does.

Both benchmarks currently share the `SkillFlowAdapter` base and both want the
same orchestration primitive. Rather than build a SkillFlow-specific
orchestrator, this design lands a general **family-run framework** that any
benchmark can consume via plugin composition.

## Non-goals

- **Not multi-trial training.** Family runs do not update model weights. They
  update a per-family scratch space (files bind-mounted into the sandbox) that
  the agent reads at prompt time. Weight-updating training is v1.5+.
- **Not distributed adaptation.** A family runs on one worker per trial (like
  any trial) but its state store is centralised (S3). We do not attempt to
  synchronise mid-trial state across workers.
- **Not a general workflow engine.** Family runs are strictly sequential
  within a family; no branching, no fan-in/fan-out. Families run
  independently and in parallel across families.

## Concepts

- **Family** — a set of tasks within a batch, identified by a stable
  `family_key` derived from each task. Default: the first path segment of
  `task_id` (matches upstream SkillFlow's dataset-folder convention).
- **Sequence** — the deterministic ordering of a family's task_ids. Produced
  by a **sequencer** plugin.
- **Family state** — an opaque per-family scratch space, materialised into
  the sandbox before each trial and updated by an **adapter** plugin after
  each trial. Default backend: an S3 prefix in the `artifacts` bucket.
- **Advance predicate** — after a trial reaches terminal state, decides
  whether the family moves forward (evaluate the adapter), retries (re-queue
  the same task), skips (advance without adapting), or aborts (mark the
  family failed).
- **Adapter** — mutates family state between trials. `skill_patcher_llm` is
  the reference implementation; `noop` is available for benchmarks that only
  want ordering without adaptation.
- **Failure policy** — decides what happens when an adapter fails
  (`stall_family` retries with backoff; `skip_and_advance` continues without
  the failed update; `abort_family` terminates the family).

## Configuration

Family runs are opt-in at two layers:

**Catalog defaults** in `benchmarks.json`:

```json
{
  "name": "skillflow-iterative",
  "display_name": "SkillFlow (iterative shared skills)",
  "series": "skill",
  "upstream": {"kind": "git", "locator": "..."},
  "family_run_defaults": {
    "family_key_extractor": {"name": "instance_id_prefix", "params": {"depth": 1}},
    "sequencer":            {"name": "ranking_file",
                             "params": {"path": "ALL_TASK_DIFFICULTY_RANKING.json"}},
    "advance_predicate":    {"name": "always_on_terminal"},
    "adapter":              {"name": "skill_patcher_llm"},
    "failure_policy":       {"name": "stall_family",
                             "params": {"max_retries": 3, "backoff_sec": 60}},
    "state_backend":        {"name": "s3_artifacts", "params": {}},
    "mount_path":           "/root/.skills"
  }
}
```

**Batch trial_config override**:

```jsonc
{
  "trial_config": {
    "family_run": {
      "enabled": true,
      // Any of the six roles may be overridden; omitted keys fall back to
      // the catalog default (or the framework default if the catalog is
      // silent).
      "adapter": {
        "name": "skill_patcher_llm",
        "params": {
          "model": "anthropic/claude-sonnet-4-6",
          "max_tokens": 8192,
          "provider_connection_id": "11111111-1111-4111-8111-111111111111"
        }
      }
    }
  }
}
```

The batch runner resolves the two layers into a single
`ResolvedFamilyRunSpec` at accept time and stores it on the batch row. It is
immutable once persisted; changing family-run behaviour requires a new batch.

For `skill_patcher_llm`, batch acceptance calls
`normalize_evolver_provider_connection()` before persisting the resolved spec
or materializing family state. It recursively rejects secret-like adapter
parameter names (for example API keys, authorization headers, bearer tokens,
credentials, passwords, cookies, and secret references), canonicalizes the
optional `provider_connection_id` as a UUID, and authorizes that connection
against the represented batch team (`submission_team_id`). The team must own
the connection or have an explicit provider share; inaccessible connections
fail closed without revealing whether another team owns them. Credentials stay
behind the provider connection and never enter `family_run_spec`.

Omitting `provider_connection_id` is the explicit platform-provider path. The
orchestrator still asks the Control Plane for a `family_evolver` step JWT with
an explicit null provider; the JWT carries an authoritative null provider
claim, while the request header and `loom.provider_connection_id` body field
remain absent. The Gateway then
uses its platform-credentialed route rather than inheriting the completed
trial's provider implicitly.

## Data model

Two additions.

**`batches.family_run_spec` JSONB** — the resolved spec for this batch. `NULL`
means the batch is not family-run; existing batches are unaffected.

**`trials.family_key` TEXT** — populated at batch-submit time when
`family_run_spec IS NOT NULL`, otherwise `NULL`. Kept as a denormalised
column so the scheduler's join against `batch_family_state` is a single
predicate rather than a JSONB probe. Indexed via
`idx_trials_family_key (batch_id, family_key)` alongside the existing
per-batch indexes.

**`batch_family_state`** — one row per family within a family-run batch:

```sql
CREATE TABLE batch_family_state (
    batch_id        UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    family_key      TEXT NOT NULL,
    task_sequence   TEXT[] NOT NULL,             -- ordered task_ids
    current_index   INT NOT NULL DEFAULT 0,
    state           TEXT NOT NULL,               -- pending | running |
                                                 -- adapting | done | stalled | aborted
    state_uri       TEXT,                        -- opaque per-backend handle
    attempt_count   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    last_error      TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, family_key)
);

CREATE INDEX idx_batch_family_state_adapting
    ON batch_family_state (next_attempt_at)
    WHERE state = 'adapting';
```

The partial index keeps the orchestrator's poll query cheap regardless of
how many family rows exist.

## Plugin protocols

All plugins are discovered via `importlib.metadata` entry points. Third-party
packages add plugins without touching Loom core, same pattern as
`loom.benchmarks` (Plan 24).

```python
# src/loom/family_run/protocols.py

class FamilyKeyExtractor(Protocol):
    def key_for(self, task: TaskRow) -> str: ...

class Sequencer(Protocol):
    def sequence(self, family_key: str, tasks: list[TaskRow]) -> list[str]:
        """Return ordered task_ids for one family."""

class AdvancePredicate(Protocol):
    def decide(self, *, trial: TrialRow, family: FamilyState,
               spec: ResolvedFamilyRunSpec) -> AdvanceDecision:
        """Return `advance | retry | skip | abort` after trial terminates."""

class Adapter(Protocol):
    async def initialize_state(self, *, family_key: str,
                               spec: ResolvedFamilyRunSpec,
                               backend: StateBackend) -> str:
        """Seed the family's state at batch accept time; return state_uri."""
    async def evolve(self, *, trial: TrialRow, family: FamilyState,
                     backend: StateBackend, gateway: GatewayClient) -> str:
        """Mutate state after a trial; return new state_uri
        (may be the same handle if the backend is content-addressed)."""

class FailurePolicy(Protocol):
    def on_adapter_failure(self, *, family: FamilyState,
                           exception: Exception) -> FailureAction:
        """Return `retry_with_backoff(sec) | skip_and_advance | abort_family`."""

class StateBackend(Protocol):
    async def initialize(self, *, batch_id: UUID, family_key: str) -> str:
        """Provision an empty state store; return state_uri."""
    async def download(self, state_uri: str, dst: Path) -> None: ...
    async def upload(self, state_uri: str, src: Path) -> str:
        """Upload contents; may return a new state_uri (content-addressed
        backends) or the same one (mutable backends)."""
```

Entry-point groups:

- `loom.family.keys`
- `loom.family.sequencers`
- `loom.family.advance`
- `loom.family.adapters`
- `loom.family.failure_policies`
- `loom.family.state_backends`

## Shipped plugins

### Framework defaults (in `loom` core)

- **`instance_id_prefix`** (`family_key_extractor`) — `task_id.split('/', depth)[:depth].join('/')`. Depth defaults to 1.
- **`alphabetical`** (`sequencer`) — sorted by `task_id`.
- **`ranking_file`** (`sequencer`) — reads `<task-bundle-of-first-task>/../<path>` (default `ALL_TASK_DIFFICULTY_RANKING.json`), a JSON array of task names, and orders by that; appends unranked tasks last, alphabetically. Matches upstream SkillFlow behaviour.
- **`submitted_order`** (`sequencer`) — order of `POST /batches` payload.
- **`always_on_terminal`** (`advance_predicate`) — advance on any terminal state (success, failed, cancelled). Default.
- **`success_or_retry_exhausted`** (`advance_predicate`) — advance on success; retry-in-place on failure until `retry_budget` exhausted, then advance.
- **`noop`** (`adapter`) — leaves state untouched. Useful for pure ordering.
- **`stall_family`** (`failure_policy`) — retry adapter with exponential backoff; after `max_retries`, transition family to `stalled` (operator can `admin family-run resume` to unstall). Default.
- **`skip_and_advance`** (`failure_policy`) — log the failure, advance without adapting.
- **`abort_family`** (`failure_policy`) — mark family `aborted`, remaining trials cancelled.
- **`s3_artifacts`** (`state_backend`) — stores at `s3://loom-<env>-artifacts/family-state/<batch_id>/<family_key>/`. Uses `.tar.gz` snapshots keyed by upload timestamp; `state_uri` is the object key. Default.

### Reference-level plugin (in `loom` core; largest single addition)

- **`skill_patcher_llm`** (`adapter`) — port of SkillFlow's `SkillPatchEvolver`. Compacts the completed trial's trajectory (step budget + observation-length limit), sends a fixed prompt template to the evolver LLM via the standard gateway (billing recorded in `llm_calls` with `dialect="family_evolver"`), receives a JSON patch (`{"add": [{path, content}], "modify": [{path, content}], "delete": [path]}`), applies to a local checkout of the current state, uploads via the state_backend. Model selection precedence: `trial_config.family_run.adapter.params.model` → cluster default `skill_evolver_default_model` (see below) → framework default (`anthropic/claude-sonnet-4-6`). For each call, the orchestrator supplies the real completed trial id and represented batch team, then exchanges its dedicated `family:evolve` credential at Control Plane `/admin/step-tokens` for an `llm:call` step JWT bound to `(trial_id, team_id, provider_connection_id, step_id="family_evolver")`. The dedicated credential cannot call the Gateway directly. The Control Plane repeats the owner/share check against the real trial team before minting the JWT.

### Cluster config

`config/loom-schema.toml` gains these knobs (codegen produces the
corresponding `WorkerSettings`/`ControlPlaneSettings` fields; the
orchestrator reads from the CP settings):

| Key | Type | Default | Purpose |
|---|---|---|---|
| `skill_evolver_default_model` | `str` | `"anthropic/claude-sonnet-4-6"` | Model for `skill_patcher_llm` when the batch doesn't override. |
| `family_orchestrator_poll_sec` | `float` | `5.0` | Cadence for the orchestrator's `adapting` scan. |
| `family_adapter_call_timeout_sec` | `float` | `300.0` | Per-call timeout wrapping the adapter's `evolve()`. Trip → `failure_policy`. |
| `family_state_download_timeout_sec` | `float` | `120.0` | Bounds worker-side state materialisation before trial start. |

### Local orchestrator secret source

The family orchestrator constructs `ControlPlaneSettings`, so a host-local
process requires `LOOM_CP_STEP_JWT_SIGNING_KEY` even though the orchestrator
does not mint step JWTs itself. Copy `.env.example` to the ignored repo-root
`.env` and keep that one variable as the local source for all three consumers:
dev compose maps it to both the Control Plane and LLM Gateway, while a manual
orchestrator process reads the `LOOM_CP_*` name directly.

```bash
set -a
. ./.env
set +a
uv run python -m loom_family_orchestrator
```

Do not print the value or pass it on the command line. The checked-in fallback
is development-only; staging and production continue to obtain the key from
their existing protected secret authority.

### Deterministic local seed

`loom service up` runs the dev seeder after migrations. It idempotently upserts
25 `anthropic-poster-design` smoke tasks across the five canonical
SkillLearnBench baseline rows, plus three same-family `skillflow-iterative`
tasks. All 28 rows use the checked-in `fixture://family-runs-dev/smoke` bundle,
so local startup does not depend on a catalog download or network access. The
SkillFlow ranking fixture is snapshotted into task tags at seed time, allowing
service-side sequencing to consume the intended non-alphabetical order before
a worker has materialised the bundle.

The default compose file uses mutable local `loom-*:dev` images. For that file,
`loom service up` executes `docker compose up -d --build` before waiting for
Postgres, running migrations, or seeding. Compose/BuildKit reuses a fresh cache
and rebuilds changed inputs, preventing a stale service image from starting
against a newer host migration tree. Custom compose files containing only
immutable image tags retain the existing no-build behaviour.

## Trial lifecycle changes

1. **Batch submission** (`loom_service.routes.batches.submit`)
   - Resolve `family_run_spec` from catalog + trial_config.
   - Enumerate tasks; group by `family_key_extractor.key_for(...)`.
   - For each family:
     - `state_backend.initialize(batch_id, family_key)` → provisions an
       empty backing store, returns `state_uri_empty`.
     - `adapter.initialize_state(family_key, spec, backend)` — seeds
       the store with any initial content the adapter needs (e.g.
       `skill_patcher_llm` with an `init_from_skill_method` param
       copies the chosen upstream skill set into the store); returns
       `state_uri_seeded` (may equal `state_uri_empty` for mutable
       backends, or a new handle for content-addressed ones).
     - `sequencer.sequence(family_key, family_tasks)` → `task_sequence`.
   - Insert `batches` row with `family_run_spec` JSONB.
   - Insert one `batch_family_state` row per family, `state='pending'`, `current_index=0`, `state_uri=state_uri_seeded`.

2. **Trial creation**
   - Trials are created up-front for all tasks (existing behaviour) but for family-run batches, `trials.requires_caps` gains no changes; scheduling gating is done via the family-state predicate below.

3. **Claim query** (`loom_control_plane.scheduler.claim`)
   - Predicate added when `family_run_spec IS NOT NULL`:
     ```sql
     AND EXISTS (
       SELECT 1 FROM batch_family_state bfs
        WHERE bfs.batch_id = t.batch_id
          AND bfs.family_key = t.family_key
          AND bfs.state = 'pending'
          AND bfs.task_sequence[bfs.current_index + 1] = t.task_id::text
     )
     ```
     (`task_sequence` is 1-indexed in Postgres arrays; `current_index` is 0-indexed in the row semantics.)
   - `trials` gains a `family_key TEXT` column populated at batch-submit time so the join is cheap.
   - On successful claim, the CP flips `batch_family_state.state → 'running'` in the same transaction as the trial-claim UPDATE.

4. **Pre-start** (`loom_worker.main_loop._spawn_trial`)
   - For family-run trials, read `state_uri` from the CP claim response; call `state_backend.download(state_uri, <staging_dir>)` before container start.
   - Sandbox `StartOptions.volumes` gets `(staging_dir, mount_path, "rw")` appended. `mount_path` defaults to `/root/.skills`; adapter can override via `family_run_spec.mount_path`.
   - Also passes `LOOM_FAMILY_STATE_DIR=<mount_path>` env var so the agent can find its state deterministically.

5. **Finalize** (`loom_control_plane.routes.trials.patch_state`)
   - On terminal state for a family-run trial:
     - Load `batch_family_state`.
     - Call `advance_predicate.decide(...)`.
     - `retry` → increment `attempt_count`, leave `current_index` unchanged, `state='pending'`.
     - `skip` → increment `current_index`; if end of sequence, `state='done'`; else `state='pending'`.
     - `advance` → `state='adapting'`; the orchestrator picks it up.
     - `abort` → `state='aborted'`; cancel remaining family trials.
   - All state transitions happen in the same transaction as the trial's terminal PATCH so an ordering fault cannot leave the family stuck.

6. **Adapter orchestrator** (new: `src/loom_family_orchestrator/`)
   - Long-running service, one replica per cluster (like `campaign_runner`).
   - Every N seconds (config `family_orchestrator_poll_sec`, default 5):
     ```sql
     SELECT * FROM batch_family_state
      WHERE state = 'adapting'
        AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
      ORDER BY updated_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1;
     ```
   - Load the completed trial's trajectory + current state.
   - Instantiate the batch's `Adapter` plugin from `family_run_spec`.
   - Call `adapter.evolve(...)` → new `state_uri`.
   - Bump `current_index`, transition `state='pending'` (or `state='done'` if end of sequence).
   - On exception: consult `failure_policy.on_adapter_failure(...)`; apply `retry_with_backoff` / `skip_and_advance` / `abort_family`.

## Failure semantics

Every plugin failure is surfaced in `batch_family_state.last_error` and
`trials.failure_message` when applicable. The `stalled` state is a hard stop
that requires an operator action:

```bash
loom admin family-run status <batch_id>
loom admin family-run resume <batch_id> <family_key> [--skip-adapter]
```

`--skip-adapter` acknowledges the failure and advances without evolution.

## Observability

New Prometheus metrics under the `loom_family_run_*` namespace:

- `families_total{state}` — gauge, refreshed by the orchestrator's metrics refresher.
- `family_trials_completed_total{outcome}` — counter (`success` / `failed` / `skipped`).
- `adapter_calls_total{plugin, outcome}` — counter.
- `adapter_call_duration_sec{plugin}` — histogram.
- `state_backend_bytes_transferred{direction}` — counter (`upload` / `download`).

Grafana dashboard `family-runs.json` gets shipped alongside the ops dashboards.

## Backward compatibility

- All existing batches have `family_run_spec IS NULL` — the scheduler predicate is a no-op for them.
- Existing SkillLearnBench catalog entries (24 static-skill baseline rows) stay unchanged and continue to run in atomic-trial mode. Online mode adds new catalog rows (`skilllearnbench-online-*`) with `family_run_defaults`.
- The current `SkillFlow` and `SkillLearnBench` adapters are extended to propagate `family_run_defaults` from the catalog into converted-task metadata, but their per-instance conversion logic is unchanged.

## Testing

- **Unit tests per plugin** — one file per plugin, exercising the protocol contract with fakes.
- **Integration test: framework skeleton** — end-to-end family-run batch with `noop` adapter and 2 tasks × 1 family, backed by a testcontainer Postgres. Asserts serial ordering + state transitions.
- **Integration test: `skill_patcher_llm`** — mock evolver LLM returns a fixed patch; assert state store shows the patched files after each trial.
- **Integration test: failure paths** — `stall_family` + `skip_and_advance` + `abort_family` each get a scenario that triggers a plugin exception and asserts the correct terminal state.
- **Property test: scheduler predicate** — for randomly generated (families, sequences, current_index) triples, every trial claim respects the ordering invariant.

## Rollout

Land in a chain of PRs against dev, each independently reviewable and mergeable:

1. **Framework skeleton** (`src/loom/family_run/` + protocols + `noop` adapter + `s3_artifacts` backend + `alphabetical`/`ranking_file`/`submitted_order` sequencers + `always_on_terminal`/`success_or_retry_exhausted` advance predicates + `stall_family`/`skip_and_advance`/`abort_family` failure policies + `instance_id_prefix` key extractor + migration + scheduler predicate + worker pre-start hook + CP finalize integration + unit + integration tests). No orchestrator yet — batches with `family_run_spec` but a `noop` adapter run to completion via the pre-start-mount path alone.
2. **Adapter orchestrator + `skill_patcher_llm`** (`src/loom_family_orchestrator/` service + `Dockerfile.family-orchestrator` + `k8s/family-orchestrator.yaml` + `skill_patcher_llm` adapter + integration test with mock LLM).
3. **SkillFlow iterative catalog + adapter propagation** (new `skillflow-iterative` catalog row; `SkillFlowAdapter` passes `family_run_defaults` through; integration test with two-task family, mock evolver).
4. **SkillLearnBench online-mode catalog + baseline matrix** (24 static-skill rows for the baseline coverage explicitly asked for; N online-mode rows seeded from each of `human_authored | b1 | b2 | b3 | b4`; integration test seeded from `b3-teacher-feedback-claude-haiku-4-5`).

Each PR runs full fast tier + ruff + mypy + a targeted integration test before merge. Each PR references the umbrella issue via `Refs #<umbrella>` (never `Fixes` — the umbrella closes only after all four land).

## Alternatives considered

- **Advisory locks per family** instead of a scheduler predicate. Simpler code, but no queryable family progress and lock-timeout tuning is fragile under worker crashes. Rejected in favour of first-class state.
- **Runner-side dispatch** (a Python coordinator submitting family trials one at a time). Puts the runner in the critical path; runner crash halts family progress. Rejected in favour of scheduler-side gating.
- **Worker-inline evolution** (worker calls the evolver LLM before ACKing terminal state). Ties evolution to worker lifetime; multi-worker debugging becomes harder. Rejected in favour of a dedicated orchestrator service.
- **Reuse `campaign_runner`** for orchestration. Campaign runner already handles trial fanout; family-run adaptation is a different loop shape (per-trial hook, not per-batch fanout). Adapters would end up split across two runners. Rejected in favour of a purpose-built service.

## References

- Upstream SkillFlow iterative runner: `iterative_shared_skills_runner.py` in `ZhangZi-a/SkillFlow` — the paper-faithful reference for the sequencing + evolver loop.
- Upstream SkillLearnBench skill layouts: `cxcscmu/SkillLearnBench` — 25 pre-computed `skills/<method>/<family>/` directories consumed by the atomic-trial mode; the online mode initialises from any of them.
- Loom Plan 24 (dataset discovery): entry-point registry pattern reused for family-run plugins.
- Loom campaign runner (`src/loom_service/batch_runner.py`): reference for a CP-sibling long-running service.
