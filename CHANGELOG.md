# Changelog

All notable changes to Loom will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- **Docs restructure (2026-06-09, #268).** `docs/` now reflects
  current code rather than development history. Deleted `docs/plans/`
  (32 frozen execution scripts, ~50K lines), `docs/specs/` (5 dated
  design docs, superseded), and `docs/notes/` (1 TB-2 probe, redundant
  with the SHA pin + pin test). Added `docs/index.md` (navigation
  entry point), `docs/user-guide.md` (researcher-facing `loom` CLI
  guide), and `docs/architecture/{overview,driver-protocol,
  benchmark-adapter,agent-adapter,trajectory-and-atif,cli-mode,
  service-mode}.md` (focused architecture docs, ≤300 lines each).
  Renamed `docs/task-authoring-guide.md` → `docs/authoring-a-task.md`.
  Cross-refs rewired in README, NOTICE, CONTRIBUTING,
  `src/loom_cli/__init__.py`, package READMEs, TB-2 docstrings,
  `.github/ISSUE_TEMPLATE/config.yml`, `legacy/README.md`. Two
  code-review passes during the PR caught + fixed invented APIs (state
  machine, DRF SQL, Gateway routes, AgentAdapter Protocol shape,
  verifier file paths), Protocol blocks missing `: ...` bodies, and
  stale plan-number references.

### Added
- **Plan 26 — Daytona cloud driver (2026-06-08, closes #252).** New
  top-level `src/loom_drivers/daytona/` package implements the Loom
  `Driver` Protocol against the `daytona>=0.184,<0.200` async SDK
  (no Protocol extension required — probe Task 1 confirmed the
  surface fits). `loom run --backend daytona` dispatches to
  DaytonaDriver via _driver_factory. Compute-seconds + cost surface
  through `/api/v1/usage` via two new bucket fields
  (`daytona_compute_seconds`, `daytona_cost_usd`) backed by the new
  generic `cloud_compute_records` table (migration `0008`, per
  amendment A26.1 unified with Plan 27's Modal driver via a
  `cloud_provider` column). Cancel/SIGINT teardown is bounded to 30s
  by the in-process `LiveSandboxRegistry` + atexit handler. Domain
  allowlists resolve via in-sandbox `getent ahosts` before being
  promoted to /32 CIDRs (Daytona's network API is CIDR-only).
  Session-based streaming exec bridges `get_session_command_logs_async`
  callbacks to ExecHandle's stdout/stderr AsyncIterators.
  45 new tests cover config / client / network mapping / images
  (warm pool) / registry / exec-stream / driver lifecycle / usage
  schema / CLI dispatch / cancel-path budget. Live integration test
  opt-in via `LOOM_RUN_DAYTONA_INTEGRATION=1 + DAYTONA_API_KEY`
  (default skipped, costs ~\$0.01/run). httpx MockTransport contract
  test deferred — Daytona SDK's `_api_client` is OpenAPI-generated
  and not easily transport-swappable; driver-protocol conformance
  covered via AsyncMock seam.
- **CI coverage Phase 1: measurement-only (2026-06-08, closes #260).**
  `repository-checks` now runs `pytest --cov=src --cov=packages` and
  uploads `coverage.xml` as a workflow artifact; a PR-comment step
  posts the total coverage % + TOTAL line summary. No
  `--cov-fail-under` gate yet — collecting baseline data across a few
  PRs first; Phase 2 sets the floor. Also fills two pre-existing CI
  gaps: `tests/loom_cli` (Plan 23) and
  `packages/loom-benchmark-terminal-bench-2/tests` (Plan 25) are now
  gated. Workflow installs the TB-2 sibling alongside the others.
  Baseline at this commit: 71% (8864 statements, 2581 missed). Big
  zeros are loom_service routes + several loom_worker entry points
  (those need integration tests, not unit tests).
- **Plan 25 — Terminal-Bench-2.0 canonical adapter (2026-06-08,
  closes #251).** New sibling package
  `packages/loom-benchmark-terminal-bench-2/` (Apache-2.0) pinned
  to upstream commit `91e10457b5410f16c44364da1a34cb6de8c488a5`
  (terminal-bench-core v0.1.1). Adapter implements
  `BenchmarkAdapter` (no Protocol extension — TB-2 fits the existing
  surface): walks `tasks/<slug>/` via `list_instances`, emits Loom's
  canonical task layout (`task.toml` + `instruction.md` + bundled
  `tests/` + verifier shim that translates pytest exit code 0/non-0
  into the ScriptVerifier JSON contract). Multi-service
  docker-compose fallback warns + uses client image only. New
  `to_tb2_report()` produces canonical `BenchmarkResults` JSON.
  `loom run --tb2-report PATH` writes that JSON alongside ATIF.
  Discoverable as `terminal-bench-2` via the `loom.benchmarks`
  entry-point — `loom datasets list` surfaces it automatically.
  29 tests + Harbor-reference snapshot guard.
- **Plan 24 — Dataset discovery (2026-06-08, closes #250).** New
  `loom datasets list/show/install/refresh-registry` subcommands
  union three sources: built-in entry-points (every adapter in
  `packages/loom-benchmarks` now declares itself via
  `[project.entry-points."loom.benchmarks"]`), an in-tree default
  JSON registry at `src/loom_cli/registry_data/default-registry.json`
  (14 entries: 13 built-ins + `terminal-bench-2` advertised for
  Plan 25), and an optional CP-service `/api/v1/benchmarks` query
  when `LOOM_SERVER_URL` is set. Override registry URL via
  `--registry-url` or `LOOM_REGISTRY_URL`; HTTP responses cached
  24h under `${LOOM_CACHE_DIR:-~/.cache}/loom/registry/`.
  `loom datasets install <slug>` shells out to
  `[sys.executable, "-m", "pip", "install", spec]` after rejecting
  shell metacharacters. ~40 new tests covering each source +
  dispatch + union precedence (builtin > remote > registry).
- **Harbor-parity arc spec + 5 detailed plans (2026-06-08).** Closes
  every capability gap between Loom and Harbor
  (`harbor-framework/harbor`) so Loom can act as a strict superset.
  Arc spec at `docs/specs/2026-06-08-loom-harbor-parity-arc-design.md`;
  five priority-ordered work items:
  - **Plan 23** — Ad-hoc `loom run` CLI: stateless `Trial.run()`
    reuse with a new `UpstreamDirectGatewayClient` against the
    openai/anthropic/google SDKs, local rate-card file, XDG config.
    15 TDD tasks; researchers `pip install loom && loom run` on a
    laptop without standing up Postgres/MinIO/CP/Gateway/Worker.
  - **Plan 24** — Dataset discovery: `loom datasets list/show/install`
    over three union'd sources (entry-points-based built-in
    registry, in-tree default registry JSON, optional CP service).
    13 tasks; migrates all 13 existing `loom_benchmarks` adapters
    to declare themselves via `[project.entry-points."loom.benchmarks"]`
    while keeping Plan 14's `loom_benchmark_tool` working.
  - **Plan 25** — Terminal-Bench-2.0 canonical adapter: new sibling
    package `packages/loom-benchmark-terminal-bench-2/` pinned to
    upstream commit `91e10457` (terminal-bench-core v0.1.1,
    Apache-2.0). `loom run --tb2-report` emits scores in TB-2's
    canonical JSON shape alongside Loom's native ATIF. 15 tasks;
    the upstream probe concluded the existing `BenchmarkAdapter`
    Protocol needs no extension.
  - **Plan 26** — Daytona cloud driver: `src/loom_drivers/daytona/`
    implements the Driver Protocol against `daytona>=0.184` (async
    SDK). NetworkPolicy → security-group mapping, atexit/SIGINT
    orphan cleanup with a 30s cancel budget, cost telemetry in a
    new generic `cloud_compute_records` table (migration `0008`).
    20 tasks.
  - **Plan 27** — Modal cloud driver: `src/loom_drivers/modal/`
    bridges Modal's sync SDK via `asyncio.to_thread`. Additive
    `Capabilities.gpu_types` field, `--gpu` CLI flag, cross-driver
    byte-equivalence test (docker / daytona / modal). 16 tasks.
  Cross-plan review amendments: A26.1 unified Plan 26's
  `loom_daytona_usage` schema into the generic `cloud_compute_records`
  table Plan 27 expected (with a `cloud_provider` column);
  A27.1 dropped Plan 27's defensive create-if-not-exists branch
  since Plan 26 now guarantees the table.

### Changed
- **`loom_benchmarks.registry.REGISTRY` is now lazy entry-points-backed
  (2026-06-08, part of Plan 24).** Replaced the hard-coded
  `dict[str, BenchmarkAdapter]` with an `_EntryPointRegistry`
  MutableMapping that loads adapters from `loom.benchmarks`
  entry-points on first access. `loom_benchmark_tool list/import` and
  any third-party importer of `REGISTRY` keep working unchanged.
  MutableMapping (not just Mapping) so tests can
  `monkeypatch.setitem(REGISTRY, ...)` to inject stub adapters.

### Removed
- **`VERSION` file (2026-06-08).** No source code read it (only
  `legacy/` artifacts and `.github/ISSUE_TEMPLATE/release.yml`
  referenced it). Across 22 plans and 237 commits it sat at
  `0.0.0` despite the codebase being at `loom-service-v0.22` —
  it was actively lying about the project version. Future SemVer
  releases will use `pyproject.toml [project] version` as the
  single source of truth (root + any published `packages/`
  siblings). Updated `.github/ISSUE_TEMPLATE/release.yml`,
  `CONTRIBUTING.md` (Release Flow + Known Gaps), and issue #254
  to drop `VERSION` references.

### Changed
- **CONTRIBUTING.md rewritten + issue tracker cleaned up
  (2026-06-08).** The previous CONTRIBUTING.md described a
  textbook GitHub-flow workflow that didn't match the actual
  single-owner direct-to-dev practice (0 merge commits in 232
  commits ahead of origin; placeholder CI; no `vX.Y.Z` tags).
  Rewrote to document the active
  workflow (per-plan TDD commits + per-plan tags + plan-shipping
  trio), kept the GitHub-flow content as a clearly-marked
  "Future Contributor Workflow" section. Companion GitHub issue
  cleanup: closed 26 stale pre-Loom issues with per-issue
  superseded-by replacement pointers; created 8 Loom-current
  issues (harbor-parity arc epic #248, Plans 23–27 sub-issues
  #249–#253, Plan 22.5 real-CI #247, GitHub-flow standup #254);
  added labels `loom:arc`, `loom:plan`, `gap`, `deferred:v1.5`,
  `superseded`. The CONTRIBUTING.md "Known Gaps" section now
  points at `label:gap` issue filters instead of an inline punch
  list.
- **Docs reorganization (2026-06-08).** Moved
  `docs/superpowers/{specs,plans}/` → `docs/{specs,plans}/`.
  The `superpowers` umbrella exposed the Claude Code plugin name
  in the docs tree without adding semantic value (no third
  sibling, no signal to outside contributors). Rewrote all 79
  cross-references across 39 files via `git mv` (renames
  preserved at 100% similarity).
- **README.md + NOTICE.md no longer claim "seven plans" / v0.7
  as latest (2026-06-08).** Status block was frozen at 2026-06-05/06;
  updated to reflect Plans 0–22 shipped + harbor-parity arc ready.
  Latest tag is `loom-service-v0.22`; v0.7 demoted to milestone tag.
  Reframed as benchmark-agnostic (not targeting SkillFlow +
  SkillLearnBench specifically).

### Fixed
- **SPA dev-deps security bump (2026-06-08, closes #255).** Cleared
  5 Dependabot advisories (2 critical happy-dom RCE/VM-escape, 1
  high happy-dom fetch-credentials, 1 moderate vite path-traversal,
  1 moderate esbuild dev-server CSRF). Bumped `vite` 5→8,
  `happy-dom` 14→20, `vitest` 1→4, `@vitejs/plugin-react` 4→6 in
  `web/package.json`. All dev-only — no production artifact ships
  these. Build still ~71 KB gzipped; 18 vitest tests still pass.
- **`migrations/env.py` preserves existing loggers across alembic
  invocations (2026-06-08).** Python's `logging.config.fileConfig`
  defaults to `disable_existing_loggers=True`, which silently
  disables every logger configured before alembic runs. In the
  full integration suite, migration fixtures running before
  `test_trial_runner.py::test_runner_logs_fenced_response` were
  disabling the `loom_worker.trial_runner` logger and making
  caplog miss the warning the test asserts on. Pass
  `disable_existing_loggers=False` explicitly. Suite now 251
  passed (was 250 + 1 flake).

### Added
- **Plans 21+22 — SPA scaffold + read pages + write/admin (2026-06-08,
  tags `loom-spa-read-v0.21` and `loom-service-v0.22`).** New top-level
  `web/` directory: React 18 + Vite + TypeScript + TanStack Query +
  React Router. Token-paste auth stored in `localStorage`; the API
  client surfaces 401s through a callback that auto-clears the token.
  Hand-stubbed types in `src/api/schema.d.ts` match the actual Plan
  17-20 service surface (re-generate via `npm run gen-api` once
  `loom_service` is reachable). 11 pages: TrialsList (state-filtered
  cursor list), TrialDetail (header + paginated trajectory viewer +
  ATIF/trajectory download gated on `*_ready` flags), Tasks (license
  + benchmark filter), Benchmarks, Settings (token paste + active
  tokens list with revoke), CampaignsList, NewCampaign (JSON
  validation before POST), CampaignDetail (5s polling while
  submitted/running, stops on terminal, cancel button),
  RateCardsAdmin (admin-only proxy to Gateway's `/admin/rate-cards`),
  UsageDashboard (date-range + group_by + inline SVG bar chart),
  NotFound. Components: Layout (redirects unauthenticated to
  Settings), NavBar, EventTimeline (per-kind summary +
  click-to-expand JSON for the v0.7 trajectory event catalog),
  Pagination, JsonViewer, LoadingState, ErrorState, EmptyState. Tests
  (vitest + happy-dom + @testing-library/react): 18 across api client
  (header attachment, 401 callback, 403 detail extraction, 204),
  AuthContext (localStorage round-trip), Pagination helpers,
  EventTimeline rendering, and NewCampaign JSON validation +
  POST-body shape. `npm run build` produces a ~74 KB gzipped bundle.
  Skipped from the plan-doc: `Dockerfile.service`, `Dockerfile.web`,
  k8s manifests, system smoke test — those belong in a
  deploy-focused follow-up since they need real infrastructure to
  exercise meaningfully.
- **Plan 20 — Rate cards + teams + usage endpoints (2026-06-08, tag
  `loom-service-admin-v0.20`).** Closes the remaining admin/team
  surfaces on the service layer. `lifespan` now provisions a second
  `httpx.AsyncClient` as `app.state.gateway_client` (independent of
  the CP client) so a slow CP doesn't starve the rate-card proxy.
  New routes:
  - `GET/POST /api/v1/rate-cards` + `GET /api/v1/rate-cards/{id}` —
    thin proxies to Gateway's `/admin/rate-cards` (shipped Plan 4).
    Gated on the `admin:rate_cards` scope Plan 17 introduced; reuses
    Plan 18's `forward()`/`propagate()` so Retry-After +
    X-RateLimit-* carry through.
  - `GET /api/v1/teams/{team_id}` — team row + TeamQuota
    (fair_share_weight, max_attempts, in_flight_count,
    license_allowlist) + member tokens (8-char hash prefix only,
    raw secret never recoverable). Cross-team check fires BEFORE
    the not-found probe so a team caller can't enumerate which
    team UUIDs exist (403, not 404, on unknown).
  - `GET /api/v1/usage` — date_trunc rollup over
    `llm_calls JOIN trials` with `group_by ∈ {day, week, month}`,
    cross-team enforcement, defense-in-depth `degraded` flag if the
    table is ever absent. Per bucket: trial_count, succeeded_count,
    failed_count, llm_input_tokens, llm_output_tokens, total_cost_usd.
  Adapted to actual v0.7 schema (quota fields live on TeamQuota, not
  Team; no Trial.aggregate_reward / Task.name columns; llm_calls is
  canonical). 375 unit+contract+property; 247 integration (+5 rate
  cards + 5 teams + 7 usage); ruff + mypy strict clean across 127
  source files. **Audit follow-ups (same-day):** usage rollup field
  names renamed to `trials_currently_succeeded` /
  `trials_currently_failed` to make point-in-time semantics
  explicit (the count is `Trial.state` at query time, NOT the state
  at `captured_at`); legacy `succeeded_count`/`failed_count` aliases
  kept for the SPA's first-pass migration. Lifespan now refuses to
  start if `LOOM_SVC_GATEWAY_URL` has a non-root path prefix —
  httpx silently strips any prefix when forwarder paths are
  absolute, which would route writes to the wrong URL.
- **Plan 19 — Campaigns: table, routes, runner (2026-06-07, tag
  `loom-campaigns-v0.19`).** First-class campaign concept. Migration
  `0007` adds the `campaigns` table + `trials.campaign_id` FK
  (ON DELETE SET NULL) + `trials.idempotency_key` (partial unique
  index `WHERE NOT NULL`). New `Campaign` SQLAlchemy model.
  `Trial.campaign_id` + `Trial.idempotency_key` columns added.
  Control Plane `POST /trials` now accepts `idempotency_key` +
  `campaign_id` payload fields; if `idempotency_key` matches an
  existing row, returns the canonical trial_id. The INSERT path uses
  `pg_insert + ON CONFLICT DO NOTHING` with `index_where` matching
  the partial unique index. New service-layer routes under
  `/api/v1/campaigns`: POST (creates + materializes
  `expected_trial_count` from `task_filter`), GET list (cursor
  paginated, team-scoped, state-filtered), GET detail (with per-state
  trial summary + reward/cost rollup from `Trial.result` JSONB
  since v0.7 trials has no top-level aggregate_reward/cost_usd
  columns), POST cancel (cancels campaign + cascades to still-active
  trials). Task filter is allowlisted to `{license, task_ids,
  benchmark_id}` — a typo like `liscense` returns 400 instead of
  silently matching zero tasks. New `campaign_runner.py` exposes
  `next_campaign_state` (pure state machine; unit-tested across 8
  cases), `run_once`, and `run_loop`. The runner uses a
  release-lock-before-HTTP pattern: SELECT … FOR UPDATE SKIP LOCKED
  the campaigns, materialize pending tasks, COMMIT (release the
  lock), submit trials over HTTP, then re-open a transaction to
  advance state. Without the split, holding FOR UPDATE deadlocks
  against the CP-side trial INSERT's FK key-share lock on the same
  campaign row. Concurrent-runner correctness is preserved by the
  deterministic idempotency key `{campaign_id}::{task_id}` —
  duplicate submissions collapse via ON CONFLICT DO NOTHING. The
  runner is spawned from `loom_service`'s lifespan as
  `loom-svc-campaign-runner`. 375 unit+contract+property (+8 state
  machine); 220 integration (+7 CRUD + 4 idempotency + 2 migration +
  3 runner e2e); ruff + mypy strict clean across 124 source files.
  **Audit follow-ups (same-day):** runner now reads a CP-side bearer
  token from `LOOM_SVC_CAMPAIGN_RUNNER_CP_TOKEN` and forwards it on
  every submit — without the token the loop logs a single warning
  and skips ticks instead of spamming the CP with 401s (audit C1).
  CP /trials pre-validates `campaign_id` and returns 400 on
  unknown UUID instead of letting the FK violation surface as a 500
  (audit C2). Idempotency-key lookup is scoped to the caller's
  team_id; a cross-team key collision returns 409 with a generic
  "collision with another team's trial" detail instead of leaking
  the other team's trial_id (audit H1). Campaign create rejects a
  task_filter that materializes to zero tasks (`task_ids=[]`, no
  matching license, etc.) with 400 — otherwise the campaign would
  sit in `submitted` forever (audit M2). 3 audit-regression tests
  pin the behavior.
- **Plan 18 — `loom_service` read routes + Control Plane forwarders
  (2026-06-07, tag `loom-service-read-v0.18`).** Adds the read-side of
  the service API + the two write proxies (POST /trials, /cancel) that
  let the SPA (Plan 21+) cover the trial lifecycle without bypassing
  the service layer. New `pagination.py` ships an unsealed
  base64(json) cursor that round-trips `(submitted_at, id)` —
  deliberately debuggable (not a JWT) since it carries only public
  sort keys. `LoomServiceSettings` now exposes `trajectories_bucket`
  + `artifacts_bucket` (defaults `trajectories` / `artifacts` — the
  buckets the worker's TrajectoryWriter + finalize.py actually write
  to). New routes:
  - `GET /api/v1/trials` (filter by team/task/state; cursor paginated;
    reward + cost extracted from `Trial.result` JSONB)
  - `GET /api/v1/trials/{id}` (detail + presigned ATIF + trajectory
    URLs anchored on the real `<team>/<trial>/{atif.json,events.jsonl}`
    key shape)
  - `GET /api/v1/trials/{id}/trajectory` (paginated event read via
    boto3 get_object; line-index cursor)
  - `GET /api/v1/trials/{id}/trajectory/download` (302 to presigned)
  - `GET /api/v1/trials/{id}/atif` (302 to presigned)
  - `GET /api/v1/tasks` + `GET /api/v1/tasks/{id:path}` (Task PK is a
    string like `humaneval/HumanEval/0`, so detail uses `{path}`)
  - `GET /api/v1/benchmarks` + `GET /api/v1/benchmarks/{id}`
  - `POST /api/v1/trials` → forwarded to CP with the caller's bearer
    intact; local `submit` scope check short-circuits unauthorized
  - `POST /api/v1/trials/{id}/cancel` → same shape + same-team check
  Shared `tests/integration/conftest.py` brings up a module-scoped
  MinIO container + seeds events.jsonl + atif.json so trajectory +
  ATIF tests don't pay per-test container start-up. 367
  unit+contract+property (+6 pagination); 198 integration (+34 new:
  11 trials + 4 trajectory + 2 atif + 7 tasks + 5 benchmarks + 5
  forwarder); ruff + mypy strict clean across 122 source files.
  Adapted aggressively to the actual v0.7 schema (no
  `aggregate_reward`/`cost_usd`/`campaign_id` columns, str PKs on
  Task + Benchmark, no `Artifact` model — extracted from `Trial.result`
  JSONB instead). **Audit follow-ups (same-day):** trajectory route
  stream-decodes events.jsonl via `iter_lines()` instead of
  materializing the whole object — a 100k-event trial would otherwise
  cost ~600 MB resident before slicing (audit H1). New migration 0006
  creates `idx_trials_submitted_at_id_desc` so the keyset-pagination
  query has a supporting btree (audit H2). `propagate()` returns a
  `JSONResponse` with an allowlist of upstream headers (Retry-After,
  Location, X-RateLimit-*, X-Idempotency-Key) — Plan 19's rate-limited
  campaign submits depend on Retry-After (audit H3). Trial detail now
  carries `atif_ready` + `trajectory_ready` flags so the SPA can
  avoid rendering download links that would 404 on pre-finalize
  trials (audit M1). `_extract_reward` / `_extract_cost` defensive-cast
  so a malformed `Trial.result` JSONB falls through to `None`/`0.0`
  rather than crashing the route (audit M2). 2 audit-regression
  integration tests added.
- **Plan 17 — `loom_service` skeleton + tokens routes + `admin:rate_cards`
  scope (2026-06-07, tag `loom-service-skeleton-v0.17`).** First of a
  6-plan service-layer arc (17-22) that exposes a thin REST API +
  SPA on top of the v0.7 runtime core. New `src/loom_service/`
  package: `LoomServiceSettings` (env-prefix `LOOM_SVC_`, defaults
  port 8090), FastAPI factory + lifespan opening async SQLAlchemy
  engine + boto3 S3 + httpx Control-Plane client. `python -m
  loom_service` boots uvicorn directly. `GET /api/v1/health` ships in
  Task 2. `auth_guards.py` exports `require_human_or_admin` (rejects
  worker tokens + step-session JWTs with 403), `is_admin` /
  `require_scope` / `require_team_or_admin`. `/api/v1/tokens`
  exposes the full CRUD surface: GET list (team callers see own team
  only; admin sees all), POST mint (team callers restricted to
  `team`-type with scopes ⊆ `{read:own, submit}`; admin may mint
  anything cross-team), DELETE revoke by 8-hex-char prefix. Migration
  `0005` adds `admin:rate_cards` to existing `admin:tokens` holders
  (idempotent, downgradeable) so Plan 20's `/admin/rate-cards` write
  routes can require it cleanly without a manual grant. 376
  unit+contract+property pass (+15 auth guard + 3 config); 174
  integration pass (+10 tokens + 3 migration + 1 health); ruff + mypy
  strict clean across 115 source files. **Audit follow-ups (same-day):**
  DELETE /tokens filters by `team_id` BEFORE the prefix scan so a
  team caller can't probe (or revoke via prefix collision) another
  team's token; cross-team lookups return 404 instead of 403 leaking
  existence (audit H1+M1). Hex prefix validation is strict
  (`[0-9a-f]{8}`). Multi-match prefix collisions return 409 instead of
  silently picking the first non-deterministic row. POST /tokens
  rejects `type=admin` with a populated `team_id` (admin tokens are
  global; silent-drop was a confused-deputy hazard, audit H2) and
  rejects unrecognized scopes with 400 against a known-scope allowlist
  (audit M2). Migration 0005 skips tokens with `revoked_at IS NOT
  NULL` (audit M4). 3 audit-regression integration tests added.
- **Plan 16 — `verify` CLI + system smoke tests + license-allowlist
  closure (2026-06-07, tag `loom-benchmarks-v0.16`).** Closes the
  benchmark-integrations arc. New `src/loom_benchmark_tool/oracle_runner.py`
  shells out to `docker run` / `cp` / `exec` to spin up a per-task
  image, run `solution/solve.sh` if present, then invoke
  `pytest tests/` or `verifier/run.sh` — returns an `OracleResult`
  dataclass with pass/return-code/stdout-tail/stderr-tail. New
  `ObjectStore.list_task_prefixes` ships on both Fake + Minio
  (anchored on `task.toml` keys so multi-segment instance_ids like
  HumanEval's `HumanEval/0` round-trip correctly). `verify_cmd.py`
  replaces Plan 14's `NotImplementedError` skeleton with a working
  pipeline: list → sample (deterministic via seeded random) → download
  → validate `task.toml` against `TaskConfig` → run Oracle → aggregate
  report. `__main__` prints a summary + per-failure stderr_tail and
  exits non-zero on any failure. Two new system smoke tests under
  `tests/system/` exercise the docker-compose stack end-to-end:
  `test_benchmark_humaneval_smoke.py` (default-on, ≤10 min) and
  `test_benchmark_swe_bench_smoke.py` (opt-in via
  `LOOM_RUN_SWE_BENCH_SMOKE=1` because per-instance images are ~5 GB).
  `compose_stack` fixture extended to surface `db_url` +
  `minio_access_key` + `minio_secret_key` so smoke tests can drive
  `run_import` directly. New integration test
  `test_benchmark_license_happy_path.py` proves the full Plan-13
  guardrail story: a benchmark-imported MIT task POSTs cleanly under
  the default allowlist; an LCB task (CC-BY-NC-4.0 per the Plan 15
  audit) 403s — the latter is the regression guard for any future
  attempt to re-tag LCB as MIT. 408 unit+contract+property pass (+5
  new); ruff + mypy strict clean across 107 source files.
  **Audit follow-ups (same-day):** verify pipeline is now fail-soft
  per task — a broken `task.toml` or a `download_prefix` exception
  captures `passed=False` with the error in `stderr_tail` instead of
  aborting the whole sample. The per-task tempdir slug must match
  `[A-Za-z0-9._+-]+` before any docker-exec interpolation
  (defense-in-depth even though list-form subprocess calls block
  direct shell injection). `docker run`/`exec` get a `--` separator so
  an image value starting with `-` can't be parsed as a flag.
  `solve.sh` stdout+stderr now captured into the `OracleResult` tail
  so SWE-Bench failures surface the real reason. Verify exits
  `SystemExit(2)` when `total=0` so an operator pointing at the wrong
  benchmark name catches the typo instead of seeing a silent green.
  Two new audit-regression tests pin C1 (fail-soft) and H1 (slug
  validation).
- **Plan 15 — Twelve more benchmark adapters (2026-06-07, tag
  `loom-benchmark-adapters-v0.15`).** Brings the v1 slate from 1 to 13
  adapters (HumanEval shipped Plan 14): SWE-Bench Verified, SWE-Bench
  (full), SWE-Bench Multimodal (with base64-embedded screenshots in
  `instruction.md`), OSWorld + WebArena + BFCL (structured-verifier
  shells via descriptor JSON), MBPP + LiveCodeBench (pytest verifier;
  LCB emits one stdin-driven pytest file per public + private test
  case), GAIA (llm-judge verifier; rubric template at
  `verifier/rubric.md` with `{candidate_answer}` left as a verify-time
  placeholder), AIME (script verifier — extracts the last integer from
  the agent's final line via standalone `verifier/check.py` so we
  don't have to thread shell escapes), SkillFlow + SkillLearnBench
  (passthrough — upstream bundle is already in Loom layout; we
  re-stamp the namespaced task_id). New `loom_benchmarks/judges/`
  sub-package holds llm-judge rubric templates. Every adapter pipes
  `task_id` + `name` through `toml_string` to apply the Plan 14 audit
  lesson preemptively — no upstream-controlled value reaches `task.toml`
  unescaped. MBPP + LiveCodeBench + SkillFlow + SkillLearnBench
  contract tests run subprocess `pytest` against the converted
  task dir to prove the bundles are end-to-end runnable. 60
  unit+contract tests in the package (+29 new); 156 integration green.
  Registry exposes 13 adapters via `python -m loom_benchmark_tool list`.
  **Audit follow-ups (same-day):** LiveCodeBench license tag was MIT
  in the plan; corrected to `CC-BY-NC-4.0` per spec §7 (the non-default
  allowlist tag is what defeats the Plan-13 submit-time guardrail until
  an operator opts in for non-commercial use). AIME was Apache-2.0;
  corrected to `proprietary-MAA` per spec §7 (MAA owns the problem
  text; Plan 16 ships the `--accept-maa-terms` import gate). GAIA
  rubric switched from `str.format` to `<<MARKER>>` substitution so
  set-notation reference answers like `{Honolulu, Quincy}` AND the
  literal `{"score": ...}` JSON in the rubric both survive
  convert→verify two-pass. GAIA attachment file_name sanitized to
  basename (rejects `../poison` / absolute paths). AIME verifier
  `check.py` extracts the LAST integer on the final line so
  `"45 is partial; final: 100"` correctly grades 100 not 45. MBPP +
  HumanEval tests/ dirs gain an explicit conftest that puts the task
  dir on sys.path so `from solution import ...` resolves regardless
  of pytest invocation cwd. 65 unit+contract tests in the package
  (+5 new); ruff + mypy strict clean.
- **Plan 14 — `loom-benchmarks` core package + HumanEval reference adapter
  (2026-06-07, tag `loom-benchmarks-core-v0.14`).** New sibling package
  at `packages/loom-benchmarks/` (PyPI-publishable, depends on
  `datasets`, `httpx`, `pydantic`): `BenchmarkAdapter` Protocol + value
  types (`UpstreamSource`, `BenchmarkInstance`, `ConvertedTask`) +
  conversion utilities (`pytest_from_test_strings`,
  `pytest_from_unittest`, `structured_verifier_script`,
  `embed_base64_image`, `download_files_from_record`, `sha256_of_dir`) +
  kind-dispatched `fetch_upstream` (HuggingFace / git / https-tarball
  with content-hash cache + `.fetch_complete` sentinel) +
  `HumanEvalAdapter` (writes a runnable `solution/` + `tests/` bundle
  whose canonical solution passes the upstream `check` under
  `pytest`). Tarball extraction uses `filter="data"` to refuse unsafe
  upstream archives. New `src/loom_benchmark_tool/` CLI (`python -m
  loom_benchmark_tool {list,import,verify}`): `import` fetches → for
  each instance converts → uploads bundle to MinIO under
  `s3://{bucket}/{benchmark}/{instance_id}/` → upserts `benchmarks` +
  `tasks` rows (idempotent ON CONFLICT). `import_cmd` is typed against
  `ObjectStore` Protocol so the end-to-end integration test injects a
  `FakeObjectStore`. `verify` is a documented Plan-16 placeholder.
  336 unit+contract+property + 155 integration (+2 from new e2e + 1
  from Plan 13 audit follow-ups); ruff + mypy strict clean across 106
  source files (+23 from new tool + package). **Audit follow-ups
  (same-day):** instance_id allowlist + segment-traversal check at
  import time (rejects `..`, leading/trailing `/`, NUL, control
  chars, shell-special bytes); `upload_task_dir` refuses empty,
  traversal, or absolute prefixes (parity with Plan 13's
  `download_prefix`); new `toml_string` helper escapes quote /
  backslash / control / NUL when interpolating upstream-supplied
  values into `task.toml` (HumanEvalAdapter uses it for `id` + `name`);
  git fetcher detects commit-SHA revisions and falls back to
  `git init && fetch <sha> && checkout FETCH_HEAD` so adapters can
  pin by content-addressed SHA (the spec's recommended default);
  `python -m loom_benchmark_tool import` flags fall back to env
  vars (`LOOM_DB_URL`, `LOOM_MINIO_*`) so MinIO secrets don't leak
  through `ps`. 374 unit+contract+property pass (+9 new safety tests +
  7 in package).
- **Plan 13 — Bundle store + license tracking (2026-06-07, tag `loom-bundle-store-v0.13`).**
  Foundation for Plan 14's benchmark integrations. Schema migration
  0004 adds: `benchmarks` table (id = human-readable PK for `ON
  CONFLICT DO UPDATE` upserts per amendment A13.2), `tasks.license` +
  `tasks.benchmark_id`, `team_quotas.license_allowlist text[]` with
  expanded default `[MIT, Apache-2.0, BSD-3-Clause, CC-BY-4.0]`
  (amendment A13.1 — CC-BY-4.0 added so the v1 benchmark slate's
  MBPP/GAIA pass without operator action), and `tokens.last_seen_at`
  (amendment A13.3 — service-layer hook). `ObjectStore.download_prefix`
  ships on both FakeObjectStore and MinioObjectStore (S3 paginator
  + `download_file`). Worker's new `_materialize_task_dir` replaces
  the bare `tempfile.mkdtemp()`: `s3://bucket/prefix/` sources pull
  fixture content via download_prefix; `fixture://` / `git+` / None
  leave the dir empty (operator-runbook flow). License enforcement at
  POST /trials: a task with a license not in the team's allowlist 403s
  with the allowlist surfaced in the detail. **Audit follow-ups
  (same-day):** migration 0004 now creates `tasks_benchmark_id_idx`
  and the FK uses `ON DELETE SET NULL` (orphaned tasks survive a
  benchmark deletion instead of cascading-fail or seq-scanning);
  `download_prefix` refuses empty prefix with `ValueError` and skips
  keys whose suffix contains `..` (path traversal); `_materialize_task_dir`
  is typed against the `ObjectStore` Protocol (not concrete
  `MinioObjectStore`), rejects `s3://bucket/` with empty prefix, and
  removes its tempdir if `download_prefix` raises so failed claims
  don't leak `/tmp` inodes; license enforcement uses `is not None` so
  an empty-string license from a buggy importer no longer bypasses.
  336 unit+contract+property + 153 integration green; 83 source files
  mypy-strict clean.
- **Plan 12 — 11 concrete agent adapters (2026-06-07, tag `loom-agent-adapters-v0.12`).**
  Ships every adapter from spec §8.1: codex, opencode, aider, openhands,
  openhands-sdk, swe-agent, mini-swe-agent, claude-code, gemini-cli,
  qwen-cli, kimi-cli. Each is a frozen dataclass satisfying the
  `AgentAdapter` Protocol, self-registers at import time, and picks a
  capture mechanism per the spec table: 5 stream_stdout_jsonl, 2
  tail_log_file (aider's chat history, swe-agent's trajectory.jsonl), 1
  poll_local_http (openhands server mode), 3 tail_pty (codex, qwen-cli,
  kimi-cli — degraded TUI fidelity). Amendment A12.1 applied: claude-code
  uses `sh -c "cd workdir && claude --print ..."` (NOT
  `--instruction`/`--workdir`/`--no-update-check`) with telemetry +
  auto-update disabled via env vars. 11 per-adapter contract tests + a
  parametrized registry round-trip sweep + 27 pre-existing capture/
  registry/hello tests = 61 tests in the launcher; lint + mypy strict
  clean across 21 launcher source files. Main repo's 324 unit+contract+
  property tests unchanged. **Argv flags for openhands / swe-agent /
  opencode / gemini-cli / openhands-sdk / mini-swe-agent are best-guess
  against each project's published CLI** and will be re-tuned alongside
  the per-adapter sandbox Dockerfiles when those land.
- **Plan 11 — SubprocessAgent + worker integration (2026-06-07, tag `loom-subprocess-agent-v0.11`).**
  `loom.agent.subprocess.SubprocessAgent` wraps any
  `loom_launcher.AgentAdapter` as an `AgentRuntime` satisfying
  `loom.agent.base.AgentRuntime`. Three bridge functions adapt the
  loom and loom-launcher type universes (Driver↔SandboxAccess,
  ExecHandle↔ExecHandle, ModelSpec↔ModelSpec — amendment A11.2).
  `HttpControlPlaneClient.mint_step_token` + `.get_trial_llm_calls`
  (amendment A11.1) shipped; the latter is the finalize-time read the
  worker uses to project LLMCallEvents into the trajectory before ATIF
  projection. `_default_agent_factory` routes by `task_config.agent.name`:
  oracle / litellm / claude-code-inbox / any registered launcher
  adapter. `TrajectoryWriter.write_raw_dict()` is the permissive,
  no-validation writer SubprocessAgent uses to forward adapter events
  (Plan 12 adapters emit valid shapes; v1.5 reintroduces validation).
  324 unit+contract+property + 146 integration green; 100 source files
  clean.
- **Plan 10 — `loom-launcher` PyPI package (2026-06-07, tag `loom-launcher-v0.10`).**
  New in-repo package at `packages/loom-launcher/` (sibling pyproject)
  shipping the `AgentAdapter` Protocol, four capture primitives
  (`stream_stdout_jsonl`, `tail_log_file`, `poll_local_http`, `tail_pty`),
  and a name→instance registry with collision detection. The package
  duplicates `ExecHandle` and `ModelSpec` from `loom.driver.base` /
  `loom.models.types` (Plan 11's `SubprocessAgent._bridge()` adapts
  between them) so the PyPI distribution doesn't pull in unrelated
  server-side deps. `ExecHandle.sandbox: SandboxAccess | None` is the
  Plan 9 amendment A11.2 hook that lets `tail_log_file` and
  `poll_local_http` reach inside the sandbox container without
  importing `loom.driver.base.Driver`. `HelloAdapter` ships as a
  placeholder reference for contract tests. 21 launcher tests
  (registry + 3 capture primitives + HelloAdapter round-trip) green;
  ruff + mypy strict clean across 10 launcher source files. Loom main
  repo 324 unit+contract+property + 99 source files unchanged.
- **Plan 9 — Gateway multi-dialect + step JWT (2026-06-07, tag `loom-gateway-multidialect-v0.9`).**
  Three new Gateway dialect endpoints (`POST /v1/messages` Anthropic,
  `POST /v1/responses` OpenAI Responses, `POST /v1beta/models/{model_path}`
  Gemini) — all native httpx-passthrough per amendment A9.1, no LiteLLM
  round-trip, so `cache_control`, `tool_use` content blocks, OpenAI
  reasoning details, and Gemini `thoughtsTokenCount` survive intact.
  `loom.auth.verify_bearer_token` gains a JWT branch (`loom_step_<jwt>`,
  HS256) that fires only when a `signing_key` is supplied — preserves
  the v0.7 `AuthContext` field names per amendment A9.0. Control Plane
  gains `POST /admin/step-tokens` (worker mints per-step tokens for
  agents) and `GET /trials/{trial_id}/llm-calls` (amendment A9.2 —
  worker reads at finalize). New `llm_calls` table is the canonical
  per-call cost/usage row store; the `DialectAdapter` extracts tokens
  from each dialect's native response into the same `TokenUsage` shape.
  324 unit+contract+property + 140 integration green; lint + mypy strict
  clean across 99 source files.
- **Plan 8 — Driver Protocol streaming exec (2026-06-06, tag `loom-driver-streaming-v0.8`).**
  Additive `exec_streaming(argv, env_vars, cwd, user) -> ExecHandle` on
  the `Driver` Protocol. Existing buffered `exec()` is unchanged; only
  agent runtimes that need to stream multi-megabyte agent output reach
  for the streaming path. Both `FakeDriver` and `DockerDriver` ship
  implementations. `ExecHandle.kill()` is documented best-effort
  (docker exec across PID namespaces can't be reliably signaled —
  callers wanting a hard deadline use `asyncio.wait_for` and bound
  execution via timeouts inside the agent command). 313
  unit+contract+property + 125 integration green; lint + mypy strict
  clean across 93 source files.

### Fixed
- **Plan 7 post-review hardening (2026-06-06).** 9 findings from the
  post-Plan-7 self-audit, plus regression tests for the schema-drift
  and resource-leak classes.
  - **Bug 1 (HIGH):** `tests/fixtures/tasks/in-box-cli/task.toml` had a
    bogus `mode = "in-box"` under `[agent]`; `AgentDefaults` is
    `extra="forbid"`. Dropped — runtime class fixes the mode.
  - **Bug 2 (HIGH):** `tests/system/test_full_stack_worker_crash.py`
    used `retry_on = ["crash"]`; the enum value is `"worker_crash"`.
  - **Bug 3 (HIGH):** `docker-compose.test.yml` baked a placeholder
    worker token at compose-up; the worker container failed
    registration before any test ran. Rewrote
    `tests/system/docker_compose.py` to do two-stage compose-up: deps
    → seed (mints real worker token) → worker (with token in env). The
    worker service is now gated by `profiles: ["worker"]`.
  - **Bug 4 (MEDIUM):** `src/loom_worker/main_loop.py:_spawn_trial`
    was leaking a `tempfile.mkdtemp()` per trial. Added
    `shutil.rmtree` in a `try/finally` so cleanup fires on success,
    agent error, and cancellation.
  - **Bug 5 (MEDIUM):** `docs/task-authoring-guide.md` showed a
    `POST /admin/tasks` example as if usable in v0.7. Rewrote the
    Submitting section to point at `scripts/seed_test_data.py`.
  - **Bug 6 (MEDIUM):** `deploy/Dockerfile.worker` was installing
    `docker.io` (daemon + CLI). Worker uses the Docker SDK for Python
    (already a dep) to talk to the bind-mounted socket — no CLI
    binary needed. Image is now noticeably smaller.
  - **Bug 7 (LOW):** Documented the open-to-any-authenticated-token
    scope policy in `src/loom_control_plane/routes/tasks.py`.
  - **Bug 8 (LOW):** Documented the Pod Security Admission "restricted"
    profile incompatibility on the docker-sock hostPath in
    `deploy/k8s/worker.yaml`.
  - **Bug 9 (LOW):** Comment explaining the `1e-9` float tolerance in
    `tests/property/test_drf_fairness_property.py`.
  - **3 new regression tests (14 cases):** fixture TaskConfig parses,
    every RetryReason round-trips + the literal `"crash"` is rejected,
    `_spawn_trial`'s mkdtemp cleanup fires on success/exception/cancel.
  - `scripts/seed_test_data.py` is now idempotent for the shared rate
    card + already-seeded tasks so multi-stage system-test seeding
    doesn't crash on duplicate keys.
  - 307 unit+contract+property + 121 integration tests green; lint +
    mypy strict clean across 93 source files. Tag
    `loom-v0.7-runtime-core` force-moved to the fix commit.

### Added
- **Plan 7 — System E2E + Ops (2026-06-06, tag `loom-v0.7-runtime-core`).**
  Closes the runtime core: workers can now resolve trial → task config
  via a new bundle endpoint, the canonical task fixtures live on disk,
  hypothesis-driven property tests guard the state machine + backoff +
  DRF + ATIF projection, full-stack docker-compose tests exercise the
  hello/multi-step/cancel/worker-crash paths, deploy YAML covers dev
  compose + k8s, and operator-facing docs ship.
  - **Control Plane:** new `GET /tasks/{id}/bundle` → `{id, checksum,
    config, source}`. Worker uses this as the second round-trip after
    a successful claim, removing Plan 6's NotImplementedError stub.
  - **Worker:** `HttpControlPlaneClient.get_task_bundle` + `_spawn_trial`
    now fetches the bundle, validates `TaskConfig`, and pulls the
    checksum from the bundle. `task_dir` uses `tempfile.mkdtemp()`;
    production ops mounts a shared volume or clones `bundle["source"]`
    (documented in `docs/operator-runbook.md`).
  - **Canonical fixtures** (`tests/fixtures/tasks/`): hello-world (CI
    canary), multi-step-3 (per-step rewards + `min_reward` gate),
    in-box-cli (claude-code placeholder), healthcheck-flaky (DockerDriver
    retry exercise), large-artifact (boto3 multipart). pytest's
    `norecursedirs` excludes them from host collection.
  - **Property tests** (`tests/property/`, hypothesis):
    - state machine: terminal states absorbing + no invalid transitions
    - backoff: delay within jitter bounds + zero-jitter determinism
    - DRF: per-pick min-ratio invariant + weighted-fairness within
      `1/min(weight)` + empty queues return None
    - ATIF: projection metadata + steps deterministic across re-runs
  - **System tests** (`tests/system/`, opted-out by default via
    `addopts = "--ignore=tests/system"`): full-stack hello, multi-step,
    cancellation, worker-crash-then-retry. Session fixture brings up
    `deploy/docker-compose.test.yml` (postgres + minio + gateway +
    control-plane + worker) and tears it down at session end.
    Skippable via `LOOM_SKIP_SYSTEM_TESTS=1`.
  - **Deploy:** `deploy/docker-compose.{dev,test}.yml` +
    `Dockerfile.{control-plane,gateway,worker}` + `deploy/k8s/` (postgres
    + minio StatefulSets, control-plane/gateway/worker Deployments,
    nginx Ingress).
  - **scripts/seed_test_data.py:** bootstraps a team + chosen fixture
    + tokens + rate card directly into Postgres. `--print {team|worker|both}`
    chooses which token to emit.
  - **Docs:** README rewritten for post-shipment state;
    `docs/operator-runbook.md` covers initial deploy, upgrade, rollback,
    rate-card + token rotation, alarm matrix, backup/restore, capacity
    planning. `docs/task-authoring-guide.md` walks through writing a
    new task fixture from scratch.
  - 293 unit+contract+property + 121 integration tests; lint + mypy
    strict clean across 93 source files. **Loom v0.7 runtime core is
    runnable end-to-end.**

- **Plan 6 — Worker (2026-06-06, tag `loom-worker-v0.6`).** Long-lived
  worker process that claims trials from the Control Plane and runs them
  in-process via `Trial.run()`. New top-level package `src/loom_worker`
  with `python -m loom_worker` entry point. Highlights:
  - `loom_worker.config.WorkerSettings`: `LOOM_WORKER_`-prefixed
    BaseSettings with SecretStr token + MinIO creds.
  - `loom_worker.control_plane_client.HttpControlPlaneClient`: async
    `register` / `claim` / `heartbeat` / `patch_state` /
    `patch_trajectory_index`. 409 (fence) surfaces as `False` from the
    bool-returning methods.
  - `loom_worker.heartbeat.HeartbeatThread`: dedicated daemon OS thread
    insulated from the asyncio loop. Swallows tick exceptions so a
    transient PATCH failure can't kill the heartbeat. (Internal Event
    renamed `_stop_event` after collision with Thread's private `_stop`.)
  - `loom_worker.orphan_cleanup.cleanup_orphan_trajectories`: startup
    sweep of the local trajectory cache — deletes JSONL files for trials
    that are terminal, unknown, or owned by a different worker.
  - `loom_worker.runner_pool.RunnerPool`: `asyncio.Semaphore`-gated
    spawn/in_flight/wait_all/cancel_all primitive.
  - `loom_worker.trial_runner.LocalTrialRunner`: wires a TrialContext
    from a claim payload + factories and invokes Plan 3's `Trial.run`
    with a state-patch callback that logs fence rejections and swallows
    transient errors.
  - `loom_worker.signal_handler`: installs SIGTERM/SIGINT handlers that
    flip a `ShutdownState.shutting_down` flag.
  - `loom_worker.main_loop.run_worker`: register → orphan cleanup →
    heartbeat thread → claim+spawn loop → drain (timeout → cancel_all).
  - **Known v1 limitation:** the Plan 5 claim endpoint returns the
    trial's `config` (TrialConfig) but not the full `TaskConfig` body —
    `_fetch_task_config` raises NotImplementedError pointing at Plan 7,
    so the worker is effectively a register + heartbeat skeleton until
    Plan 7 expands the claim payload.
  - 286 unit+contract + 117 integration tests + 1 skip (E2E); lint +
    mypy strict clean across 92 source files.

### Fixed
- **Plan 5 post-review hardening (2026-06-06).** Fixes for 6 findings from
  the post-Plan-5 self-audit, plus regression tests for each.
  - **Bug 1 (HIGH):** `PATCH /trials/{id}/state` now validates `state`
    against the `TrialState` enum and `failure_reason` against the
    `FailureReason` enum at the route boundary. Previously arbitrary
    strings would land in the DB column.
  - **Bug 2 (HIGH):** `POST /artifacts/upload-url` now rejects keys with
    `..` / `.` / empty segments / leading `/` / NUL bytes. Previously a
    team A token could presign a PUT against team B's namespace on the
    wire even though the key path looked team-A-scoped.
  - **Bug 3 (HIGH):** `DELETE /admin/worker-tokens/{prefix}` now requires
    `prefix` to be 4–64 hex characters. Previously `%` or punctuation
    fell through to the `LIKE :prefix` clause and could revoke every
    token in the table.
  - **Bug 4 (MEDIUM):** state PATCH source-state-restricted per target
    via SQL `AND state = ANY(:allowed_from)` — no more
    `succeeded → queued` reversals, no more unreachable targets like
    `queued` / `claimed`.
  - **Bug 5 (MEDIUM):** `POST /workers/register` now validates each
    `capabilities` entry via `loom.models.capabilities.Capabilities`
    (extra=forbid). Typo'd OS / GPU vendor / network policy values are
    rejected with 400 instead of silently never matching any DRF claim.
  - **Bug 6 (MEDIUM):** `POST /artifacts/upload-url` with a team token
    now additionally checks the requested `trial_id` belongs to that
    team (403 otherwise). Worker tokens still resolve team from the
    trial row.
  - 17 new integration regression tests added across
    `test_state_patch_fenced.py`, `test_signed_urls.py`,
    `test_token_admin.py`, and a new `test_worker_register.py`.
  - Full suite green: 270 unit+contract + 108 integration; lint + mypy
    strict clean across 82 source files. Tag `loom-control-plane-v0.5`
    moved forward to this fix commit.

### Added
- **Plan 5 — Control Plane (2026-06-06, tag `loom-control-plane-v0.5`).**
  Authoritative writer for trial state + worker registry. New sibling
  service `loom_control_plane` alongside `loom_llm_gateway`. Highlights:
  - `loom.auth`: hoisted from `loom_llm_gateway.auth` so both services
    share the same bearer-token verification helper (re-export shim
    kept for compat).
  - `loom_control_plane.config`: `LOOM_CP_`-prefixed BaseSettings.
  - `loom_control_plane.scheduler.requires_caps`: pure transform from
    `TaskConfig` → `RequiredCapabilities` (unions baseline + step phase
    network policies; submitters never specify caps directly).
  - `loom_control_plane.scheduler.claim`: single SQL CTE + UPDATE with
    `FOR UPDATE SKIP LOCKED` implementing DRF — `in_flight_count /
    fair_share_weight`, then `submit_priority`, then oldest
    `submitted_at`. Mounted_fs intentionally omitted in v1.
  - `loom_control_plane.scheduler.crash_detector`: background sweep
    that reclaims trials from workers whose `last_seen_at` is older
    than `expiry_sec` (back to queued + 30s backoff via
    `next_attempt_at`). Lifespan-managed with shielded cancellation.
  - Routes:
    - `POST /trials` — auth + task lookup + caps derivation + defensive
      team_quota upsert + trial INSERT.
    - `POST /trials/claim` — bearer (worker:claim) → flatten caps →
      claim_one → 200 trial config or 204.
    - `POST /workers/register` + `POST /workers/{id}/heartbeat`.
    - `PATCH /trials/{id}/state` — fenced by `(id, worker_id)` predicate;
      409 if worker has lost claim.
    - `PATCH /trials/{id}/trajectory_index` — fenced trajectory_index
      JSONB write.
    - `POST /artifacts/upload-url` — presigned MinIO URL; team-id
      resolution from token (team) or trial row (worker).
    - `POST /trials/{id}/cancel` — source-state-aware (only
      queued/claimed/running → cancelled).
    - `GET /trials/{id}` — full trial fetch with cross-team 403.
    - `GET /trials/{id}/trajectory` — 302 redirect to presigned
      get_object URL.
    - `POST /admin/worker-tokens` + `DELETE /admin/worker-tokens/{prefix}`
      — admin-scope-gated token issue/revoke.
  - `loom_control_plane.metrics`: Prometheus enumeration (Counters,
    Gauges, Histogram) per spec §7.3.
  - 270 unit+contract tests + integration suite green; lint + mypy
    strict clean across 82 source files.
- **Plan 4 — LLM Gateway (2026-06-06, tag `loom-llm-gateway-v0.4`).**
  Sibling service `loom_llm_gateway` (top-level package alongside
  `loom`): OpenAI-compatible chat endpoint with bearer auth, rate-card
  lookup + cost computation, LiteLLM passthrough, admin rate-card
  upsert. Plus the worker-side `HttpLLMGatewayClient` that LiteLLMAgent
  now talks to in production. 262 unit+contract tests + 41 integration
  tests; lint + mypy strict clean across 64 source files. Highlights:
  - `loom_llm_gateway.config`: pydantic-settings BaseSettings with
    `LOOM_GW_` env prefix; SecretStr for provider keys.
  - `loom_llm_gateway.auth`: bearer token verification (hash lookup
    against `tokens` table, expiry + revocation check).
  - `loom_llm_gateway.rate_card`: Pydantic models, most-specific
    (provider/model/tier/region) lookup, TTL-refresh in-memory cache
    with explicit `invalidate()`, stable `hash_table` (excludes
    captured_at) + `compute_cost_usd`.
  - `loom_llm_gateway.litellm_wrapper`: `acompletion` thin wrapper +
    `parse_litellm_response` mapping provider-specific usage counters
    (Anthropic cache_creation/cache_read, thinking_tokens) into typed
    fields + `provider_extras` dict[str, int].
  - `loom_llm_gateway.routes`: `health`, `chat` (POST /v1/chat/completions
    with required `loom` block + cost computation + rate_card_hash on
    response), `admin` (POST /admin/rate-cards INSERT…ON CONFLICT,
    invalidates cache).
  - `loom_llm_gateway.app`: FastAPI factory with lifespan ctxmgr owning
    the async engine + session factory + RateCardCache; `python -m
    loom_llm_gateway` entry point.
  - `loom.agent.http_gateway_client.HttpLLMGatewayClient`: worker-side
    HTTP client implementing the `LLMGatewayClient` Protocol from Plan 3.
- **Plan 3 — Agent + Verifier + Trial (2026-06-05, tag `loom-agent-verifier-trial-v0.3`).**
  Three agent runtimes, five verifiers, and the Trial.run() orchestrator
  that wires it all together. 245 unit/contract tests, 26 integration
  tests (incl. one real-Docker E2E running OracleAgent + PytestVerifier
  against `python:3.11-alpine`). Lint + mypy strict clean across 51
  source files. Highlights:
  - `loom.agent.base`: AgentRuntime + InBoxAgentRuntime Protocols (single
    `run` method; `setup` for in-box only).
  - `loom.agent.gateway_client`: LLMGatewayClient Protocol +
    FakeLLMGatewayClient (Plan 4 wires the real HTTP backend).
  - `loom.agent.oracle`: OracleAgent — uploads + runs solution/solve.sh,
    emits EnvExecEvent, raises AgentError on non-zero exit.
  - `loom.agent.litellm`: LiteLLMAgent — single-call tool loop over
    LLMGatewayClient; max_turns guard raises AgentError.
  - `loom.agent.claude_code`: ClaudeCodeAgent — in-box runtime that
    setup()s /loom/, runs `claude --instruction ...`, then downloads +
    forwards /loom/trajectory.jsonl events to the host writer.
  - `loom.verifier.base`: Verifier Protocol + VerifierFactory registry.
  - `loom.verifier.pytest_verifier`: runs pytest in-sandbox with
    --junitxml; downloads + parses junit XML (avoids the 10MB exec
    stdout cap). missing_tests + parse_failure surface as
    `VerifierResult.error`.
  - `loom.verifier.script_verifier`: runs a script with
    `LOOM_VERIFIER_OUTPUT=`, parses the JSON it writes.
  - `loom.verifier.structured`: validates an artifact against a JSON
    Schema (jsonschema 4.x).
  - `loom.verifier.llm_judge`: submits a trajectory excerpt + rubric to
    the gateway, parses a JSON {rewards, confidence, rationale}.
  - `loom.verifier.composite`: MEAN/MIN/MAX/WEIGHTED + custom AggregatorFn.
    Errored children contribute 0 (spec §5.4).
  - `loom.trial.artifacts`: ArtifactCollector — POSIX glob via
    `find -path -type f -print0`, download + put_object.
  - `loom.trial.phase_network`: async context manager that swaps the
    driver's NetworkPolicy for a phase, asyncio.shield-restores baseline
    on exit (survives cancellation).
  - `loom.trial.finalize`: project events → ATIF → put_object.
  - `loom.trial.step_runner.run_step`: per-step body — agent (timeout +
    AgentError handling) → artifacts → verifier (timeout). First-error
    wins; step_end always fires.
  - `loom.trial.trial.Trial.run`: full lifecycle — driver.start →
    TrialStartEvent → optional InBox setup → step loop → reward
    aggregation (mean/min/final) → TrialEndEvent → shielded driver.stop
    → trajectory commit → timed shielded finalize → timed shielded
    state PATCH. Cancellation re-raises after cleanup completes.
- **Plan 2 — Driver + Trajectory (2026-06-05, tag `loom-driver-trajectory-v0.2`).**
  Sandbox execution + JSONL trajectory pipeline + ATIF v1.7 projection.
  184 unit/contract + 24 integration tests, all green; ruff clean; mypy
  strict-clean across 32 source files. Highlights:
  - `loom.driver.base`: Driver Protocol (7 async methods), StartOptions,
    MAX_EXEC_STREAM_BYTES.
  - `loom.driver.fake`: in-memory deterministic FakeDriver + command-table
    handler with truncation enforcement.
  - `loom.driver.docker`: production DockerDriver with start/stop, exec
    (timeout + stdout/stderr cap), upload/download (tarball-based),
    healthcheck loop, network-policy enforcement. Container gets
    `cap_add=["NET_ADMIN"]` so iptables works inside its netns.
  - `loom.driver.network_policy`: pure-data translator from NetworkPolicy
    to iptables shell commands. Public is a true no-op (vanilla images
    work); NoNetwork/Allowlist require iptables in the image. Allowlist
    pins resolved IPv4 IPs into /etc/hosts so connections bypass DNS
    after the default DROP.
  - `loom.trajectory.storage`: ObjectStore Protocol + FakeObjectStore +
    boto3-backed MinioObjectStore.
  - `loom.trajectory.writer`: TrajectoryWriter — local-first JSONL append
    + multipart upload. Enforces 5 MiB S3-multipart-part floor for all
    mid-trial flushes; final close flush has no minimum.
  - `loom.trajectory.reader` + `loom.trajectory.excerpt`: TrajectoryReader
    with iter_all/iter_kind/tail + four excerpt strategies (tail/all/
    tool_use_only/step_summary) with token-budget pruning.
  - `loom.trajectory.atif`: ATIF v1.7 models with consistency validator;
    `project_to_atif` deterministic projection from Loom events.
  - Contract test tier (`tests/contract/`): parametrized Driver contract
    suite that auto-registers each impl. FakeDriver + DockerDriver both
    pass the 4-test obligation set.
- **Plan 1 — foundation library (2026-06-05, tag `loom-foundation-v0.1`).**
  Shared types, errors, validators, and persistence layout that every
  later plan depends on. 116 unit tests, all green; ruff clean, mypy
  strict-clean across 21 source files. Highlights:
  - `loom.errors`: full LoomError hierarchy + `classify_failure()`
    mapping uncaught exceptions to `FailureReason` (spec §5.2).
  - `loom.models`: primitive types, NetworkPolicy / MCP / healthcheck /
    skill / exec / capabilities models, full `TaskConfig` schema with
    `normalize_steps`, content-addressed `task_checksum`, `TrialConfig`
    + retry math with deterministic jitter, `VerifierResult` /
    `CheckResult` / `TrialResult` / `StepResult`, full trajectory event
    catalog (21 event kinds) as a Pydantic discriminated union.
  - `loom.log`: structlog JSON config + `bind_trial_context`
    contextmanager (correlation via contextvars).
  - `loom.db`: SQLAlchemy DeclarativeBase + ORM models mirroring spec
    §4.7, Alembic environment, two migrations (`0001_initial_schema`
    + `0002_in_flight_count_trigger`), integration test via
    testcontainers Postgres 16.
  - Caveat (resolved 2026-06-05 alongside Plan 2): integration test
    requires Docker daemon access. `hongjian` was added to the `docker`
    group; existing shells use `sg docker -c "..."` until they reload.
- **Plan 0 — repo preparation (2026-06-05).** Archived pre-Loom code,
  tests, docs, configs, and CI workflows to `legacy/`; rewrote
  `pyproject.toml` to Loom minimum deps; added Loom-facing `README.md`
  and `NOTICE.md`; replaced 3 CI workflows with a stub during buildout.
  Eight sequential commits, all reversible (git mv to legacy/, never rm).
  Ready for Plan 1 implementation.

## [Pre-Loom]

For changes prior to the Loom rebuild, see `legacy/CHANGELOG-pre-loom.md`.
