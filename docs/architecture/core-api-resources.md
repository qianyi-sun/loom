# Core API Resources

Last updated: 2026-06-01

Related trackers:

- Backend epic: https://github.com/carinrc/agentic-data-platform/issues/49
- Core API resources: https://github.com/carinrc/agentic-data-platform/issues/52
- Run lifecycle API: https://github.com/carinrc/agentic-data-platform/issues/53
- Postgres persistence foundation: https://github.com/carinrc/agentic-data-platform/issues/51
- Auth/RBAC/ops baseline: https://github.com/carinrc/agentic-data-platform/issues/57
- Frontend and PM dashboard API contract: https://github.com/carinrc/agentic-data-platform/issues/58
- Frontend MVP epic: https://github.com/carinrc/agentic-data-platform/issues/88

## Goal

Issues #52 and #53 expose the first Postgres-backed resource and run lifecycle
surface for PM dashboards, researcher inspection, and future frontend
development. The API is still an internal control-plane surface, but it now has
stable routes for projects, benchmark tasks, queued run submission, lifecycle
events, run summaries, run detail trajectories, artifacts, evaluator feedback,
PM progress summaries, frontend session login, model/harness discovery,
run-scoped telemetry, artifact bundle download, cancel, and retry.

The implementation deliberately reuses existing domain and dashboard projection
contracts rather than introducing a second API-only data model.

## Service Wiring

`create_app(...)` accepts an optional SQLAlchemy `database_engine`. In
production or Compose environments, it builds an engine from `DATABASE_URL`.
Resource route modules receive a FastAPI session dependency that opens a
SQLAlchemy session per request through `session_scope(...)`.

If no database is configured, resource endpoints return `503` through the shared
dependency boundary. Health and readiness endpoints remain available for service
operations.

The v0 internal auth boundary is intentionally simple for the shared dev/pilot
environment. `INTERNAL_AUTH_TOKENS` maps `user_id=token` pairs, and service
callers can send `Authorization: Bearer <token>`. The frontend uses
`POST /auth/login` with `WEB_LOGIN_CREDENTIALS` to create an HTTP-only
`adp_session` cookie, then calls the same resource routes through the session
boundary. Health, readiness, OpenAPI/docs, and static `/app/` frontend routes
stay public; API resource routes require either a valid bearer token or a valid
web session. Route handlers load the authenticated user from Postgres and
enforce project membership roles against the project owner's team:

- `viewer`: read project, run, artifact, and evaluation resources.
- `member`: viewer permissions plus create/cancel/retry run operations and
  project metadata updates.
- `owner`/`admin`: reserved higher roles with member-or-better access in v0.

This is a dev-safe boundary, not the final production SSO design.

## Endpoint Surface

| Endpoint | Purpose | Backing repository/projection |
| --- | --- | --- |
| `POST /auth/login` | Exchange configured dev credentials for an HTTP-only web session cookie | `IdentityRepository.get_user()` + signed session cookie |
| `GET /auth/session` | Inspect the current authenticated web/API user | `IdentityRepository.get_user()` |
| `POST /auth/logout` | Clear the web session cookie | session cookie boundary |
| `GET /teams` | List project-owning teams | `IdentityRepository.list_teams()` |
| `GET /teams/{team_id}` | Inspect one team | `IdentityRepository.get_team()` |
| `GET /projects` | List projects, optionally by `owner_team_id` | `ProjectRepository.list_projects()` |
| `GET /projects/{project_id}` | Inspect one project | `ProjectRepository.get_project()` |
| `PATCH /projects/{project_id}` | Update name, description, or status | `ProjectRepository.update_project()` |
| `GET /benchmarks` | List persisted benchmark suite versions | `BenchmarkCatalogRepository.list_fixture_catalogs()` |
| `GET /benchmarks/{suite_name}` | Inspect one suite/version | `BenchmarkCatalogRepository.get_fixture_catalog()` |
| `GET /task-families` | List task-family summaries for one suite/version | `BenchmarkFixtureCatalog.task_families` |
| `GET /task-families/{task_family}` | Inspect one task family and its tasks | `BenchmarkFixtureCatalog.task_families` |
| `GET /tasks` | List task instances for one suite/version | `BenchmarkCatalogRepository.get_fixture_catalog()` |
| `GET /tasks/{task_family}/{instance_id}` | Inspect one task instance | `BenchmarkCatalogRepository.get_task_instance()` |
| `POST /harbor/task-uploads` | Upload and validate a zipped Harbor-compatible task directory for a project | `Artifacpilot groupjectStore` + `AuditEventRepository` |
| `GET /models` | List frontend-selectable API models from configured provider discovery or static allowlist | `ServiceSettings` + provider config refs |
| `GET /harnesses` | List frontend-selectable launch harnesses including Docker terminal and Harbor-compatible local Docker smoke | static launch catalog |
| `GET /agents` | List Harbor built-in and custom import-path agents with safe adapter metadata | `HarborAgentProvider` |
| `GET /harbor/agent-adaptation` | Preflight one selected Harbor agent/model/backend combination and report required env contract or actionable gaps | `HarborAgentProvider` + `DevProviderConfigRegistry` |
| `POST /runs` | Create a durable queued run for worker execution | `RunRepository.create_run()` + `RunDashboardProjection` |
| `GET /runs` | List dashboard-ready run summaries with filters | `RunRepository.list_runs()` + `RunDashboardProjection` |
| `GET /runs/{run_id}` | Inspect one dashboard-ready run plus full trajectory and lifecycle events | `RunRepository.get_run()` + `RunRepository.list_status_events()` |
| `GET /runs/{run_id}/events` | Replay durable run lifecycle/progress events after an optional `after_seq` watermark | `RunRepository.list_status_events(after_seq=...)` |
| `GET /runs/{run_id}/stream` | Stream replayable run events as SSE, using the same Postgres event source as `/events` | `RunRepository.list_status_events(after_seq=...)` |
| `GET /runs/{run_id}/telemetry` | Inspect scoped run, worker, host, and sandbox health for live monitors | `RunRepository.get_run()` + stdlib host metrics |
| `POST /runs/{run_id}/cancel` | Cancel queued/provisioning/running/evaluating runs | `RunRepository.cancel_run()` |
| `POST /runs/{run_id}/retry` | Requeue failed/canceled runs as a new internal attempt | `RunRepository.retry_run()` |
| `GET /runs/{run_id}/artifacts` | List sanitized artifact references for a run | `RunDashboardProjection.artifacts` |
| `GET /runs/{run_id}/artifact-bundle` | Download one sanitized zip containing manifest, run projection, trajectory, evaluation, artifact metadata, lifecycle events, and available artifact payload files from the configured object store | `RunDashboardProjection` + `RunRepository.list_status_events()` + `Artifacpilot groupjectStore` |
| `GET /runs/{run_id}/evaluation` | Inspect latest evaluator summary and, when present, multiple evaluator outputs for a run | `RunDashboardProjection.evaluator` + `RunDashboardProjection.evaluator_results` |
| `GET /dashboard/progress` | Summarize accessible run progress for PM/research dashboards | `RunRepository.list_runs()` + aggregate projection |
| `GET /ops/metrics` | Return scoped run status counts and queue depth visible to the authenticated user | `RunRepository.list_runs()` |

Static frontend assets are served at `GET /app/` by the same FastAPI service.
The frontend is intentionally no-build for the first MVP slice so `shared dev`
does not need Node/npm to serve the owner-testable UI.

`benchmark_version` is a query parameter for benchmark detail, task-family, and
task routes because current versions can contain slashes, such as
`hf:zhang-ziao/SkillFlow-Task@...`.

`GET /runs` supports the first dashboard filters: `project_id`, `status`,
`benchmark_suite`, `task_family`, `task_instance_id`, `created_by_user_id`,
`created_after`, and `created_before`.
When `project_id` is omitted, the route only returns runs whose projects belong
to teams where the authenticated user has membership.

`GET /runs/{run_id}` is the heavier researcher detail payload. It includes the
same `run` projection used by lists, a bounded `trajectory` preview array with
command, cwd, timestamps, exit code, stdout/stderr previews, changed paths,
model call id, and turn metadata, plus lifecycle events. Full trajectory and log
payloads are object-store artifacts surfaced through artifact metadata and the
artifact-bundle download. The list endpoint intentionally does not embed
trajectories.

Evaluator projections expose `evaluator` as the latest primary summary for
existing clients and `evaluator_results` as the full side-by-side collection
when a run has multiple outputs such as Harbor verifier reward plus platform LLM
judge feedback. `GET /runs/{run_id}/evaluation` keeps the old single-summary
shape for single-evaluator runs and adds `evaluator_results` only when multiple
results exist.

`GET /dashboard/progress` returns the PM summary surface. It accepts optional
`project_id` and `owner_team` filters, enforces the same project viewer boundary
as run reads, and returns a global `summary` plus per-project rows with status
counts, queue depth, terminal run count, artifact count, turn count, evaluator
completion count, average evaluator score, and latest update timestamp.

`GET /models` returns only safe model selection metadata: provider id, provider
config id, display/model name, API mode, source, disabled/error state, basic
model-family metadata, endpoint dialect hints, and freshness timestamp. It
never returns raw API keys or secret refs. When a provider base URL and API key
are present, it calls the provider's OpenAI-compatible `/models` endpoint by
default. `MODEL_PROVIDER_MODELS` is an optional allowlist after discovery or a
static fallback when discovery fails or a provider does not support `/models`.
With no provider configured, it returns a local scripted dev model.

`GET /harnesses` returns the v0 launch harness catalog. `docker-terminal` is
the platform-native Docker terminal path. `harbor-local-docker` is a
frontend-visible Harbor local-Docker surface that submits real
`metadata.harbor_run`. For no-key smoke it uses a generated `harbor-cli-smoke`
task template with the Harbor Oracle agent and deterministic `smoke/noop`
model. For catalog-backed Harbor benchmarks, the frontend uses the selected
API model plus selected Harbor agent, and the worker resolves the model
provider secret into the Harbor agent environment only at execution time. The
first Harbor benchmark provider exposes a versioned `HarborTerminalBench`
catalog using `terminal-bench@2.0` and can map uploaded Harbor task archives
into checksum-versioned catalog entries for admin/custom benchmark onboarding.

`GET /agents` returns authenticated launch agent metadata. The first
implementation is `HarborAgentProvider`: it lists Harbor built-in agents,
supports custom Harbor `--agent-import-path` entries, reports whether an agent
is Harbor built-in, external CLI, or custom import mode, and exposes supported
`harness_id`, sandbox backend, trajectory support, and required secret
references. Mainstream built-ins also expose `model_adapter` metadata that
records the adapter id, endpoint dialects, API-key env names, and base-URL env
names the worker will synthesize. Required secrets are safe references such as
`env:OPENAI_API_KEY`; raw key values are not accepted or returned. This lets
launch and dashboard consumers distinguish Harbor agents from native platform
runners before a full agent-selection UI is added.

`GET /harbor/agent-adaptation` is a launch preflight surface for #150. It
accepts the selected project, harness, Harbor agent, model id, and provider
config id, then returns `ready` or `blocked`, the adapter metadata, env names
that will be populated from safe provider refs, and actionable gaps such as a
missing model provider config or provider endpoint dialect mismatch. It never
returns raw provider secret values.

`POST /harbor/task-uploads` is the first user-facing custom benchmark intake
path. A project member uploads a `.zip` Harbor task directory; the API validates
`instruction.md`, `task.toml`, `environment/Dockerfile`, `tests/`, declared
artifacts, whether verifier tests mention `reward.txt`, configured upload byte
limits, file count limits, and uncompressed materialization limits. The archive
is stored in the configured object store and an audit event records task name,
source metadata, environment settings, resource timeouts, declared artifacts,
size, checksum, and storage key. The response includes
`launch_metadata.harbor_run.task_archive_storage_key`, which can be copied into
a normal `POST /runs` request so the worker materializes the uploaded task and
launches Harbor with `harbor run -p`.

`POST /runs` accepts the durable submission envelope the worker will later
consume: `project_id`, a compatibility `owner_team` display hint, a benchmark
`task`, API-only `model` configuration, Docker-terminal `runner` configuration,
one or more `evaluators`, optional `created_by_user_id`, and caller metadata.
The service derives the stored run owner-team snapshot from the authorized
project record rather than trusting the request body. The checked-in example
payload is `docs/examples/run-create-request.json`.

Run submission requires `task.metadata.instruction` to be a non-empty string.
This keeps malformed terminal-agent requests from reaching the worker, where
the model-provider prompt context requires the instruction.

Model and evaluator payloads can include `provider_config_id` plus `secret_ref`
values such as `env:MODEL_PROVIDER_API_KEY` or
`env:EVALUATOR_PROVIDER_API_KEY`. The API persists those safe references and
redacts sensitive metadata keys such as `api_key`, `access_token`,
`authorization`, `password`, `secret`, and `token` before writing run records or
serializing dashboard payloads.

## Response Shape Principles

- Success responses include `request_id` when request middleware attaches one.
- Protected endpoints without a valid bearer token return structured
  `401 unauthorized`; protected frontend calls can authenticate with the
  HTTP-only web session cookie; project membership failures return structured
  `403 forbidden`.
- Missing resources map to `404`; unconfigured database access maps to `503`.
- Project and team responses mirror small repository read models.
- Benchmark and task responses mirror `BenchmarkFixtureCatalog` and
  `BenchmarkFixtureInstance`.
- Run responses reuse `RunDashboardProjection.to_dict()` so dashboards and API
  consumers share the same visible status/progress/evaluator payload.
- Run list responses stay lightweight. Run detail exposes bounded terminal
  stdout/stderr previews so researcher views can inspect recent execution
  context without inflating Postgres rows. The full terminal trajectory remains
  object-store-backed and downloadable from artifact bundles.
- Run projections include `created_by_user_id`, `failure_reason`, and submitted
  evaluator configurations so dashboard clients can display ownership and
  planned evaluator mode before worker execution produces evaluator results.
- PM progress responses are aggregate-only and do not expose model prompts,
  terminal stdout/stderr, artifact URIs, or provider metadata.
- Run detail and lifecycle-mutating responses include `lifecycle_events` with
  a monotonic `seq` watermark plus `event_type`, `from_status`, `to_status`,
  `attempt_id`, `reason`, `actor_user_id`, `request_id`, and `created_at`.
  `GET /runs/{run_id}/events?after_seq=N` replays missed events from the same
  durable source, and `GET /runs/{run_id}/stream` emits SSE frames with
  `id: <seq>` so browser monitors can reconnect without losing status changes.
  The stream also honors the browser `Last-Event-ID` header on reconnect.
- Backend writers use `agentic_data_platform.domain.execution_events` as the
  v1 execution-event contract. Current run events cover `run.created`,
  `run.dispatched`, `run.claimed`, `run.started`, `run.evaluating`,
  `run.succeeded`, `run.failed`, `run.canceled`, `run.retried`,
  `run.recovered`, `run.worker_failed`, and `run.worker_subprocess_failed`.
  Recovery metadata currently uses canonical reason codes for
  `stale_dispatched`, `stale_worker_heartbeat`, `terminal_result_mismatch`,
  `canceled_resource_cleanup`, `artifact_upload_expired`, and
  `projection_refresh_failed`; not every listed recovery path is implemented
  yet, but later #160 slices should reuse these codes instead of minting local
  strings.
- Invalid cancel/retry transitions return structured `409 conflict` errors
  through the shared service error boundary.
- Invalid nested create payloads and blank cancel/retry reasons return
  structured `422 validation_error` responses before persistence mutates run
  state.
- Artifact responses do not expose local `file://` URIs or absolute host paths.
  The projection keeps stable artifact ids, media types, sizes, and
  object-store-safe `storage_key` values, and strips query strings from external
  URLs. Absolute paths, `file://` values, traversal keys, drive-letter paths,
  and query/fragment-bearing keys are suppressed.
- Artifact object metadata uses `agentic_data_platform.domain.artifact_metadata`
  as the v1 contract for content type, upload status, object key, object byte
  size, object SHA-256, and future log/trajectory chunk metadata. Current
  object-store writes mark successfully persisted payloads as `completed` and
  include `artifact_metadata_schema`, `upload_status`, `storage_key`,
  `object_size_bytes`, and `object_sha256` in metadata. Upload-state repository
  transitions and chunk-index APIs remain #159 follow-up work.
- Artifact bundle downloads are zip archives. The MVP bundle includes sanitized
  run metadata, trajectory JSONL, evaluator summary, artifact metadata,
  lifecycle events, and any artifact payload files available from the configured
  `Artifacpilot groupjectStore`. Missing object payloads are reported as generic bundle
  manifest errors without leaking host paths or backend exception strings. Raw
  Harbor `jobs/` payload preservation is implemented through the #65 ingestor
  path and appears as object-store-backed payload files when the worker and API
  share the same artifact store.
- Telemetry responses expose run status, queue/worker state, host CPU/RAM/disk
  saturation indicators, and sandbox status without command output, env vars,
  host paths, or secret refs.
- OpenAPI examples are registered for the core list/detail routes so frontend
  and integration consumers can inspect expected payloads through
  `/openapi.json`.
- `POST /runs`, cancel, retry, and project update operations write structured
  `audit_events` with actor user id, project id, run id where applicable,
  request id, and event payload. The ops metrics endpoint exposes v0 run status
  counts, queue depth, and visible project count after applying the
  authenticated user's project membership boundary.
- Request middleware emits `request_completed` service logs with request id,
  method, path, status code, and authenticated user id when present.

## Current Limits

- Queue execution uses the Postgres `runs` table as the documented durable v0
  queue. `RunRepository.dispatch_queued_runs(...)` lets a scheduler move
  eligible rows from `queued` to `dispatched` after checking global, backend,
  and project active-run capacity, and records `run.dispatched` lifecycle
  events. Workers then prefer `RunRepository.claim_next_dispatched_run(...)`
  before the legacy queued-claim compatibility path. Redis remains available in
  the dev stack for a later queue/cache backend, but it is not the current
  source of truth.
- #157 has started the durable live-event migration. The current slice reuses
  `run_status_events.id` as the replay sequence, exposes replay and SSE routes,
  and keeps telemetry polling available. It does not yet implement Redis
  fanout, typed sandbox/resource sample events, or projection tables for every
  summary.
- The long-running worker now uses the Docker terminal sandbox executor for
  API-created runs, while `worker-smoke` keeps a fixture executor for
  deterministic deployment validation. The first OpenAI-compatible terminal
  agent model provider path is available for API-model runs. #158 has started
  an opt-in subprocess-isolated worker path where the parent worker claims the
  run, launches `agentic_data_platform.worker.execution_child`, and reloads the
  terminal state after the child persists the result. A production
  LLM-judge evaluator provider remains follow-up work.
- Provider configuration is still dev-scoped. The current implementation
  supports safe provider config references, env secret references, and redaction
  of raw secret-looking metadata. Production secret management should replace
  dev env vars before real internal workloads are onboarded.
- Auth is still dev-scoped. `INTERNAL_AUTH_TOKENS` and
  `WEB_LOGIN_CREDENTIALS` should be replaced by the selected internal SSO or
  GitHub-org login flow before production use. Quota and retention policy are
  documented placeholders; enforcement is not in this slice.
- Frontend model discovery is provider-config driven but minimal. Credentialed
  environments discover provider models by default. The static
  `MODEL_PROVIDER_MODELS` list should be reserved for allowlisting, fallback, or
  controlled pilots; production provider discovery still needs provider-specific
  filtering, caching policy, and capability detection.
- The `harbor-local-docker` harness is now a frontend-to-Harbor launch surface
  for both no-key smoke runs and catalog-backed benchmark runs. The #62
  provider slice gives the API a Harbor benchmark read model, #63 adds
  authenticated Harbor agent discovery, #131 adds registry dataset sync, #142
  wires selected API models and Harbor agents into run creation, and #144
  selects `terminal-bench@2.0` as the first non-smoke acceptance target.
  Frontend Harbor task upload remains #143 for admin/custom benchmark
  onboarding rather than the ordinary evaluation flow.
- Retry creates a new internal `run_attempts` row for the same `run_id`, while
  the public run projection shows the latest attempt. Full attempt-detail APIs
  should be added when worker-managed retries preserve per-attempt artifacts and
  trajectories.
- Artifact, evaluator, and dashboard progress routes are projection-backed.
  Dedicated artifact, evaluation, and analytics repositories should be added
  when upload/download, pagination, retention, detailed evaluator history, and
  high-volume PM reporting are implemented.
- List endpoints do not yet include pagination or quota-aware filtering. Add
  these before broad internal rollout.
- `GET /runs` currently hydrates complete `RunRecord` objects before projecting
  summaries. Replace this with a lightweight summary query before storing large
  trajectories or exposing high-volume run lists.
- The first service tests use SQLite-backed migrations for speed. Shared dev
  Postgres validation happens through the GitHub Actions deployment path.
