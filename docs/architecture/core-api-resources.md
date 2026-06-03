# Core API Resources

Last updated: 2026-06-02

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
artifact chunk metadata, PM progress summaries, frontend session login,
model/harness discovery, run-scoped telemetry, artifact bundle download,
cancel, and retry.

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
| `GET /runs` | List dashboard-ready run summaries with filters | `RunRepository.list_run_dashboard_summaries()` + `run_dashboard_projections` fallback |
| `GET /runs/{run_id}` | Inspect one dashboard-ready run plus full trajectory and lifecycle events | `RunRepository.get_run_dashboard_summary()` + bounded `RunRepository.get_run()` detail fields + `RunRepository.list_status_events()` |
| `GET /runs/{run_id}/events` | Replay durable run lifecycle/progress events after an optional `after_seq` watermark | `RunRepository.list_status_events(after_seq=...)` |
| `GET /runs/{run_id}/stream` | Stream replayable run events as SSE, using the same Postgres event source as `/events` | `RunRepository.list_status_events(after_seq=...)` |
| `GET /runs/{run_id}/telemetry` | Inspect scoped run, worker, host, and sandbox health for live monitors | `RunRepository.get_run()` + stdlib host metrics |
| `POST /runs/{run_id}/cancel` | Cancel queued/provisioning/running/evaluating runs | `RunRepository.cancel_run()` |
| `POST /runs/{run_id}/retry` | Requeue failed/canceled runs as a new internal attempt | `RunRepository.retry_run()` |
| `GET /runs/{run_id}/artifacts` | List sanitized artifact references for a run | `RunRepository.get_run_dashboard_summary()` |
| `GET /runs/{run_id}/artifact-chunks` | List project-scoped stdout/stderr/trajectory/artifact chunk metadata with optional attempt, artifact, kind, sequence cursor, and bounded limit filters | `RunRepository.list_artifact_chunks()` |
| `GET /runs/{run_id}/artifact-chunks/content` | Download object-store bytes for one project-scoped completed chunk selected by artifact id, chunk kind, and sequence | `RunRepository.get_artifact_chunk()` + `Artifacpilot groupjectStore` |
| `GET /runs/{run_id}/artifact-bundle` | Download one sanitized zip containing manifest, run projection, trajectory, evaluation, artifact metadata, lifecycle events, and available artifact payload files from the configured object store | `RunDashboardProjection` + `RunRepository.list_status_events()` + `Artifacpilot groupjectStore` |
| `GET /runs/{run_id}/evaluation` | Inspect latest evaluator summary and, when present, multiple evaluator outputs for a run | `RunRepository.get_run_dashboard_summary()` |
| `GET /dashboard/progress` | Summarize accessible run progress for PM/research dashboards | `RunRepository.list_dashboard_progress_records()` + aggregate projection |
| `GET /ops/metrics` | Return scoped run status counts, queue depth, and scheduler capacity-blocked diagnostics visible to the authenticated user | `RunRepository.list_runs()` + `RunRepository.list_scheduler_capacity_blocks()` |

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
to teams where the authenticated user has membership. Clean
`run_dashboard_projections` rows are the preferred read source for list and
detail-summary payloads; missing or dirty rows fall back to hydrated
`RunRecord` projection so legacy and in-flight runs keep the same response
shape.

`GET /runs/{run_id}` is the heavier researcher detail payload. Its `run`
summary now uses the same clean `run_dashboard_projections` payload preferred
by lists, falling back to hydrated `RunRecord` projection when the projection is
missing or dirty. The bounded `trajectory` preview array still comes from
detail child rows and includes command, cwd, timestamps, exit code,
stdout/stderr previews, changed paths, model call id, and turn metadata, plus
lifecycle events. Full trajectory and log payloads are object-store artifacts
surfaced through artifact metadata and the artifact-bundle download. The list
endpoint intentionally does not embed trajectories.

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

The first durable dashboard projection table is `run_dashboard_projections`.
It stores one dashboard-safe payload per run, the source attempt id, the latest
lifecycle `seq` used to build the payload, a `dirty` flag, refresh reason, and
refresh timestamps. Terminal worker results, terminal status transitions, and
terminal recovery paths such as terminal-result mismatch and stale active
heartbeat recovery upsert this row immediately. Scheduler recovery also runs a
bounded projection refresh sweep for terminal runs whose projection is
missing, dirty, or older than the run row and records same-status
`projection.refreshed` events with scheduler id, execution task id, refresh
reason, prior projection state, and source event sequence metadata. `GET
/runs`, `GET /runs/{run_id}` summary fields, `GET /runs/{run_id}/artifacts`,
`GET /runs/{run_id}/evaluation`, and `GET /dashboard/progress` now use clean
projection rows for list, detail, aggregate, artifact, evaluator, and score
data, and fall back to hydrated `RunRecord` values only when a row is missing
or dirty. Current `GET /runs/{run_id}` still hydrates bounded trajectory
preview rows for detail compatibility; larger future detail payloads should
remain object-store-backed instead of moving into Postgres.

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
records the adapter id, endpoint dialects, API-key env names, base-URL env
names the worker will synthesize, and safe default agent kwargs when the runner
needs a durable compatibility pin such as the OpenHands CLI `openhands-ai`
version supported by the current Harbor package. Required secrets are safe
references such as `env:OPENAI_API_KEY`; raw key values are not accepted or
returned. This lets launch and dashboard consumers distinguish Harbor agents
from native platform runners before a full agent-selection UI is added.

`GET /harbor/agent-adaptation` is a launch preflight surface for #150. It
accepts the selected project, harness, Harbor agent, model id, and provider
config id, then returns `ready` or `blocked`, the adapter metadata, env names
that will be populated from safe provider refs, adapter default kwargs that
will be passed to Harbor, and actionable gaps such as a missing model provider
config or provider endpoint dialect mismatch. It never returns raw provider
secret values.

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
  `run.recovered`, `run.worker_failed`, `run.worker_subprocess_failed`,
  `worker.subprocess_started`, `worker.subprocess_completed`,
  `scheduler.capacity_blocked`, `artifact.chunk_recorded`,
  `artifact.upload_expired`, `artifact.upload_status_changed`,
  `log.chunk_recorded`, `evaluator.completed`, `evaluator.failed`,
  `projection.refreshed`, `sandbox.container_started`,
  `sandbox.container_completed`, and `sandbox.container_cleanup`.
  Evaluator events carry summary-only metadata such as evaluator id, mode,
  status, score, safe artifact refs, worker id, and execution task id; they do
  not embed full metrics, verbal feedback, judge prompts, or local file paths.
  Recovery metadata currently uses canonical reason codes for implemented
  `stale_dispatched`, `stale_worker_heartbeat`, `terminal_result_mismatch`,
  `docker_container_cleanup`, and `artifact_upload_expired` paths, plus
  reserved `canceled_resource_cleanup` and `projection_refresh_failed` paths.
  Later #160 slices should reuse these codes instead of minting local strings.
- Scheduler and worker writers use
  `agentic_data_platform.domain.execution_metadata` as the v1 attempt metadata
  contract. `run_attempts.metadata.execution.scheduler` records the scheduler
  lease and `execution_task_id`; `run_attempts.metadata.execution.runner`
  records worker claim, heartbeat, process status, and terminal completion
  metadata. Lifecycle event metadata includes `execution_task_id` where the
  writer knows the current attempt. Worker heartbeat and result persistence can
  validate that identifier so stale child completions from a previous attempt do
  not overwrite a newer retry. Subprocess children also acquire an
  attempt-level execution lock before benchmark execution; duplicate deliveries
  with the same `execution_task_id` return the current run snapshot without
  entering Harbor/Docker/model execution twice.
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
  size, object SHA-256, and log/trajectory chunk metadata. Current object-store
  writes mark successfully persisted payloads as `completed` and include
  `artifact_metadata_schema`, `upload_status`, `storage_key`,
  `object_size_bytes`, and `object_sha256` in metadata. The
  `artifact_chunks` table indexes ordered stdout/stderr/trajectory/artifact
  chunks by run, attempt, artifact, kind, and sequence, with object key, media
  type, nullable byte size/SHA-256 until completion, upload status, and optional
  error reason. `GET
  /runs/{run_id}/artifact-chunks` exposes this bounded metadata to authenticated
  project viewers without returning object payload bytes or storage
  credentials. Repository upload transaction APIs now own chunk upload state:
  `start_artifact_chunk_upload(...)` creates or idempotently observes a
  `started` row, `complete_artifact_chunk_upload(...)` requires real object
  size/SHA-256 before marking it `completed`, and
  `fail_artifact_chunk_upload(...)` records a diagnosable `failed` state without
  fake object metadata. Upload-state repository recovery can now expire stale
  `pending` or `started` rows to `expired` with a durable `run.recovered`
  event plus typed `artifact.upload_expired` and
  `artifact.upload_status_changed` events. Worker result persistence now writes
  object-backed terminal stdout/stderr chunks, records their metadata against
  the current attempt's trajectory artifact, and appends typed
  `log.chunk_recorded` events. Non-log trajectory/artifact chunks append
  `artifact.chunk_recorded` events. When a chunk's upload state changes,
  `artifact.upload_status_changed` records the previous and current status with
  the same safe chunk/object identifiers. These events carry object metadata such
  as chunk sequence, storage key, optional size/SHA-256 when known, media type,
  and upload status, not payload bytes. `GET
  /runs/{run_id}/artifact-chunks/content` downloads a
  completed chunk payload through the platform API by artifact id, chunk kind,
  and sequence; non-completed upload states return structured errors without
  fetching object storage or exposing storage keys. Dedicated object upload
  transaction APIs remain #159 follow-up work.
- Artifact bundle downloads are zip archives. The MVP bundle includes sanitized
  run metadata, trajectory JSONL, evaluator summary, artifact metadata,
  lifecycle events, and any artifact payload files available from the configured
  `Artifacpilot groupjectStore`. Bundle `lifecycle-events.json` uses the same event
  payload shape as replay APIs, including the monotonic `seq` watermark, so
  deployment smokes can compare bundle events with `/events` and one-shot SSE
  replay. Missing object payloads are reported as generic bundle manifest errors
  without leaking host paths or backend exception strings. Artifacts whose DB
  metadata has non-`completed` `upload_status` are treated as unavailable even
  if an object key exists; the manifest records `upload_status` and any
  `upload_error_reason` so partial upload state is visible to operators and
  recovery jobs. Scheduler recovery expires stale `pending` and `started`
  upload rows after `SCHEDULER_STALE_ARTIFACT_UPLOAD_TIMEOUT_SECONDS` so the
  bundle can report `expired` instead of silently reading a stale object key.
  Raw Harbor `jobs/` payload preservation is implemented through
  the #65 ingestor path and appears as object-store-backed payload files when
  the worker and API share the same artifact store.
- Telemetry responses expose run status, queue/worker state, host CPU/RAM/disk
  saturation indicators, and sandbox status without command output, env vars,
  host paths, or secret refs.
- OpenAPI examples are registered for the core list/detail routes so frontend
  and integration consumers can inspect expected payloads through
  `/openapi.json`.
- `POST /runs`, cancel, retry, and project update operations write structured
  `audit_events` with actor user id, project id, run id where applicable,
  request id, and event payload. The ops metrics endpoint exposes v0 run status
  counts, queue depth, visible project count, and scheduler capacity-blocked
  queued-run diagnostics after applying the authenticated user's project
  membership boundary.
- Request middleware emits `request_completed` service logs with request id,
  method, path, status code, and authenticated user id when present.

## Current Limits

- Queue execution uses the Postgres `runs` table as the documented durable v0
  queue. `RunRepository.dispatch_queued_runs(...)` lets a scheduler move
  eligible rows from `queued` to `dispatched` after checking global, backend,
  project, provider, model, agent, and benchmark active-run capacity, and
  records `run.dispatched` lifecycle events with the matched capacity keys. Its
  candidate ordering is project fair-share: the scheduler considers each
  project's oldest queued run before considering a second queued run from the
  same project, then capacity gates decide which candidates can dispatch. The
  Postgres implementation keeps the fair-share window ranking in a read-only
  candidate-id query and locks the selected queued rows in a separate
  `FOR UPDATE SKIP LOCKED` query so scheduler loops do not combine row locks
  with window functions. Before those reads, Postgres dispatch takes a
  transaction-scoped advisory lock so concurrent scheduler instances cannot make
  capacity decisions from stale active counts and over-dispatch different
  locked rows.
  Queued rows blocked by a cap record current
  `execution.scheduler.capacity_blocked` metadata and a
  `scheduler.capacity_blocked` event only when the blocker signature changes.
  `/ops/metrics` reports visible blocked counts by dimension and a bounded run
  list so operators can distinguish real queue depth from capacity saturation.
  Workers then prefer `RunRepository.claim_next_dispatched_run(...)` before the
  legacy queued-claim compatibility path. Redis remains available in the dev
  stack for a later queue/cache backend, but it is not the current source of
  truth.
- #157 has started the durable live-event and projection migration. The current
  slices reuse `run_status_events.id` as the replay sequence, expose replay and
  SSE routes, keep telemetry polling available, persist terminal dashboard
  projection rows that scheduler recovery can refresh in bounded batches, and
  make `/runs`, `/runs/{run_id}` summary fields, `/runs/{run_id}/artifacts`,
  `/runs/{run_id}/evaluation`, and `/dashboard/progress` prefer clean
  projection rows before falling back to hydrated run records. The first typed
  artifact/log/upload/evaluator/projection
  event slices record chunk, upload-expiry, upload-status-change, evaluator
  completion/failure, and projection refresh metadata without embedding
  payloads. The remaining #157 work is Redis fanout, broader typed sandbox
  resource-sample events, and larger future detail payloads that should be
  object-store-backed rather than hydrated from high-volume child rows.
- The long-running worker now uses the Docker terminal sandbox executor for
  API-created runs, while `worker-smoke` keeps a fixture executor for
  deterministic deployment validation. The first OpenAI-compatible terminal
  agent model provider path is available for API-model runs. #158 has started
  an opt-in subprocess-isolated worker path where the parent worker claims the
  run, launches `agentic_data_platform.worker.execution_child` with the current
  `execution_task_id`, and reloads the terminal state after the child persists
  the result. The child acquires the attempt execution lock before work starts,
  stale child completions are ignored if the current attempt changed while the
  child was running, and duplicate deliveries are skipped before they reach the
  executor. The parent records metadata-only `worker.subprocess_started` and
  `worker.subprocess_completed` events around the child process boundary for
  replay/SSE consumers; those events carry worker id, execution task id,
  child-entrypoint module, timeout, and return code without storing argv,
  secrets, paths, or logs. Nonzero, timeout, and incomplete-result child
  failures include bounded, redacted stdout/stderr tails in run failure
  metadata. Docker terminal sandbox execution records metadata-only
  `sandbox.container_started` and `sandbox.container_completed` events for
  each sandbox command through short repository transactions. These events
  carry worker id, execution task id, sandbox command index, image, resource
  limits, sandbox status, exit code, timeout flag, changed-path count, and a
  Docker cidfile container id when available; they exclude command text,
  stdout/stderr, host workspace paths, provider secrets, and payload bytes.
  Docker terminal sandbox containers now carry platform/run/resource labels;
  scheduler recovery
  can remove labeled containers for recovered active runs after closing its DB
  recovery transaction, records same-status `sandbox.container_cleanup`
  evidence, and the worker CLI keeps the manual operator cleanup path. Shared
  dev deploy now also verifies a killed-parent cleanup path by starting a live
  labeled container from a helper parent process, terminating that parent, and
  requiring scheduler recovery to remove the surviving container before
  API/frontend smokes continue. Broader long-running runtime leak auditing
  remains #158/#160 follow-up work. A production LLM-judge evaluator provider
  remains follow-up work.
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
- Artifact and evaluator routes use dashboard-safe projection payloads. The
  first durable `run_dashboard_projections` table now keeps terminal run
  projection payloads recoverable, and `/dashboard/progress` uses those clean
  rows for aggregate PM/research summaries before falling back to current run
  records. Dedicated artifact, evaluation, projection, and analytics
  repositories should be added when upload/download, pagination, retention,
  detailed evaluator history, and high-volume PM reporting are
  implemented.
- List endpoints do not yet include pagination or quota-aware filtering. Add
  these before broad internal rollout.
- `GET /runs/{run_id}` now reads its dashboard-safe summary from clean
  projection rows when available, but still hydrates bounded trajectory preview
  rows for the detail payload. Keep larger future detail payloads object-backed
  and add dedicated paginated reads before broad rollout.
- The first service tests use SQLite-backed migrations for speed. Shared dev
  Postgres validation happens through the GitHub Actions deployment path.
