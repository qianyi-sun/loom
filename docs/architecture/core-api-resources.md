# Core API Resources

Last updated: 2026-05-29

Related trackers:

- Backend epic: https://github.com/carinrc/agentic-data-platform/issues/49
- Core API resources: https://github.com/carinrc/agentic-data-platform/issues/52
- Run lifecycle API: https://github.com/carinrc/agentic-data-platform/issues/53
- Postgres persistence foundation: https://github.com/carinrc/agentic-data-platform/issues/51
- Auth/RBAC/ops baseline: https://github.com/carinrc/agentic-data-platform/issues/57
- Frontend and PM dashboard API contract: https://github.com/carinrc/agentic-data-platform/issues/58

## Goal

Issues #52 and #53 expose the first Postgres-backed resource and run lifecycle
surface for PM dashboards, researcher inspection, and future frontend
development. The API is still an internal control-plane surface, but it now has
stable routes for projects, benchmark tasks, queued run submission, lifecycle
events, run summaries, run detail trajectories, artifacts, evaluator feedback,
PM progress summaries, cancel, and retry.

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
environment. `INTERNAL_AUTH_TOKENS` maps `user_id=token` pairs, and callers send
`Authorization: Bearer <token>`. Health, readiness, and OpenAPI/docs routes stay
public; API resource routes require a valid bearer token. Route handlers load the
authenticated user from Postgres and enforce project membership roles against the
project owner's team:

- `viewer`: read project, run, artifact, and evaluation resources.
- `member`: viewer permissions plus create/cancel/retry run operations and
  project metadata updates.
- `owner`/`admin`: reserved higher roles with member-or-better access in v0.

This is a dev-safe boundary, not the final production SSO design.

## Endpoint Surface

| Endpoint | Purpose | Backing repository/projection |
| --- | --- | --- |
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
| `POST /runs` | Create a durable queued run for worker execution | `RunRepository.create_run()` + `RunDashboardProjection` |
| `GET /runs` | List dashboard-ready run summaries with filters | `RunRepository.list_runs()` + `RunDashboardProjection` |
| `GET /runs/{run_id}` | Inspect one dashboard-ready run plus full trajectory and lifecycle events | `RunRepository.get_run()` + `RunRepository.list_status_events()` |
| `POST /runs/{run_id}/cancel` | Cancel queued/provisioning/running/evaluating runs | `RunRepository.cancel_run()` |
| `POST /runs/{run_id}/retry` | Requeue failed/canceled runs as a new internal attempt | `RunRepository.retry_run()` |
| `GET /runs/{run_id}/artifacts` | List sanitized artifact references for a run | `RunDashboardProjection.artifacts` |
| `GET /runs/{run_id}/evaluation` | Inspect latest evaluator summary for a run | `RunDashboardProjection.evaluator` |
| `GET /dashboard/progress` | Summarize accessible run progress for PM/research dashboards | `RunRepository.list_runs()` + aggregate projection |
| `GET /ops/metrics` | Return scoped run status counts and queue depth visible to the authenticated user | `RunRepository.list_runs()` |

`benchmark_version` is a query parameter for benchmark detail, task-family, and
task routes because current versions can contain slashes, such as
`hf:zhang-ziao/SkillFlow-Task@...`.

`GET /runs` supports the first dashboard filters: `project_id`, `status`,
`benchmark_suite`, `task_family`, `task_instance_id`, `created_by_user_id`,
`created_after`, and `created_before`.
When `project_id` is omitted, the route only returns runs whose projects belong
to teams where the authenticated user has membership.

`GET /runs/{run_id}` is the heavier researcher detail payload. It includes the
same `run` projection used by lists, a full `trajectory` array with command,
cwd, timestamps, exit code, stdout, stderr, changed paths, model call id, and
turn metadata, plus lifecycle events. The list endpoint intentionally does not
embed full trajectories.

`GET /dashboard/progress` returns the PM summary surface. It accepts optional
`project_id` and `owner_team` filters, enforces the same project viewer boundary
as run reads, and returns a global `summary` plus per-project rows with status
counts, queue depth, terminal run count, artifact count, turn count, evaluator
completion count, average evaluator score, and latest update timestamp.

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
  `401 unauthorized`; project membership failures return structured
  `403 forbidden`.
- Missing resources map to `404`; unconfigured database access maps to `503`.
- Project and team responses mirror small repository read models.
- Benchmark and task responses mirror `BenchmarkFixtureCatalog` and
  `BenchmarkFixtureInstance`.
- Run responses reuse `RunDashboardProjection.to_dict()` so dashboards and API
  consumers share the same visible status/progress/evaluator payload.
- Run list responses stay lightweight. Full terminal stdout/stderr trajectory is
  exposed on run detail so researcher views can inspect execution without
  forcing PM lists to hydrate large traces.
- Run projections include `created_by_user_id`, `failure_reason`, and submitted
  evaluator configurations so dashboard clients can display ownership and
  planned evaluator mode before worker execution produces evaluator results.
- PM progress responses are aggregate-only and do not expose model prompts,
  terminal stdout/stderr, artifact URIs, or provider metadata.
- Run detail and lifecycle-mutating responses include `lifecycle_events` with
  `event_type`, `from_status`, `to_status`, `attempt_id`, `reason`,
  `actor_user_id`, `request_id`, and `created_at`.
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

- Queue execution uses the Postgres `runs` table as the documented v0 queue.
  `RunRepository.claim_next_queued_run(...)` lets a worker claim one queued run
  and record lifecycle events. Redis remains available in the dev stack for a
  later queue/cache backend, but it is not the current source of truth.
- The long-running worker now uses the Docker terminal sandbox executor for
  API-created runs, while `worker-smoke` keeps a fixture executor for
  deterministic deployment validation. Provider-backed live model/evaluator
  calls remain follow-up work before real pilot workloads run through this
  service.
- Provider configuration is still dev-scoped. The current implementation
  supports safe provider config references, env secret references, and redaction
  of raw secret-looking metadata. Production secret management should replace
  dev env vars before real internal workloads are onboarded.
- Auth is still dev-scoped. `INTERNAL_AUTH_TOKENS` should be replaced by the
  selected internal SSO or GitHub-org login flow before production use. Quota
  and retention policy are documented placeholders; enforcement is not in this
  slice.
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
