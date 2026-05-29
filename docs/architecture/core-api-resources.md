# Core API Resources

Last updated: 2026-05-28

Related trackers:

- Backend epic: https://github.com/carinrc/agentic-data-platform/issues/49
- Core API resources: https://github.com/carinrc/agentic-data-platform/issues/52
- Run lifecycle API: https://github.com/carinrc/agentic-data-platform/issues/53
- Postgres persistence foundation: https://github.com/carinrc/agentic-data-platform/issues/51

## Goal

Issues #52 and #53 expose the first Postgres-backed resource and run lifecycle
surface for PM dashboards, researcher inspection, and future frontend
development. The API is still an internal control-plane surface, but it now has
stable routes for projects, benchmark tasks, queued run submission, lifecycle
events, run summaries, artifacts, evaluator feedback, cancel, and retry.

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
| `GET /runs/{run_id}` | Inspect one dashboard-ready run plus lifecycle events | `RunRepository.get_run()` + `RunRepository.list_status_events()` |
| `POST /runs/{run_id}/cancel` | Cancel queued/provisioning/running/evaluating runs | `RunRepository.cancel_run()` |
| `POST /runs/{run_id}/retry` | Requeue failed/canceled runs as a new internal attempt | `RunRepository.retry_run()` |
| `GET /runs/{run_id}/artifacts` | List sanitized artifact references for a run | `RunDashboardProjection.artifacts` |
| `GET /runs/{run_id}/evaluation` | Inspect latest evaluator summary for a run | `RunDashboardProjection.evaluator` |

`benchmark_version` is a query parameter for benchmark detail, task-family, and
task routes because current versions can contain slashes, such as
`hf:zhang-ziao/SkillFlow-Task@...`.

`GET /runs` supports the first dashboard filters: `project_id`, `status`,
`benchmark_suite`, `task_family`, `task_instance_id`, `created_by_user_id`,
`created_after`, and `created_before`.

`POST /runs` accepts the durable submission envelope the worker will later
consume: `project_id`, `owner_team`, a benchmark `task`, API-only `model`
configuration, Docker-terminal `runner` configuration, one or more
`evaluators`, optional `created_by_user_id`, and caller metadata. The checked-in
example payload is `docs/examples/run-create-request.json`.

## Response Shape Principles

- Success responses include `request_id` when request middleware attaches one.
- Missing resources map to `404`; unconfigured database access maps to `503`.
- Project and team responses mirror small repository read models.
- Benchmark and task responses mirror `BenchmarkFixtureCatalog` and
  `BenchmarkFixtureInstance`.
- Run responses reuse `RunDashboardProjection.to_dict()` so dashboards and API
  consumers share the same visible status/progress/evaluator payload.
- Run projections include `created_by_user_id`, `failure_reason`, and submitted
  evaluator configurations so dashboard clients can display ownership and
  planned evaluator mode before worker execution produces evaluator results.
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

## Current Limits

- Queue execution uses the Postgres `runs` table as the documented v0 queue.
  `RunRepository.claim_next_queued_run(...)` lets a worker claim one queued run
  and record lifecycle events. Redis remains available in the dev stack for a
  later queue/cache backend, but it is not the current source of truth.
- The current worker executor is a fixture terminal benchmark path for service
  and deployment smoke checks. Real Docker terminal execution and provider-backed
  model/evaluator calls remain follow-up work before real pilot workloads run
  through this service.
- Model and evaluator configs are stored as metadata only in this slice.
  Provider registration, secret indirection, and API-key boundaries remain
  tracked under #56.
- Retry creates a new internal `run_attempts` row for the same `run_id`, while
  the public run projection shows the latest attempt. Full attempt-detail APIs
  should be added when worker-managed retries preserve per-attempt artifacts and
  trajectories.
- Artifact and evaluator routes are projection-backed. Dedicated artifact and
  evaluation repositories should be added when upload/download, pagination,
  retention, and detailed evaluator history are implemented.
- List endpoints do not yet include pagination, auth scoping, or quota-aware
  filtering. Add these before broad internal rollout.
- `GET /runs` currently hydrates complete `RunRecord` objects before projecting
  summaries. Replace this with a lightweight summary query before storing large
  trajectories or exposing high-volume run lists.
- The first service tests use SQLite-backed migrations for speed. Shared dev
  Postgres validation happens through the GitHub Actions deployment path.
