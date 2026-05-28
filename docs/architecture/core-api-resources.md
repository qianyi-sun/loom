# Core API Resources

Last updated: 2026-05-28

Related trackers:

- Backend epic: https://github.com/carinrc/agentic-data-platform/issues/49
- Core API resources: https://github.com/carinrc/agentic-data-platform/issues/52
- Postgres persistence foundation: https://github.com/carinrc/agentic-data-platform/issues/51

## Goal

Issue #52 exposes the first Postgres-backed resource reads for PM dashboards,
researcher inspection, and future frontend development. The API is still an
internal control-plane surface, but it now has stable routes for projects,
benchmark tasks, run summaries, artifacts, and evaluator feedback.

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
| `GET /runs` | List dashboard-ready run summaries | `RunRepository.list_runs()` + `RunDashboardProjection` |
| `GET /runs/{run_id}` | Inspect one dashboard-ready run | `RunRepository.get_run()` + `RunDashboardProjection` |
| `GET /runs/{run_id}/artifacts` | List sanitized artifact references for a run | `RunDashboardProjection.artifacts` |
| `GET /runs/{run_id}/evaluation` | Inspect latest evaluator summary for a run | `RunDashboardProjection.evaluator` |

`benchmark_version` is a query parameter for benchmark detail, task-family, and
task routes because current versions can contain slashes, such as
`hf:zhang-ziao/SkillFlow-Task@...`.

## Response Shape Principles

- Success responses include `request_id` when request middleware attaches one.
- Missing resources map to `404`; unconfigured database access maps to `503`.
- Project and team responses mirror small repository read models.
- Benchmark and task responses mirror `BenchmarkFixtureCatalog` and
  `BenchmarkFixtureInstance`.
- Run responses reuse `RunDashboardProjection.to_dict()` so dashboards and API
  consumers share the same visible status/progress/evaluator payload.
- Artifact responses do not expose local `file://` URIs or absolute host paths.
  The projection keeps stable artifact ids, media types, sizes, and
  object-store-safe `storage_key` values, and strips query strings from external
  URLs. Absolute paths, `file://` values, traversal keys, drive-letter paths,
  and query/fragment-bearing keys are suppressed.
- OpenAPI examples are registered for the core list/detail routes so frontend
  and integration consumers can inspect expected payloads through
  `/openapi.json`.

## Current Limits

- The API is read-heavy. Run submission, cancellation, retries, queueing, and
  worker orchestration are tracked separately under backend follow-up issues.
- `RunRepository` still hydrates the latest snapshot into one `RunRecord`; the
  public API does not expose multi-attempt history yet.
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
