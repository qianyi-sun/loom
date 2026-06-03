# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Added #157/#158/#160 Docker terminal sandbox lifecycle events. The
  worker-managed Docker terminal executor now records metadata-only
  `sandbox.container_started` and `sandbox.container_completed` events for
  each sandbox command through short repository transactions, and the browser
  event stream helper recognizes those typed events. Docker runs use a cidfile
  so completed events can include a container id when Docker provides one.
  Event metadata stays bounded to worker/task ids, sandbox command index,
  image, resource limits, status, timing, exit code, timeout flag, and
  changed-path count; it does not persist command text, stdout/stderr, host
  workspace paths, secrets, or payload bytes.
- Fixed #156 shared-dev scheduler race smoke repeatability. Synthetic race
  smoke teams and projects now use a run-scoped display name, so repeated
  deploys against the same durable Postgres database no longer trip the
  `teams.name` uniqueness constraint left by earlier smoke evidence.
- Added #157/#158 worker subprocess lifecycle events. `SubprocessRunWorker`
  now records metadata-only `worker.subprocess_started` and
  `worker.subprocess_completed` events around the child process boundary, and
  the browser event stream helper recognizes those typed events for live
  monitoring. Event metadata keeps only worker id, execution task id,
  child-entrypoint module, timeout, and return code; it does not persist child
  argv, local paths, secrets, or logs.
- Added #156 scheduler multi-instance race hardening and deploy validation.
  PostgreSQL dispatch now takes a transaction-scoped advisory lock before
  reading active capacity and locking queued candidates, so concurrent
  scheduler loops cannot over-dispatch different locked rows from stale
  capacity counts. Shared-dev deploy now runs
  `agentic_data_platform.scheduler.race_smoke` before API/frontend smokes to
  validate the multi-scheduler capacity boundary against the real Postgres
  stack, while canceling its synthetic runs after recording pre-cleanup evidence
  so it does not leave queued or active capacity behind.
- Fixed the #156 project fair-share scheduler dispatch query for PostgreSQL.
  Fair-share ranking now happens in a read-only candidate-id query, and row
  locking happens in a separate `FOR UPDATE SKIP LOCKED` query. This preserves
  the #214 project fair-share behavior while avoiding PostgreSQL's
  `FOR UPDATE is not allowed with window functions` failure that killed the
  shared-dev scheduler and left API smoke runs queued.
- Added #156 project fair-share scheduler candidate ordering. Dispatch now
  considers each project's oldest queued run before considering a second queued
  run from the same project, while preserving the existing global, backend,
  project, provider, model, agent, and benchmark capacity gates plus
  `scheduler.capacity_blocked` diagnostics.
- Moved run detail summary reads onto the #157 durable projection read path.
  `GET /runs/{run_id}`, `GET /runs/{run_id}/artifacts`, and
  `GET /runs/{run_id}/evaluation` now prefer clean `run_dashboard_projections`
  payloads for dashboard-safe status/progress/artifact/evaluator summaries,
  falling back to hydrated `RunRecord` projection only when the projection row
  is missing or dirty. The heavier detail trajectory preview remains bounded
  child-row data.
- Added dedicated #159 artifact chunk upload transaction repository APIs.
  `RunRepository.start_artifact_chunk_upload(...)`,
  `complete_artifact_chunk_upload(...)`, and
  `fail_artifact_chunk_upload(...)` now own the chunk upload lifecycle,
  preserve idempotent started writes, append metadata-only
  `artifact.upload_status_changed` events only for real state transitions, and
  allow started/failed chunk rows to omit object size/SHA-256 until a completed
  payload is available.
- Fixed a shared-dev scheduler Docker cleanup smoke race. The synthetic cleanup
  smoke now claims its own queued run by `run_id` before marking it stale, so
  pre-existing queued backlog cannot steal the claim and leave the smoke
  container outside scheduler recovery.
- Moved `GET /runs` onto the #157 durable projection read path where clean
  `run_dashboard_projections` rows are available. Run lists now return the
  stored dashboard-safe payload for clean projections and fall back to hydrated
  `RunRecord` projection only when a row is missing or dirty.
- Added a #157/#159 artifact upload transition event slice. Artifact chunk
  status changes and stale upload expiry now append metadata-only
  `artifact.upload_status_changed` events with previous/current upload status
  and safe chunk or artifact identifiers, so replay/SSE/bundle consumers can
  distinguish upload-state transitions from chunk materialization events.
- Added a #157/#160 projection recovery observability slice. Scheduler
  terminal projection repair now emits same-status `projection.refreshed`
  events with scheduler id, execution task id, refresh reason, previous
  projection state, and source event sequence metadata so projection-only
  recovery is durable through `/runs/{run_id}/events`, SSE replay, and artifact
  bundles.
- Added the first #157 typed evaluator event slice. Worker result persistence
  now appends metadata-only `evaluator.completed` or `evaluator.failed` events
  after `run.evaluating` and before the terminal lifecycle event, with safe
  evaluator id/mode/status/score/artifact-ref metadata and without embedding
  verbal feedback, metrics, judge prompts, or local file paths. Frontend smoke
  replay now requires the evaluator completion event.
- Added a #158/#160 parent-death Docker cleanup deploy smoke. The scheduler
  cleanup smoke now supports `--mode parent-death`, which starts a live labeled
  sandbox container from a helper parent process, terminates that parent, and
  requires scheduler recovery to remove the surviving container plus persist
  `sandbox.container_cleanup` evidence. Shared-dev deploy runs this smoke by
  default before API/frontend smokes.
- Added the first #158/#160 scheduler-driven Docker-owned resource cleanup
  slice. Docker terminal sandbox containers now carry stable
  platform/run/resource labels, scheduler recovery can remove labeled
  containers for stale recovered active runs and emit
  `sandbox.container_cleanup` evidence, and the worker CLI still exposes
  `--cleanup-run-containers` with an optional attempt filter for manual
  operator recovery. Shared-dev deploy validation now also runs a scheduler
  Docker cleanup smoke that creates a labeled container, recovers a synthetic
  stale active run, and verifies removal plus cleanup-event evidence before API
  and frontend smokes. A follow-up fix makes that smoke accept Docker's
  short-container-ID removal output when matching it against the full ID
  returned by `docker run -d`.
- Clarified the main README product direction for new contributors by removing
  local-reference wording and replacing MVP-facing status text with
  product-grade platform terminology.
- Added a main developer platform guide for repo admins and primary developers,
  covering the current architecture, API surface, runtime topology, progress,
  priority issues, and maintainer responsibilities.
- Added the first #157/#159 typed artifact/log event slice. Artifact chunk
  writes now append `artifact.chunk_recorded` or `log.chunk_recorded` lifecycle
  events with object metadata only, and stale upload expiry appends
  `artifact.upload_expired` alongside the existing recovery event.
- Added the first #159 persisted artifact chunk index slice.
  `artifact_chunks` now records ordered stdout/stderr/trajectory/artifact chunk
  metadata with run, attempt, artifact, chunk kind, sequence, object key, size,
  SHA-256, upload status, and optional upload error reason. Repository methods
  can idempotently record and list chunk indexes.
- Added a #159 run artifact chunk metadata API. `GET
  /runs/{run_id}/artifact-chunks` lists bounded, project-scoped chunk metadata
  by optional attempt, artifact, kind, sequence cursor, and limit so live
  monitors and operators can discover object-backed log/trajectory chunks
  without downloading payload bytes through Postgres.
- Added the first automatic #159 terminal log chunk writer. Worker result
  persistence now writes terminal stdout/stderr chunks to object storage and
  records their chunk metadata against the trajectory artifact for the current
  execution attempt.
- Added a #159 artifact chunk payload download API. `GET
  /runs/{run_id}/artifact-chunks/content` returns object-store payload bytes for
  a project-scoped completed chunk selected by artifact id, chunk kind, and
  sequence while rejecting incomplete upload states without exposing storage
  keys.
- Added a #160 terminal result mismatch recovery slice. Scheduler recovery now
  detects active runs whose latest attempt runner metadata already records a
  terminal child process result, marks the run failed with
  `recovery=terminal_result_mismatch`, refreshes the terminal dashboard
  projection, and reports `terminal_mismatch_run_ids` in scheduler recovery
  output before stale-heartbeat recovery can misclassify the failure.
- Split the #150 OpenHands adapter contract into separate OpenHands CLI and
  OpenHands SDK specs. The CLI path now passes a Harbor-compatible
  `openhands-ai` version through `--agent-kwarg version=1.6.0` so Harbor 0.9.0
  does not resolve to the incompatible latest `openhands-ai` package, while the
  SDK path keeps its existing runtime contract.
- Added #156 scheduler capacity-blocked diagnostics. Capacity-saturated queued
  runs now retain current `execution.scheduler.capacity_blocked` metadata,
  emit `scheduler.capacity_blocked` only when the blocker signature changes,
  surface blocked run details in scheduler one-shot output, and expose
  project-scoped blocked counts by dimension through `/ops/metrics`.
- Moved `/dashboard/progress` onto the #157 durable projection read path where
  clean `run_dashboard_projections` rows are available. Progress aggregation
  now uses terminal projection payloads for artifact, turn, evaluator, status,
  and score counts, while falling back to hydrated `RunRecord` values when a
  projection row is missing or dirty.
- Added the first #157/#160 durable dashboard projection refresh slice.
  Terminal worker results, terminal status transitions, and stale active
  recovery now upsert `run_dashboard_projections` rows with the current
  dashboard-safe payload and latest lifecycle `seq`; scheduler recovery also
  performs a bounded terminal projection refresh sweep for missing, dirty, or
  stale projection rows and logs projection-only recovery work from the
  long-running scheduler loop.
- Added #159/#160 stale artifact upload expiry recovery. Scheduler recovery can
  now mark stale `pending` or `started` artifact uploads as `expired`, record
  previous status / scheduler / expiry metadata on the artifact row, emit a
  same-status `run.recovered` event with `recovery=artifact_upload_expired`,
  and report expired artifact ids in scheduler recovery output.
- Added #159/#160 artifact bundle upload-state reporting. Bundle generation now
  treats artifact metadata with non-`completed` `upload_status` as unavailable
  DB truth, skips object-store reads for those artifacts, and records
  `upload_status` plus `upload_error_reason` in `manifest.json`
  `artifact_content_errors`.
- Added #156 expanded scheduler capacity gates. `RunScheduler` and
  `RunRepository.dispatch_queued_runs(...)` now support env-configured
  provider, model, agent, and benchmark active-run caps in addition to global,
  backend, and project limits, and `run.dispatched` metadata records those
  capacity keys for operator diagnostics.
- Added #158 subprocess child log-tail diagnostics. Worker subprocess
  nonzero, timeout, and incomplete-result failures now persist bounded,
  redacted child stdout/stderr tails plus return-code/stage metadata under the
  run failure summary, while keeping full logs out of Postgres.
- Added the first #158 duplicate-delivery guard. Subprocess children now acquire
  an attempt-level execution lock before entering benchmark execution; duplicate
  deliveries with the same `execution_task_id` return the current run snapshot
  without running Harbor/Docker/model work twice. The lock is stored in
  `run_attempts.metadata.execution.runner` and survives heartbeat metadata
  updates.
- Hardened the generated `harbor-cli-smoke` task environment by pre-creating
  writable `/logs/verifier` and `/logs/artifacts` directories, preventing
  Harbor verifier stdout redirection from failing before `tests/test.sh` can
  write `reward.txt`.
- Updated `scripts/setup-dev-env.sh` to populate a blank
  `SANDBOX_HOST_WORKSPACE_ROOT` in `.env.local` with an absolute repo-local
  runtime path while preserving existing non-empty values and provider secrets.
  This keeps Docker-socket-backed Harbor/worker child containers pointed at a
  host-visible sandbox workspace.
- Extended `frontend-smoke` deployment validation to read
  `/runs/{run_id}/events` and one-shot `/runs/{run_id}/stream` after a
  terminal Harbor run and require ordered durable lifecycle replay through
  create, dispatch, worker claim, run, evaluation, and success events with
  execution-task metadata before telemetry checks pass. The artifact bundle
  check now also requires `lifecycle-events.json` to match the same replayed
  events and reports lifecycle/SSE event counts in the smoke JSON output.
- Added a shared-dev deployment recovery preflight before API/frontend smokes.
  `scripts/deploy-dev.sh` now runs one scheduler `--recover-once` pass with
  `DEPLOY_STALE_ACTIVE_RECOVERY_SECONDS` so orphaned active runs from a prior
  worker restart are marked with durable `run.recovered` evidence before they
  can hold global scheduler capacity and block deployment validation.
- Added the first #158 stale execution-task guard. Workers now carry the current
  `execution_task_id` into subprocess children, validate it before child
  execution, attach it to heartbeat/result persistence, and ignore stale child
  completions when a retry has already created a newer attempt.
- Updated shared-dev deployment to run API, scheduler, and worker as the
  long-running service set. `scripts/deploy-dev.sh` now stops and starts
  `api scheduler worker` for both local and remote deployment paths so
  API-created runs use the explicit scheduler dispatch topology after
  deployment.
- Added the first #156/#158 execution-attempt metadata contract slice. Scheduler
  dispatch now records a versioned scheduler lease block on the latest
  `run_attempts.metadata`, worker claim/heartbeat/terminal result persistence
  records a versioned runner process block, and lifecycle event metadata now
  includes the current `execution_task_id` for stale-task rejection and
  duplicate-delivery hardening.
- Added the first shared execution event contract slice for #157/#160. The
  backend now centralizes current run lifecycle event names and Phase 1 recovery
  reason codes in `agentic_data_platform.domain.execution_events`, and
  repository, worker, subprocess-failure, recovery, and run-audit paths use the
  shared contract while preserving existing string values in Postgres and API
  responses.
- Added the first #159 artifact metadata contract slice. Artifact persistence
  now centralizes object content types, upload states, and chunk metadata
  vocabulary in `agentic_data_platform.domain.artifact_metadata`; object-store
  writes annotate completed artifacts with schema version, upload status,
  storage key, object byte size, and object SHA-256 while preserving existing
  artifact refs and bundle behavior.
- Added local browser-control tooling for owner-visible frontend validation.
  `scripts/setup-browser-tools.sh` installs the `browser` development extra and
  Chromium for Playwright, while
  `agentic_data_platform.service.frontend_browser_smoke` drives a real browser
  through login and catalog readiness checks without adding browser runtime
  dependencies to the API or worker services.
- Added #160 stale-run recovery slices for dispatched and active worker states.
  The scheduler can now requeue stale `dispatched` runs back to `queued`, fail
  active runs with expired worker heartbeats, record durable `run.recovered`
  lifecycle events with scheduler metadata, expose timeout and batch-size
  settings, and run recovery before dispatch in the long-running scheduler
  loop. Workers now persist attempt-level heartbeat metadata while executing.
- Added the first #158/#160 subprocess cancel-cleanup slice. Managed subprocess
  workers can now monitor active run cancellation, terminate the child process,
  and return the existing `canceled` terminal state instead of waiting for the
  child process to exit naturally.
- Added the first #159 object-first log/artifact slice. Terminal stdout/stderr
  stored in Postgres trajectory rows are now bounded previews with truncation
  metadata, while full trajectory/log payloads remain available through
  object-store artifacts and artifact bundles continue to record missing object
  payloads in the manifest instead of failing the whole download.
- Added the first #156 scheduler/capacity-gate slice. Runs can now move through
  `queued -> dispatched -> provisioning`, `RunRepository.dispatch_queued_runs`
  gates dispatch by global, backend, and project active-run capacity,
  `RunScheduler` reads those limits from service settings, workers prefer
  dispatched runs while keeping the legacy queued-claim path for compatibility,
  and dashboard/ops/telemetry status summaries include the `dispatched` state.
- Added the first #157 durable run-event replay and SSE slice. Lifecycle
  events now expose monotonic `seq` watermarks, `GET /runs/{run_id}/events`
  supports `after_seq` replay, `GET /runs/{run_id}/stream` emits replayable SSE
  events from the same Postgres source, and the no-build frontend uses
  `EventSource` with polling fallback for run monitor refresh.
- Added the first #158 worker subprocess-isolation slice. `SubprocessRunWorker`
  can claim a DB-backed queued run, delegate execution to the short-lived
  `agentic_data_platform.worker.execution_child` process, and reload terminal
  state from Postgres; `WORKER_SUBPROCESS_ISOLATION_ENABLED` keeps the path
  opt-in while #154 scheduler/event/storage work continues.
- Added Harbor agent/model adaptation coverage for #150. Mainstream Harbor
  agents now expose model adapter metadata and required secret refs, the worker
  infers adapter env for agents such as `opencode` without relying on frontend
  one-off fields, `/models` returns basic model-family metadata,
  Codex/Responses-API provider dialect mismatches fail before Harbor launch,
  and `/harbor/agent-adaptation` reports ready/blocked launch preflight state
  without exposing raw provider secrets.
- Preserved Harbor diagnostics when verifier/result ingestion fails for #151.
  Worker failures caused by malformed or incomplete Harbor `jobs/` output now
  keep the Harbor runner report, raw `jobs/` archive, partial trajectory when
  available, and a redacted `harbor_ingestion_diagnostics` artifact in the run
  record and downloadable artifact bundle.
- Added catalog-backed Harbor launch support for #131, #142, and #144. The
  frontend can now select a Harbor agent and use the selected API model for
  catalog-backed Harbor runs, while preserving the deterministic `oracle` +
  `smoke/noop` no-key smoke path. The default Harbor catalog target is now
  registry-versioned `terminal-bench@2.0`, the worker maps selected model
  provider secrets into Harbor agent env only at execution time, and
  `agentic_data_platform.harbor.registry_sync` can list or sync Harbor registry
  datasets into the benchmark catalog with freshness metadata.
- Added `scripts/setup-dev-env.sh` and made `.env.local` the single Compose env
  file. The setup script creates `.env.local` from `.env.example` when missing,
  so real local model-provider base URLs, optional model allowlists, and API
  keys can be used for manual Harbor acceptance without committing secrets.
- Made provider model discovery the default frontend catalog path for #147.
  With `MODEL_PROVIDER_BASE_URL` and `MODEL_PROVIDER_API_KEY` configured,
  `/models` calls the provider `/models` endpoint first; `MODEL_PROVIDER_MODELS`
  is now an optional allowlist or static fallback, and the frontend shows
  discovery/fallback/error status without exposing secrets.
- Added the Harbor E2E manual test runbook for #139. The owner checklist now
  covers frontend login, model/harness/benchmark/task selection, Harbor
  local-Docker launch, live queue/CPU/RAM/sandbox telemetry, Harbor verifier
  feedback, and artifact bundle inspection. Browser task upload remains #143
  as an admin/custom benchmark onboarding path rather than the ordinary
  evaluation path.
- Tightened shared dev exposure and Harbor upload limits. Compose service ports
  now default to loopback host bindings, Harbor task uploads enforce configured
  archive byte, file count, and uncompressed materialization limits, and the
  deployment/docs now preserve the reverse-proxy-only shared-host assumption.
- Added runtime dependency constraints and CI Docker runtime coverage. CI now
  installs with `constraints/dev-runtime.txt`, builds `Dockerfile.dev`, and
  validates the development Compose configuration; the dev image installs
  Harbor 0.9.0 and related runtime dependencies through the same constraints,
  including the `httpx2` TestClient runtime expected by current Starlette.
- Added the first Harbor agent provider for #63. The platform now exposes
  `AgentProvider` / `HarborAgentProvider`, authenticated `GET /agents`,
  Harbor built-in agent metadata, custom `--agent-import-path` validation, safe
  required-secret references, supported harness/sandbox metadata, and
  Harbor-vs-native runner distinctions for launch/dashboard consumers.
- Added the first Harbor benchmark catalog provider for #62. The provider
  exposes a versioned `HarborTerminalBench` dataset catalog, maps Harbor
  uploaded task archives into checksum-versioned catalog entries, and keeps
  source type, environment, verifier, artifact convention, and `harbor_run`
  launch metadata in the shared benchmark read model.
- Documented the Harbor native integration design for #102, including the
  recommended CLI fallback/native backend split, Harbor capability probe,
  programmatic `JobConfig`/trial-hook candidate path, file-based `jobs/`
  ingestion boundary, provider responsibilities for #62/#63, progress events,
  failure categories, and verification sequence. Added the first code boundary
  by naming the current CLI path `HarborCliRunnerBackend`, retaining
  `HarborRunnerBackend` as a compatibility alias, accepting
  `metadata.harbor_run.backend`, recording `backend: cli` in runner reports, and
  exposing `probe_harbor_native_capabilities()`.
- Added the first Harbor-compatible task upload path for #67. The API now
  accepts zipped Harbor task directories at `POST /harbor/task-uploads`,
  validates `instruction.md`, `task.toml`, Docker environment files, verifier
  tests, declared artifacts, and reward-file expectations, persists the archive
  in the configured object store, records an audit event with task metadata,
  and returns `metadata.harbor_run.task_archive_storage_key` for launching the
  uploaded task through the existing Harbor worker/backend path.
- Documented the user runner and pipeline integration contract for #21,
  including the task manifest, runner result schema, artifact path rules,
  lifecycle mapping, registration metadata, local validation expectations, and
  how #67 Harbor-compatible task uploads reuse the same run lifecycle.
- Added queued worker execution for original SkillFlow/SkillLearnBench wrapper
  runs for #125. `DockerTerminalWorkerExecutor` now recognizes original wrapper
  runner contracts with `metadata.wrapper_run`, writes the wrapper task
  manifest from persisted run data, invokes the configured runner entrypoint,
  captures a platform trajectory turn and final workspace snapshot, persists
  wrapper result/artifact files, and converts wrapper evaluator output into the
  existing run evaluator/dashboard model.
- Mapped platform model provider refs into original upstream SkillFlow and
  SkillLearnBench runner shapes for #123. SkillFlow now writes a suite-native
  `artifacts/skillflow-job-config.json`, SkillLearnBench receives upstream
  `--agent`/`--model` arguments, provider secrets are copied from safe
  `env:` refs into runner-specific API-key variables only at subprocess time,
  and stdout/stderr plus copied text artifacts are redacted before persistence.
- Added the first real OpenAI-compatible terminal-agent provider path for #116.
  Docker terminal workers can now resolve configured model refs through the dev
  provider registry, call `/chat/completions`, parse JSON terminal actions,
  preserve the scripted no-cost smoke fallback, and normalize provider failures
  into run-visible errors without exposing API keys.
- Pinned the SkillFlow task dataset for #115. The checked-in SkillFlow catalog
  now records Hugging Face dataset commit
  `ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc`, the real-upstream smoke defaults
  to that revision, and `agentic_data_platform.benchmarks.upstream_sources`
  writes a separate `adp-skillflow-task-assets-lock.json` for hydrated
  task-family assets.
- Added an opt-in shared dev real-upstream wrapper smoke path for #114. The
  new `benchmark-real-upstream-smoke` Compose service materializes the pinned
  SkillFlow runner, hydrates the selected Hugging Face task-family subset, and
  invokes the existing executable wrapper smoke on a Docker-ready host when the
  deploy workflow is manually dispatched with the real-upstream smoke flag.
- Added a platform-maintained SkillFlow upstream source patch for #117. The
  upstream source lock records the applied patch id and SHA-256, and the patch
  updates the pinned SkillFlow runner to Harbor's `Task.is_valid_dir` and
  `await Job.create(...)` APIs. Real SkillFlow smoke now reaches Harbor job
  execution and records Docker-environment failures as upstream artifacts.
- Expanded SkillFlow JSON report normalization for real Harbor job `result.json`
  stats, including completed/errored trial counts, evaluator error count, and
  mean score.
- Corrected the SkillFlow executable wrapper command for #22 so single-family
  runs pass `--dataset-path test_tasks/<task_family>` to the pinned upstream
  runner. Real upstream smoke exposed broader SkillFlow/Harbor API drift, which
  is handled through the tracked source patch above instead of temporary runtime
  wrapper shims.
- Added suite-specific upstream evaluator report normalization for #22.
  SkillFlow JSON reports and SkillLearnBench `report.csv` outputs are summarized
  into `artifacts/evaluator-report.json`, normalized wrapper `metrics`, and an
  `evaluator_report` artifact ref.
- Added a reusable SkillFlow/SkillLearnBench wrapper smoke entrypoint for #22.
  `python -m agentic_data_platform.benchmark_wrappers.smoke` can run fixture
  dry-runs in CI or executable local-upstream checks when an upstream root is
  available.
- Added benchmark wrapper upstream config synthesis for #22. Original
  SkillFlow/SkillLearnBench wrapper runs now write a redacted
  `artifacts/upstream-config.json` runner config artifact from platform model
  metadata, and SkillFlow uses the generated config path instead of the
  committed upstream baseline config.
- Added first upstream output artifact normalization for #22. Executable
  wrapper runs now copy generated upstream output files into
  `artifacts/upstream-output/` and expose them as `upstream_output` artifacts.
- Added the pilot group native workflow architecture target for #103,
  including trial/retry/refinement attempts, verifier rewards, LLM-judge
  feedback, final workspaces, artifact bundles, and future skill object hooks.
- Added multi-evaluator run result semantics for Harbor verifier and platform
  LLM judge outputs while preserving the latest evaluator summary.
- Added a fixture-backed Harbor `jobs/` result ingestor that maps trajectories,
  verifier rewards, collected artifact manifests, and raw jobs archives into
  shared platform records.
- Added the first Harbor local Docker runner backend slice, including injectable
  `harbor run` execution, runner report artifacts, and worker attachment of
  ingested Harbor verifier results.
- Installed Harbor 0.9.0 in the dev image, aligned runner commands with the
  current `harbor run` CLI, and added a deploy-time real Harbor CLI local
  Docker smoke check.
- Fixed the generated Harbor CLI smoke task metadata so Harbor 0.9.0 recognizes
  it as a valid task instead of treating the path as an empty dataset.
- Added Harbor ingestor support for verifier rewards persisted in trial
  `result.json` when standalone reward files are absent.
- Installed the Docker Compose CLI plugin in the dev image so Harbor local
  Docker jobs can call `docker compose` from inside the worker container.
- Updated the frontend launch path and `frontend-smoke` to submit real
  `metadata.harbor_run`, materialize the generated Harbor smoke task in the
  worker, ingest Harbor verifier output, and validate artifact bundle download.

## 0.0.0 - 2026-05-27

- Initialized private repository for agentic data generation and evaluation platform planning.
- Added project brief, GitHub templates, CI placeholder, and deployment placeholder.
