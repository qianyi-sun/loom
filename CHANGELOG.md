# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

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
