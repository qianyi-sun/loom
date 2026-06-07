# Changelog

All notable changes to Loom will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
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
