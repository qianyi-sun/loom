# Changelog

All notable changes to Loom will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
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
