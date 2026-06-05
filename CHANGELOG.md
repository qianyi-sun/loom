# Changelog

All notable changes to Loom will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
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
  - Caveat: integration test (`tests/integration/test_alembic_migrations.py`)
    requires Docker daemon access. Current user is not in the `docker`
    group; one-time setup needed: `sudo usermod -aG docker hongjian`
    + re-login before the test can run.
- **Plan 0 — repo preparation (2026-06-05).** Archived pre-Loom code,
  tests, docs, configs, and CI workflows to `legacy/`; rewrote
  `pyproject.toml` to Loom minimum deps; added Loom-facing `README.md`
  and `NOTICE.md`; replaced 3 CI workflows with a stub during buildout.
  Eight sequential commits, all reversible (git mv to legacy/, never rm).
  Ready for Plan 1 implementation.

## [Pre-Loom]

For changes prior to the Loom rebuild, see `legacy/CHANGELOG-pre-loom.md`.
