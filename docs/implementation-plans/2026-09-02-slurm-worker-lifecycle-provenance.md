# Slurm Worker Lifecycle Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Link elastic workers to their exact active Slurm jobs at registration and prevent GB10 node lifecycle from classifying or draining those Slurm-owned workers.

**Architecture:** Carry the controller's existing four-field Slurm provenance group through worker registration, validate it against a locked `SlurmWorkerJob`, and assign the existing `worker_id` foreign key in the worker-insert transaction. Centralize an active exact-link classifier and use it at all hostname-based GB10 lifecycle boundaries while leaving the release gate fail-closed.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy async ORM, PostgreSQL, httpx, pytest, Ruff, mypy.

**Spec:** `docs/architecture/2026-09-02-slurm-worker-lifecycle-provenance.md`

## Global Constraints

- Never classify ownership by hostname, pool, freshness, backend, or name alone.
- Provenance is exactly the all-or-none group `sandbox_identity`, `candidate_sha`, `slurm_job_id`, and `compose_project`.
- Registration must match an active job on inferred cluster, environment/sandbox, pool, hostname, concurrency, candidate, job ID, and compose project.
- Worker insert and job linkage are one row-locked database transaction; no migration or backfill is allowed.
- Slurm observations cannot create or replace ownership.
- Only active internally consistent links are excluded; terminal, unlinked, incomplete, wrong-host, wrong-pool, wrong-concurrency, or otherwise inconsistent workers fail closed.
- Do not weaken Slurm containment, builder isolation, or the GB10 release gate.
- Do not resume or rewrite protected rollout request `req-790a37630e664e7b`; activation uses a fresh request after merge.
- Work only in `.worktrees/slurm-worker-lifecycle-provenance`; do not push `docs/superpowers`.

---

### Task 1: Carry and transactionally bind Slurm registration provenance

**Files:**

- Modify: `src/loom_worker/control_plane_client.py`
- Modify: `src/loom_worker/main_loop.py`
- Modify: `src/loom_control_plane/routes/workers.py`
- Modify: `src/loom_control_plane/slurm_worker_jobs.py`
- Modify: `tests/unit/test_worker_claim_loop.py`
- Modify: `tests/integration/test_control_plane_client.py`
- Modify: `tests/integration/test_slurm_worker_jobs.py`

**Interfaces:**

- `HttpControlPlaneClient.register(..., sandbox_identity: str | None = None, candidate_sha: str | None = None, slurm_job_id: str | None = None, compose_project: str | None = None)` emits the group only when complete and rejects partial groups.
- `_register_worker_with_retry()` passes the four existing settings only when `settings.slurm_job_id` is nonempty and fails locally if any companion is empty.
- `SlurmWorkerRegistrationProvenance` represents the normalized exact group.
- `parse_slurm_worker_registration_provenance(payload: Mapping[str, Any]) -> SlurmWorkerRegistrationProvenance | None` applies the server-side all-or-none and format checks.
- `lock_slurm_worker_job_for_registration(session, *, provenance, hostname, pool_name, max_concurrent) -> SlurmWorkerJob` returns one unlinked active exact job under `FOR UPDATE` or raises `SlurmWorkerRegistrationError`.

- [ ] **Step 1: Write failing worker propagation tests**

  Extend `_RegistrationSettings` with a complete Slurm group and add a test
  whose fake client requires these exact registration kwargs:

  ```python
  {
      "sandbox_identity": "production",
      "candidate_sha": "a" * 40,
      "slurm_job_id": "40740",
      "compose_project": "loom-production-aaaaaaaaaaaa-40740",
  }
  ```

  Add a separate partial-settings test that expects `ValueError` before the
  fake client's `register()` method is called. Preserve the existing legacy
  test proving non-Slurm settings emit no new kwargs.

- [ ] **Step 2: Run the worker propagation tests and verify RED**

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_worker_claim_loop.py -k 'register_worker_with_retry'
  ```

  Expected: the complete-group test fails because the kwargs are absent and
  the partial-group test fails because registration is attempted.

- [ ] **Step 3: Implement minimal worker and HTTP-client propagation**

  In `_register_worker_with_retry()`, strip `slurm_job_id`; if present, strip
  and require all four fields before updating `register_kwargs`. Extend the
  client signature and independently enforce `any == all` for the four
  optional values before updating the JSON payload. Do not send deployment
  labels from a non-Slurm worker.

- [ ] **Step 4: Run propagation tests GREEN**

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_worker_claim_loop.py -k 'register_worker_with_retry'
  ```

- [ ] **Step 5: Write failing exact-link integration tests**

  In `test_control_plane_client.py`, seed one active `SlurmWorkerJob` with
  cluster `gb10`, environment/sandbox `production`, pool `gb10`, nodelist
  `trt-gb10-1`, concurrency `6`, candidate `a * 40`, job ID `40740`, and
  compose project `loom-production-aaaaaaaaaaaa-40740`. Register through the
  real ASGI client and assert the resulting job references the returned worker.

  Add literal cases for partial provenance and mismatches in candidate,
  hostname, pool, concurrency, terminal state, and an already-linked job.
  Each case must assert HTTP 400/409 and that no additional `Worker` row was
  committed. Add a Slurm reconciliation test proving an observation cannot
  populate an empty `worker_id` or replace an existing different link.

- [ ] **Step 6: Run exact-link tests and verify RED**

  ```bash
  uv run --no-sync pytest -q \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py \
    -k 'slurm or provenance or (worker and observation)'
  ```

  Expected: exact registration leaves `worker_id` null, invalid groups are
  accepted, and reconcile still establishes observer-supplied links.

- [ ] **Step 7: Implement the row-locked registration authority**

  Add the provenance dataclass/parser and error type in
  `slurm_worker_jobs.py`. Select the exact `SlurmWorkerJob` with:

  ```python
  select(SlurmWorkerJob).where(
      SlurmWorkerJob.slurm_cluster_id == slurm_cluster_for_pool(pool_name),
      SlurmWorkerJob.environment == provenance.sandbox_identity,
      SlurmWorkerJob.sandbox_identity == provenance.sandbox_identity,
      SlurmWorkerJob.pool_name == pool_name,
      SlurmWorkerJob.nodelist == hostname,
      SlurmWorkerJob.requested_concurrency == max_concurrent,
      SlurmWorkerJob.candidate_sha == provenance.candidate_sha,
      SlurmWorkerJob.job_id == provenance.slurm_job_id,
      SlurmWorkerJob.compose_project == provenance.compose_project,
      SlurmWorkerJob.state.in_(ACTIVE_STATES),
  ).with_for_update()
  ```

  Reject no match or non-null `worker_id` with the same non-secret conflict.
  In `register_worker()`, parse before opening the write transaction, lock the
  job inside that transaction, insert and flush the worker, set
  `job.worker_id = worker_id`, then commit once. Change reconciliation so an
  observation only confirms the same existing ID; it never assigns or
  overwrites the field.

- [ ] **Step 8: Run Task 1 tests, Ruff, and mypy GREEN**

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_worker_claim_loop.py \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py
  uv run --no-sync ruff check \
    src/loom_worker/control_plane_client.py \
    src/loom_worker/main_loop.py \
    src/loom_control_plane/routes/workers.py \
    src/loom_control_plane/slurm_worker_jobs.py \
    tests/unit/test_worker_claim_loop.py \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py
  uv run --no-sync mypy --strict \
    src/loom_worker/control_plane_client.py \
    src/loom_worker/main_loop.py \
    src/loom_control_plane/routes/workers.py \
    src/loom_control_plane/slurm_worker_jobs.py
  ```

- [ ] **Step 9: Commit Task 1**

  ```bash
  git add src/loom_worker/control_plane_client.py \
    src/loom_worker/main_loop.py \
    src/loom_control_plane/routes/workers.py \
    src/loom_control_plane/slurm_worker_jobs.py \
    tests/unit/test_worker_claim_loop.py \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py
  git commit -m "fix(workers): bind registrations to Slurm jobs"
  ```

### Task 2: Exclude only proven active Slurm ownership from GB10 lifecycle

**Files:**

- Modify: `src/loom_control_plane/slurm_worker_jobs.py`
- Modify: `src/loom_control_plane/gb10_worker_lifecycle.py`
- Modify: `tests/integration/test_gb10_worker_lifecycle.py`

**Interfaces:**

- `fetch_active_exact_slurm_worker_ids(session, *, pool_names: set[str]) -> set[UUID]` returns only active internally consistent job/worker links.
- `_reconcile_worker_registry_for_host_intents()` and `_reconcile_worker_drain_state_for_host_intent()` exclude those IDs before changing registry drain state.
- `_fetch_matching_workers()` excludes those IDs before node selection and unlinked inventory construction.
- `node_status_to_dict()` does not expose a reported `worker_id` that belongs to the excluded set.

- [ ] **Step 1: Add test helpers for exact linked Slurm jobs**

  Extend the lifecycle cleanup fixture to delete `SlurmWorkerJob` before
  `Worker`. Add `_seed_slurm_worker_job()` that links an existing worker with
  literal production metadata matching the worker's host, pool, and
  concurrency. The helper accepts state and individual mismatch overrides so
  each negative case is independently visible.

- [ ] **Step 2: Write failing lifecycle ownership tests**

  Add tests proving an active exact linked Slurm worker:

  - is not drained by a desired `stopped` or `draining` host intent;
  - is not drained by a stopped node report;
  - is not selected as a node worker and is absent from `unlinked_workers`.

  Add table-driven tests proving terminal state, missing candidate, wrong
  cluster, environment/sandbox inconsistency, wrong hostname, wrong pool, and
  wrong concurrency remove the exemption. At least one terminal linked fresh
  worker must appear in unlinked inventory and become drained under stopped
  intent.

- [ ] **Step 3: Run lifecycle ownership tests and verify RED**

  ```bash
  uv run --no-sync pytest -q \
    tests/integration/test_gb10_worker_lifecycle.py -k 'slurm'
  ```

  Expected: hostname-based selection/draining includes the active exact Slurm
  worker because no ownership classifier is applied.

- [ ] **Step 4: Implement the exact-link classifier**

  Join `SlurmWorkerJob` to `Worker` through `worker_id`, restrict state to
  `ACTIVE_STATES` and requested pools, then retain only rows satisfying:

  ```python
  job.job_id
  and job.sandbox_identity
  and job.candidate_sha
  and job.compose_project
  and job.environment == job.sandbox_identity
  and job.slurm_cluster_id == slurm_cluster_for_pool(job.pool_name)
  and worker.pool_name == job.pool_name
  and worker.hostname == job.nodelist
  and worker.max_concurrent == job.requested_concurrency
  ```

  Return worker UUIDs only. Keep this helper beside the registry/link
  authority so lifecycle does not duplicate the trust rules.

- [ ] **Step 5: Apply the classifier at every lifecycle boundary**

  Fetch excluded IDs for the relevant pools before both host-intent queries
  and add `Worker.id.not_in(excluded_ids)` when nonempty. In status fetching,
  remove excluded IDs before constructing `worker_by_id`, `workers_by_node`,
  or `worker_rows`; suppress a node report's fallback ID when it is excluded.
  Do not change freshness, ranking, or release mismatch logic.

- [ ] **Step 6: Run lifecycle tests, Ruff, and mypy GREEN**

  ```bash
  uv run --no-sync pytest -q \
    tests/integration/test_gb10_worker_lifecycle.py
  uv run --no-sync ruff check \
    src/loom_control_plane/slurm_worker_jobs.py \
    src/loom_control_plane/gb10_worker_lifecycle.py \
    tests/integration/test_gb10_worker_lifecycle.py
  uv run --no-sync mypy --strict \
    src/loom_control_plane/slurm_worker_jobs.py \
    src/loom_control_plane/gb10_worker_lifecycle.py
  ```

- [ ] **Step 7: Commit Task 2**

  ```bash
  git add src/loom_control_plane/slurm_worker_jobs.py \
    src/loom_control_plane/gb10_worker_lifecycle.py \
    tests/integration/test_gb10_worker_lifecycle.py
  git commit -m "fix(gb10): separate Slurm worker ownership"
  ```

### Task 3: Preserve fail-closed release behavior and deliver the change

**Files:**

- Modify only if a regression gap is found: `tests/loom_cli/test_gb10_release_gate.py`
- Include: `docs/architecture/2026-09-02-slurm-worker-lifecycle-provenance.md`
- Include: `docs/implementation-plans/2026-09-02-slurm-worker-lifecycle-provenance.md`
- Modify Task 1-2 files only for validated review findings.

**Interfaces:**

- Existing `gb10_release_target_mismatches()` remains fail-closed for every
  worker lifecycle reports as fresh node or unlinked drift.
- The branch produces one reviewed PR to `dev`, followed by a fresh protected
  rollout request bound to the merged SHA.

- [ ] **Step 1: Re-run the release-gate rogue-worker regressions**

  ```bash
  uv run --no-sync pytest -q \
    tests/loom_cli/test_gb10_release_gate.py
  ```

  Confirm the suite still rejects a fresh worker on a stopped host and a fresh
  unlinked duplicate. If either behavior lacks a direct literal test, add that
  test first, run it against an intentionally weakened local condition to
  prove RED, restore the condition, and run GREEN. Do not add a hostname or
  backend exemption to the release gate.

- [ ] **Step 2: Run the complete affected verification**

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_worker_claim_loop.py \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py \
    tests/integration/test_gb10_worker_lifecycle.py \
    tests/loom_cli/test_gb10_release_gate.py
  uv run --no-sync ruff check \
    src/loom_worker/control_plane_client.py \
    src/loom_worker/main_loop.py \
    src/loom_control_plane/routes/workers.py \
    src/loom_control_plane/slurm_worker_jobs.py \
    src/loom_control_plane/gb10_worker_lifecycle.py \
    tests/unit/test_worker_claim_loop.py \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py \
    tests/integration/test_gb10_worker_lifecycle.py \
    tests/loom_cli/test_gb10_release_gate.py
  uv run --no-sync mypy --strict \
    src/loom_worker/control_plane_client.py \
    src/loom_worker/main_loop.py \
    src/loom_control_plane/routes/workers.py \
    src/loom_control_plane/slurm_worker_jobs.py \
    src/loom_control_plane/gb10_worker_lifecycle.py
  git diff --check
  ```

  Then run the repository's changed-path CI planner and every selected local
  lane available in this worktree. Run the broader non-Docker suite if the
  planner or review shows a shared worker-registration dependency.

- [ ] **Step 3: Perform iterative review until clean**

  Review the complete base-to-head diff for partial-provenance acceptance,
  link races, observer overwrite, environment/cluster confusion, terminal-job
  exemptions, missing lifecycle boundaries, accidental release weakening,
  and secret-bearing diagnostics. Fix every Critical or Important issue with
  a failing regression first, rerun its focused suite, and repeat whole-branch
  review until no blocking finding remains.

- [ ] **Step 4: Commit documentation and final review fixes**

  ```bash
  git add docs/architecture/2026-09-02-slurm-worker-lifecycle-provenance.md \
    docs/implementation-plans/2026-09-02-slurm-worker-lifecycle-provenance.md \
    src/loom_worker/control_plane_client.py \
    src/loom_worker/main_loop.py \
    src/loom_control_plane/routes/workers.py \
    src/loom_control_plane/slurm_worker_jobs.py \
    src/loom_control_plane/gb10_worker_lifecycle.py \
    tests/unit/test_worker_claim_loop.py \
    tests/integration/test_control_plane_client.py \
    tests/integration/test_slurm_worker_jobs.py \
    tests/integration/test_gb10_worker_lifecycle.py \
    tests/loom_cli/test_gb10_release_gate.py
  git commit -m "docs(workers): specify Slurm lifecycle provenance"
  ```

  Omit unchanged paths from the actual commit. Do not add anything below
  `docs/superpowers`.

- [ ] **Step 5: Push, open PR, pass CI, and merge**

  Fetch current `origin/dev`; rebase if it moved and rerun all selected
  verification. Push `fix/slurm-worker-lifecycle-provenance`, open a non-draft
  PR to `dev`, monitor checks for the exact current head, address every valid
  review finding test-first, and merge only after required CI succeeds without
  administrative bypass.

- [ ] **Step 6: Activate with fresh protected authority**

  Fetch the merged `dev` SHA, install/upgrade the supported protected rollout
  authority to that exact candidate, and submit a new rollout request. Do not
  alter or resume epoch 239. Require final convergence to report no desired or
  runtime drift and no rogue fresh workers while the Slurm job registry still
  reports legitimate elastic capacity separately.

- [ ] **Step 7: Verify the original operational goal**

  Through supported control-plane and Slurm interfaces, require both GB10 and
  OLDLAB builder supervisors healthy, dispatch queued native builder jobs, and
  verify ready immutable task-image materializations for `arm64` and `x86_64`.
  Resolve the exact Devansh source task from its authoritative batch, submit a
  fresh replay with the original provider/model/agent, and require terminal
  success, `llm_calls_count > 0`, `no_call == false`, numeric reward, nonempty
  verifier rewards without verifier error, valid ATIF 1.7, and parseable
  trajectory JSONL.
