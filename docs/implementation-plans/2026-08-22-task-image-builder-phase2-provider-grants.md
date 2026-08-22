# Task-image builder Phase 2 provider/grants implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the first inert Phase 2 increment: a disabled rootless Slurm provider contract, one-invocation-per-grant recovery journal, and attempt/lease-scoped append-only publication evidence.

**Architecture:** Keep the proven exclusive Phase 1 builder untouched. Add a separate strict rootless policy and pure held-job renderer, persist grants and transition events before any external submission, reconcile complete Slurm inventories by exact immutable request identity, and persist every pushed digest under the materialization attempt that produced it. No production composition constructs the provider from the disabled policy in this increment.

**Tech Stack:** Python 3.11, Pydantic v2, SQLAlchemy 2 async ORM, PostgreSQL/Alembic, pytest, RFC 8785 canonical JSON, Slurm 23.11 request grammar.

**Spec:** `archive/docs/architecture/2026-08-18-dynamic-task-image-builder-design.md`

## Global constraints

- Work only in `/home/hongjian/loom/.worktrees/task-image-builder-phase2-rootless` on `feat/task-image-builder-rootless-grants`.
- Do not modify or push any path under `docs/superpowers/**`.
- Preserve the active exclusive `task_image_builder_policies` and their rollback capacity byte-for-byte.
- Rootless policy is disabled and retains non-empty activation blockers.
- Render no `--exclusive`, `--reservation`, `--nodelist`, Docker socket, bearer token, registry credential, or broad environment export.
- Every rendered job is held, non-requeueing, native-cluster constrained, and carries `loom-task-builder-v1:grant=<uuid>`.
- A grant can move from `issued` to `submitting` only once; ambiguous submission is reconciled, never reinvoked.
- Publication evidence is append-only and distinct across attempt/lease epochs even when the OCI digest is identical.
- Use test-first red/green/refactor cycles and commit each independently reviewable task.

---

### Task 1: Strict inert provider policy and held Slurm renderer

**Files:**

- Create: `src/loom_control_plane/task_image_build_environment.py`
- Create: `tests/unit/test_task_image_build_environment.py`

**Interfaces:**

- Produces: `RootlessBuildResourceRequestV1`, `SlurmBuildEnvironmentPolicyV1`, `SlurmBuildGrantV1`, `SlurmBuildJobObservationV1`, `SlurmBuildInventoryV1`, `BuildEnvironmentProvider`, `SlurmBuildEnvironmentProvider`, `render_rootless_builder_sbatch_request()`.
- Consumes: existing `loom_control_plane.elastic_slurm_worker_controller.SbatchRequest` and RFC 8785 for request digests.

- [ ] **Step 1: Write failing strict-contract tests**

  Add literal expectations proving a valid request renders these arguments:

  ```python
  assert request.args == (
      "/usr/bin/sbatch", "--parsable", "--hold", "--no-requeue",
      "--nodes=1", "--ntasks=1", "--cpus-per-task=8", "--mem=32768M",
      "--time=02:00:00", "--partition=loom-task-builder",
      "--account=loom-task-builder", "--qos=loom-task-image-builder-rootless-gb10",
      "--constraint=loom_rootless_buildkit", "--export=NONE",
      "--comment=loom-task-builder-v1:grant=11111111-1111-1111-1111-111111111111",
  )
  assert request.stdin == (
      "#!/usr/bin/env bash\nset -euo pipefail\n"
      "exec /usr/local/libexec/loom-task-builder-supervisor "
      "--grant-id 11111111-1111-1111-1111-111111111111\n"
  )
  ```

  Also assert forbidden fields are rejected, all models reject unknown keys, digest/comment drift is rejected, and `SlurmBuildEnvironmentProvider.submit_once()` raises `BuildEnvironmentDisabledError` before a fake runner records a call when policy `enabled=False` or blockers are non-empty.

- [ ] **Step 2: Run the new unit file and verify RED**

  Run: `uv run pytest -q tests/unit/test_task_image_build_environment.py`

  Expected: collection fails because `loom_control_plane.task_image_build_environment` does not exist.

- [ ] **Step 3: Implement minimal strict models, canonical digest, renderer, and provider protocol**

  Use strict frozen Pydantic models. `SlurmBuildEnvironmentPolicyV1` has exactly: schema, enabled, activation blockers, cluster/architecture, submitting identity, partition/account/QoS, feature constraint, supervisor path, sbatch path, resource request. Validate the cluster/architecture and cluster-specific QoS pairs. `SlurmBuildEnvironmentProvider` receives an injected runner protocol; it exposes render/inventory/cancel/release, but submission checks the disabled/blocked gate first.

- [ ] **Step 4: Run the provider tests and verify GREEN**

  Run: `uv run pytest -q tests/unit/test_task_image_build_environment.py`

  Expected: all tests pass with no warnings.

- [ ] **Step 5: Commit the provider contract**

  ```bash
  git add src/loom_control_plane/task_image_build_environment.py tests/unit/test_task_image_build_environment.py
  git commit -m "feat(builder): add inert rootless Slurm provider contract"
  ```

### Task 2: Durable grant, attempt, event, and publication-evidence schema

**Files:**

- Create: `migrations/versions/0108_task_image_builder_phase2_foundation.py`
- Modify: `src/loom/db/schema.py`
- Create: `tests/integration/test_task_image_builder_phase2_migration.py`

**Interfaces:**

- Produces ORM models `TaskImageBuildGrant`, `TaskImageBuildGrantEvent`, `TaskImageMaterializationAttempt`, and `TaskImagePublicationEvidence`.
- `TaskImageMaterializationAttempt` uniquely binds `(materialization_id, lease_epoch)` to attempt number and builder identity.
- `TaskImagePublicationEvidence` uniquely deduplicates exact replay by `(materialization_attempt_id, component, registry_image)` but permits the same digest under a later attempt row.

- [ ] **Step 1: Write the failing migration integration test**

  Upgrade a disposable PostgreSQL database through `0108` and assert the four tables, checks, foreign keys, and partial unique `(slurm_cluster_id, slurm_job_id)` binding exist. Insert two attempt rows for one materialization with different lease epochs and insert the same component/digest once under each; assert both evidence rows persist. Assert a duplicate in one attempt fails the unique constraint.

- [ ] **Step 2: Run the migration test and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_builder_phase2_migration.py`

  Expected: fail because revision `0108` and the four tables do not exist.

- [ ] **Step 3: Add revision 0108 and matching ORM models**

  Grant states are exactly `issued`, `submitting`, `bound`, `released`, and `revoked`. Persist request JSON plus SHA-256, comment, settle deadline, one invocation timestamp, optional exact job ID, state timestamps, and journal sequence. Grant events use monotonic per-grant sequence with unique `(grant_id, sequence)`. Attempt/evidence rows use `ON DELETE RESTRICT`; no production path deletes evidence.

- [ ] **Step 4: Backfill only provable active attempts**

  In the migration, insert attempt rows only for current `claimed`/`running` materializations having positive attempt count/lease epoch and non-null `claimed_by`. Do not guess historical attempt numbers from legacy JSON history.

- [ ] **Step 5: Run migration/schema tests and verify GREEN**

  Run: `uv run pytest -q tests/integration/test_task_image_builder_phase2_migration.py tests/integration/test_task_image_materialization_migration.py`

- [ ] **Step 6: Commit the durable schema**

  ```bash
  git add migrations/versions/0108_task_image_builder_phase2_foundation.py src/loom/db/schema.py tests/integration/test_task_image_builder_phase2_migration.py
  git commit -m "feat(builder): add durable rootless grant and publication ledgers"
  ```

### Task 3: Claim-bound append-only publication evidence

**Files:**

- Modify: `src/loom_control_plane/task_image_materializations.py`
- Modify: `src/loom_control_plane/routes/task_image_materializations.py`
- Modify: `src/loom_worker/control_plane_client.py`
- Modify: `src/loom_worker/task_image_builder.py`
- Modify: `tests/integration/test_task_image_materialization_store.py`
- Modify: `tests/integration/test_task_image_materialization_routes.py`
- Modify: `tests/unit/test_control_plane_client_task_images.py`
- Modify: `tests/unit/test_task_image_builder.py`

**Interfaces:**

- `claim_task_image_materialization()` appends one `TaskImageMaterializationAttempt` in the same transaction as the lease transition.
- `PublicationRequest` and worker/client protocol add positive `attempt_count`.
- Every publication, completion, or failure inserts immutable evidence against the exact stored attempt; JSON history remains a compatibility projection keyed by attempt, lease, component, digest, and builder.

- [ ] **Step 1: Write failing store tests for the incident defect**

  Claim attempt 1/lease 1, publish a digest, requeue and claim attempt 1 under a later lease, publish the identical digest, and assert two evidence rows and two JSON history entries with their distinct lease epochs. Retry the exact second publication and assert it remains one row for that attempt. Publish using a nonexistent or wrong-builder attempt binding and assert `TaskImageLeaseConflictError` without an evidence insert.

- [ ] **Step 2: Run focused store tests and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_materialization_store.py -k 'publication or attempt'`

  Expected: fail because claims do not create attempt rows and digest-only JSON dedup collapses the second publication.

- [ ] **Step 3: Implement attempt creation and evidence insertion**

  Insert attempts within the locked claim transaction. Use PostgreSQL `INSERT ... ON CONFLICT DO NOTHING` only for the exact evidence replay key. Resolve publication calls through `(materialization_id, attempt_count, lease_epoch, builder_id)` before insert; never derive stale attempt identity from the current materialization row.

- [ ] **Step 4: Propagate attempt count through route and worker client**

  Add `attempt_count` to `PublicationRequest`, `TaskImageBuilderControlPlane.record_task_image_publication`, `HttpControlPlaneClient.record_task_image_publication`, and the closure created from `TaskImageBuildClaim`. Update literal request-body tests.

- [ ] **Step 5: Run all publication/worker tests and verify GREEN**

  Run:

  ```bash
  uv run pytest -q \
    tests/integration/test_task_image_materialization_store.py \
    tests/integration/test_task_image_materialization_routes.py \
    tests/unit/test_control_plane_client_task_images.py \
    tests/unit/test_task_image_builder.py
  ```

- [ ] **Step 6: Commit attempt-scoped evidence**

  ```bash
  git add src/loom_control_plane/task_image_materializations.py src/loom_control_plane/routes/task_image_materializations.py src/loom_worker/control_plane_client.py src/loom_worker/task_image_builder.py tests/integration/test_task_image_materialization_store.py tests/integration/test_task_image_materialization_routes.py tests/unit/test_control_plane_client_task_images.py tests/unit/test_task_image_builder.py
  git commit -m "fix(builder): retain publication evidence per attempt lease"
  ```

### Task 4: One-invocation grant journal and authoritative reconciliation

**Files:**

- Create: `src/loom_control_plane/task_image_build_grants.py`
- Create: `tests/unit/test_task_image_build_grants.py`
- Create: `tests/integration/test_task_image_build_grant_store.py`

**Interfaces:**

- Produces `issue_task_image_build_grant()`, `begin_task_image_build_submission()`, `classify_task_image_build_inventory()`, `reconcile_task_image_build_submission()`, `record_task_image_build_release()`, and `TaskImageBuildGrantConflictError`.
- Reconciliation returns typed `wait`, `bind`, `revoke`, or `cancel_then_reconcile` decisions and exact cancellable live job IDs.

- [ ] **Step 1: Write failing pure reconciliation tests**

  Cover literal cases: incomplete controller/accounting inventory waits; authoritative zero before settle waits; authoritative zero after settle revokes; one exact pending held job binds; one terminal match revokes; any immutable request mismatch, live non-held job, mixed live/terminal set, or multiple match returns `cancel_then_reconcile` for live IDs and never binds.

- [ ] **Step 2: Run pure tests and verify RED**

  Run: `uv run pytest -q tests/unit/test_task_image_build_grants.py`

- [ ] **Step 3: Implement the pure classifier**

  Validate comment, submitting identity, request digest/spec, account, partition, QoS, resource request, and held state. Treat unknown Slurm states and non-authoritative inventory as wait/fail-closed. Sort cancellation IDs for deterministic results.

- [ ] **Step 4: Write failing store tests for one invocation**

  Issue a grant and assert journal event 1. Begin submission and assert state `submitting`, one invocation timestamp, and event 2. A second begin must raise without changing the timestamp/sequence. Bind one exact held candidate, commit, and only then record release. Prove zero/terminal revocation and that cancellation-required inventory does not mark the grant bound or released.

- [ ] **Step 5: Run store tests and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_build_grant_store.py`

- [ ] **Step 6: Implement locked transitions and append-only events**

  Lock the grant row for every transition, append exactly one next-sequence event, and flush in the same transaction. `begin_task_image_build_submission()` accepts only `issued`; no code path returns a grant to `issued`. Binding stores one job ID and event before an external caller may release it. Revocation records a bounded reason and never clears request identity.

- [ ] **Step 7: Run grant tests and verify GREEN**

  Run: `uv run pytest -q tests/unit/test_task_image_build_grants.py tests/integration/test_task_image_build_grant_store.py`

- [ ] **Step 8: Commit grant recovery behavior**

  ```bash
  git add src/loom_control_plane/task_image_build_grants.py tests/unit/test_task_image_build_grants.py tests/integration/test_task_image_build_grant_store.py
  git commit -m "feat(builder): journal recoverable held-job grants"
  ```

### Task 5: Disabled deployment policy and operator-facing contract

**Files:**

- Create: `deploy/task-image-builder/rootless-provider-v1.toml`
- Create: `src/loom_cli/task_image_rootless_provider_policy.py`
- Create: `tests/ops/test_task_image_rootless_provider_policy.py`
- Modify: `docs/architecture/task-image-materialization.md`

**Interfaces:**

- Produces `load_task_image_rootless_provider_policy()` returning exactly two native disabled policies.
- The checked-in policy has no active supervisor/service composition and no legacy reservation/node/Docker fields.

- [ ] **Step 1: Write failing policy tests**

  Assert x86_64/OLDLAB and arm64/GB10 rows render the cluster-specific rootless QoS, shared `loom-task-builder` partition/account, resource profile from prerequisites v1, `enabled=false`, and non-empty blockers. Reject `enabled=true` while any blocker remains and reject keys named `reservation`, `allowed_nodes`, `nodelist`, `exclusive`, `docker_socket`, `registry_credentials`, or `builder_token`.

- [ ] **Step 2: Run policy tests and verify RED**

  Run: `uv run pytest -q tests/ops/test_task_image_rootless_provider_policy.py`

- [ ] **Step 3: Add strict loader and disabled policy**

  Keep the active Phase 1 `deploy/environment-state/staging.toml` builder blocks unchanged. The new file is inert input only; it is not wired to a timer, supervisor, route, or autoscaler.

- [ ] **Step 4: Document evidence and activation boundary**

  Add a concise section explaining that JSON history is compatibility data while `task_image_publication_evidence` is the append-only attempt/lease audit source, and that the rootless provider remains disabled until guard/executor/credential/publication acceptance exists.

- [ ] **Step 5: Run policy/deployment tests and verify GREEN**

  Run:

  ```bash
  uv run pytest -q tests/ops/test_task_image_rootless_provider_policy.py tests/ops/test_task_image_builder_deployment_contract.py
  ```

- [ ] **Step 6: Commit inert deployment policy**

  ```bash
  git add deploy/task-image-builder/rootless-provider-v1.toml src/loom_cli/task_image_rootless_provider_policy.py tests/ops/test_task_image_rootless_provider_policy.py docs/architecture/task-image-materialization.md
  git commit -m "ops(builder): keep rootless provider policy fail closed"
  ```

### Task 6: Full verification, review, and PR

**Files:**

- Modify only files required by verified review findings.

- [ ] **Step 1: Run focused tests, lint, types, migration, and diff checks**

  ```bash
  uv run pytest -q \
    tests/unit/test_task_image_build_environment.py \
    tests/unit/test_task_image_build_grants.py \
    tests/integration/test_task_image_builder_phase2_migration.py \
    tests/integration/test_task_image_build_grant_store.py \
    tests/integration/test_task_image_materialization_store.py \
    tests/integration/test_task_image_materialization_routes.py \
    tests/unit/test_control_plane_client_task_images.py \
    tests/unit/test_task_image_builder.py \
    tests/ops/test_task_image_rootless_provider_policy.py \
    tests/ops/test_task_image_builder_deployment_contract.py
  uv run ruff check src/loom_control_plane/task_image_build_environment.py src/loom_control_plane/task_image_build_grants.py src/loom_control_plane/task_image_materializations.py src/loom_control_plane/routes/task_image_materializations.py src/loom_worker/control_plane_client.py src/loom_worker/task_image_builder.py src/loom_cli/task_image_rootless_provider_policy.py tests/unit/test_task_image_build_environment.py tests/unit/test_task_image_build_grants.py tests/integration/test_task_image_builder_phase2_migration.py tests/integration/test_task_image_build_grant_store.py tests/integration/test_task_image_materialization_store.py tests/integration/test_task_image_materialization_routes.py tests/unit/test_control_plane_client_task_images.py tests/unit/test_task_image_builder.py tests/ops/test_task_image_rootless_provider_policy.py
  uv run mypy src/loom_control_plane/task_image_build_environment.py src/loom_control_plane/task_image_build_grants.py src/loom_control_plane/task_image_materializations.py src/loom_worker/control_plane_client.py src/loom_worker/task_image_builder.py src/loom_cli/task_image_rootless_provider_policy.py
  git diff --check origin/dev...HEAD
  test -z "$(git diff --name-only origin/dev...HEAD | rg '^docs/superpowers/' || true)"
  ```

- [ ] **Step 2: Self-review invariants and mutation coverage**

  Confirm no production composition submits the new provider; no retry path can call `sbatch` twice for one grant; incomplete inventory cannot revoke/bind/resubmit; exact binding is durable before release; duplicate digest across lease epochs yields two evidence rows; and the Phase 1 config diff is empty.

- [ ] **Step 3: Request independent code review and resolve every Critical/Important finding**

  Review `origin/dev...HEAD` against this plan and the Phase 2 spec. Verify each finding against code before changing it, then rerun the affected red/green test and full focused suite.

- [ ] **Step 4: Rebase on current origin/dev and rerun verification**

  Fetch, inspect upstream changes, rebase the worktree branch, and rerun Step 1 on the rebased SHA.

- [ ] **Step 5: Push branch and create a non-draft PR targeting dev**

  The PR body must state that the increment is inert, leaves both active exclusive builders unchanged, fixes attempt/lease attribution, references issues #1462 and #1463 without closing them, and lists exact verification evidence.

- [ ] **Step 6: Monitor required CI, reply to inline review threads, and merge only through protected auto-merge**

  Required current-head checks are `repository-checks`, `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`. Do not bypass, admin-merge, or push directly to `dev`.

- [ ] **Step 7: Verify protected merge and post-merge inert state**

  Confirm the PR is merged, `origin/dev` contains the merge SHA, the new policy is still disabled with blockers, active Phase 1 policy remains unchanged, and issues #1462/#1463 remain open.
