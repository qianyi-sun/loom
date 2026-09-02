# Task-image builder Phase 2A projection authority implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the inert, durable authority core that binds one released rootless Slurm build grant to one authenticated node/cgroup, projects one replay-safe bootstrap credential, exchanges it for one short-lived session, and accepts monotonic containment attestations.

**Architecture:** Extend the existing one-invocation Slurm grant with a strict immutable authority document, but keep Slurm submission and credential state in separate tables. A new `loom_task_image_authority` package owns canonical wire contracts and locked database transitions; encrypted exact-replay material uses the existing `SecretStore`. This increment exposes no HTTP route, node daemon, deployment, token provisioner, or activation path.

**Tech Stack:** Python 3.11, Pydantic v2 strict models, SQLAlchemy 2 async ORM, PostgreSQL/Alembic migration `0124`, RFC 8785 JSON Canonicalization Scheme, SHA-256, pytest.

**Spec:** `docs/architecture/2026-09-02-task-image-builder-phase2-production.md`

## Global constraints

- Implement after the design PR merges, in `/home/hongjian/loom/.worktrees/task-image-builder-phase2a-projection` on branch `feat/task-image-builder-phase2a-projection` from the then-current `origin/dev`.
- Do not modify or push any path under `docs/superpowers/**` or `.superpowers/**`.
- Preserve `deploy/environment-state/staging.toml`, both active Phase 1 builder policies, reservations, supervisors, and autoscalers byte-for-byte.
- Keep `deploy/task-image-builder/rootless-provider-v1.toml` disabled with all existing blockers.
- Do not add an HTTP route, service unit, installer, cluster mutation, raw secret log, or production composition in this increment.
- A grant authority binds purpose, environment, pool, native cluster/architecture, request, release, build policy, containment policy, resource profile, issue time, and expiry.
- Existing rootless build-grant rows are forbidden at migration `0124`; the disabled provider makes any such row unexpected authority that must be investigated rather than guessed or backfilled.
- Exact request retries may return the same still-valid encrypted response. A changed payload under an existing idempotency key is rejected.
- Bootstrap and session tokens are independently random, prefixed, stored only as SHA-256 plus an encrypted `SecretStore` reference, and compared with `hmac.compare_digest`.
- Projection requires a released, unrevoked, unexpired grant bound to the exact Slurm cluster/job. Exchange consumes the one semantic bootstrap. Attestation cannot change immutable cgroup or attachment identity.
- All tests follow red/green/refactor and every independently reviewable task is committed separately.

---

### Task 1: Canonical grant-authority and projection contracts

**Files:**

- Create: `src/loom_task_image_authority/__init__.py`
- Create: `src/loom_task_image_authority/contracts.py`
- Create: `tests/unit/test_task_image_projection_contracts.py`

**Interfaces:**

- Produces `StrictTaskImageAuthorityModel`, `TaskImageBuildGrantAuthorityV1`, `TaskImageGuardPrincipalV1`, `TaskImageProjectionRequestV1`, `TaskImageContainmentAttachmentV1`, `TaskImageProjectionChallengeV1`, `TaskImageAttachmentProofV1`, `TaskImageProjectionReceiptV1`, `TaskImageBootstrapExchangeV1`, `TaskImageBuildSessionV1`, and `TaskImageContainmentAttestationV1`.
- Produces `canonical_authority_bytes(model) -> bytes` and `canonical_authority_sha256(model) -> str` using `rfc8785.dumps(model.model_dump(mode="json", exclude_none=False))`.
- Produces `new_bootstrap_token() -> str`, prefixed `loom_tibp_`, and `new_session_token() -> str`, prefixed `loom_tibs_`, each with at least 256 random bits.
- `TaskImageAttachmentProofV1` establishes `attestation_generation: Literal[1]` and a bounded `attestation_expires_at`; later `TaskImageContainmentAttestationV1` documents generations greater than or equal to 1.

- [ ] **Step 1: Write strict contract tests**

  Use fixed UUIDs, literal digests, and UTC timestamps. The authority fixture is:

  ```python
  AUTHORITY = TaskImageBuildGrantAuthorityV1(
      purpose="production",
      shadow_campaign_id=None,
      environment="staging",
      pool_id="staging-gb10-task-image",
      slurm_cluster_id="gb10",
      cpu_arch="arm64",
      slurm_request_sha256="1" * 64,
      builder_release_sha256="2" * 64,
      build_policy_sha256="3" * 64,
      containment_policy_sha256="4" * 64,
      resource_profile_sha256="5" * 64,
      issued_at=datetime(2026, 9, 2, 14, 0, tzinfo=UTC),
      expires_at=datetime(2026, 9, 2, 16, 0, tzinfo=UTC),
  )
  ```

  Assert its exact RFC 8785 bytes and SHA-256 against hand-fixed literals. Assert every model rejects unknown fields, zero UUIDs, zero/all-uppercase/malformed digests, naive timestamps, non-UTC-equivalent invalid ordering, unsafe cgroup paths, PID/UID/GID/inode values outside positive signed-63-bit bounds, duplicate/unsorted link/program/map IDs, and attachment paths outside the request cgroup.

  Assert `purpose="production"` requires `shadow_campaign_id=None`, while `purpose="shadow"` requires a nonzero campaign UUID. Assert challenge expiry is after issue time, proof observation is inside its challenge interval, session expiry is no later than grant expiry, and attestation expiry is after issue time.

  Mutation target: removing any identity/digest/time/relationship validator must fail at least one literal test.

- [ ] **Step 2: Run the contract file and verify RED**

  Run: `uv run pytest -q tests/unit/test_task_image_projection_contracts.py`

  Expected: collection fails because `loom_task_image_authority.contracts` does not exist.

- [ ] **Step 3: Implement the minimal strict contracts**

  Use a frozen `ConfigDict(extra="forbid", strict=True)` base and normalize JSON arrays to tuples only in the base model's `mode="before"` validator. Use annotated constrained types for identifiers, digests, positive signed-63-bit integers, safe absolute cgroup paths, and token shapes. Canonical timestamps serialize as UTC `Z`; canonical JSON contains no omitted optional fields.

  Keep secret-bearing receipt/session models out of canonical request digests by exposing explicit `public_binding()` methods that return only their nonsecret fields. Do not implement signing or persistence in this task.

- [ ] **Step 4: Run contract tests and verify GREEN**

  Run: `uv run pytest -q tests/unit/test_task_image_projection_contracts.py`

- [ ] **Step 5: Commit canonical contracts**

  ```bash
  git add src/loom_task_image_authority/__init__.py src/loom_task_image_authority/contracts.py tests/unit/test_task_image_projection_contracts.py
  git commit -m "feat(builder): define projection authority contracts"
  ```

### Task 2: Migration 0124 and matching ORM

**Files:**

- Create: `migrations/versions/0124_task_image_builder_projection_authority.py`
- Modify: `src/loom/db/schema.py`
- Create: `tests/integration/test_task_image_projection_migration.py`

**Interfaces:**

- Adds non-null `authority_spec JSONB`, `authority_sha256 VARCHAR(64)`, and `grant_expires_at TIMESTAMPTZ` to `task_image_build_grants` after proving it is empty.
- Produces ORM models `TaskImageBuildProjection`, `TaskImageBuildProjectionEvent`, and `TaskImageBuildContainmentAttestation`.
- Projection states are exactly `challenged`, `projected`, `exchanged`, `revoked`, and `expired`.
- Event types are exactly `challenged`, `challenge_replayed`, `projected`, `projection_replayed`, `exchanged`, `exchange_replayed`, `attested`, `attestation_replayed`, `revoked`, and `expired`; a bounded `event_key` makes each semantic event/replay idempotent.

- [ ] **Step 1: Write the failing migration test**

  Upgrade a disposable PostgreSQL database to `0123`, insert one syntactically valid `task_image_build_grants` row, and assert upgrading to `0124` fails with the bounded message `unexpected pre-authority task-image build grants`. Roll back that transaction, delete the row, upgrade successfully, and assert:

  ```python
  assert {
      "authority_spec", "authority_sha256", "grant_expires_at"
  } <= grant_columns
  assert {
      "task_image_build_projections",
      "task_image_build_projection_events",
      "task_image_build_containment_attestations",
  } <= table_names
  ```

  Inspect and assert all named check, unique, foreign-key, and lookup constraints. Insert a challenged projection, prove a second row for the same grant fails, insert generations 1 and 2, prove duplicate generation fails, then downgrade to `0123` and verify all three new tables and three grant columns are absent.

  Mutation target: removing the empty-table migration guard or any grant/generation uniqueness constraint must fail.

- [ ] **Step 2: Run migration test and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_migration.py`

  Expected: Alembic cannot resolve revision `0124`.

- [ ] **Step 3: Add the guarded migration and ORM models**

  Use a PostgreSQL `DO` block to raise if `task_image_build_grants` contains any row before adding non-null authority fields without defaults. Add state-shape constraints that require challenge fields in every state, projection fields from `projected` onward, exchange/session fields only in `exchanged`, and exactly one terminal timestamp/reason shape for `revoked` or `expired`.

  Use `ON DELETE RESTRICT` from projections to grants, events to projections, attestations to projections, and secret references as opaque text. Add unique `(grant_id, event_sequence)`, `(grant_id, event_type, event_key)`, `(grant_id, request_id)`, `(grant_id, proof_id)`, `(grant_id, exchange_id)`, `(grant_id, generation)`, and `(session_id)` protections plus indexes for active session and attestation expiry scans. Replay event keys are fixed per phase, so an exact retry storm can create at most one replay event for that phase; attestation event keys are the decimal generation.

- [ ] **Step 4: Run migration and adjacent schema tests**

  Run:

  ```bash
  uv run pytest -q \
    tests/integration/test_task_image_projection_migration.py \
    tests/integration/test_task_image_builder_phase2_migration.py \
    tests/integration/test_task_image_materialization_migration.py
  ```

- [ ] **Step 5: Commit schema**

  ```bash
  git add migrations/versions/0124_task_image_builder_projection_authority.py src/loom/db/schema.py tests/integration/test_task_image_projection_migration.py
  git commit -m "feat(builder): persist projection and attestation authority"
  ```

### Task 3: Bind immutable authority into held Slurm grants

**Files:**

- Modify: `src/loom_control_plane/task_image_build_environment.py`
- Modify: `src/loom_control_plane/task_image_build_grants.py`
- Modify: `tests/unit/test_task_image_build_environment.py`
- Modify: `tests/unit/test_task_image_build_grants.py`
- Modify: `tests/integration/test_task_image_build_grant_store.py`

**Interfaces:**

- `SlurmBuildGrantV1` gains `authority: TaskImageBuildGrantAuthorityV1` and `authority_sha256: str`.
- `issue_slurm_build_grant(policy, *, grant_id, authority)` requires the authority's cluster/architecture/request digest to equal the policy request and recomputes `authority_sha256`.
- `issue_task_image_build_grant()` persists the canonical authority JSON, its digest, and its exact expiry.
- `_stored_grant()` reconstructs and validates the full grant; no database row can silently substitute authority JSON or expiry.

- [ ] **Step 1: Write failing grant-binding tests**

  Change the test `_grant()` helpers to construct the fixed authority above with `slurm_request_sha256=canonical_request_sha256(policy.request_identity())` and call:

  ```python
  grant = issue_slurm_build_grant(
      policy,
      grant_id=_GRANT_ID,
      authority=authority,
  )
  ```

  Assert mismatched environment, cluster, architecture, request digest, expired issue interval, altered authority digest, and changed authority JSON are rejected before rendering or persistence. Assert the rendered `sbatch` arguments/stdin remain byte-identical to their current literal expectations and contain no authority or secret fields.

  In the store test, mutate `row.authority_spec` after issuance and assert `_stored_grant()` raises validation rather than reconciling or binding the job.

- [ ] **Step 2: Run focused grant tests and verify RED**

  Run:

  ```bash
  uv run pytest -q \
    tests/unit/test_task_image_build_environment.py \
    tests/unit/test_task_image_build_grants.py \
    tests/integration/test_task_image_build_grant_store.py
  ```

  Expected: failures show `SlurmBuildGrantV1` and issuance do not yet accept or persist authority.

- [ ] **Step 3: Implement the authority binding without changing Slurm rendering**

  Import the contracts package into the existing environment/grant modules. Validate the policy request first, validate all authority cross-bindings, then construct the frozen grant. Persist `authority_spec=authority.model_dump(mode="json", exclude_none=False)`, `authority_sha256`, and `grant_expires_at` during the existing `issued` transaction.

  In every submission, reconciliation, and release transition, reconstruct the full grant and reject `now >= grant_expires_at` before any external action or held-job release. Expiry never makes an authority usable and never releases a held job. Do not add any provider invocation or projection call.

- [ ] **Step 4: Run grant tests and verify GREEN**

  Run the Step 2 command and require no new warnings.

- [ ] **Step 5: Commit grant authority binding**

  ```bash
  git add src/loom_control_plane/task_image_build_environment.py src/loom_control_plane/task_image_build_grants.py tests/unit/test_task_image_build_environment.py tests/unit/test_task_image_build_grants.py tests/integration/test_task_image_build_grant_store.py
  git commit -m "feat(builder): bind immutable projection authority to grants"
  ```

### Task 4: Challenge and sealed-projection store transitions

**Files:**

- Create: `src/loom_task_image_authority/store.py`
- Create: `tests/integration/test_task_image_projection_store.py`

**Interfaces:**

- Produces `TaskImageProjectionConflictError`, `TaskImageProjectionAuthorizationError`, and `TaskImageProjectionExpiredError`.
- Produces `request_task_image_projection(session, *, principal, request, now, challenge_nonce_factory) -> TaskImageProjectionChallengeV1`.
- Produces `complete_task_image_projection(session, *, principal, proof, now, secret_store, bootstrap_token_factory) -> TaskImageProjectionReceiptV1`.
- Every function operates inside the caller's transaction and locks rows; it never commits.

- [ ] **Step 1: Write failing integration tests for challenge issuance**

  Create and durably release one exact grant, then call `request_task_image_projection()` with a principal bound to `gb10/trt-gb10-1`. Assert one `challenged` row and event exist. Retry the same request ID and canonical bytes and assert the identical challenge plus one `challenge_replayed` event. Reuse the request ID with one changed cgroup inode and assert conflict without state mutation.

  Independently mutate principal node/cluster, job ID, account, QoS, partition, architecture, request digest, supervisor UID/GID, supervisor executable digest, grant state, grant expiry, and request observation time. Each mutation must reject before a projection or secret is created.

- [ ] **Step 2: Run the challenge tests and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_store.py -k challenge`

  Expected: import fails because `loom_task_image_authority.store` does not exist.

- [ ] **Step 3: Implement challenge issuance and replay**

  Lock the grant then projection row in that order. Require `state="released"`, exact job/native authority, and `now < min(grant_expires_at, request.observed_at + 60 seconds)`. Generate a UUID challenge nonce and a deadline no later than 60 seconds or grant expiry. Store canonical JSON/digests and append one monotonic event. Exact replay returns the stored validated challenge; changed replay raises conflict.

- [ ] **Step 4: Write failing projection-completion tests**

  Use a transaction-local fake `SecretStore` that records plaintext only in test memory. Submit an exact attachment proof with initial attestation generation 1 and assert:

  ```python
  assert receipt.bootstrap_token.startswith("loom_tibp_")
  assert row.state == "projected"
  assert row.bootstrap_token_hash == hashlib.sha256(
      receipt.bootstrap_token.encode("utf-8")
  ).digest()
  assert row.bootstrap_secret_ref.startswith("loom://task-image-bootstrap/")
  assert receipt.bootstrap_token not in json.dumps(row.proof_json)
  ```

  Assert the same transaction inserts exactly one generation-1 containment attestation and sets the projection high-water/expiry fields. Retry the exact proof and receive the same still-valid token from encrypted storage without adding another attestation. Change the proof under the same ID, use the wrong challenge, expire the challenge, change any cgroup/attachment identity, or report the wrong policy/resource digest and assert rejection without a second secret. Make `SecretStore.put()` fail and assert the projection remains `challenged` after rollback.

- [ ] **Step 5: Run projection tests and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_store.py -k 'projection or proof'`

- [ ] **Step 6: Implement projection completion**

  Re-lock grant and projection, validate principal/request/challenge/proof/grant timing, and compare every immutable cgroup/attachment field. Require the proof to establish attestation generation 1 with expiry after `now` and no later than the grant deadline. Generate `loom_tibp_` plus `secrets.token_urlsafe(48)`, store it under namespace `task-image-bootstrap`, insert the attestation, persist only token digest/ref and attestation high-water fields, and append `projected` in the same transaction. Exact replay reads the same encrypted value only while valid; all other replay is a conflict.

- [ ] **Step 7: Run all projection-store tests and verify GREEN**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_store.py`

- [ ] **Step 8: Commit challenge/projection store**

  ```bash
  git add src/loom_task_image_authority/store.py tests/integration/test_task_image_projection_store.py
  git commit -m "feat(builder): fence one-use bootstrap projection"
  ```

### Task 5: Bootstrap exchange, session authorization, attestation, and revocation

**Files:**

- Modify: `src/loom_task_image_authority/store.py`
- Modify: `tests/integration/test_task_image_projection_store.py`

**Interfaces:**

- Produces `exchange_task_image_bootstrap(session, *, request, now, secret_store, session_token_factory) -> TaskImageBuildSessionV1`.
- Produces `record_task_image_containment_attestation(session, *, principal, attestation, now) -> TaskImageContainmentAttestationV1`.
- Produces `authorize_task_image_build_session(session, *, grant_id, raw_session_token, now) -> TaskImageBuildSessionAuthorization` with nonsecret grant/purpose/pool/architecture/attestation bindings.
- Produces `revoke_task_image_projection(session, *, grant_id, reason, now) -> None` and `expire_task_image_projection(session, *, grant_id, now) -> None`.

- [ ] **Step 1: Write failing bootstrap exchange tests**

  Exchange the exact bootstrap once and assert state `exchanged`, a different session ID/token, session expiry bounded by grant/bootstrap/attestation deadlines, and no plaintext in database JSON/events. Retry the same exchange ID/body/token and assert the identical encrypted session receipt. Change an exchange field, use a wrong token, attempt a second semantic exchange ID, exchange after expiry, or use a revoked grant and assert authorization failure without creating another session.

  Mutation target: accepting only a matching grant UUID while ignoring the bootstrap hash or exchange digest must fail.

- [ ] **Step 2: Run exchange tests and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_store.py -k exchange`

- [ ] **Step 3: Implement one semantic exchange and session checks**

  Compare token digests with `hmac.compare_digest`, generate `loom_tibs_` plus `secrets.token_urlsafe(48)`, encrypt it in namespace `task-image-session`, and persist the request digest/session hash/ref atomically. `authorize_task_image_build_session()` requires exact token hash, exchanged state, live grant/session, and a fresh matching attestation; it returns no raw secret.

- [ ] **Step 4: Write failing monotonic attestation tests**

  Begin with generation 1 created by the attachment proof. Assert authorization succeeds before attestation/session/grant expiry. Replay identical generation 1 and assert one row plus an `attestation_replayed` event. Submit changed generation 1, skip to generation 3, change cgroup/link/program/map/policy/resource identity, use a different node boot ID, or attest after grant termination/revocation and assert rejection. Submit generation 2 with the same immutable attachment and later bounded expiry and assert the high-water fields advance.

  Make the current attestation stale and assert session authorization fails even though the bearer token and session expiry still match.

- [ ] **Step 5: Run attestation tests and verify RED**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_store.py -k 'attestation or session'`

- [ ] **Step 6: Implement attestation, authorization, and terminal transitions**

  Require the next new generation to equal `high_water + 1`; exact current-generation canonical replay is idempotent. Lock grant then projection, compare the immutable attachment digest, insert append-only evidence, and update high-water fields. `revoke` accepts a bounded lowercase reason, is idempotent only for the exact same reason, and permanently blocks replay/authorization. `expire` is allowed only when the earliest applicable deadline is reached. Neither transition deletes audit rows or rewrites grant submission history.

- [ ] **Step 7: Run all store tests and verify GREEN**

  Run: `uv run pytest -q tests/integration/test_task_image_projection_store.py`

- [ ] **Step 8: Commit exchange and attestation state machine**

  ```bash
  git add src/loom_task_image_authority/store.py tests/integration/test_task_image_projection_store.py
  git commit -m "feat(builder): authorize attested build sessions"
  ```

### Task 6: Package boundary, full verification, review, and protected merge

**Files:**

- Create: `tests/ops/test_task_image_authority_package_boundary.py`
- Modify only files required by verified review findings.

**Interfaces:**

- The package-boundary test proves no route, app, service unit, installer, external command runner, Slurm runner, or production composition imports the new store.
- The rootless policy remains disabled and Phase 1 deployment files have an empty diff.

- [ ] **Step 1: Write and run the package-boundary test**

  Parse imports with the Python AST rather than grepping source strings. Assert `loom_task_image_authority` imports only contracts, SQLAlchemy, schema models, and the `SecretStore` protocol; it cannot import FastAPI, subprocess, Slurm runners, Docker, BuildKit, registry clients, or deployment composition. Walk production imports and assert no executable entry point imports `loom_task_image_authority.store` in this increment.

  Run: `uv run pytest -q tests/ops/test_task_image_authority_package_boundary.py`

- [ ] **Step 2: Run focused and adjacent tests**

  ```bash
  uv run pytest -q \
    tests/unit/test_task_image_projection_contracts.py \
    tests/unit/test_task_image_build_environment.py \
    tests/unit/test_task_image_build_grants.py \
    tests/integration/test_task_image_projection_migration.py \
    tests/integration/test_task_image_projection_store.py \
    tests/integration/test_task_image_build_grant_store.py \
    tests/integration/test_task_image_builder_phase2_migration.py \
    tests/ops/test_task_image_authority_package_boundary.py \
    tests/ops/test_task_image_rootless_provider_policy.py \
    tests/ops/test_task_image_builder_deployment_contract.py
  ```

- [ ] **Step 3: Run static verification**

  ```bash
  uv run ruff check \
    src/loom_task_image_authority \
    src/loom_control_plane/task_image_build_environment.py \
    src/loom_control_plane/task_image_build_grants.py \
    tests/unit/test_task_image_projection_contracts.py \
    tests/unit/test_task_image_build_environment.py \
    tests/unit/test_task_image_build_grants.py \
    tests/integration/test_task_image_projection_migration.py \
    tests/integration/test_task_image_projection_store.py \
    tests/integration/test_task_image_build_grant_store.py \
    tests/ops/test_task_image_authority_package_boundary.py
  uv run mypy \
    src/loom_task_image_authority \
    src/loom_control_plane/task_image_build_environment.py \
    src/loom_control_plane/task_image_build_grants.py
  git diff --check origin/dev...HEAD
  test -z "$(git diff --name-only origin/dev...HEAD | rg '^(docs/superpowers|\.superpowers)/' || true)"
  test -z "$(git diff --name-only origin/dev...HEAD -- deploy/environment-state/staging.toml || true)"
  ```

- [ ] **Step 4: Self-review security mutations and scope**

  Confirm each realistic mutation has a failing test: wrong grant/job/node/boot/principal/UID/GID/PID/cgroup; wrong request/release/policy/resource digest; stale/future/expired time; changed idempotency body; second semantic exchange; plaintext persistence; stale/skipped/equivocating attestation; and session use after revocation. Confirm no test asserts merely that a source string exists and no production route can call the store.

- [ ] **Step 5: Commit package-boundary evidence**

  ```bash
  git add tests/ops/test_task_image_authority_package_boundary.py
  git commit -m "test(builder): prove projection authority remains inert"
  ```

- [ ] **Step 6: Rebase and repeat verification on current `origin/dev`**

  Fetch, inspect upstream migration and builder changes, rebase without weakening any conflict, normalize the worktree host-release manifest to filesystem mode `0644` if shared-repository checkout recreated it as `0664`, and rerun Steps 2 and 3.

- [ ] **Step 7: Open a non-draft PR and use protected merge only**

  The PR targets `dev`, links the permanent builder follow-up, states that the increment is inert, and includes exact test counts and current-head SHA. Required checks are `repository-checks`, `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`. Respond to every review finding with evidence, rerun affected tests, and enable the repository's normal auto-merge. Do not admin-merge or push directly to `dev`.

- [ ] **Step 8: Verify post-merge inert state**

  Confirm the squash merge is in `origin/dev`, migration `0124` is at head, the rootless provider remains disabled with all blockers, no authority service or node guard is running, Phase 1 configuration is unchanged, and the next branch starts from that exact merge.
