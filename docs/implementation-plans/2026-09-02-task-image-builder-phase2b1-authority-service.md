# Task-image builder Phase 2B1 authority service implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the production-shaped but inert mTLS authority service that exposes Phase 2A projection transitions only to exact node-guard principals.

**Architecture:** A new `loom_task_image_authority` entry point serves a small FastAPI surface over mandatory client-certificate TLS. A current-UID-owned `0600` registry binds hashed bearer tokens to one cluster, node, and canonical scope set; the node guard is the only client and mediates bootstrap exchange so allocation processes receive neither the node bearer nor a TLS private key. The deployment artifact is zero-replica, default-deny, unreferenced by live composition, and the rootless provider remains disabled.

**Tech Stack:** Python 3.11, FastAPI/uvicorn, Pydantic v2, SQLAlchemy async sessions, PostgreSQL/Alembic schema `0125`, LocalEncryptedSecretStore, prometheus-client, pytest/testcontainers, Kubernetes YAML.

**Spec:** `docs/architecture/2026-09-02-task-image-builder-phase2-production.md`

## Global constraints

- Phase 1 stays active and no Phase 1 deployment, Slurm reservation, or host-Docker path changes.
- `deploy/task-image-builder/rootless-provider-v1.toml` stays disabled for OLDLAB and GB10 with every existing activation blocker.
- The node guard is the only mTLS client; no TLS private key or node bearer is projected into a Slurm allocation.
- Every state-changing route requires both a CA-authenticated TLS connection and an exact hashed node bearer with the required scope.
- Request bodies are at most `MAX_CONTRACT_BYTES == 65536`; OpenAPI, Swagger, and ReDoc remain disabled.
- Authorization and expiry failures share one bounded HTTP response; no response reveals whether a grant, token, principal, or projection exists.
- Raw bearer, bootstrap, session, database, and keyring secrets never enter logs, metrics, URLs, response errors, checked-in manifests, or deployment examples.
- Service startup validates application Alembic head `0125` and fails closed on unsafe principal/keyring files or database drift.
- No authority route claims materializations, invokes Slurm, starts a supervisor, loads BPF, pushes an image, signs a publication, or authorizes trial execution.
- Do not modify `docs/superpowers/**` or `.superpowers/**`; use only this repository-owned implementation-plan path.

---

### Task 1: Strict node-principal and secret-keyring configuration

**Files:**

- Create: `src/loom_task_image_authority/auth.py`
- Create: `src/loom_task_image_authority/config.py`
- Create: `tests/unit/test_task_image_authority_auth.py`
- Create: `tests/unit/test_task_image_authority_config.py`

**Interfaces:**

- `TaskImagePrincipalVerifier.from_file(path: Path) -> TaskImagePrincipalVerifier` reads one bounded current-UID-owned `0600` nonsymlink regular JSON file.
- `TaskImagePrincipalVerifier.verify_bearer(header: str | None) -> TaskImageGuardPrincipalV1` compares all stored 32-byte token digests without an early exit.
- Registry documents have `schema_version=1` and `1..4096` principals with exact `principal_id`, nonzero lowercase `token_sha256`, `slurm_cluster_id`, `node_name`, and canonical scopes.
- Principal IDs, token digests, and `(slurm_cluster_id, node_name)` pairs are unique; each node has one non-ambiguous principal.
- `TaskImageAuthoritySettings` uses prefix `LOOM_TASK_IMAGE_AUTHORITY_` and requires principal, DB URL, secret-store keyring, server certificate, key, and client-CA files.
- `read_owner_only_bytes()` and `read_owner_only_secret()` reject symlinks, metadata races, wrong owner/mode, oversize payloads, invalid UTF-8, NULs, and multiline values.
- `load_secret_store_keyring()` accepts a `schema_version=1` JSON document with one primary `{version, key_base64}` and unique lower-version fallbacks; every decoded key is exactly 32 bytes.
- `build_uvicorn_kwargs()` always returns `ssl_cert_reqs=ssl.CERT_REQUIRED`, no server header, and the configured certificate/key/client CA.

- [ ] **Step 1: Write failing principal-registry tests**

  Write table-driven tests that create an owner-only registry containing:

  ```json
  {
    "schema_version": 1,
    "principals": [{
      "principal_id": "gb10-trt-gb10-1",
      "token_sha256": "49544425f5a2f7a5789fa74760173ba2db8476019ebf3ba4d20b3cf7ad775839",
      "slurm_cluster_id": "gb10",
      "node_name": "trt-gb10-1",
      "scopes": ["task-image:attest", "task-image:project"]
    }]
  }
  ```

  The non-secret test bearer is the literal `phase2b1-test-node-bearer`; the
  digest above is its checked SHA-256.

  Assert exact authentication returns `TaskImageGuardPrincipalV1`, while malformed/missing headers, whitespace variants, wrong tokens, zero/uppercase digests, duplicate IDs/tokens/node pairs, noncanonical or duplicate scopes, wrong native node binding, unknown fields, symlinks, FIFOs, wrong owner/mode, read races, invalid JSON, and payloads above 1 MiB all fail with bounded messages that contain no bearer.

- [ ] **Step 2: Run principal tests and verify RED**

  Run: `uv run --no-sync pytest -q tests/unit/test_task_image_authority_auth.py`

  Expected: collection fails because `loom_task_image_authority.auth` does not exist.

- [ ] **Step 3: Implement immutable registry verification**

  Define strict frozen Pydantic registry models and store verifier entries as `tuple[tuple[bytes, TaskImageGuardPrincipalV1], ...]`. Parse `Authorization` only with `re.fullmatch(r"Bearer ([^\\s]{1,4096})")`, SHA-256 the presented token, compare every candidate with `hmac.compare_digest`, and raise only `TaskImageAuthorityAuthorizationError("invalid task-image authority credentials")` on authentication failure.

- [ ] **Step 4: Run principal tests and verify GREEN**

  Run the Step 2 command and require all tests to pass without warnings.

- [ ] **Step 5: Write failing settings/keyring/TLS tests**

  Cover all owner-only file checks, strict environment parsing, port/rate/concurrency bounds, primary/fallback version uniqueness and ordering, strict base64 decoding, exact 32-byte keys, and the literal TLS result:

  ```python
  assert build_uvicorn_kwargs(settings) == {
      "host": "127.0.0.1",
      "port": 8445,
      "ssl_certfile": str(settings.tls_cert_file),
      "ssl_keyfile": str(settings.tls_key_file),
      "ssl_ca_certs": str(settings.tls_client_ca_file),
      "ssl_cert_reqs": ssl.CERT_REQUIRED,
      "server_header": False,
  }
  ```

- [ ] **Step 6: Run configuration tests and verify RED**

  Run: `uv run --no-sync pytest -q tests/unit/test_task_image_authority_config.py`

- [ ] **Step 7: Implement settings and keyring loading**

  Return a frozen `TaskImageSecretStoreKeyring(primary_key, primary_version, fallback_keys)` dataclass. Never preserve base64 strings after decoding and never include key bytes in exceptions or repr output.

- [ ] **Step 8: Run Task 1 tests and commit**

  Run:

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_task_image_authority_auth.py \
    tests/unit/test_task_image_authority_config.py
  git add src/loom_task_image_authority/auth.py src/loom_task_image_authority/config.py \
    tests/unit/test_task_image_authority_auth.py tests/unit/test_task_image_authority_config.py
  git commit -m "feat(builder): authenticate task-image node principals"
  ```

### Task 2: Bind guard-mediated exchange and revocation in the durable store

**Files:**

- Modify: `src/loom_task_image_authority/contracts.py`
- Modify: `src/loom_task_image_authority/store.py`
- Modify: `tests/unit/test_task_image_projection_contracts.py`
- Modify: `tests/integration/test_task_image_projection_store.py`

**Interfaces:**

- Add `TaskImageProjectionRevocationV1(grant_id, reason, observed_at)` with a lowercase reason matching `^[a-z][a-z0-9_]{0,63}$`.
- `exchange_task_image_bootstrap(..., principal: TaskImageGuardPrincipalV1, request, ...)` requires the exact projection principal with `task-image:project` scope before token comparison or replay.
- `revoke_task_image_projection(..., principal: TaskImageGuardPrincipalV1, request: TaskImageProjectionRevocationV1, now)` requires the same principal and exact request timing; identical reason replay is idempotent and a changed reason conflicts.
- `expire_task_image_projection()` remains an internal service transition and is not exposed to node principals.

- [ ] **Step 1: Write failing contract and store tests**

  Add strict revocation-contract tests, then mutate principal ID, cluster, node, scopes, revocation grant, future/stale observation, and reason. Prove a wrong guard cannot exchange even with the correct raw bootstrap, cannot replay another node's exchange, and cannot revoke another projection. Prove failures occur before session secret creation or terminal state mutation.

- [ ] **Step 2: Run focused tests and verify RED**

  Run:

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_task_image_projection_contracts.py \
    tests/integration/test_task_image_projection_store.py \
    -k 'exchange or revocation or revoke'
  ```

  Expected: the new revocation contract and required principal arguments are unavailable.

- [ ] **Step 3: Implement principal-bound exchange/revocation**

  Reuse `_validated_principal()` and `_require_principal()` after locking grant then projection. Require `task-image:project`; compare the principal binding before comparing bearer hashes; require `request.observed_at <= now < request.observed_at + 60 seconds` and `now` before grant/projection deadlines. Persist only the bounded revocation reason and canonical nonsecret request digest in the existing append-only event.

- [ ] **Step 4: Run the complete Phase 2A store suite and commit**

  Run:

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_task_image_projection_contracts.py \
    tests/integration/test_task_image_projection_store.py
  git add src/loom_task_image_authority/contracts.py src/loom_task_image_authority/store.py \
    tests/unit/test_task_image_projection_contracts.py \
    tests/integration/test_task_image_projection_store.py
  git commit -m "fix(builder): bind bootstrap exchange to the node guard"
  ```

### Task 3: Bounded mTLS authority API

**Files:**

- Create: `src/loom_task_image_authority/api.py`
- Create: `src/loom_task_image_authority/__main__.py`
- Modify: `src/loom_task_image_authority/__init__.py`
- Modify: `pyproject.toml`
- Create: `tests/integration/test_task_image_authority_api.py`
- Create: `tests/integration/test_task_image_authority_mtls.py`

**Interfaces:**

- Add console script `loom-task-image-authority = "loom_task_image_authority.__main__:main"`.
- `create_app(settings, *, verifier=None, now_factory=None, challenge_nonce_factory=None, bootstrap_token_factory=None, session_token_factory=None) -> FastAPI` owns its async engine/session factory and loads no cluster or Slurm client.
- Routes are exactly:

  ```text
  GET /healthz
  GET /metrics
  PUT /v1/projections/{grant_id}/challenge
  PUT /v1/projections/{grant_id}/attachment
  PUT /v1/projections/{grant_id}/exchange
  PUT /v1/projections/{grant_id}/attestations/{generation}
  PUT /v1/projections/{grant_id}/revocation
  ```

- Challenge/attachment/exchange/revocation require `task-image:project`; attestation requires `task-image:attest`.
- Path/body grant and generation mismatches return `409 {"detail":"task-image authority conflict"}`.
- Invalid contracts return `422 {"detail":"invalid task-image authority contract"}`.
- Missing/wrong bearer returns 401; missing scope returns 403; all store authorization/expiry failures return `403 {"detail":"task-image authority rejected"}`; idempotency conflicts return the same bounded 409 response.
- State transitions commit exactly once per successful request. SecretStore uses the loaded keyring and the same request transaction; exceptions roll back both authority and encrypted receipt rows.
- Metrics expose aggregate route/outcome counters, readiness, and in-flight count only; labels never contain grant, job, node, principal, task, token, or free-form reason values.

- [ ] **Step 1: Write failing API tests against real stores**

  Seed released grants/projections in disposable PostgreSQL and use HTTPX ASGI transport. Cover every route's happy path and exact replay, path/body mismatch, body-size boundary, invalid JSON/unknown fields, missing/wrong bearer, wrong scope/node/cluster, store conflict/expiry, keyring failure, database rollback, schema-behind startup, health readiness, disabled OpenAPI paths, response content type, and absence of all presented secrets in responses/metrics/log records.

- [ ] **Step 2: Run API tests and verify RED**

  Run: `uv run --no-sync pytest -q tests/integration/test_task_image_authority_api.py`

  Expected: collection fails because `loom_task_image_authority.api` does not exist.

- [ ] **Step 3: Implement the application and entry point**

  Follow the capacity-manager service pattern but keep this surface independent. Startup reads the DB URL/keyring from owner-only files, creates a SERIALIZABLE async engine, calls `loom.db.schema_startup.assert_schema_at_head(..., db_url_env_var="LOOM_TASK_IMAGE_AUTHORITY_DB_URL")`, and records readiness only after all checks succeed. Use a receive-wrapper body-limit middleware so chunked requests cannot bypass 65536 bytes. Construct `LocalEncryptedSecretStore` per request transaction with the immutable loaded keyring.

- [ ] **Step 4: Run API tests and verify GREEN**

  Run the Step 2 command and require no new warnings.

- [ ] **Step 5: Write and run the real TLS test RED/GREEN**

  Generate one test CA, server certificate, trusted client certificate, and untrusted client certificate. Start uvicorn with `build_uvicorn_kwargs(settings)` on a loopback ephemeral port. Assert TCP/TLS fails before HTTP for no client certificate and the untrusted certificate, while the trusted certificate reaches `/healthz`; also assert TLS 1.2+ and server certificate hostname verification. Run:

  `uv run --no-sync pytest -q tests/integration/test_task_image_authority_mtls.py`

- [ ] **Step 6: Run Task 3 tests and commit**

  Run:

  ```bash
  uv run --no-sync pytest -q \
    tests/integration/test_task_image_authority_api.py \
    tests/integration/test_task_image_authority_mtls.py
  git add src/loom_task_image_authority pyproject.toml \
    tests/integration/test_task_image_authority_api.py \
    tests/integration/test_task_image_authority_mtls.py
  git commit -m "feat(builder): serve bounded projection authority APIs"
  ```

### Task 4: Inert deployment artifact and package boundary

**Files:**

- Create: `deploy/task-image-builder/authority-service-v1.yaml`
- Create: `deploy/task-image-builder/authority-principals-v1.example.json`
- Create: `tests/ops/test_task_image_authority_deployment.py`
- Modify: `tests/ops/test_task_image_authority_package_boundary.py`
- Modify: `docs/architecture/2026-09-02-task-image-builder-phase2-production.md`

**Interfaces:**

- The manifest contains one `ServiceAccount` with `automountServiceAccountToken: false`, one internal `Service`, one `Deployment` with `replicas: 0`, and one ingress+egress default-deny `NetworkPolicy`.
- The Pod runs as a nonroot fixed UID/GID, drops all capabilities, uses runtime-default seccomp, read-only root filesystem, no privilege escalation, bounded CPU/memory/PIDs, and mounts only read-only secret/config files plus an in-memory `/tmp`.
- The artifact creates no Secret, token, certificate, bearer, principal registry, keyring, database URL, ingress, RBAC grant, host path, privileged container, or live replica.
- No canonical cluster render, staging environment state, compose file, service launcher, provider factory, worker, or control-plane route references this manifest or imports the authority API.
- The example registry contains only SHA-256 digests of explicit non-secret example strings documented beside the file; it contains no usable bearer and validates through the same registry parser after mode is set to `0600` in tests.
- Package-boundary evidence permits `loom_task_image_authority.api` as the sole executable importer of the authority store and continues to reject imports from public service/control-plane routes, workers, materialization/provider code, Slurm runners, or deployment activation.

- [ ] **Step 1: Write failing deployment and package-boundary tests**

  Parse YAML and Python AST. Assert the exact object set and disabled security context above, validate the example registry, walk live composition references, and prove the provider policy still has two `enabled = false` entries and unchanged blockers. The old Phase 2A assertion that no executable imports the store must fail specifically because the new dedicated API is now the one allowed executable boundary.

- [ ] **Step 2: Run tests and verify RED**

  Run:

  ```bash
  uv run --no-sync pytest -q \
    tests/ops/test_task_image_authority_deployment.py \
    tests/ops/test_task_image_authority_package_boundary.py
  ```

- [ ] **Step 3: Add the inert artifacts and narrow the boundary**

  Use image reference `ghcr.io/qianyi-sun/loom-control-plane:${IMAGE_TAG}` and command `python -m loom_task_image_authority`. Reference only non-created names `loom-task-image-authority-runtime` and `loom-task-image-authority-tls`; omission is intentional so applying the artifact still cannot start without later credential provisioning, even if an operator manually scales it.

- [ ] **Step 4: Run deployment tests and commit**

  Run the Step 2 command, then:

  ```bash
  git add deploy/task-image-builder/authority-service-v1.yaml \
    deploy/task-image-builder/authority-principals-v1.example.json \
    tests/ops/test_task_image_authority_deployment.py \
    tests/ops/test_task_image_authority_package_boundary.py \
    docs/architecture/2026-09-02-task-image-builder-phase2-production.md \
    docs/implementation-plans/2026-09-02-task-image-builder-phase2b1-authority-service.md
  git commit -m "test(builder): keep Phase 2B authority deployment inert"
  ```

### Task 5: Full verification, review, and protected merge

**Files:**

- Modify only files required by verified review findings.

**Interfaces:**

- The Phase 2B1 PR remains inert and becomes the exact base of Phase 2B2.

- [ ] **Step 1: Run focused and adjacent tests**

  ```bash
  uv run --no-sync pytest -q \
    tests/unit/test_task_image_authority_auth.py \
    tests/unit/test_task_image_authority_config.py \
    tests/unit/test_task_image_projection_contracts.py \
    tests/integration/test_task_image_projection_store.py \
    tests/integration/test_task_image_authority_api.py \
    tests/integration/test_task_image_authority_mtls.py \
    tests/integration/test_task_image_projection_migration.py \
    tests/integration/test_task_image_build_grant_store.py \
    tests/ops/test_task_image_authority_deployment.py \
    tests/ops/test_task_image_authority_package_boundary.py \
    tests/ops/test_task_image_rootless_provider_policy.py \
    tests/ops/test_task_image_builder_deployment_contract.py
  ```

- [ ] **Step 2: Run static and schema verification**

  ```bash
  uv run --no-sync ruff check \
    src/loom_task_image_authority \
    tests/unit/test_task_image_authority_auth.py \
    tests/unit/test_task_image_authority_config.py \
    tests/integration/test_task_image_authority_api.py \
    tests/integration/test_task_image_authority_mtls.py \
    tests/ops/test_task_image_authority_deployment.py \
    tests/ops/test_task_image_authority_package_boundary.py
  uv run --no-sync mypy src/loom_task_image_authority
  uv run --no-sync alembic -c migrations/alembic.ini heads
  git diff --check origin/dev...HEAD
  test -z "$(git diff --name-only origin/dev...HEAD | rg '^(docs/superpowers|\\.superpowers)/')"
  test -z "$(git diff --name-only origin/dev...HEAD -- deploy/environment-state/staging.toml)"
  ```

- [ ] **Step 3: Run the full repository suite**

  Run: `uv run --no-sync pytest -q`

  Record the exact pass/skip count and warning set. Do not claim completion from focused tests alone.

- [ ] **Step 4: Self-review security mutations and scope**

  Confirm tests independently mutate TLS trust, bearer, principal ID, node, cluster, scope, request/grant IDs, timestamps, idempotency bodies, token hashes, secret-store failures, body sizes, file ownership/mode/type/races, key versions, and startup schema. Confirm no secret appears in logs/responses/metrics, no state commits on errors, only the dedicated API imports the store, the deployment is zero-replica/default-deny/uncomposed, and Phase 1/rootless-provider files have the intended empty diff.

- [ ] **Step 5: Reconcile current `origin/dev` and repeat affected verification**

  Fetch the exact base, inspect overlapping migration/authority/deployment changes, update through a normal merge or GitHub update-branch path without force-push, and rerun Steps 1-2 plus any affected full-suite shard.

- [ ] **Step 6: Open a non-draft PR and use protected merge only**

  Target `dev`, link design PR #1732 and predecessor PR #1747, state that Phase 1 remains active and Phase 2B1 is zero-replica/inert, and include current-head evidence. Address every review thread with technical evidence. Require `repository-checks`, `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`, then enable normal squash auto-merge. Never admin-merge or push directly to `dev`.

- [ ] **Step 7: Verify post-merge inert state**

  Fetch `origin/dev`, verify the PR's squash commit is an ancestor and has the same tested tree, require one Alembic `0125` head, confirm the authority Deployment still has zero replicas and default-deny policy, confirm the two rootless policies remain disabled with all blockers, confirm staging/Phase 1 files have no diff, and start Phase 2B2 from that exact merge.
