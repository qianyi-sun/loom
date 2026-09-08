# Task-image builder Phase 2D1 registry credentials and upload implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task. Every
> behavior change follows a red/green/refactor cycle, and completion or merge
> requires fresh exact-head verification.

**Goal:** Add renewable, exact attempt/component-scoped OCI Distribution
publication credentials and bounded OCI upload/candidate evidence without
granting task-image readiness.

**Architecture:** The task-image authority derives one repository from the
durable production attempt and frozen component, signs a short-lived standard
Distribution bearer token with a dedicated RSA key, and stores each credential
generation and exact replay receipt append-only. The node guard transports the
opaque credential through a sealed memfd. The non-root supervisor uploads a
previously validated OCI layout with bounded resumable requests and records an
immutable publication candidate, but neither registry state nor that candidate
can mark a materialization ready.

**Tech stack:** Python 3.11, FastAPI, Pydantic, SQLAlchemy/PostgreSQL, RFC 8785,
PyJWT/cryptography RS256, encrypted `SecretStore`, Unix `SOCK_SEQPACKET` and
sealed memfds, Go 1.23.4, OCI Distribution v2, TLS 1.3, pytest, Ruff, strict
mypy, Go race tests, and protected GitHub CI.

**Spec:** `docs/architecture/2026-09-02-task-image-builder-phase2-production.md`

## Global constraints

- Base every change on verified Phase 2C squash
  `6e85733cad36d9b2919f7c07b05c39496a525624` in
  `/home/hongjian/loom/.worktrees/task-image-builder-phase2d-publication`.
- Migration `0131` is available only because `origin/dev` was fetched on
  2026-09-04 and its public head was confirmed as `0130`; re-fetch before the
  first migration commit and renumber if that fact changes.
- Keep both rootless provider policies disabled,
  `production_certification_allowed=false`, `certified_nodes=[]`, the Phase 2
  blocker present, and all authority/provider production replicas at zero.
- Keep the Phase 1 builder, registry credential, reservation, rollback, and
  retention paths unchanged.
- The caller may name only a component from the frozen claim plan. It may not
  submit a repository, registry scope, action, origin, issuer, service,
  subject, token lifetime, or token generation.
- A publication token grants only `pull,push` on one exact derived repository;
  it grants no catalog, namespace, cache, delete, base-registry, shadow, or
  cross-component authority.
- Production repositories are
  `loom-task-image-attempts/<architecture>/<attempt-id>/<component-segment>`.
  `task` maps to `task`; a sidecar maps to
  `sidecar-sha256-<sha256(canonical-component-name)>`, avoiding case and
  punctuation collisions within the Distribution repository grammar.
- Registry tokens use a dedicated RSA key of at least 3072 bits and RS256.
  The `kid` is the RFC 7638 SHA-256 thumbprint of the public JWK. Claims are
  exactly `iss`, `sub`, `aud`, `exp`, `nbf`, `iat`, `jti`, and one sorted
  `access` entry of type `repository` with actions `pull,push`.
- Credential lifetime is at most 45 seconds and no later than the current
  grant, session, containment attestation, or materialization lease. Docker
  Distribution's verifier may retain its fixed clock-skew leeway; that bounded
  residual token acceptance never grants Loom readiness.
- Credential renewal creates a new generation; it never extends a token. It
  requires the exact predecessor, a newer attestation/session generation, and
  a successful same-attempt heartbeat recorded after the predecessor was
  issued. At most 512 generations may exist per attempt/component.
- Raw tokens live only in the authority signer, encrypted `SecretStore`, a
  sealed memfd, locked supervisor memory, and unavoidable bounded TLS request
  buffers. They never enter paths, arguments, environment, Docker config,
  BuildKit session auth, build arguments, logs, metrics, ledgers, database
  columns, or persistent allocation storage.
- The guard never parses a token, registry response, OCI graph, Dockerfile,
  build argument, repository scope, or upload body. It only proxies fixed typed
  operations and seals opaque credential response bytes.
- The uploader accepts only its root-configured HTTPS origin and CA, disables
  redirects, uses TLS 1.3, bounds headers/bodies/timeouts/chunks, validates
  every upload `Location`, and never follows a cross-origin or cross-repository
  URL.
- A registry `HEAD`, successful blob/manifest response, candidate row, or
  supervisor report cannot change a materialization to `ready`, populate
  `registry_images`, or produce an execution grant. Phase 2D2 is the sole next
  increment allowed to validate and sign publication readiness.
- On registry or publication transport failure, release the lease as retryable
  infrastructure work; do not consume deterministic failure budget or label a
  registry outage as containment loss.
- Do not modify or push `.superpowers/**` or `docs/superpowers/**`.

---

### Task 1: Immutable credential and publication-candidate schema

**Files:**

- Create: `migrations/versions/0131_task_image_registry_credentials.py`
- Modify: `src/loom/db/schema.py`
- Create: `tests/integration/test_task_image_registry_credential_migration.py`

**Interfaces:**

- `TaskImageRegistryCredentialGeneration` is append-only. Its primary key is
  `credential_id`; `request_id` is globally unique; and
  `(materialization_attempt_id, component, generation)` is unique. It binds the
  full rootless attempt identity, issuing session generation, containment
  attestation generation/digest, optional renewal heartbeat event, exact
  predecessor, repository, key ID, request/public-response digests, token hash,
  encrypted response reference, and issue/expiry times.
- Generation 1 has no predecessor or heartbeat. Generation N > 1 has the exact
  generation N-1 predecessor and a non-null heartbeat operation ID. SQL checks
  bound IDs, digests, repository grammar, generation range 1..512, secret
  reference namespace, and time order without storing a raw token.
- `TaskImagePublicationCandidate` is append-only and unique by `operation_id`
  and by `(materialization_attempt_id, component)`. It binds the credential,
  reporting session, exact attempt/lease, derived repository, component,
  `sha256:` manifest digest, manifest size, OCI tar SHA-256/size, platform,
  canonical response JSON/SHA-256, and server observation time.
- Neither table has a foreign key or trigger that updates
  `task_image_materializations.registry_images` or lifecycle state.

- [ ] **Step 1: Write the failing migration test**

  Test upgrade/downgrade from `0130`; inspect every table, foreign key, unique
  constraint, check, index, and secret column; reject zero IDs, malformed
  digests/repositories, generation 0/513, broken predecessor shapes, missing
  renewal heartbeat, raw-token-like secret references, duplicate component
  generations, and duplicate candidates. Assert downgrade removes only the two
  new tables.

- [ ] **Step 2: Run the migration test and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/integration/test_task_image_registry_credential_migration.py`

  Expected: collection or revision lookup fails because migration `0131` and
  the two ORM models do not exist.

- [ ] **Step 3: Implement migration and ORM models**

  Use an empty-table migration with no inferred or backfilled authority. The
  credential attempt foreign key consumes the existing six-column rootless
  operation binding; the session and attestation keys consume their existing
  unique bindings; the heartbeat references the unique operation ID. Keep
  downgrade dependency-safe: drop candidates before credential generations.

- [ ] **Step 4: Run the migration test and confirm GREEN**

  Run:
  `.venv/bin/pytest -q tests/integration/test_task_image_registry_credential_migration.py`

- [ ] **Step 5: Commit Task 1**

  Commit message: `feat(builder): add registry publication authority schema`

---

### Task 2: Exact repository derivation and Distribution token signer

**Files:**

- Create: `src/loom_task_image_authority/registry_token.py`
- Modify: `src/loom_task_image_authority/config.py`
- Create: `tests/unit/test_task_image_registry_token.py`
- Modify: `tests/unit/test_task_image_authority_config.py`

**Interfaces:**

- `publication_repository(*, purpose, shadow_campaign_id, cpu_arch,
  attempt_id, component) -> str` accepts Phase 2D1 production only, validates
  canonical nonzero UUID/architecture/component inputs, and returns the exact
  repository specified in Global constraints. Shadow derivation is represented
  in the signature but rejected until a real `TaskImageShadowCampaign` exists.
- `DistributionRegistryTokenIssuer.issue(*, credential_id, repository,
  issued_at, expires_at) -> IssuedRegistryToken` signs exactly one repository
  access entry. `IssuedRegistryToken` exposes `token` with a redacted repr and
  the derived `key_id`, `issuer`, `service`, and HTTPS origin.
- `load_distribution_registry_token_issuer(settings)` reads a stable
  current-UID-owned mode-0600 PEM with the existing no-follow reader, requires
  an unencrypted 3072-bit-or-larger RSA private key with exponent 65537, and
  derives rather than accepts the key ID.
- Registry settings are an all-present-or-absent group:
  `registry_origin`, `registry_service`, `registry_issuer`, and
  `registry_signing_key_file`. Missing configuration leaves the credential
  provider unavailable; partial or unsafe configuration fails settings
  validation. Origins are HTTPS origin-only and credential-free.

- [ ] **Step 1: Write failing repository and signer tests**

  Generate a temporary 3072-bit key. Assert exact task/sidecar repositories,
  stable RFC 7638 `kid`, RS256 header, exact claim keys/values/order-independent
  semantics, one access entry, token hash redaction, and public-key signature
  verification. Mutate purpose, campaign, architecture, attempt/component,
  issuer/service/origin, times, algorithm, key size/exponent, PEM type, file
  ownership/mode/symlink, and extra JWT scope; each must fail closed.

- [ ] **Step 2: Run signer tests and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/unit/test_task_image_registry_token.py tests/unit/test_task_image_authority_config.py`

- [ ] **Step 3: Implement the pure derivation and signer**

  Encode RSA `n` and `e` as unpadded base64url, canonicalize the JWK thumbprint
  input with RFC 8785, and call `jwt.encode(..., algorithm="RS256",
  headers={"kid": key_id, "typ": "JWT"})`. Claims use integer UTC seconds,
  `sub="loom-task-image-builder:<credential-id>"`, and
  `access=[{"type":"repository","name":repository,
  "actions":["pull","push"]}]`.

- [ ] **Step 4: Run signer/config tests and confirm GREEN**

  Run the Step 2 command.

- [ ] **Step 5: Commit Task 2**

  Commit message: `feat(builder): mint exact distribution registry tokens`

---

### Task 3: Renewable credential authority and inert candidate recording

**Files:**

- Modify: `src/loom_task_image_authority/contracts.py`
- Create: `src/loom_task_image_authority/registry_credentials.py`
- Modify: `src/loom_task_image_authority/materializations.py`
- Modify: `src/loom_task_image_authority/http_contracts.py`
- Create: `tests/unit/test_task_image_registry_contracts.py`
- Create: `tests/integration/test_task_image_registry_credentials.py`

**Interfaces:**

- `TaskImageRegistryCredentialRequestV1` contains only current session
  possession, `request_id`, exact materialization/attempt/lease IDs,
  `component`, and the nullable predecessor ID/generation pair. Its public
  binding replaces `session_token` with `session_token_sha256`.
- `TaskImageRegistryCredentialV1` contains the complete immutable authority
  binding plus exact origin/issuer/service/repository/actions/key ID and the
  bearer token. Its public binding replaces `bearer_token` with
  `bearer_token_sha256`; canonical public binding remains under 64 KiB.
- `TaskImagePublicationCandidateRequestV1` contains current session possession,
  `operation_id`, attempt/lease, credential ID/generation, component, manifest
  digest/size, OCI tar SHA-256/size, and `linux/amd64` or `linux/arm64`. It has no
  caller-supplied repository or registry origin.
- `issue_session_registry_credential(session, *, authorization, request, now,
  issuer, secret_store, credential_id_factory) ->
  TaskImageRegistryCredentialV1` locks grant, projection/current session,
  materialization, attempt, and latest component credential in that order. It
  derives the repository from the stored claim plan, applies the 45-second
  deadline intersection, returns only a still-live encrypted exact replay, and
  enforces the renewal rules in Global constraints.
- `record_session_publication_candidate(...) ->
  TaskImagePublicationCandidateResponseV1` rechecks the live lease and current
  session; locks and matches the exact issued credential; independently derives
  its repository/component/platform; records or exactly replays one candidate;
  and never writes materialization lifecycle/readiness fields.
- Expose a non-private `lock_current_task_image_build_session_authority` and
  `lock_session_materialization_lease` from `materializations.py` so all Phase
  2D transitions preserve one documented lock order rather than duplicating
  queries.

- [ ] **Step 1: Write failing strict-contract tests**

  Round-trip canonical requests/responses and mutate every UUID, digest,
  generation, predecessor pair, component, action, origin, time, token, size,
  and platform independently. Assert raw secret-bearing models cannot use the
  nonsecret canonicalizer and their repr never includes the token.

- [ ] **Step 2: Run contract tests and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/unit/test_task_image_registry_contracts.py`

- [ ] **Step 3: Implement strict contracts and shared lock helpers**

  Preserve current materialization behavior while renaming/exporting the two
  lock helpers. Extend `canonical_public_binding_sha256` only with the new
  secret-bearing response/request types; do not weaken the existing secret
  guard.

- [ ] **Step 4: Write failing credential state tests**

  Cover first issuance, encrypted exact replay, expired replay, changed request
  ID body, two concurrent issuers, exact predecessor, early renewal, 513th
  generation, missing/stale/wrong-attempt heartbeat, unchanged attestation,
  superseded session, expired grant/session/attestation/lease, wrong
  architecture/component/purpose, signer failure, SecretStore failure, and
  transaction rollback. Assert only token hash and public binding persist.

- [ ] **Step 5: Write failing candidate tests**

  Cover one candidate per component, exact replay, changed replay, wrong
  credential/generation/component/platform/digest/size, cross-attempt/session,
  stale lease, and partial multi-component publication. Snapshot all
  materialization readiness fields before and after and assert they are
  byte-for-byte unchanged.

- [ ] **Step 6: Run state tests and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/integration/test_task_image_registry_credentials.py`

- [ ] **Step 7: Implement minimal state transitions**

  Use `hmac.compare_digest` for token/response hashes, RFC 8785 for public
  request/response and candidate response digests, explicit `now`, the existing
  encrypted `SecretStore`, and SQL row locks. Select a qualifying heartbeat
  event only when it is type `heartbeat`, matches all attempt/lease/grant
  fields, was issued under the current session generation, and was recorded
  after the predecessor credential.

- [ ] **Step 8: Run all Task 3 tests and confirm GREEN**

  Run the Step 2 and Step 6 commands.

- [ ] **Step 9: Commit Task 3**

  Commit message: `feat(builder): authorize renewable publication credentials`

---

### Task 4: Fixed authority HTTP routes and unavailable-by-default wiring

**Files:**

- Modify: `src/loom_task_image_authority/api.py`
- Modify: `src/loom_task_image_authority/__main__.py`
- Modify: `tests/integration/test_task_image_authority_api.py`
- Modify: `tests/integration/test_task_image_authority_mtls.py`
- Modify: `tests/ops/test_task_image_authority_deployment.py`
- Modify: `tests/ops/test_task_image_authority_package_boundary.py`

**Interfaces:**

- Add only these fixed routes:
  `PUT /v1/projections/{grant}/materializations/{materialization}/registry-credential`
  and
  `PUT /v1/projections/{grant}/materializations/{materialization}/publication-candidate`.
  There is no caller-selected route, scope, repository, or registry URL.
- `create_app(..., registry_token_issuer=None, credential_id_factory=uuid4,
  candidate_id_factory=uuid4)` uses an injected issuer in tests; otherwise it
  loads one only when the complete optional settings group is present. An
  absent issuer returns bounded 503 from the credential route.
- Both routes reuse `task-image:project`, strict body parsing, the current
  session authorization, serializable transaction wrapper, and bounded JSON
  responses. Authorization failures are indistinguishable 403, replay/body
  conflicts are 409, and signer/storage/infrastructure failures are 503. No
  exception text or secret appears in response or metrics.

- [ ] **Step 1: Write failing API, mTLS, and package-boundary tests**

  Test success and exact replay through a real test database; wrong path/body
  binding; missing/wrong bearer scope; missing client certificate; duplicate
  JSON keys; request/response bounds; partial settings; unavailable signer;
  redacted failures; and candidate inertness. Assert OpenAPI remains disabled
  and public control-plane routes do not expose these operations.

- [ ] **Step 2: Run API tests and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/integration/test_task_image_authority_api.py tests/integration/test_task_image_authority_mtls.py tests/ops/test_task_image_authority_deployment.py tests/ops/test_task_image_authority_package_boundary.py`

- [ ] **Step 3: Implement routes and optional production issuer loading**

  Add explicit contract dependencies and route functions. Keep the existing
  middleware body cap at 64 KiB. Do not add live environment values, Secrets,
  mounts, replicas, services, or policy exceptions to deployment artifacts.

- [ ] **Step 4: Run API tests and confirm GREEN**

  Run the Step 2 command.

- [ ] **Step 5: Commit Task 4**

  Commit message: `feat(builder): expose fixed registry authority routes`

---

### Task 5: Opaque guard credential transport and candidate acknowledgement

**Files:**

- Modify: `src/loom_task_image_builder_guard/protocol.py`
- Modify: `src/loom_task_image_builder_guard/authority.py`
- Modify: `src/loom_task_image_builder_guard/service.py`
- Modify: `tests/unit/test_task_image_builder_guard_protocol.py`
- Modify: `tests/unit/test_task_image_builder_guard_authority.py`
- Modify: `tests/unit/test_task_image_builder_guard_service.py`
- Modify: `tests/integration/test_task_image_builder_guard_local_flow.py`

**Interfaces:**

- Add local operations `registry-credential` and `publication-candidate`.
  Credential requests contain the existing lease binding plus one canonical
  component and exactly one sealed current-session descriptor. Candidate
  requests add only credential/candidate digest and size fields plus that
  descriptor. Neither operation accepts a repository, origin, scope, action,
  token, upload location, or arbitrary route.
- `AuthorityClient.registry_credential(...) -> SealedAuthorityPayload` calls
  the fixed credential route and seals the response without parsing it.
- `AuthorityClient.publication_candidate(...) ->
  PublicationCandidateAcknowledgement` calls the fixed candidate route and
  strictly parses only the nonsecret binding needed for the local receipt.
- The service checks the current local peer/session and fixed request shape,
  proxies the operation, sends credential bytes through a sealed memfd with
  acknowledgement, and never adds either response to the durable guard ledger.

- [ ] **Step 1: Write failing local protocol tests**

  Assert exact accepted shapes and independently reject missing/extra fields,
  wrong IDs/digests/sizes/component, descriptors on nonsecret operations,
  absent/multiple/unsealed secret descriptors, token/repository/URL fields,
  duplicate keys, and oversized packets.

- [ ] **Step 2: Run protocol tests and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_protocol.py`

- [ ] **Step 3: Implement only the two fixed protocol shapes**

  Raise the parser field-count ceiling only to the exact candidate request
  maximum. Reuse current canonical UUID/digest/positive-integer/component
  validators and do not add a generic operation payload.

- [ ] **Step 4: Write failing authority/service/integration tests**

  Test exact HTTP paths and methods, opaque byte identity, sealed descriptor
  transfer/ack/close, strict nonsecret candidate response parsing, peer death,
  current-session mismatch, timeout, wrong response IDs, response bounds, no
  token in exceptions, and unchanged ledger bytes before/after both operations.

- [ ] **Step 5: Run transport tests and confirm RED**

  Run:
  `.venv/bin/pytest -q tests/unit/test_task_image_builder_guard_authority.py tests/unit/test_task_image_builder_guard_service.py tests/integration/test_task_image_builder_guard_local_flow.py`

- [ ] **Step 6: Implement opaque proxy methods and service dispatch**

  Reuse `_current_session`, `_session_request`, `_seal_response`, and
  `_send_local_response`; close every descriptor in `finally`. Keep authority
  client route strings as literals in dedicated methods.

- [ ] **Step 7: Run all Task 5 tests and confirm GREEN**

  Run the Step 2 and Step 5 commands.

- [ ] **Step 8: Commit Task 5**

  Commit message: `feat(builder): proxy sealed registry capabilities`

---

### Task 6: Locked supervisor credential parser and guarded renewal source

**Files:**

- Modify: `cmd/loom-task-image-builder-supervisor/protocol.go`
- Modify: `cmd/loom-task-image-builder-supervisor/protocol_test.go`
- Create: `cmd/loom-task-image-builder-supervisor/registry_credential.go`
- Create: `cmd/loom-task-image-builder-supervisor/registry_credential_test.go`
- Modify: `cmd/loom-task-image-builder-supervisor/orchestrator.go`
- Modify: `cmd/loom-task-image-builder-supervisor/orchestrator_test.go`

**Interfaces:**

- `GuardClient.RegistryCredential(...)` returns one `*SecretBuffer`; its local
  request contains no repository or URL. `GuardClient.PublicationCandidate(...)`
  returns a strict nonsecret acknowledgement.
- `RegistryCredential` owns the sealed/locked `SecretBuffer`; token bytes alias
  that buffer and are never decoded into a persistent Go string. `Close`
  zeroizes/unlocks the complete response. Parsing validates every authority
  binding, expected component/repository/origin/service, predecessor, platform,
  issue/expiry interval, actions, and bearer-token grammar.
- `PublicationCredentialSource.Next(ctx, set, component, predecessor)` obtains
  generation 1 directly. Before renewal it rotates the build session to a
  newer containment attestation, records a successful materialization
  heartbeat, and then requests the next exact credential. It atomically closes
  the predecessor only after the successor parses successfully.
- `PublicationCredentialSource.Record(...)` submits only the immutable local
  OCI/candidate evidence and validates the acknowledgement binding.

- [ ] **Step 1: Write failing GuardClient request/response tests**

  Verify exact canonical local packets, one session memfd, credential response
  memfd validation/ack/close, candidate response binding, peer credentials,
  deadlines, and rejection of extra rights or response fields.

- [ ] **Step 2: Run protocol tests and confirm RED**

  Run: `go test ./cmd/loom-task-image-builder-supervisor -run 'GuardClient.*(Registry|Publication)'`

- [ ] **Step 3: Implement GuardClient methods**

  Extend the concrete interface with dedicated methods; do not route through a
  caller-supplied operation string outside the existing private helper.

- [ ] **Step 4: Write failing credential parser/source tests**

  Start from a valid credential secret and independently mutate token, IDs,
  component, repository, origin/service/issuer/key ID, action order/content,
  generation/predecessor, platform, and times. Assert close zeroizes the token.
  Test renewal call order `session-renew -> heartbeat -> credential`, failure at
  each boundary, no premature predecessor close, and candidate binding.

- [ ] **Step 5: Run parser/source tests and confirm RED**

  Run: `go test ./cmd/loom-task-image-builder-supervisor -run 'RegistryCredential|PublicationCredentialSource'`

- [ ] **Step 6: Implement locked parser and source**

  Use the existing strict field scanner so the token remains a JSON-string
  slice of locked bytes. Derive the sidecar repository with Go SHA-256 using
  the same exact input bytes as Python. Reuse `SessionManager`, and validate the
  heartbeat receipt before requesting the successor credential.

- [ ] **Step 7: Run all Task 6 tests and confirm GREEN**

  Run the Step 2 and Step 5 commands.

- [ ] **Step 8: Commit Task 6**

  Commit message: `feat(builder): renew locked registry credentials`

---

### Task 7: Bounded resumable OCI Distribution uploader

**Files:**

- Create: `cmd/loom-task-image-builder-supervisor/registry_upload.go`
- Create: `cmd/loom-task-image-builder-supervisor/registry_upload_test.go`
- Modify: `cmd/loom-task-image-builder-supervisor/oci.go`
- Modify: `cmd/loom-task-image-builder-supervisor/oci_test.go`

**Interfaces:**

- `RegistryUploadPolicy` contains the independently trusted HTTPS origin,
  service, pinned CA pool/server identity, 4 MiB chunk size, 15-second request
  timeout, 64 KiB response/header budget, and TLS 1.3 minimum/maximum. Its
  constructor rejects redirects, ambient proxy use, credential-bearing URLs,
  custom paths, and nil/ambient trust.
- `OCIRegistryUploader.Upload(ctx, output, credentialSource) ->
  UploadedManifest` streams the existing OCI tar without extracting persistent
  files or loading layers into memory. It uses `HEAD` only to skip already
  present content, performs `POST/PATCH/PUT ?digest=` blob uploads, validates
  same-origin same-repository upload locations and ranges, and finally PUTs the
  bounded top-level manifest by digest with its exact media type.
- Each request obtains a successor credential before the current one has 15
  seconds remaining. A chunk is retained in at most one 4 MiB buffer until its
  acknowledged range is unambiguous; transport ambiguity queries the exact
  upload location and never starts a second upload blindly.
- `UploadedManifest` is accepted only when `Docker-Content-Digest` exactly
  equals the locally validated top-level digest. This is upload evidence, not
  registry-byte verification or readiness.
- Extend `OCIOutput` with the validated top-level media type and manifest size.
  Bound index/manifest JSON to 4 MiB, descriptors to 256, layers to 128, each
  tar entry to the job quota, and total tar bytes to the existing build quota.

- [ ] **Step 1: Write failing OCI limit tests**

  Add oversized index/manifest, descriptor/layer count, negative/overflow size,
  duplicate blob, missing payload, digest mutation, and tar-size cases. Assert
  valid output returns exact manifest media type/size without changing the
  current platform/digest evidence.

- [ ] **Step 2: Run OCI tests and confirm RED**

  Run: `go test ./cmd/loom-task-image-builder-supervisor -run 'OCI'`

- [ ] **Step 3: Implement bounded OCI metadata scanning**

  Replace unbounded `io.ReadAll` calls with exact bounded reads for JSON and
  streaming digest verification for layers. Keep tar path/type/duplicate
  checks and return immutable metadata needed by the uploader.

- [ ] **Step 4: Write failing fake-registry uploader tests**

  Use a TLS 1.3 `httptest` registry that enforces the presented bearer token.
  Cover absent and present blobs, multi-chunk upload, credential rotation,
  resumed ambiguous PATCH, exact manifest PUT, component isolation, 401/403,
  404/416/429/5xx, malformed/multiple headers, oversized bodies, wrong ranges,
  wrong digest, redirect, cross-origin/repository `Location`, context timeout,
  TLS/CA failure, and token absence from errors.

- [ ] **Step 5: Run uploader tests and confirm RED**

  Run: `go test ./cmd/loom-task-image-builder-supervisor -run 'RegistryUpload'`

- [ ] **Step 6: Implement the minimal streaming uploader**

  Use an `http.Client` with a custom transport (`Proxy=nil`, no redirects,
  disabled compression, bounded idle connections). Construct all initial paths
  locally with escaped fixed repository segments; accept upload locations only
  after canonical URL resolution and exact origin/path-prefix validation.
  Discard bounded response bodies and close them on every branch.

- [ ] **Step 7: Run all Task 7 tests and confirm GREEN with race detection**

  Run: `go test -race ./cmd/loom-task-image-builder-supervisor`

- [ ] **Step 8: Commit Task 7**

  Commit message: `feat(builder): upload OCI layouts with bounded registry IO`

---

### Task 8: Inert end-to-end publication handoff and failure semantics

**Files:**

- Modify: `cmd/loom-task-image-builder-supervisor/outcome.go`
- Modify: `cmd/loom-task-image-builder-supervisor/orchestrator.go`
- Modify: `cmd/loom-task-image-builder-supervisor/main.go`
- Modify: `cmd/loom-task-image-builder-supervisor/orchestrator_test.go`
- Modify: `cmd/loom-task-image-builder-supervisor/main_test.go`
- Modify: `cmd/loom-task-image-builder-supervisor/integration_test.go`
- Modify: `tests/integration/test_task_image_builder_phase2c_flow.py`

**Interfaces:**

- `RegistryPublicationHandoff` uploads components in canonical plan order,
  records a candidate immediately after each manifest acknowledgement, closes
  all credential buffers on every branch, and returns
  `ErrPublicationVerificationUnavailable` after the final candidate. It can
  never return success in Phase 2D1.
- The orchestrator injects its guarded `PublicationCredentialSource` into the
  handoff. Any registry/credential/candidate/verification-unavailable error
  closes BuildKit and releases the materialization as retryable infrastructure
  work. It does not call deterministic failure or containment-revoking failure.
- The real main program continues to install `DisabledPublicationHandoff`.
  There is no registry upload policy/configuration in production composition,
  so merged Phase 2D1 code cannot request a credential or perform an upload.

- [ ] **Step 1: Write failing handoff and orchestrator tests**

  Cover two components, credential renewal mid-blob, candidate-after-manifest
  ordering, partial first-component side effects, second-component failure,
  candidate ambiguity/replay, context cancellation, secret cleanup, and every
  retry classification. Assert final materialization remains non-ready and
  deterministic failure count unchanged.

- [ ] **Step 2: Run orchestration tests and confirm RED**

  Run:
  `go test ./cmd/loom-task-image-builder-supervisor -run 'Publication|Orchestrator'`

- [ ] **Step 3: Implement handoff and retryable release behavior**

  Keep `productionPublicationHandoff` initialized to the disabled
  implementation and make its disabled path perform no credential call. The
  test-only real handoff receives a validated upload policy directly; do not
  add environment/config discovery.

- [ ] **Step 4: Run cross-language inertness and full Go tests**

  Run:
  `.venv/bin/pytest -q tests/integration/test_task_image_builder_phase2c_flow.py`

  Run:
  `go test -race ./cmd/loom-task-image-builder-supervisor`

- [ ] **Step 5: Commit Task 8**

  Commit message: `feat(builder): stage inert registry publication handoff`

---

### Task 9: Documentation, regression gates, review, PR, and protected merge

**Files:**

- Create: `docs/runbooks/task-image-builder-phase2d1-publication.md`
- Modify only if required by existing release/package tests:
  `scripts/ops/task_image_builder_provider_release.py`
- Modify: relevant tests under `tests/ops/`

**Interfaces:**

- The runbook states that Phase 2D1 is merged but inactive, lists exact token
  and candidate invariants, documents the future RSA/JWKS and registry token
  auth ceremony without real endpoints/secrets, and says Phase 2D2 plus shadow
  acceptance and Phase 4 activation are still required.
- Release/package checks include the new compiled Go code and Python authority
  modules without introducing a live config, credential, `current` symlink,
  node feature, systemd enablement, reservation, or provider activation.

- [ ] **Step 1: Write failing operational regression assertions**

  Assert provider policies/certification remain disabled; examples contain no
  usable token key or registry endpoint; deployment manifests contain no
  registry signing secret/mount/env; Phase 1 files remain byte-identical; and
  the authority route returns 503 without complete optional configuration.

- [ ] **Step 2: Run operational tests and confirm RED where coverage is new**

  Run:
  `.venv/bin/pytest -q tests/ops/test_task_image_authority_deployment.py tests/ops/test_task_image_authority_package_boundary.py tests/ops/test_task_image_builder_deployment_contract.py tests/ops/test_task_image_builder_provider_release.py`

- [ ] **Step 3: Add the runbook and minimal release/package updates**

  Do not add activation commands that can be mistaken for a completed
  credential ceremony. Name the required future registry settings and JWKS
  trust relationship, with placeholders described in prose rather than
  deployable example secret values.

- [ ] **Step 4: Run focused Phase 2D1 verification**

  Run all new/modified authority, migration, guard, supervisor, deployment,
  and Phase 2C regression tests from Tasks 1-9.

- [ ] **Step 5: Run repository-wide quality gates**

  Run: `.venv/bin/ruff check src tests migrations scripts`

  Run: `.venv/bin/mypy --strict src`

  Run the repository's exact migration-head, task-image, provider-release,
  deterministic dual-architecture build, and package-boundary commands from
  `.github/workflows/ci.yml` and the Phase 2C plan. Run Go formatting, vet,
  normal tests, and race tests with pinned Go 1.23.4.

- [ ] **Step 6: Perform fresh self-review and correct every finding**

  Inspect the full diff against `origin/dev`; trace every token/scope/URL,
  lock/replay/renewal, descriptor close/zeroization, upload ambiguity, and
  readiness path; search for secrets and activation drift; then rerun every
  affected gate after corrections.

- [ ] **Step 7: Commit Task 9**

  Commit message: `docs(builder): document inert registry publication phase`

- [ ] **Step 8: Push only the feature branch and open a PR to `dev`**

  Do not force-push or bypass protections. Include the exact base/head, threat
  boundaries, migration result, focused/full test counts, deterministic release
  digests, and explicit statement that builders remain production-disabled.

- [ ] **Step 9: Review the exact PR head, address comments, and rerun gates**

  Use `superpowers:requesting-code-review` and
  `superpowers:receiving-code-review`; verify reviewer claims technically,
  amend via ordinary follow-up commits, and require all protected checks on the
  final head.

- [ ] **Step 10: Squash merge only after exact-head gates pass**

  Use `superpowers:finishing-a-development-branch` and
  `superpowers:verification-before-completion`. Confirm the PR-head tree equals
  the squash-merge tree, then run post-merge default-deny/inertness checks. Do
  not delete the worktree or claim Phase 2D, shadow acceptance, builder
  activation, or incident acceptance complete.
