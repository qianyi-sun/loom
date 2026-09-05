# Task-image builder Phase 2D2 implementation plan

**Goal:** Verify uploaded bytes, sign complete same-attempt publication evidence,
and provide lease-fenced rootless readiness as part of the full activation goal.

**Architecture:** Durable verification jobs snapshot authority, stream registry
bytes outside transactions, and atomically commit signed readiness after fresh
authority checks. The allocation supervisor maintains liveness throughout.

**Tech stack:** Python 3.11, PostgreSQL/SQLAlchemy, RFC 8785, Ed25519, Go 1.23.4,
the existing authority/guard protocol and protected GitHub CI.

**Spec:** `docs/architecture/2026-09-05-task-image-builder-phase2d2-verification.md`.

## Global constraints

- Work in the existing isolated worktree on the D2 feature branch from D1 squash.
- Never push directly to dev or push `.superpowers/**` or `docs/superpowers/**`.
- Preserve working Phase 1 credentials, capacity, runtime and retention paths.
- D2 stays inactive until execution trust, shadow and architecture-fence gates.
- No readiness from HEAD, uploader receipts, partial statements or expired lease.
- Do not hold database locks across registry or signing-service I/O.
- Preserve original attempt identity while allowing valid session successors.

## Task 1: Streaming descriptor graph verification

Create `src/loom_task_image_authority/oci_verification.py` and
`tests/unit/test_task_image_oci_verification.py`.

Interface: frozen `OCIDescriptor(media_type, digest, size)`;
`OCIContentReader.read(kind, descriptor) -> AsyncIterator[bytes]`, where kind is
`manifest` or `blob` and the reader is repository-bound;
`verify_oci_graph(reader, root, platform, limits) -> VerifiedOCIGraph`.
The result binds root, runnable manifest, config, ordered layers and platform.

- [x] Write tests with an in-memory asynchronous chunk reader and independently
  hashed fixtures. Verify both architectures, OCI and Docker media types, direct
  and single-image index roots, and correct descriptor request kinds.
- [x] Run the new test module and confirm failure from the missing verifier.
- [x] Implement bounded streaming, strict JSON/descriptor validation, exact
  platform/rootfs checks, conflict detection and mandatory iterator close.
- [x] Mutate digests, sizes, platform, duplicate keys, URLs, foreign media,
  nested indexes, layer count, total bytes and chunk limits; prove rejection.
  Test cancellation and transport failures preserve closure and classification.
- [x] Run the new module, Ruff, strict mypy and authority package boundary tests;
  review and commit the verified module.

Task 1 evidence: 59 graph tests and 14 package/deployment tests passed;
Ruff and strict mypy passed. Independent review findings were fixed and
re-reviewed: unsupported OS requirements reject for direct and indexed graphs,
and the isolated descriptor-conflict regression fails with its production
check disabled in memory. JSON traversal uses depth-sized iterator storage.
The module is not composed into production and does not confer readiness.

## Task 2: Repository-bound HTTPS reader

Create `src/loom_task_image_authority/registry_reader.py` and
`tests/unit/test_task_image_registry_reader.py`; extend authority configuration
and package import allowlist only for the actual bounded transport dependency.

- [ ] Test a local TLS Distribution fixture with GET manifests/blobs, exact
  repository binding and independently issued read-only credentials.
- [ ] Reject redirects, ambient proxies, credentials in URLs, wrong CA/name,
  cross-origin challenges, oversized headers/chunks and timeouts.
- [ ] Implement the Task 1 reader protocol with streaming and unconditional
  close. Authenticate only to the configured origin. Never retry with broader
  scope; refresh only the exact read capability.
- [ ] Run transport plus graph tests against real streamed responses and review.

## Task 3: Publication contracts, signer and key records

Create `publication_contracts.py`, `publication_signing.py` in the authority
package and corresponding unit tests. Add explicit immutable envelope/key/epoch
tables to `src/loom/db/schema.py` and the next unused public migration after a
fresh fetch; test migration upgrade, downgrade and constraints.

- [ ] Derive strict fields from the spec; freeze the canonical schema and domain
  before exposing any HTTP interface. Test every identity, timestamp, digest,
  algorithm and signature mutation using an independent Ed25519 verifier.
- [ ] Implement a dedicated signer protocol with signer-clock and key-interval
  validation, bounded service replies and no production in-process private key.
- [ ] Test active/verify-only/revoked states and historical verification. Reject
  backdating, unknown keys, canonical-byte substitution and cross-domain use.
- [ ] Reserve one durable epoch lock order for later revocation/start authority.
  Verify migration and all public head fixtures; review and commit.

## Task 4: Durable verification jobs and atomic readiness

Create `publication_store.py` and `publication_worker.py`, with PostgreSQL
integration tests; extend schema/migration from Task 3 with job/receipt bindings.

- [ ] Test snapshot creation/replay, unique complete candidate sets and leased
  worker generation claims under real concurrent transactions.
- [ ] Implement snapshot/read/commit using existing session/lease lock helpers.
  A clock sampled after network work controls final expiry checks.
- [ ] Exercise session renewal, lease takeover, job termination, guard expiry,
  key rotation/revocation and two completing workers during the unlocked read.
  Each stale path must leave registry_images and ready_at untouched.
- [ ] Store canonical envelopes and readiness atomically only for the complete
  frozen attempt. Validate replay without signing or rewriting rows.
- [ ] Fence the legacy completion path against rootless attempts and prove
  accepted Phase 1 completion remains unchanged. Review the complete lock graph.

## Task 5: Fixed API and supervisor completion

Extend authority API/contracts and guard fixed operation transport; extend Go
supervisor protocol, BuildKit metadata capture, publication and orchestration.

- [ ] Test bounded submit/poll operations with current session credentials and
  no caller-selected repository, signing key, URL, platform or digest set.
- [ ] Record observed base resolution metadata from pinned BuildKit, covering
  FROM scratch, multiple stages, malformed metadata and missing evidence.
- [ ] Renew session, attestation and lease while polling; close BuildKit and
  release retryably on publication infrastructure errors. Receipt completion
  must bind the same attempt and full component set.
- [ ] Run real Go/Python handoff integration plus Go race tests. Keep activation
  controlled by the later protected composition rather than enabling defaults.

## Task 6: Retention and release integration

Extend existing task-image registry GC and release metadata tests.

- [ ] Race GC with candidates, active verification jobs, completed statements
  and execution references. Deletion must lose to a live fenced reference.
- [ ] Retain failed/abandoned partial-upload evidence for bounded cleanup.
- [ ] Recompute all changed guard/provider/supervisor release identities using
  the deterministic dual-architecture assembly, not guessed hash substitutions.
- [ ] Verify inactive deployment, package boundaries and Phase 1 compatibility.

## Task 7: Review, protected merge and continuation

- [ ] Run graph/transport/signature/store/API/Go/migration tests, Ruff, mypy,
  package boundaries and deterministic dual-architecture assembly.
- [ ] Perform independent review of trust bindings, renewal/expiry, lock order,
  transaction rollback, stream closure and GC; correct all material findings.
- [ ] Open a feature PR to dev, pass all four required checks and squash merge.
  Verify the merged tree and defaults; preserve the worktree.
- [ ] Continue to signed worker keysets/start authority, native shadow campaigns,
  architecture-fence cutover and incident acceptance. Keep the full goal active.
