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

- [x] Test a local TLS Distribution fixture with GET manifests/blobs, exact
  repository binding and independently issued read-only credentials.
- [x] Reject redirects, ambient proxies, credentials in URLs, wrong CA/name,
  cross-origin challenges, oversized headers/chunks and timeouts.
- [x] Implement the Task 1 reader protocol with streaming and unconditional
  close. Authenticate only to the configured origin. Never retry with broader
  scope; refresh only the exact read capability.
- [x] Run transport plus graph tests against real streamed responses and review.

Task 2 evidence: 199 graph/reader/token/config/package-boundary tests passed;
Ruff, strict mypy and lock checks passed. Independent scoped re-review cleared
admission and socket ownership races, shutdown cleanup and deadline enforcement
after suspended yields. The reader remains uncomposed into production.

## Task 3: Publication contracts, signer and key records

Create `publication_contracts.py`, `publication_signing.py` in the authority
package and corresponding unit tests. Add explicit immutable envelope/key/epoch
tables to `src/loom/db/schema.py` and the next unused public migration after a
fresh fetch; test migration upgrade, downgrade and constraints.

- [x] Derive strict fields from the spec; freeze the canonical schema and domain
  before exposing any HTTP interface. Test every identity, timestamp, digest,
  algorithm and signature mutation using an independent Ed25519 verifier.
- [x] Implement a dedicated signer protocol with signer-clock and key-interval
  validation, bounded service replies and no production in-process private key.
- [x] Test active/verify-only/revoked states and historical verification. Reject
  backdating, unknown keys, canonical-byte substitution and cross-domain use.
- [x] Reserve one durable epoch lock order for later revocation/start authority.
  Verify migration and all public head fixtures; review and commit.

Task 3 evidence: 194 focused contracts/signing/migration/package-boundary tests
and 20 migration/lifecycle regressions passed after local fixes. Independent
review of both implementation commits found spec compliance and approved task
quality with no findings. Strict timestamp spelling and locked refusal to discard
used publication authority on downgrade are covered. Production signer transport
and authenticated keyset distribution remain uncomposed; no readiness is granted.

## Task 4: Durable verification jobs and atomic readiness

Create `publication_store.py` and `publication_worker.py`, with PostgreSQL
integration tests; extend schema/migration from Task 3 with job/receipt bindings.

Partial implementation evidence: legacy completion now rejects grant-bound
rootless attempts using the current persisted lease/attempt identity, not a
builder-name prefix or failure-budget counter. 52 materialization/session/route
integration tests passed, including ordinary Phase 1 completion. Durable snapshot
jobs are implemented and reviewed through `b9adc814c`: the covering suite passed
387 tests, and the contention-review amendment passed 154 affected tests. Real
cross-grant INSERT contention and mutation-tested post-wait expiry checks are
covered. The verifier worker and signed atomic readiness are implemented through
`3a5a0f8e5`, with the reviewed slow-commit lease gap corrected in `6ab046dce` and
real claim/renewal deferred-commit regressions completed in `730ae26ba`.

The internal current-session prerequisite is independently reviewed and complete.
It shares strict validation with bearer authentication, accepts valid successors,
and refreshes cached authority even with deferred/expired ownership attributes.
The review's stale-cache finding was reproduced and fixed in `47e933ac5`;
128 focused session/projection tests and a final 420-test combined verification
passed. No durable publication worker or readiness transition is implied.

- [x] Test snapshot creation/replay, unique complete candidate sets and leased
  worker generation claims under real concurrent transactions.
- [x] Implement snapshot/read/commit using existing session/lease lock helpers.
  A clock sampled after network work controls final expiry checks.
- [x] Exercise session renewal, lease takeover, job termination, guard expiry,
  key rotation/revocation and two completing workers during the unlocked read.
  Each stale path must leave registry_images and ready_at untouched.
- [x] Store canonical envelopes and readiness atomically only for the complete
  frozen attempt. Validate replay without signing or rewriting rows.
- [x] Fence the legacy completion path against rootless attempts and prove
  accepted Phase 1 completion remains unchanged. Review the complete lock graph.

The compact receipt contract is implemented and independently reviewed through
`8b1b01d29`; 97 focused contract/package checks passed. Receipts bind the complete
frozen snapshot, candidate identities and signed-envelope set in at most 2 KiB.
Precise stored completion time and whole-second wire time have one defined
relationship. This pure contract does not itself verify database completion.

Task 4c evidence: 232 affected PostgreSQL integration tests and 371 pure/boundary
checks passed. Independent review found one worker lease scheduling defect after
slow commits; a controller-scoped correction passed the full 21-test affected
worker run and four real deferred-commit cases (initial claim and later renewal,
expired and nearly expired returned leases). Expired claims start no external
I/O; remaining live leases renew promptly, with both sleep and transaction bounded
by the last committed expiry. Controller re-review cleared the finding. No
production signer/distribution adapter or worker scheduler is composed.

## Task 5: Fixed API and supervisor completion

Extend authority API/contracts and guard fixed operation transport; extend Go
supervisor protocol, BuildKit metadata capture, publication and orchestration.

- [x] Test bounded submit/poll operations with current session credentials and
  no caller-selected repository, signing key, URL, platform or digest set.
- [ ] Record observed base resolution metadata from pinned BuildKit, covering
  FROM scratch, multiple stages, malformed metadata and missing evidence.
- [x] Renew session, attestation and lease while polling; close BuildKit and
  release retryably on publication infrastructure errors. Receipt completion
  must bind the same attempt and full component set.
- [x] Run real Go/Python handoff integration plus Go race tests. Keep activation
  controlled by the later protected composition rather than enabling defaults.

Runtime integration exposed two upstream shapes: empty scratch exports use
explicit null layer lists (now narrowly accepted with independent review), and
buildctl invokes the Dockerfile frontend through a gateway whose outer frontend
field is empty. The runtime metadata producer must validate the recorded inner
request. The producer is implemented in `c2d034158`, with actual amd64 scratch,
multistage, external COPY and zero-layer-image source probes plus measured
dual-architecture binary hashes. Independent review approved the bounded producer
and parser slices; the controller checked their unchanged integration helpers.
Native arm64/rootless/Slurm acceptance remains pending. The standalone immutable
supervisor parser is implemented in `31256d730`.

Contained capture and immutable orchestration propagation are implemented through
`c2eba09b9`. Independent review found a real directory-rename cleanup defect,
reproduced and fixed in `f850e1b48`. The fresh full supervisor suite and race suite
passed after that fix; independent scoped re-review cleared the finding with no
new breakage. Contained capture is complete for its bounded scope. The native
scratch fixture now checks real metadata emission but was explicitly skipped
without its native runtime prerequisite.

Versioned candidate persistence and strict stored-V2 parsing are independently
reviewed through `5588c6415`. The explicit Python guard/API V2 transport is
implemented in `48a8c1543`, with review fixes in `eb21b10d8`: both API versions
share traffic limits, and real invalid-V2 requests prove response/log/metric
redaction and no persistence. The final covering API/guard run passed 219 tests;
scoped independent re-review cleared both material findings. Maximum-evidence
tests use 32768-byte packets; matching release configuration and checked source
digests remain Task 6 gates. Go V2 candidate handoff is implemented and reviewed
through `6ab2c99c8`, including mandatory actual Go/Python handoff CI. A subsequent
sidecar-only compatibility fix (`10720a4cc`) passes full Go normal/race suites.
The pure bounded status projection (`1e503fcce`) passes 62 status/receipt/job
checks plus six package checks. Go receipt (`6735c24fd`) and status (`dfd2f5440`)
parsers pass cross-language vectors and full normal/race verification; receipt
has independent review and status has focused controller review. The authenticated
HTTP submit/poll slice (`cf3e925d9`) passes eight real API tests plus six package
checks, including actual worker completion after lease clearing, successor sessions,
revocation, redaction and a five-second transaction deadline. Existing authority
API/deployment/package checks passed 51 tests. Guard submit/poll transport
(`c79dc1fac`) passes 412 guard, cross-language vector and isolated-package checks,
Ruff and strict mypy; independent focused review found no material defects.
Supervisor fixed transport (`139c643d0`) passes full normal/race/vet checks and
six required-mode real Go/Python local-flow checks; independent focused review
also passed. Both boundaries enforce compact closed status and receipt bindings;
the guard does not claim to reconstruct candidate identities from IDs-only
requests. A lifecycle-source review reproduced stale cached-session cleanup after
credential refresh; `26bfa0887` closes the current manager-owned successor, with
full race and focused session regression coverage. Supervisor publication
liveness and terminal completion handling are implemented through `bff9f04cc`.
The standalone controller has independent review with no material findings;
full Go normal/race/vet checks passed. Actual credential-source refresh through
the orchestrator closes the successor and receipt-confirmed completion skips
release. The extended real Go/Python guard flow (`eb756dadc`) passes eight
required-mode checks, including candidate recording, queued submit and completed
poll with an independently derived set hash. The full Go/guard/API/PostgreSQL/
worker composition is covered through `e9985a46c`, with mandatory race-enabled CI
coverage in `eb4bdad16`. All 12 local/composed cases passed in 72.58 seconds using
a freshly rebuilt race helper; Go normal/race/vet and 123 workflow checks passed.
Real source renewal preserves original claim provenance; corrupted registry bytes
and revocation during signing leave no readiness or signed envelope. Atomic
completion before heartbeat produces an actual conflict followed by exact receipt
confirmation and successful finish, with no release or deterministic failure.
The harness exposed and regression-tested fractional bootstrap timestamp loss;
`ef4ece7d7` preserves the exact issuance instant and has independent scoped review.
The composition uses explicit native/build/upload/signing and ASGI transport
doubles; it does not substitute for native containment or production trust.
Independent full composition review of `525d1740f..eb4bdad16` passed with no
material findings. No slice activates the provider.

Upstream reconciliation `2171ed1b7` moves only the unpublished publication
migration to `0133`, after unchanged public `0132`. Independent review approved
the single-head graph and current-head fixture changes. PostgreSQL coverage
passed 32 tests with one historical-migration setup timeout; the isolated
round-trip/ORM-parity retry passed with the unchanged timeout. No production
migration was applied.

## Task 6: Retention and release integration

Extend existing task-image registry GC and release metadata tests.

- [ ] Race GC with candidates, active verification jobs, completed statements
  and execution references. Deletion must lose to a live fenced reference.
- [ ] Retain failed/abandoned partial-upload evidence for bounded cleanup.
- [ ] Recompute all changed guard/provider/supervisor release identities using
  the deterministic dual-architecture assembly, not guessed hash substitutions.
- [ ] Verify inactive deployment, package boundaries and Phase 1 compatibility.

## Task 7: Review, protected merge and continuation

Track full activation acceptance in issue #1861. Incremental PRs reference it
without closing the remaining execution-trust, native, incident and soak gates.

- [ ] Run graph/transport/signature/store/API/Go/migration tests, Ruff, mypy,
  package boundaries and deterministic dual-architecture assembly.
- [ ] Perform independent review of trust bindings, renewal/expiry, lock order,
  transaction rollback, stream closure and GC; correct all material findings.
- [ ] Open a feature PR to dev, pass all four required checks and squash merge.
  Verify the merged tree and defaults; preserve the worktree.
- [ ] Continue to signed worker keysets/start authority, native shadow campaigns,
  architecture-fence cutover and incident acceptance. Keep the full goal active.
