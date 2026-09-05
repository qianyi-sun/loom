# Task-image publication verification and readiness

Status: implementation in progress under the approved Phase 2 production design.

Authority: `2026-09-02-task-image-builder-phase2-production.md`, with the
publication and key-lifecycle requirements in its archived predecessor.
Base: Phase 2D1 squash `565f10a14c0fef84ea4b65230dec12d82611c865`.

## Decision and alternatives

The dedicated task-image authority will verify immutable registry bytes and
commit signed publication statements for one complete materialization attempt.
Verification runs as bounded durable work outside database transactions. Short
transactions authorize a snapshot and later revalidate every mutable authority
before readiness. The existing supervisor keeps its session and lease alive
while polling the fixed publication operation.

Running verification inside the completion HTTP transaction would hold locks
through large downloads and obstruct renewal and revocation. Trusting a builder
digest or registry HEAD would leave the registry's returned bytes unchecked.
A durable verification operation supports retries and crash recovery without
either defect. Successful verification alone is evidence; the final transaction
is the only writer of rootless readiness.

## Verification boundary

The registry reader receives an authority-derived repository and exact SHA-256
descriptor. It performs authenticated GETs against a configured HTTPS origin,
using independently scoped read credentials. It has no ambient proxy, redirect,
cross-origin challenge, mutable-tag, external-layer URL, or caller-selected
endpoint behavior. TLS, headers, response sizes, chunk sizes, idle time, total
time and concurrent jobs have explicit limits. Stream closure is mandatory on
success, rejection, cancellation and connection failure.

The fixed GET transport uses verified asyncio TLS streams and the public h11
HTTP/1.1 state machine, with one connection per admitted object. Informational
responses and upgrades are rejected; no response head may be silently discarded
before Loom enforces its limits. h11 owns HTTP framing, while Loom bounds reads,
headers/trailers and deadlines and closes the connection on every exit. This
incurs additional TLS handshakes but avoids depending on private HTTP-client
internals to observe interim headers. Shutdown marks admission closed before
closing active sockets; queued reads cannot mint credentials after that point.
No timeout context remains active across a yielded chunk into caller code.

The byte verifier is independent of HTTP and persistence. Its input reader is
already bound to the repository; the verifier can request only manifests and
blobs by digest. It hashes every manifest, config and compressed layer byte,
checks exact sizes and validates the expected native Linux platform. Layer
contents are never extracted. It does not claim build reproducibility or that
the image is benign.

The initial accepted graph contains an OCI manifest or Docker schema-2 manifest,
optionally wrapped in a matching index/list containing exactly one native image.
Unknown provenance descriptors, nested indexes, foreign layers, subjects,
artifact types and descriptor URLs are rejected. The existing D1 exporter emits
a direct OCI manifest, so this restriction accepts its complete production
output without guessing how to interpret provenance. Config rootfs type and
diff-ID cardinality must match the layer list. This checks structure; compressed
blob hashes, rather than decompressed diff-ID verification, bind execution bytes.

Default ceilings: 4 MiB per JSON document, 128 layers, 256 descriptors, 100 GiB
total declared/read graph bytes, 1 MiB per input chunk, depth 32. Limits may be
lowered for a resource profile. JSON rejects duplicate keys, non-finite numbers,
invalid encoding and unknown authority-bearing graph fields. Descriptors with
the same digest but conflicting size or media type are rejected. Repeated layer
references remain ordered and are accounted for conservatively.

## Durable job and commit protocol

1. The session-authenticated request names only a fixed operation ID,
   materialization, attempt and lease epoch. Under the established grant,
   projection/session and materialization/attempt locks, derive the frozen plan,
   complete candidate set, repository destinations and containment bindings.
   Freeze a canonical input digest and create or replay the verification job.
2. A bounded authority worker claims that job with an expiring generation and
   deadline. It reads the frozen candidate descriptors and verifies bytes
   without holding database locks. Interrupted work may restart; a superseded
   worker cannot commit. No generic queue or caller-provided executable is added.
3. The supervisor renews its current session, attestation and lease while work
   runs. An original session generation is provenance, not a requirement that
   forbids legitimate successor sessions. A different attempt cannot take over.
4. Build statements from stored grant/attempt/plan/guard facts plus verified
   output and base-resolution evidence. Obtain signatures from the dedicated
   publication signer outside database locks. The signing service stamps/checks
   issue time against its clock and refuses retired or revoked signing keys.
5. Commit under the publication epoch/key lock followed by the established
   grant/projection/session, materialization/attempt, candidate/job locks.
   Recheck job generation, fresh current session and attestation, live lease,
   released live job authority, unchanged candidate-set digest, key interval and
   revocation epoch. Every component must belong to the same frozen attempt.
   Insert immutable envelopes and transition rootless readiness atomically.
   Failure rolls back the entire readiness change.

The key/epoch lock order is shared by later execution-start and revocation
transactions. No registry request or signer call occurs while these locks are
held. Completion replay validates its stored canonical receipt and cannot
rewrite ready images, re-sign a statement or resurrect revoked authority.

## Statement and signer

Use schema `loom.task-image-publication/v1`, RFC 8785 bytes and Ed25519 over
`loom-task-image-publication-v1` followed by NUL and canonical statement bytes.
Bind materialization identity/checksum, component, native platform, purpose,
attempt/lease, Slurm job, build policy/release, containment digest, output
descriptor and observed base digests. Retain the complete canonical statement,
its SHA-256, key ID, fixed algorithm and base64url signature.

Base observations come from pinned BuildKit metadata captured alongside the
output. Missing evidence cannot be represented as invented digests. Scratch
builds have an explicitly empty observed set. Metadata remains build evidence,
not an assertion that arbitrary network inputs were reproducible.

Production signing authority resides behind a dedicated host signing service or
KMS/HSM; private keys do not enter the allocation or an authority HTTP request.
Test signers use generated keys only. Key records distinguish active,
verify-only and revoked. The future signed keyset and execution-start increment
must consume these exact envelopes and serialize against this durable epoch.

### Canonical wire and signer boundary

The statement's unsigned input binds these authority-derived fields:
materialization ID and key, task ID and checksum, component, platform, purpose,
optional shadow campaign ID, attempt ID and number, lease epoch, grant ID,
original claim session ID and generation, frozen plan digest, environment/pool,
Slurm cluster and job, build-policy digest, composite builder-release digest,
native supervisor executable digest, containment-attestation digest, registry
origin and repository, verified root/runnable-manifest/config descriptors,
ordered layer descriptors, and observed base digests. Original session fields
are provenance only. None replaces the final current-authority check.

Publication timestamps use whole-second UTC `YYYY-MM-DDTHH:MM:SSZ` strings.
UUIDs are nonzero canonical lowercase strings. Integer fields are strict
non-boolean RFC 8785-safe integers. Optional campaign and key-retirement fields
are omitted when absent; JSON null is not in the signed schema. A production
statement has no campaign, and a shadow statement requires one. Origin and
repository must match purpose, campaign, native architecture, attempt and
component; the current production-only credential flow does not acquire shadow
authority by making the statement schema capable of representing it.

Observed base digests are a bounded, sorted unique list of normalized SHA-256
digests, not mutable image names. Their presence is mandatory even when empty;
the metadata-capture boundary must prove scratch/no-image inputs rather than
default missing metadata to an empty list. The ordered output layers remain a
list because repetitions and ordering have execution meaning.

The dedicated signer accepts a bounded validated unsigned input, chooses its
eligible active publication key, and adds `issued_at` using its own clock.
Active status alone is insufficient: a current signed keyset containing that
key must have been distributed before publication use. Signing eligibility
binds the durable distributed keyset version and revocation epoch; the final
transaction rechecks both. Until the later keyset-distribution composition is
available, production signing eligibility remains closed. Its only
operation signs this schema/domain; it does not expose arbitrary-byte signing.
The authority receives the complete canonical statement and signature envelope,
checks the canonical bytes, exact unchanged unsigned input, statement digest,
algorithm, pinned public key, activation interval and bounded clock skew, and
only then considers the result for the final fenced transaction. The final
transaction rechecks mutable key state and epoch independently of this earlier
cryptographic verification. A retired verify-only key can validate historical
statements but cannot sign a new publication; a revoked key cannot confer new
readiness. A stale response from an earlier signing request cannot substitute a
different attempt or component.

Public-key records retain immutable key identity/public bytes and activation
time, with monotonic retirement/revocation transitions. A singleton durable
publication-state row owns the revocation epoch and future keyset version.
It is locked before any individual key, grant, projection, session,
materialization, attempt, verification-job or trial-start row. Immutable envelope
rows bind their candidate, exact attempt/component and key through restrictive
foreign keys; they are not upgrades of legacy unsigned publication evidence.
The runtime authority never loads the production Ed25519 private key. A
service/KMS implementation and its authenticated transport must be available
and verified before composing the publication worker in production.

## Failure, retention and compatibility

Invalid/missing/inconsistent registry bytes do not consume deterministic task
failure budget or imply containment loss. Retry is bounded and backoff applies;
integrity conflicts retain diagnostic digests without logging credentials or
untrusted response bodies. Lease or containment loss rejects readiness even if
all bytes were uploaded and signed.

GC must fence candidates, verification jobs, statements and execution references
before deleting any attempt repository. Partial uploads remain cleanup evidence.
Migration adds explicit rootless publication authority rather than inferring it
for existing Phase 1 ready rows. Legacy completion must reject rootless attempts
while preserving Phase 1 behavior. D2 runtime composition remains unavailable
until execution trust, shadow acceptance and architecture-fence gates exist.

## Completion and subsequent activation

D2 acceptance requires real streamed-registry fixtures, PostgreSQL concurrency
and replay tests, signature mutation tests, expired-lease/attestation/job tests,
signer-rotation races, retention races, and a supervisor-to-authority flow.
Protected CI and an independent review precede the squash merge.

The full goal then requires worker keysets and one-use start authority, two
native shadow campaigns, architecture-capacity fences, separate OLDLAB and GB10
cutovers, task `4139e767` acceptance, soak and rollback proof. Phase 1 stays
operable throughout; a D2 merge is not completion of the activation goal.
