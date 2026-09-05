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
