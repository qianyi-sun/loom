# Task-image execution trust

Status: signed-keyset verification and durable distribution adapter implemented;
runtime distribution, complete execution grants and online start remain uncomposed.

This increment implements the keyset wire described by the
[Phase 2 production design](2026-09-02-task-image-builder-phase2-production.md).
It does not activate builders or authorize a trial runtime. The
[publication boundary](2026-09-05-task-image-builder-phase2d2-verification.md)
still requires authenticated distribution and a dedicated signer before runtime
composition.

## Signed keyset

`PublicationVerificationKeysetV1` is a closed, RFC 8785 canonical object with
schema `loom.task-image-publication-keyset/v1`, environment, positive keyset
version, nonnegative publication-revocation epoch, whole-second UTC issue and
expiry times, and 1–128 publication keys in unique lexical key-ID order. Public
bytes are canonical unpadded base64url-encoded 32-byte Ed25519 public keys.
Each entry carries its ID, active/verify-only/revoked status, activation time and
the applicable retirement/revocation times. Lifecycle validation is shared with
the publication signer; null, unknown fields, duplicate IDs/public bytes and
ambiguous encodings are rejected.

The keyset lifetime is at most fifteen minutes, also bounding the existing
distribution snapshot freshness ceiling. The canonical keyset is at most 64 KiB;
its complete signature envelope is at most 128 KiB. Refresh requires new signed
authority and a monotonic version through the durable distributor, not
new local timestamps on old bytes.

The signing preimage is `loom-task-image-publication-keyset-v1`, one NUL byte,
and the canonical keyset bytes. The signature algorithm is fixed Ed25519. The
envelope contains `canonical_keyset`, `keyset_sha256`, the execution signing
`key_id`, `algorithm` and canonical unpadded base64url `signature`. There are two
distinct digest bindings:

- `keyset_sha256` hashes the canonical inner keyset.
- `snapshot_sha256` hashes the entire canonical signature envelope. Future
  execution-grant and start bindings must use this full-envelope digest.

`ExecutionGrantTrustRoot` is trusted release configuration: execution key ID,
environment, public bytes and activation/expiry interval. Its bytes must be
pinned in the worker release, never accepted from a keyset, registry or HTTP
response. The verifier requires the keyset's entire validity interval within
that root's interval and rejects publication keys reusing the root's public
bytes. Root rotation requires the corresponding trusted release process; this
module does not install roots or fetch new trust anchors.

## Verification and compatibility

`verify_publication_keyset` verifies both canonical layers, inner digest, exact
expected version/epoch, environment, pinned root identity, domain/signature and
current validity. Expected counters come from durable authority or the exact
authenticated execution grant, never from the keyset being checked. The result
preserves original signed times and does not create a `DistributedKeysetSnapshot`:
authenticity alone is not proof that the key was distributed before signing.

`verify_keyset_publication` additionally binds the complete snapshot digest and
one exact expected unsigned publication input, including task/checksum,
component/platform, purpose/campaign, attempt/lease, source plan, registry and
containment/release provenance. It authenticates the raw keyset each time rather
than trusting a caller-constructed verification-result object. Missing or revoked
publication keys, substituted public bytes, signatures outside key activation/
retirement intervals and publication counters newer than the keyset are rejected.

Routine rotation can produce a newer keyset while retaining an older key as
verify-only. A correctly issued historical image remains verifiable after that
key retires. A revocation epoch increase for another key does not itself invalidate
an unaffected historical publication: its key must remain present and nonrevoked.
Publication issued exactly at its key's retirement boundary is rejected.

This is deliberately one-component verification, not proof of a complete image
set or a current claim. The future worker reader must bind all expected components
and exact envelopes into its versioned execution grant, verify the immutable task
source and enforce reader capability gating. Immediately before the first task
or sidecar runtime, it must obtain/consume the online one-use start authorization
serialized with current publication revocation. A valid cached keyset or this
verifier's return value cannot replace that operation.

## Durable distribution journal

Migration `0138` adds immutable signed-keyset and membership tables. The journal
retains the original canonical signature envelope, inner and complete-envelope
digests, environment, version/epoch, pinned-root fingerprint and original signed
issue/expiry times. Restrictive membership references preserve historical public
identities. SQL rejects journal/member updates, deletes and truncation; downgrade
refuses retained artifacts. Upgrade/downgrade take state-first nonwaiting locks
on all affected preexisting parents and never reset publication counters.

`publication_keyset_store.prepare_keyset` snapshots the complete retained public
key set under the existing publication singleton fence. Commit this short
transaction before calling the dedicated signer; no signer I/O occurs under
database locks. `finalize_keyset` authenticates the returned bytes against the
configured execution root and frozen request, then reacquires current state and
all keys, verifies the full snapshot and atomically journals the artifact and
advances the version. Concurrent publishers can replay only the identical current
artifact. Expired, stale or conflicting artifacts are never replaced in place.

All store operations require actual READ COMMITTED transactions and reject pending
caller writes before autoflush. Separate transaction probes reject AUTOCOMMIT;
fresh locked reads reject cached ORM state. Migration `0135`'s key-mutation
statement trigger already locks the singleton before key rows, including inserts
that do not change that singleton. READ COMMITTED is necessary to observe such
inserts after a wait. Queries fetch at most 129 keys to enforce a 128-key ceiling
without silently omitting retained historical keys. Larger inventories require
an explicit versioned selection/archival policy, not deletion or implicit eviction.

`DatabasePublicationDistribution` owns a bounded transaction and configures
isolation on its actual checked-out connection, even when its caller supplied an
AUTOCOMMIT-derived engine. It verifies canonical bytes, signature, all metadata,
membership and the entire current public key snapshot. Its `snapshot` method
returns the original signed issue/expiry; `envelope` returns the exact retained
wire. Time is rechecked after lock acquisition, persistence and transaction
cleanup. Errors after persistence require the caller to roll back the entire
transaction; a successful finalize result is not durable until commit succeeds.

Full-keyset comparison is snapshot-admission validation. Unrelated key insertion
or retirement closes new snapshot admission until a matching artifact is issued;
it does not claim to revoke work already admitted through the existing selected-key,
version, epoch and expiry checks. Key revocation continues to advance the existing
publication epoch. No separate revocation clock is introduced.

The adapter is **not runtime-composed**. Database authentication and the execution
root remain operator-configured. Retention is not proof of fleet readiness: the
authenticated claim path must deliver these exact envelopes to root-pinned capable
workers, and the serialized online one-use start gate must exist before activation.
No private signing key, signer service or worker capability is installed here.

## Evidence and remaining activation gates

Tests use independently generated execution/publication keys and directly check
Ed25519 domain separation and every top-level signed field. They exercise nested
canonicalization, duplicate JSON/keys/public bytes, invalid lifecycle/encoding,
wrong root/environment/counters, validity/root bounds, unchanged expiry,
historical rotation, revocation, substituted keys and valid alternative execution
bindings. These are cryptographic contract tests, not a production key ceremony,
authenticated distribution service or live worker acceptance.

Disposable PostgreSQL tests cover immutable audit and restrictive references,
empty migration roundtrip and busy-parent refusal, full-snapshot changes during
signing, exact replay, key-insert serialization, unsupported isolation, cached or
pending state, unchanged expiry and rollback on expiration during persistence.
These are local contract tests, not live distribution or native acceptance.

The dedicated signing service policy, versioned complete grant, source binding,
worker reader and serialized one-use start/revocation must still be implemented
and integrated with this distributor. Production remains
disabled pending those gates, genuine shadow isolation, both native containment
and scheduling campaigns, Phase 1 continuity, incident acceptance, rollback and
soak. No private signing keys, live state changes or runtime defaults are supplied
by this increment.
