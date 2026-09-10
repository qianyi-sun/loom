# Task-image execution trust

Status: signed-keyset verification, durable distribution adapter and dedicated
signer service/client implemented; host signing service provisioning, runtime
distribution, complete execution grants and online start remain uncomposed.

This document covers the keyset wire, durable distribution and dedicated signer
implementation of the
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

The keyset helper is deliberately one-component verification, not proof of a complete image
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
cleanup. An outer deadline includes checkout and cleanup; transaction-local
statement and idle timeouts independently bound server locks during an event-loop
stall. Cancellation closes the owned transaction. Errors after persistence
require the caller to roll back the entire
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
No private signing key, live signer service or worker capability is installed here.

## Dedicated signer policy and fixed clients

`loom_task_image_signer.policy` is a separate process-owned policy package, not an
in-process authority private-key provider. It has two fixed operations. Keyset
signing accepts a closed canonical preparation request containing environment,
previous/proposed version, revocation epoch and the complete ordered public-key
snapshot. It independently reads the durable authority, stamps its own clock,
signs with the configured execution root, rechecks the authority and verifies
the provider's returned signature. Version zero needs no previous artifact;
retaining the returned artifact remains a separately fenced transaction.

Publication signing requires a current committed authenticated keyset. It selects
the operator-configured publication key and checks stable environment, registry,
pool/architecture, cluster, build policy, release, supervisor and purpose/campaign
configuration. Per-allocation attestation, grant/job and image facts are supplied
by the authenticated publication verifier and remain signed and checked by the
existing publication completion authority. A per-allocation attestation digest
is deliberately not a static release setting: new legitimate allocations must
not require signer reconfiguration. The signer does not claim to re-fetch OCI
graphs or grant readiness from a signature alone.

Both operations use separate bounded READ COMMITTED transactions before and
after provider I/O, with no database locks retained while signing. Server-side
statement/idle limits complement an outer checkout/provider/cleanup-inclusive
deadline. The original signed issue/expiry and exact retained artifact are
rechecked before return. Publication and execution key bytes must differ;
providers and handles come only from trusted service composition, not requests.

The service database role needs SELECT on publication state, keys, keysets and
members, plus UPDATE on only `state.singleton_id` and `keys.key_id` to obtain
the required locks. Existing immutable-identity triggers and column grants
prevent authority mutation. This is not SQL read-only: same-value identity
updates can take locks and create row versions. A real disposable restricted
login test exercises both signing operations and rejects changes to counters,
key bytes/lifecycle, audit rows, trigger state, schema ownership and roles.
Production provisioning must verify effective privileges, no broad inherited
grants/ownership and the required immutable/state-lock triggers.

`HTTPSKeysetSigner` and `HTTPSPublicationSigner` reuse one bounded mTLS transport
but expose separate fixed operation paths. TLS identities remain operator-owned;
neither client accepts an arbitrary signing domain, key or endpoint path. The
keyset response is still untrusted until the existing cryptographic verifier and
durable finalizer accept it.

`SignerServer` explicitly binds TLS 1.3 with required client certificates and
maps the actual socket peer's DER-certificate SHA-256 to permitted operations.
CA membership alone is insufficient. Its raw socket accept loop reserves a
connection slot before accepting and allocating a TLS handshake; excess
connections remain in the finite OS backlog. Each accepted socket has a single
deadline for handshake, bounded headers/body, operation queue, policy and reply.
It executes at most one operation and closes the connection. Raw CRLF framing
is checked before h11 normalization, including rejection of bare CR/LF, duplicate
headers, ambiguous lengths, transfer encodings, folding, upgrades and forwarded
identity. Shutdown cancels and joins both handshake and policy tasks; a retained
completion callback owns socket/slot cleanup even if a task never starts.

`load_signing_key` loads an explicitly provisioned 32-byte Ed25519 seed from an
owner-only directory and regular single-link file. Descriptor-relative no-follow
traversal rejects symlink components and writable ancestors. File ownership,
mode, size and before/after metadata are checked; the derived public key must
match the configured pin. There is no missing-file generation fallback, exported
private-key API or detached signing thread. Key bytes live only in the dedicated
signer process; service-account isolation and protected key installation remain
operator prerequisites. The module does not provision them.

### Explicit startup and privilege admission

The inert `loom-task-image-signer --config /absolute/owner-only/settings.json`
entrypoint requires schema `loom.task-image-signer/v1`. Configuration supplies
the bind address/port, owner-only database URL file, TLS certificate/private key
and client CA files, distinct execution/publication public pins and seed paths,
the execution root's environment/validity interval, stable publication selections,
and exact peer certificate digests mapped to `keyset`/`publication` operations.
No credential, root, permission, bind listener or selection is discovered from
worker state. Limits default to sixteen admitted connections, two operations,
16 KiB headers, three-second handshake/read budgets and a ten-second total
connection deadline. Policy I/O has a five-second ceiling and new keysets a
five-minute lifetime, bounded by the configured root and fifteen-minute maximum.

Startup first authenticates with the dedicated database role and verifies its
effective privileges. Administrative/inherited roles, database or schema CREATE,
unrelated relation/sequence access, function ownership, callable non-trigger
SECURITY DEFINER routines, parameter permission to change
`session_replication_role`, extra column writes and missing required read/lock
privileges are rejected. The exact enabled trigger set, trigger properties and
function bodies are pinned to migrations `0135`/`0138`. Row-security filtering
and disabled, substituted or extra authority triggers close admission. Ordinary
trigger-returning routines cannot be called directly and are verified through
their attached authority triggers. These checks perform no grants or schema
changes. A trusted administrator changing privileges after startup remains
outside this service-account boundary.

Only then are signing keys loaded and the TLS listener opened. Database URLs
accept only explicit `postgresql+psycopg` credentials/destination and the closed
`sslmode`/`sslrootcert` option set. A nonnumeric-loopback destination requires
`verify-full` with an absolute CA path. All ambient `PG*` settings are rejected,
preventing libpq host/service/TLS overrides. Authority transactions explicitly
put `pg_temp` last in their search path. A listener failure reaches the service
supervisor; SIGINT/SIGTERM close/join connection and policy work before disposing
the database pool. Interrupted context cleanup retains that ordering.

Disposable tests exercise this complete startup-to-signature path with an actual
restricted login, owner-only key files and mTLS. This is not a production key
ceremony or native activation. There is still no installed service account, live
listener, worker delivery, complete execution grant or one-use start evidence.

## Complete publication-set verification

`verify_publication_set` composes the existing keyset/publication verifier over
the exact Dockerfile-backed component set derived from a frozen `TaskConfig`.
Its expected full unsigned identities, original envelope SHA-256 pins, task
snapshot and keyset counters/digest must come from an independently authenticated
execution grant; they are not inferred from the envelopes under examination.
Prebuilt-only sidecars are not extra builder components. Duplicate sidecar names,
including Dockerfile/prebuilt collisions, and non-Linux task snapshots are refused
before the expected set is derived.

The bounded inputs preserve the existing producer ordering (task first, followed
by unique lexical sidecars) and common native
task/materialization, attempt/lease/grant/session, frozen-plan, environment,
purpose/campaign and build/containment provenance. Component graphs, repository
names and observed bases may differ. Missing, extra, repeated or reordered
components and mixed otherwise-valid signed build authorities are refused.
Each pin hashes the complete original publication envelope, not its inner
statement or image digest. The immutable output uses verified native manifest
references, matching publication completion even when a root is an OCI index.

This pure helper is not runtime-composed and returns no readiness or start
authority. Signed versioned trial grants, actual immutable task-source byte
verification, old-worker capability gating and online one-use start serialized
with revocation remain required before any Phase 2 trial runtime. A verified
shadow set remains shadow evidence, never production execution authority.

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

The dedicated signing service policy and distributor are implemented but not
provisioned or runtime-composed. The versioned complete grant, source binding,
worker reader and serialized one-use start/revocation still require implementation
and integration. Production remains
disabled pending those gates, genuine shadow isolation, both native containment
and scheduling campaigns, Phase 1 continuity, incident acceptance, rollback and
soak. No private signing keys, live state changes or runtime defaults are supplied
by this increment.
