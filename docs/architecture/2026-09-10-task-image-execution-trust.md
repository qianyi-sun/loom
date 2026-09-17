# Task-image execution trust

Status: signed-keyset, complete publication-set and signed V2 execution-evidence
verification, durable distribution, dedicated signer and opt-in existing-worker
source-to-start consumption are connected through authenticated shared claims.
The backend journals grant revisions, signs committed preparations and serializes
one-use HTTP consumption with current token, worker/claim and revocation authority.
Standard entrypoints accept explicit release-owned public roots and fixed signer
connections, with bounded runtime keyset renewal and native-only readiness gating.
Host signing service/root provisioning remains incomplete. This implementation
is not live activation evidence.

This document covers the keyset, distribution, dedicated signer and
execution-evidence contracts of the
[Phase 2 production design](2026-09-02-task-image-builder-phase2-production.md).
It does not activate builders or authorize a trial runtime. The
[publication boundary](2026-09-05-task-image-builder-phase2d2-verification.md)
still requires authenticated distribution and a dedicated signer before runtime
composition.

## Existing-worker consumer boundary

The opt-in `run_worker(execution_trust=...)` assembly takes an independently
release-pinned execution root and purpose/campaign. No environment discovery,
claim payload or registry response installs that root. It requires the
authenticated shared-queue path; the legacy body-capability endpoint cannot
enable it. The assembly rejects a non-HTTPS control-plane URL before registration
or other worker effects, protecting the bearer used by both claim and start.
Default worker assembly and existing V1 claims remain unchanged.

The explicit trusted assembly advertises `task-image-execution-v2` in its measured
and digest-checked registration snapshot. Ordinary Trial-only readers use the same
authenticated shared queue, requesting exactly `['trial']`; they do not advertise
Pipeline, GPU allocation, or Pipeline input-cache capabilities. Existing Pipeline
assemblies retain their dual-kind registration and allocation/pool checks. The
server binds each claim's work-kind list to its retained registration, and a
Trial-only worker rejects an unrequested Pipeline response before dispatch.
Protected-worker credentials remain unsupported by this adapter. Capability
advertisement requires the configured root and HTTPS, not a response field or a
task-selected boolean. Default workers without that trust retain their legacy path.

`TaskImageExecutionDelivery` carries bounded original grant, plan, publication
and keyset envelopes plus the scheduler-stamped legacy claim identity. The
signed trial extension rejects mixed V1/V2 materializations and binds the trial,
team and attempt. The main loop additionally checks its actual worker ID,
architecture and task ID, verifies signatures before source preparation, and
preserves exact canonical attachments through the shared claim parser.

The existing pull-only preparation adapter uses the signed frozen snapshot and
immutable references without consulting a mutable task record. V2 bypasses the
optional local agent-layer build: a derived unsigned image cannot replace the
granted image. Source bytes/modes and the complete publication set are verified
again at runner admission, along with the actual runtime configuration and image.
Prebuilt components in the signed snapshot must use immutable image references.
The downloaded source directory must stay private and worker-owned until use;
content checks do not protect a concurrently writable shared directory.

`LocalTrialRunner` invokes the supplied start authorization before factories,
bridges, model launch, token rotation or sidecars. The consumer verifies the
original evidence and source before and after one bounded online request.
Only a canonical, exact-request-bound fresh `201` receipt is accepted over HTTPS
at the exact configured control-plane origin. Plain HTTP, embedded URL credentials,
query/fragment overrides and a differently based injected client are rejected
before sending a worker token. The unsigned receipt relies on server-authenticated
TLS; a valid signed grant cannot authenticate a forged online reply. Redirects,
ambiguous/oversized replies, changed source, expired evidence and lost responses
fail closed. Both runner and consumer mark the start attempted before awaiting:
cancellation, timeout, replay and concurrent calls cannot retry potentially
committed consumption. The existing outer task cleanup removes the owned source
directory after denial or cancellation.

The request binds grant UUID/revision, full envelope digest, exact claim and
keyset digest/version/revocation epoch. Its receipt binds the canonical request
digest, unique start UUID and a validity interval of at most thirty seconds.
The client-side latch is **not** durable one-use enforcement; the server additionally
serializes consumption with current worker/claim, grant revision and publication
revocation authority and rejects duplicates even after process restart.
No root installation or production activation is supplied by this opt-in path. Existing signed V2
evidence still requires genuine Slurm provenance; this is not yet a Nebius
publication adapter or a native AMD64/ARM64 acceptance result.

## Durable execution admission

Migration `0148` retains immutable grant revisions and one-use start receipts.
Consumption is unique by the scheduler's fresh claim UUID, across grant refreshes
and worker/control-plane restarts. Audit references retain publication and keyset
history without pinning the mutable Trial or worker row against ordinary cleanup.
Signed envelopes are fill-once; grants may be revoked but never unrevoked. Used
receipts cannot be rewritten, deleted or truncated; downgrade refuses history.

`execution_store` locks publication state, then worker, then Trial, preserving the
existing worker-before-Trial claim ordering. It independently verifies the bearer
hash, worker epoch, registered and digest-checked V2 capability, native architecture,
exact pre-start claim and cancellation state. Current strong source admission,
signed-ready materialization, completed publication set and root-verified keyset
are reread before issuance, finalization and consumption.

Preparation persists an unsigned immutable request. The caller must commit it
before external signing, then finalize in a fresh transaction against current
authority. Refresh increments the revision without changing the grant identity,
publication or purpose; older revisions cannot start. Consumption retains a
bounded receipt under the same revocation fence. A second request is rejected,
not replayed as a successful start. The API must bound checkout/transaction/commit
time and commit before returning `201`; the store's return alone is not durable
HTTP success. Database race tests exercise both duplicate consumption and a start
waiting behind revocation, but do not establish live native build acceptance.

`TaskImageExecutionService` owns connection-checkout, authentication, statement,
idle-transaction, signing and commit deadlines. The ordinary bearer helper owns
an internal commit, so it runs in a separate authentication session. Each actual
admission transaction independently locks and rechecks the worker Token with
`FOR SHARE` before publication state, worker and Trial. Token revocation cannot
race between that final check and consume. No authentication helper commits away
held admission locks. `/trials/{trial_id}/task-image/start` accepts a bounded,
duplicate-free request and returns canonical, noncacheable `201` only after commit.
Deferred commit failure, lock timeout, revoked token, wrong scope/path, cancellation
and replay do not produce a successful start acknowledgement.

`create_app(task_image_execution_factory=...)` provides explicit opt-in composition
using the app's own database engine, and rejects simultaneous protected-worker
runtime configuration. Without that factory the start route is unavailable and
the shared scheduler retains native-publication exclusion. With it, native-ready
selection requires the exact registered reader capability and native architecture;
the request body cannot opt a worker in. Selection is only provisional and retains
ordinary worker/Trial lock ordering: it does not wait on the global publication
fence. Commit releases those locks before state-first grant preparation and external
signing. The signed response stamps the scheduler's fresh claim UUID, worker epoch,
team and attempt. Old readers still use the unchanged V1 snapshot path.

Failed issuance compensates only the same unstarted, unconsumed claim through the
existing retry transition, records `task_image_admission_unavailable`, refunds the
attempt and applies a thirty-second backoff. Claim identity and audit history remain
retained. If authority changed or the database is unavailable, ordinary reclaim is
the fallback; compensation never rewrites a started, cancelled or replacement claim.
Tests exercise a refundable re-claim through the actual shared HTTP route and worker
client, and reject the earlier claim's otherwise valid signed evidence.

`/trials/{trial_id}/task-image/refresh` authenticates the worker and binds its
previous full signed-envelope/keyset identity to the current, still-unconsumed
claim. Preparation and finalization retain the same state-first fence as initial
issuance; signer I/O runs outside database locks. Only revision, signed times and
current keyset may change. Lost refresh acknowledgements may retrieve the latest
issuance; consumed claims cannot refresh. Requests are bounded to 8 KiB and canonical
responses to 4 MiB over the configured HTTPS origin, without redirects.

The worker refreshes near-expired evidence after cold preparation and always
requests current authority at runner admission, including when a keyset rolled
over while its old grant remained valid. It fully verifies fresh signed evidence,
unchanged source/image/publication bindings and actual local source before start.
Expired evidence identifies the refresh request only; it never authorizes a
runtime or gains invented validity timestamps. Grants with thirty seconds or less
of keyset/root lifetime cannot refresh. Revocation after refresh still denies
start. A lost or cancelled start acknowledgement never triggers refresh or retry.
Protected-reader composition and live native acceptance remain separate
unfinished boundaries.

## Release configuration

`LOOM_WORKER_TASK_IMAGE_EXECUTION_CONFIG_FILE` and
`LOOM_CP_TASK_IMAGE_EXECUTION_CONFIG_FILE` select independently provisioned,
current-UID-owned 0600 regular nonsymlink JSON files by absolute path. They are
optional: absent configuration keeps existing behavior. Files are bounded to
64 KiB and reject duplicate/unknown fields, private signing seed fields and
expired/not-yet-active roots. Neither response data nor a missing-file fallback
can supply a root.

The worker schema is `loom.task-image-execution-reader/v1`; the control-plane
schema is `loom.task-image-execution-admission/v1`. Both contain `root`
(`key_id`, `environment`, canonical unpadded base64url Ed25519 `public_key`,
`activated_at`, `expires_at`), `purpose` and the required `shadow_campaign_id`
only for shadow purpose. The control plane additionally requires `signer`
(`origin`, `ca_file`, `client_cert_file`, `client_key_file`). Its origin is a
fixed HTTPS origin with no caller-selected operation path; TLS paths are absolute.
The TLS client key is not a publication/execution signing key. Workers load no
private signing material or signer-package key loader.

`run_worker` loads this configuration before signals, cleanup or registration
and rejects non-HTTPS control-plane transport. The ordinary control-plane
factory loads its configuration before startup and owns signer cancellation and
close. Competing injected trust/factories and protected-worker configuration are
rejected. Application shutdown, including partial startup, drains admitted
background work before closing signer clients and database engines. These paths
do not provision credentials or install signing services.

The configured control plane owns a public-keyset renewal loop using the same
fixed mTLS connection settings and a separately pinned `keyset` operation. Its
signer peer policy must allow both `execution` and `keyset`; no signing keys enter
the control plane. Every thirty seconds, the loop authenticates the retained
snapshot and renews when no more than 150 seconds remain. A pass has a ten-second
deadline including database checkout, locks, signing and commits. Preparation
commits before signing outside database locks; finalization rechecks current
authority in a fresh transaction. Concurrent replicas may publish the same exact
snapshot or lose the authority race safely. No renewal extends historical bytes.
Returned keysets need more than 150 seconds of remaining root-bounded validity.

Native readiness starts false and requires a running renewal task plus an
authenticated signed snapshot with more than thirty seconds of remaining validity.
Signer/database outages retry without stopping the control plane or Phase 1;
expired authority, expired roots and an exited renewal task disable native claims.
Readiness is only an admission hint: issuance and start still recheck durable
authority. Shutdown sets persistent stop intent, cancels renewal, allows a
five-second grace then follows up cancellation and joins before closing clients
and engines. This also handles an operation that consumes its first cancellation.

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
set or a current claim. The worker reader must bind all expected components
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
in-process authority private-key provider. It has three fixed operations. Keyset
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

Execution signing accepts only an immutable grant ID/revision/digest and original
bounded plan/publication attachments. The aggregate request ceiling is 2 MiB,
in addition to each attachment's existing limits. It reads the committed grant,
latest revision, current keyset and consumed-start journal independently. Unknown,
revoked, superseded, consumed or substituted preparation fails before private-key
I/O. Full source/plan and signed publication-set verification runs before signing,
including the operator-configured provenance/purpose selection. The fixed execution
root signs only the execution-grant domain. Finalization and online consumption
still independently recheck the live worker/Trial and ready publication authority;
the private-key service neither needs their credentials nor grants runtime access.

All operations use separate bounded READ COMMITTED transactions before and
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

Explicitly enabling the `execution` peer operation additionally requires SELECT
on `task_image_execution_grants` and `task_image_execution_starts`, with no new
UPDATE rights. Their migration-0148 mutation triggers acquire the same publication
state fence, so read-only journal access suffices. Startup pins the exact trigger
set and function bodies before loading private keys. An execution-disabled service
continues to require only its original table scope and rejects extra journal
access. Restricted-role tests run execution signing over real mTLS and prove the
role cannot read worker credentials/Trial rows or modify grants and receipts.

`HTTPSKeysetSigner`, `HTTPSPublicationSigner` and `HTTPSExecutionSigner` reuse one bounded mTLS transport
but expose separate fixed operation paths. TLS identities remain operator-owned;
neither client accepts an arbitrary signing domain, key or endpoint path. The
keyset response is still untrusted until the existing cryptographic verifier and
durable finalizer accept it.

Execution has its own `/v1/executions/sign` path and explicit peer certificate
operation pin; publication/keyset-only peers cannot invoke it. Issuance may return
the exact already-retained signature after current-authority verification. This
does not make online start consumption replayable.

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
ceremony or native activation. There is still no installed service account or live
listener. Connected delivery and one-use start tests are disposable fixture
evidence, not live deployment acceptance.

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

This pure helper returns no readiness or start authority. The opt-in consumer
above composes signed evidence with immutable task-source byte verification;
server grant issuance, old-worker capability gating and durable online one-use
start serialized with revocation remain required before activation. A verified
shadow set remains shadow evidence, never production execution authority.

## Signed V2 execution evidence

`TaskImageExecutionGrantV2` is a closed, immutable canonical wire with schema
`loom.task-image-execution-grant/v2`. It binds a nonzero grant UUID and positive
revision, exact claim, environment/purpose/campaign, materialization identity,
native architecture, source-qualified checksum identity, historical build-plan
digest, complete publication-envelope pins and native image mapping, keyset
envelope digest/version/revocation epoch, and whole-second UTC issue/expiry.
Its validity is at most fifteen minutes and entirely within its authenticated
keyset and release-pinned execution root. Replacement revisions require future
durable authority; this verifier cannot issue, refresh or commit them.

The original frozen task configuration and source provenance are RFC 8785 JSON
object strings (`canonical_task_config`, `canonical_source_provenance`), each
bounded to 64 KiB of UTF-8. This preserves signed original bytes without returning
mutable nested task collections as verified authority. Source provenance must
include the strong content manifest and exact file-metadata digest; V1's missing-
manifest fallback is explicitly refused. The inner grant is at most 256 KiB and
its signature envelope at most 512 KiB. The envelope carries `canonical_grant`,
`grant_sha256` (inner digest), `key_id`, fixed `Ed25519`, and canonical unpadded
base64url signature. The signing preimage is `loom-task-image-execution-grant-v2`,
one NUL byte, and original canonical grant bytes. The verified result's
`envelope_sha256` identifies the **complete envelope**, not the inner digest.

Both claim variants bind trial/team/worker-registration UUIDs, actual worker lease
epoch and trial attempt count. The ordinary variant additionally requires a
nonzero `claim_id`, created and persisted atomically by its scheduler
for each new claim. It must remain stable for that claim's grant refreshes
and change on requeue/reclaim, even when `node_setup_health` refunds attempt count.
Neither the grant issuer nor the worker may invent or derive it from refundable
counters or a timestamp. Both ordinary scheduler paths (`claim_one` and
`claim_work`) now stamp a fresh UUID in `trials.legacy_claim_id` and return it as
`claim_id` in the same claim transaction. This is the last legacy claim identity,
not independent proof of a live claim; it remains retained after release and is
replaced on the next legacy claim. Historical claims have NULL, never a backfilled
identity. Server V2 delivery and durable one-use start require the explicit trusted
reader assembly described above; legacy readers remain excluded. The explicitly discriminated protected variant
additionally binds the canonical protected receipt digest, actual worker
incarnation UUID and claim high-water; these cannot be inferred from ordinary
trial attempt count. The expected claim and purpose come from independent
authenticated authority. Verifying a signature never supplies that authority.

`verify_execution_grant` verifies both canonical layers and the execution-root
signature, then the original canonical V2 build plan, frozen task/component
derivation, source location/manifest/modes identity and complete original signed
publication attachments. Pins are authenticated **before** unsigned publication
inputs are reconstructed; the complete-set verifier still checks the real
publication and keyset signatures. All publication identities must agree with
the historical plan's grant/session and current signed task/purpose/environment.
Verified native manifest references must exactly equal the signed mapping.
Historical build authorization expiry does not invalidate a reusable published
image: current execution validity comes from the new grant and keyset.

The result is immutable evidence, not a start receipt. It neither verifies actual
downloaded source bytes nor proves a current committed grant revision, worker
capability, claim liveness or one-use start consumption. No signer operation,
worker verification adapter, catalog fallback or runtime default is added by that
verifier. Those boundaries must remain fail-closed before any sidecar, task or
verifier container starts; Phase 1 behavior is unchanged.

### Refundable legacy claims and migration order

Application migration `0147` adds the nullable legacy identity without upgrading
old claims. A pre-start `node_setup_health` refund releases admission in the
`AFTER UPDATE` trigger using `OLD.attempt_count`, in the same transaction as the
counter decrement. Only a released
legacy reservation explicitly marked `trial_setup_refund` leaves the attempt/role
uniqueness fence; active reservations, other release reasons (including NULL),
and service/protected owners retain their fence. A later claim inserts a new
reservation, keeping every earlier row immutable and applying the usual shared
capacity checks and counter update. No old reservation is recycled.

Install guard migration `guard_0033` before this application migration wherever
the protected claim function exists. It changes only that function's conflict
syntax to target-free `ON CONFLICT DO NOTHING`, retaining all unique checks,
function ownership, permissions and previous claim fences. This is compatible
with both the old constraint and the new partial index. The application migration
refuses an installed incompatible guard; it never changes guard-owned code.
Guard label rollback retains compatibility. Application downgrade refuses retained
new claim identities or refund records rather than discarding fencing history.

The repair is prospective. Before activation, inventory active legacy admission
reservations on nonactive Trials. Historical refunds may already have leaked a
slot under the old trigger. This migration does not infer their ownership or
rewrite historical records/counters; any repair needs exact scoped reconciliation
and evidence. Code rollback keeps the additive schema until retained authority can
be handled safely; running an older worker is not permission for a lossy downgrade.

## Legacy-reader exclusion

The ordinary trial selector, default unified-work selector and locked unsigned V1
image snapshot reader require `ready_publication_operation_id IS NULL`. The
protected claim function applies the same predicate at both candidate selection
and locked V1 snapshot return. Only the explicitly configured, ready admission
service and exact registered V2-capable native reader enable the unified-work
selector's signed branch; that branch uses signed delivery and one-use start,
never an unsigned V1 snapshot. Protected-reader composition remains disabled.
The predicate is per materialization and compatible architecture, not a task-wide
veto: eligible Phase 1 work must not be starved by an earlier native trial or a
different native architecture.
Strong source manifests alone do not imply native readiness; Phase 1 may retain
and use those sources. Missing compatible legacy readiness cannot trigger an
unsigned native snapshot or a mutable-catalog fallback.

Guard migration `guard_0031` patches the effective existing claim function,
including its retry amendments, without changing its owner, signature, ACL,
security settings, claim identity or lock order. Application-owned bootstrap
convergence adds only SELECT on the native-publication discriminator to the
guard owner's existing column grant. Migration preflight checks that effective
permission before installing the function; it does not grant itself application
table authority. This is software support for protected convergence, not a live
grant or deployment.

The security predicate is retained on schema-label downgrade, and re-upgrade is
idempotent. Rollback must retain its narrow column permission as well; removing
it closes protected claims with a permission error rather than restoring unsafe
V1 admission. Earlier owning migrations still control eventual function removal.
Prerequisite inventory remains complete: it records readiness facts and is not
filtered as though inventory were execution permission.

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

The dedicated signing policy, durable complete-grant issuer, source-byte binding,
worker reader, serialized one-use start/revocation and runtime renewal are
implemented and opt-in composed, but are not provisioned or live-verified.
Renewal tests cover signed worker refresh/start across expiry, signer outage and
recovery, concurrent signing without held database locks, commit/lock failures,
readiness and cancellation-resistant shutdown. They do not establish native
provider acceptance. Production remains disabled pending release integration,
provisioning, genuine shadow isolation, both native containment
and scheduling campaigns, Phase 1 continuity, incident acceptance, rollback and
soak. No private signing keys, live state changes or runtime defaults are supplied
by this increment.
