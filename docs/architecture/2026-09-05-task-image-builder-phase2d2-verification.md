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

Push and verifier-pull JWTs retain Loom's RFC 7638 public-key thumbprint as their
audited key ID and include only the RSA public `kty`, `e` and `n` in a protected
`jwk` header. Pinned Distribution 2.8.3 interprets a `kid`-only token as a libtrust
fingerprint, not that thumbprint; the previous header was rejected despite a
matching trusted public certificate. Its supported JWK path derives the
libtrust identity, checks membership in the configured certificate-root keys,
then verifies the signature. Supplying a JWK does not establish trust. No private
key fields, inner JWK `kid`, or caller-provided certificate chain are emitted;
existing audit IDs and historical credential evidence are unchanged.
Both issuance methods reject encoded tokens above the existing 16 KiB bearer
ceiling before returning them; the wire model shares that same limit. Tests
cover 3072/4096-bit keys and exact-limit/overflow behavior.

Maintained tests run the actual pinned registry with a disposable trusted public
certificate: exact push/pull succeeds, other repository/action/issuer/audience
and untrusted keys fail, and the registry's 60-second expiry leeway is exercised.
Their loopback HTTP fixture tests token interoperability, not production TLS,
native routing, ongoing clock health or permanent retired-repository denial.

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
For empty images, BuildKit's explicit `null` spelling of manifest `layers` and
config `rootfs.diff_ids` is accepted as an empty list. Both fields remain required;
null cannot match a nonempty counterpart, and hashes/sizes bind the original
unmodified bytes. No other JSON or signed-publication null rule is relaxed.

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

### Compact completion receipt

The closed canonical receipt uses schema
`loom.task-image-publication-receipt/v1`, with operation, materialization and
attempt UUIDs, lease epoch, completing worker generation, frozen snapshot SHA-256,
candidate-set SHA-256, publication-set SHA-256, component count and whole-second
UTC `completed_at`. Its maximum encoded size is 2 KiB, independent of component
count; full envelopes never enter the 32 KiB guard polling packet.

Both set hashes are SHA-256 over RFC 8785 objects containing `schema` and
`components`. Candidate-set schema is
`loom.task-image-publication-candidate-set/v1`; each member contains exactly
`candidate_id` and `component`, obtainable from validated V2 acknowledgements.
Publication-set schema is `loom.task-image-publication-envelope-set/v1`; each
member additionally contains `envelope_sha256`, hashing the entire canonical
publication envelope including its signature. Members follow the frozen plan's
order: task first **when present**, then lexical sidecars. Component names and
candidate IDs are unique, with one through 128 members. Sidecar-only plans remain
valid; the receipt contract does not invent a primary Dockerfile requirement.

The candidate-set hash binds identities only, allowing the supervisor to compare
its complete acknowledged set. Full V2 acknowledgement evidence is bound by
`snapshot_sha256` and must be revalidated through that frozen snapshot; the
identity hash cannot replace it. Together with the full-envelope publication-set
hash, these receipt bindings cover the complete stored evidence. They neither
authenticate callers nor verify signatures by themselves. Completion and replay
must derive them from exact validated rows, not accept caller assertions.

Completion samples one unrounded UTC instant after acquiring all required locks
and uses it for the final authority checks. Persist that same instant as both the
materialization's `ready_at` and the terminal job's immutable completion timestamp.
Derive receipt `completed_at` by truncating that instant to whole seconds, never
rounding it or sampling a separate clock. Recheck expiry after any subsequent
potentially blocking flush; a rejection rolls back the whole transition. Replay
compares the receipt against the retained job timestamp, since a later attempt
may change the materialization's current ready state. Wire timestamps never
replace unrounded expiry checks. Receipt parsing alone does not prove completion,
current readiness or execution authorization.

The fixed submit/poll API projects a closed
`loom.task-image-publication-status/v1` object, bounded to 4 KiB. It contains grant,
operation, materialization and attempt IDs, lease epoch, state, snapshot and
candidate-set hashes, and component count. Completed status requires its exact
receipt; failed status requires one bounded failure code. Queued/running statuses
contain neither. The full durable snapshot, worker lease and registry inputs are
not poll payloads. The API authenticates the current session and exact job
binding, and validates completed history before producing this projection.

The two fixed POST operations are `publication-submit` and `publication-poll`
under `/v1/projections/{grant_id}/materializations/{materialization_id}/`.
Both accept only the existing current-session operation request: session
credentials, operation/materialization/attempt IDs and lease epoch. Registry
origin comes from trusted service configuration. Polling an unknown operation
does not enqueue it. Submission and active polling revalidate the complete frozen
input under the established lock order; a nonlocking state observation never
takes the job lock before materialization/candidate locks. Completed replay
still requires current guard/session/attestation authority, but not the lease
completion cleared. Valid successor sessions do not rewrite original provenance.
Historical signature verification does not acquire epoch/key locks after the
request's grant/job locks and is not execution-start authorization.

Each operation has a five-second transaction deadline, including lock waits
and commit; timeout cancels database work, rolls back when uncommitted and returns
the same bounded unavailable response as other infrastructure errors. An uncertain
commit can be confirmed using the same operation ID. Traffic ceilings are shared
with the existing API operations. Responses contain only canonical compact status;
request bodies and private failure details do not enter logs or metric labels.
No HTTP request starts verification tasks, calls the registry/signer, or adds
production worker scheduling; those dependencies remain explicit and inactive.

The node guard exposes these same two fixed operations over its existing local
socket protocol. Requests name only the grant, operation, materialization,
attempt and lease epoch and must carry one sealed current-session descriptor
from the same admitted supervisor peer. The closed response contains `schema`,
`operation`, `response_id`, `grant_id` and inline `publication_status`; it carries
no response descriptor and uses the normal peer-bound ACK flow. The guard's
stdlib-only parser independently checks canonical status bytes, request IDs,
safe integer bounds, whole-second UTC timestamps and embedded receipt bindings.
Both its HTTP adapter and local service revalidate this boundary. No full
snapshot, registry destination, token, signing key or arbitrary failure text is
returned. Maximum-width status plus wrapper fits within 4 KiB; V2 candidate
metadata still requires the separate 32 KiB release configuration gate.

The guard cannot reconstruct candidate identities from an IDs-only request.
The supervisor must independently hash its complete validated V2 acknowledgements
and pin the first authenticated snapshot for subsequent polls. A well-formed
status is not itself signature verification, current readiness, or trial-start
authorization. Production activation remains gated separately.

The supervisor now composes candidate upload with receipt confirmation. One
controller keeps the session and lease alive from upload start through polling;
only upload runs concurrently, and cancellation always joins it before executor
or credential cleanup. The phase is bounded to two hours, each liveness/status
operation to five seconds, and consecutive failed status calls to three with
backoff. These local bounds do not extend grant, Slurm or verification-job
authority. The current session manager owns successor credentials installed by
upload refresh and supplies the exact session for subsequent claims.

A complete validated acknowledgement set produces one publication operation ID,
retained across ambiguous submit retries. A racing heartbeat failure can be
resolved only by an exact authenticated completed poll. Receipt-confirmed
completion skips ordinary lease release because the authority already cleared
that lease; other publication failures close the executor and release retryably
without charging deterministic task-failure budget. Full HTTP/guard/Go/worker
composition, release assembly and activation remain separate acceptance gates.

The maintained composition fixture now drives the real Go orchestrator through
the sealed-descriptor guard protocol, production authority parsers, authenticated
API routes, disposable migrated PostgreSQL and the publication worker. It verifies
streamed TLS registry bytes and independently checks the durable Ed25519 test
signature. Cases cover source-driven session renewal, corrupt registry bytes,
revocation during signing, and atomic completion before a racing heartbeat. The
last case receives a real heartbeat conflict and confirms the exact completed
receipt without releasing the cleared lease. CI requires these flows with a
race-instrumented Go helper; an absent helper cannot silently skip the lane.

This fixture substitutes node/Slurm/containment/storage adapters, bundle download,
build and registry upload, test signing/keyset distribution, and the authority
HTTP byte transport (ASGI). It does not prove native containment, real registry
upload, authority mTLS networking or production signing. Those boundaries retain
their separate tests and activation gates. Bootstrap exchange preserves the
precise authority-issued receipt timestamp, including fractional seconds, when
forming its deterministic UTC observation; truncation would backdate the request
before receipt issuance and correctly fail authority admission.

### Bundle signing deadline

Bundle issuance keeps one absolute deadline: the earlier of frozen plan
authorization expiry and request-start plus configured URL lifetime, rounded
down to a UTC second for SigV4. Listing or signing delays never move it forward.
The backend receives that deadline, not a duration calculated before I/O. It
must calculate the signature's duration from the actual signing timestamp and
use credentials valid through the requested deadline.

The provider checks a live clock before/after listing, before/after each signing
call, and after constructing/serializing the complete capability. Clock
regression or exhausted authorization fails closed. A returned URL's signing
timestamp cannot be later than the post-signing observation, and its encoded
expiry must equal the common deadline. Requiring the same expiry for every
object avoids advertising a capability lifetime beyond an earlier-expiring URL.
This permits valid signing after request start without extending authorization.

Deterministic tests cover elapsed listing/signing time, expiry during I/O and
response construction, backward/future clocks, exact deadline matching, and
subsecond rounding. The composed fixture retains an actual advancing signing
clock. These are provider-contract checks: the configured runtime described below
composes the timestamp/deadline contract and bounded network I/O;
test backends and post-call clock checks are not that evidence. The provider
continues to fail closed when none is configured. The HTTP composition described
below also validates encrypted replays and checks freshness across persistence,
transaction commit and response serialization.

The CPU-only S3 presigner now signs exact public-origin path-style
`/bucket/key` GET capabilities from an explicit immutable credential snapshot.
It owns the signing timestamp once and derives the SigV4 duration from that
timestamp to the absolute deadline. It reuses botocore's S3 canonicalization
and HMAC without invoking the base debug logging of canonical queries/signatures
or changing process-global signing state. Temporary tokens require known expiry
covering the whole requested window; static credentials have no ambient-provider
fallback. Targets, UTF-8 key length and final URL size are bounded.

An isolated pinned MinIO fixture verifies these signatures over trusted TLS,
including spaces, plus signs, percent signs, Unicode and literal `%2F` in keys.
Changed bucket/key/method/Host-port/signature and expired or wrong-secret
requests are rejected by the actual server. This establishes the signing
primitive, not an S3 backend or IAM policy: test credentials are disposable and
the synchronous compatibility provider defaults to bucket-host paths. Production wiring must
compose explicit path-style validation, bounded asynchronous listing/parsing,
owned credentials and unlocked I/O followed by fresh database admission. No
storage network work should retain the heartbeat/renewal authority locks.

The authority also has a CPU-only ListObjectsV2 signer bound to the exact bucket,
prefix, page size, continuation token and immutable deadline. These listing URLs
are authority-only bearer material, never builder capabilities. A bounded XML
parser checks request identity, single key decoding, exact sizes, unique ordered
objects, counts and continuation progression. It rejects oversized input before
parser allocation and bounds XML depth, nodes and text; DTDs, entities and
processing instructions are refused. Unsupported encodings return a fixed error
without echoing response-controlled text. This in-memory bound is not a network
receive or decompression bound; those remain the asynchronous owner's job.

The pinned native MinIO fixture encodes spaces as `+` and literal plus signs as
`%2B` in URL-encoded listing XML. The parser requires an explicit `form` versus
`percent` contract instead of guessing from key text. Actual TLS tests paginate
using both independent SDK and production signatures, preserve space/plus/Unicode/
literal-percent identities, fetch the parsed keys, and verify rejection of changed
signed listing parameters. This evidence does not establish cross-page inventory
immutability or production wiring.

A separate one-shot asynchronous TLS reader now consumes these authority-only
listing URLs for one fixed origin and bucket with an explicit trusted CA. It
bounds raw response headers before HTTP parsing, total received bytes and body
bytes; it refuses redirects, informational responses, compression and trailers.
Each request owns a connection, aborting it on success, failure or cancellation.
Connection-handoff cleanup retains ownership even when cancellation races with
the returned socket. Admission and all I/O share the caller's monotonic deadline,
with finite connect/idle/per-read ceilings and no retry loop. Lifecycle shutdown
cancels and joins active and queued reads. Actual TLS tests exercise malformed
wire input, trust failures, queue/deadline/cancellation behavior and orphaned
connection disposal; the pinned MinIO integration exercises production signing,
reader and XML parser together across paginated responses.

This is a transport primitive, not production activation. Path-style consumer
alignment and deployment credential provisioning remain required; the HTTP route
and configured runtime support the unlocked async composition below.

The native MinIO backend now owns the reader and an explicit static signing
identity. It rejects ambient or session-token credentials and returns only
complete, nonempty inventories. Across pages it bounds object count and bytes,
total XML bytes, page count, lexical key progression and continuation-token
cycles. Raw network bytes are bounded by the page-count ceiling multiplied by
the reader's per-page wire ceiling. One monotonic operation deadline includes all
pages and signing; advancing wall time can shorten, never reset, that deadline.
Every signing/read/parse boundary checks authorization expiry and clock regression.
Closing the backend closes the owned reader and disables both listing and signing.
The pinned TLS MinIO tests exercise exact complete inventories and subsequent
GETs, as well as rejection of missing or excessive inventories. These do not
prove immutable source provenance, retained metadata integrity, or a configured
live service. Database admission is covered separately below.

An asynchronous capability provider composes that backend with the same inventory,
deadline, object and response validation used by the synchronous injected-backend
compatibility path. Addressing is explicitly `path` or `bucket-host`; the native
MinIO composition selects `path`. Real TLS tests issue capabilities and fetch
their exact object URLs, including ports, spaces, plus signs, Unicode and literal
`%2F`. These component checks must not be treated as an activated service.

The bundle HTTP route now uses two short READ COMMITTED transactions around
unlocked storage I/O. Preparation authenticates the current bearer and checks the
grant, generation, materialization lease, retirement state and frozen claim inputs.
It releases its database session before awaiting listing/signing. Finalization
authenticates again, re-locks current authority and compares the prepared inputs
before persisting one encrypted capability and operation receipt atomically.
Concurrent requests for the same operation return the validated persisted winner;
replay skips storage listing but not capability validation. Exact operation-ID
unique-constraint conflicts return 409 and roll back the losing secret.

The original claim is provenance for the grant-owned attempt, not a requirement
to keep using its original session. Its canonical payload/digest and original
attempt identity remain checked. A renewed successor can issue a newly bound
capability without changing frozen build inputs; the superseded bearer cannot
finish an in-flight issuance or replay a claim as the successor.

Issuance lifetime is bounded by the current grant, attestation, session and lease.
Heartbeat extension never extends an already minted capability. Live clock checks
cover lock waits, secret persistence, both sides of commit, cached replay and
response serialization; expiry or regression fails closed. Expiry during commit
can leave an expired receipt, but it cannot return an expired capability. Disposable
PostgreSQL HTTP tests cover heartbeat/renewal progress during listing, release,
retirement/revocation, source drift, cancellation, concurrent winner/replay, clock
changes during actual row-lock waits and persistence/response expiry. Actual SQL
constraint faults verify conflict mapping and rollback; they are not a full
cross-grant concurrent HTTP test. The synchronous injected-provider compatibility
path also runs outside HTTP transactions, but is not production async I/O evidence.

### Configured bundle runtime

The service entrypoint constructs the native provider only when
`LOOM_TASK_IMAGE_AUTHORITY_BUNDLE_BACKEND=minio`. The default is `disabled`;
origin/bucket settings alone do not enable issuance. MinIO mode requires all of
these authority-prefixed settings:

| Suffix after `LOOM_TASK_IMAGE_AUTHORITY_` | Required value |
| --- | --- |
| `BUNDLE_PUBLIC_HTTPS_ORIGIN` | Canonical origin-only HTTPS URL, with exact public port if needed |
| `BUNDLE_EXPECTED_BUCKET` | Exact approved bundle bucket |
| `BUNDLE_REGION` | Explicit signing region |
| `BUNDLE_CREDENTIALS_FILE` | Stable owner-only regular file, owned by the service UID with mode 0600 |
| `BUNDLE_READER_CA_FILE` | Explicit trusted CA for that endpoint |

The credential file is bounded to 16 KiB and contains exactly `schema_version`
(integer 1), `access_key` and `secret_key`. Duplicate fields, extra fields,
temporary tokens, invalid keys, symlinks and permissive credential-file modes are
rejected without returning their content. No ambient AWS identity or region is
used. The identity is read once per lifespan; rotation requires a service restart.
It stays in the authority process, never in a builder allocation.

The native backend always selects path-style URLs and the existing bounded
transport/inventory defaults: 256 entries/page, 16 pages, 16 MiB total listing XML,
a 30-second inventory deadline, four simultaneous reads, five-second connect/idle
limits, and 32 KiB headers/4 MiB body/8 MiB wire limits per page. Existing
`BUNDLE_MAXIMUM_OBJECTS`, `BUNDLE_MAXIMUM_BYTES` and `BUNDLE_URL_EXPIRY_SECONDS`
further bound each capability; grant/session/lease expiry can shorten its lifetime
and I/O time budgets.

Lifespan startup validates configuration and credentials, constructs the owned
backend, and checks the database before advertising service readiness. Invalid
configured startup stays unready. Failed/cancelled startup and normal shutdown
close the backend and dispose the database engine; a later lifespan creates a
fresh backend. Schema-check failure releases resources immediately, rather than
retaining an idle pool while serving unready. Injected compatibility providers remain caller-owned and cannot
be combined with native mode. Readiness establishes local construction/schema
validity, not remote credential usability or native builder activation.

Maintained tests separately exercise configured runtime issuance and exact GETs
against pinned TLS MinIO, and historical-clock HTTP/SQL lifecycle composition
with only the storage wire response substituted. They do not establish a complete
HTTP-to-native-Go download. Deployment still needs the approved native identity,
bucket policy and owned credential-file copies: Kubernetes Secret projections
are not automatically suitable owner-only regular files. No deployment manifest
has been enabled by adding these settings.

Native downloader alignment is also still required: the production Go
`DownloadBundle` currently accepts a different `schema`/`files` capability than
the authority's `schema_version`/`objects`, and builds requests with empty URL
queries. It cannot consume these signed object capabilities unchanged. The
authority/guard/Go-orchestrator fixture substitutes bundle download and therefore
does not prove this boundary. Align exact signed URL preservation, independently
trusted TLS configuration, deadline/session bindings and authenticated content
metadata together; do not remove the downloader's integrity checks to accept an
incomplete capability.

### Registration-bound bundle content

The legacy mode sidecar is not a per-file content manifest. The existing task
checksum also cannot replace those checks: its delimiter-only stream can hash
different distributions of bytes across the same file paths identically. A
maintained regression constructs such inputs with equal mode metadata. Phase 1's
checksum algorithm is retained for compatibility, not promoted to stronger native
content authority.

The new `loom.task-image-bundle-content.v1` contract binds sorted data-file paths,
individual SHA-256 hashes, byte sizes and portable `0644`/`0755` modes, together
with the legacy task checksum and mode-sidecar digest. Its bytes use RFC8785;
the mode sidecar retains its distinct Python sorted/ASCII-escaped JSON encoding.
The parser requires the expected manifest digest, exact canonical bytes, bounded
file/path/byte counts, no file/directory conflicts and consistent mode provenance.
Content-manifest and mode-sidecar bytes each have a separate 4 MiB ceiling.

Trusted capture walks a current-UID-owned regular tree using descriptor-relative
no-follow operations. It refuses symlinks, hardlinks and special files, bounds
tree traversal and file reads, and checks file/directory identities before and
after capture. The upload path can explicitly accept this captured manifest: it
rechecks the complete source before writes, then opens and verifies each exact
file after preceding asynchronous operations. The immutable verified bytes are
passed directly to storage. Drift fails without publishing a new manifest; any
already-written objects remain incomplete, non-authoritative artifacts.

Manifest-aware upload requires a content-digest-bound data prefix. It writes the
new manifest last at `loom-bundle-manifests/v1/sha256/<digest>.json`, outside the
data prefix, while keeping the old mode sidecar inside it. Thus legacy prefix
downloads retain the same authored files, checksums and executable modes. These
checks establish bytes supplied to storage, not remote storage immutability or
native download acceptance. A manifest object may already exist from a different
prefix; its existence is not a successful-publication receipt for this prefix.
Registration must require the complete upload to succeed before binding provenance.

The native storage backend can now retrieve that exact registered manifest over
its configured TLS origin and CA. Its authority-only reader permits only the
digest-derived reserved object path; the listing entrypoint remains restricted
to the bucket listing path. Both share the bounded concurrency/shutdown owner,
wire limits and shrinking absolute deadlines. The manifest read preserves the
signed request target, checks the registered SHA-256 and canonical encoding,
and binds the decoded task checksum and mode digest to the supplied frozen
values. It rechecks authorization after parsing and does not return expired
results. Tests exercise the actual signer/reader/parser over TLS and pinned
MinIO, including missing and corrupted objects. This establishes manifest-read
integrity only. The storage backend's stronger composition also matches the
complete prefix key/size set against the registered data descriptors plus the
exact canonical mode-sidecar size. One clock history and shrinking operation
deadline cover manifest retrieval and every listing page. Data limits remain
2,000 files / 512 MiB, with one separate transport object and its authenticated
size added only after manifest verification. Full terminal pages may require an
empty continuation probe; that probe retains all page/byte/deadline bounds and
rejects any additional object. The legacy listing entrypoint retains its limits.
This comparison does not hash stored data or sidecar bytes: native download must
verify every data-file hash, and must never download the transport sidecar into
the build context. Native V2 capability issuance consumes this composition.
Manifest presence is never proof of a completed prefix upload.

The explicitly versioned `loom.task-image-bundle-capability.v2` retains the
grant/current-session/materialization/time/count envelope and adds the registered
manifest digest. Its objects contain only data-file relative paths, sizes,
per-file SHA-256 hashes, portable modes and short-lived exact GET URLs. It does
not duplicate the manifest or download the mode sidecar. Reconstructing the
canonical manifest from these descriptors (excluding URLs) must reproduce the
registered digest and mode provenance. The asynchronous provider checks complete
inventory before signing and independently validates the backend's manifest
against the frozen plan. Limits remain 2,000 data files, 512 MiB data, 4,096 bytes
per URL and 8 MiB per capability.

Issuance uses one exact whole-second deadline bounded by current authorization;
storage/signing/validation cannot extend it. Clock regression or expiry rejects
the result. Cancellation propagates into the owned inventory reader before
signing. New and encrypted-replay capabilities use the same CPU-only versioned
validator, binding descriptors, limits, exact URL origin/path/deadline and the
current issuance session without repeating storage work. The unlocked HTTP
prepare/finalize flow and bounded encrypted reader preserve either explicit
version. Legacy V1 capabilities cannot satisfy a V2 plan, and the synchronous
compatibility provider rejects strong plans before storage. Pinned TLS MinIO
tests exercise verified upload, inventory, V2 issuance and actual signed GETs,
including Unicode/plus/percent paths. These are authority interoperability tests,
not by themselves evidence for native admission.

The native Go V2 reader independently reconstructs those manifest and legacy
mode bytes. Paired Python/Go vectors cover quotes, HTML-sensitive characters,
Unicode line separators, BMP/astral ordering and surrogate pairs. It rejects
malformed UTF-8, escaped lone surrogates, missing/null mandatory fields, changed
descriptors, identity/session mismatches, foreign origins/paths, expiry, duplicate
query parameters and V1 downgrade. Go's ordinary JSON encoder is not used as a
general RFC8785 encoder; a closed-schema encoder covers the manifest's validated
path/integer domain and preserves the distinct legacy mode encoding.

The new registered downloader consumes that reader and separately supplied TLS
trust. It preserves complete signed URLs, disables proxy/ambient authorization,
redirects and automatic decompression, and bounds headers, TLS/dial/header waits,
socket read idle time and the whole operation deadline. Informational responses
are rejected. Data is written through no-follow descriptors into a newly owned
private input directory, separate from allocation runtime/output state. Each
file must match its registered size/hash and portable mode. Clock and context
checks surround I/O; filesystem sync cannot be interrupted, so a final post-sync
check prevents expired acceptance. Failure cleans through the pinned private
directory descriptor and surfaces ambiguous residual cleanup.

The verified result hands off a duplicate directory descriptor, never a cached
absolute pathname. A job-directory rename/replacement regression proves that
acceptance and cleanup stay on the original input, preserving replacement data.
The descriptor-aware executor constructor now owns a duplicate input FD and lends
exactly child FD 3 to `buildctl` for its context/Dockerfile local directories.
Daemon and readiness launches retain no extra descriptors. The pinned executable
FD is kept outside the child-remapping slot; mandatory cgroup launch and readback
remain unchanged, without fallback. A real ELF exec test exercises descriptor
inheritance across directory replacement with only cgroup admission substituted;
it is not containment certification. Executor tests cover root context and
sidecar names, plan-slice ownership and exact component-path binding. Executor
close now cancels its single active build and joins all build-local deferred
cleanup before releasing input. Concurrent closes serialize cleanup ownership;
waiters honor cancellation. An unsuccessful join reports ambiguous cleanup and
retains input for a later retry/allocation cleanup. The orchestration layer now
joins build wrappers before cleanup. A failed join retains allocation descriptors;
failed executor close retains quota-owned input and suppresses clean Finish.
Normal completion joins builds, finishes publication, closes the executor and
removes verified input before reporting built. A pathname reconstructed from the allocation directory is not
equivalent authority. An actual pinned TLS MinIO
fixture now exercises verified upload → V2 authority issuance → this real Go
downloader, including executable Unicode paths, without substituting a fake
downloader. A separate actual TLS/download orchestration fixture connects a V2
claim to descriptor input ownership with fake guard and executor boundaries. This
fixture is data-transfer acceptance, not a rootless native build or activation.

The supervisor configuration now accepts optional `bundle` trust with exact
`origin`, `bucket` and `ca` fields. The CA contains only `path` and bare `sha256`;
its path must name a regular, single-link, owner-verified 0444 data file within
the selected content-addressed release, below no-follow 0555 directories. Reads
are bounded to 128 KiB and verify unchanged metadata and exact content digest.
Only valid CA certificate PEM blocks are accepted, never private keys, ignored
malformed prefixes or non-certificate data. The owned certificate pool survives
later file replacement, and TLS does not augment it with ambient system roots.
A real TLS downloader regression verifies both properties. Absent trust remains
unavailable rather than granting capability-selected endpoints or certificates;
explicit null or incomplete trust rejects configuration. Release assembly still
needs to install/hash this data member and require configured trust before native
claims. Configuration parsing alone neither enables claims nor certifies a release.

The Go claim reader now retains the V2 manifest, checksum, mode provenance,
bucket/prefix and quotas as a separate immutable `RegisteredBundlePlan`. It
preserves the original builder identity and rejects missing/null strong fields,
V1 downgrade and invalid component/context bindings. Character limits match the
Python plan (including astral Unicode): 512 for task ID, 4096 for prefix and
component paths, plus the 64 KiB encoded plan ceiling. V1 does not gain registered
content authority. `runClaim` now requires configured trust and an explicit
registered executor factory for V2; missing dependencies fail without a V1
fallback. The production factory uses `NewExecutorWithContext`; independent
startup, publication and authority-derivation gates remain closed.

Registered preparation now composes actual guard capability issuance and the real
TLS downloader with a single prebuild liveness owner. A session manager lends at
most one owned, bounded locked-memory snapshot outside its mutex, so bundle I/O
does not block heartbeat/renewal. Superseding the original secret does not destroy
that active copy; returning from its callback destroys it. Production secrets
use distinct private anonymous mappings, locked before credential reads, then
zeroed and unmapped on close. Heap slices can share pages and Linux memory locks
are not reference-counted: closing the old owner previously unlocked a live
snapshot's page. Regression coverage checks disjoint pages and surviving locks.
Borrowed token slices are invalid after the owning credential closes. Registry
HTTP requests therefore own an explicit dial/connection scope: cancellation fences
late detached dials, joins admitted dialers, closes sockets and joins token writers
before credential rotation. Response-body closure alone is insufficient because
HTTP can return an early response while a writer still borrows the token. Connection
close serializes header cleanup with writing, and synchronized request-body closure
joins readers before upload chunks can be reused. Tests reproduce the early-response
case with real Go HTTP transport and controlled I/O scheduling, plus late-dial and
body-read cleanup races. The captured session
binds issuance only and never rewrites original claim provenance. Generation drift
during issuance gets at most three fresh-operation attempts; unchanged-generation
errors do not retry. Real TLS tests cover successor success, the attempt ceiling,
unchanged-error rejection and destruction of discarded capabilities.

One ten-minute prebuild ceiling bounds the phase; each issuance has a 45-second
ceiling and liveness calls have five seconds. Capability/grant/session/lease limits
remain independent and may expire earlier. Cancellation and failure join fetch
before input disposal, including success racing cancellation. Clock regression,
expiry and failed liveness reject acceptance. Cleanup ambiguity remains a distinct
error without exposing transport secrets. Native `runClaim` now calls this
preparation, then admits Start through the fresh current session with a five-second
operation bound. Exact operation, grant, attempt, materialization, epoch, running
state and future lease expiry are required; session expiry and clock regression
reject admission before executor construction. Tests exercise these bindings,
cancelled/unjoined input consumers, cleanup retention and no built outcome after
failed close. Guard/runtime assembly and the remaining publication/execution/
capacity activation gates still prevent production activation.

Supervisor startup no longer requires embedding the composite release digest
inside the ELF whose bytes contribute to that digest. It opens the kernel's
`/proc/self/exe`, derives a selector only from the exact fixed
`releases/<sha256>/bin/loom-task-builder-supervisor` path, and walks installation
ancestors through no-follow directory descriptors. Ancestors must be root-owned
and non-writable by group/other; the release and `bin` directories must be 0555.
The running executable must be the same root-owned, single-link 0555 inode as the
installed member, with unchanged metadata. Deleted paths, replacements, hardlinks,
symlinks and noncanonical selectors fail before guard access or environment changes.
The fixed root-owned supervisor config must name that same release.

This resolves a digest circularity, not a new source of release authority: the
installer still validates the composite inventory and member hashes, and the guard
independently verifies the ELF digest and grant-bound release before credentials.
There is no argv, environment or `current` symlink selector. Tests cover descriptor
identity failures and a real subprocess's kernel executable path; the latter
substitutes only the already-trusted fixture installation root and owner UID.
Offline conformance rejects a live config at both its historical path and the
actual `/etc/loom-task-image-builder/supervisor-config.json` path. No live config
is created by staging or by these startup changes.

Producer orchestration has not yet been switched to this stronger contract.
Registration provenance, native materialization identity, publication/retention
bindings, capability metadata and the real Go downloader must change together.
The additive identity foundation in migration `0136` reserves a separate identity
for strong manifests. `bundle_content_manifest_sha256` is empty only for legacy
rows; stronger rows carry the exact bare digest from source provenance. The
natural unique tuple includes this discriminator. Legacy v1 keys are unchanged;
v2 keys include the digest under a new domain, with database checks enforcing
both the key derivation and exact provenance binding. Existing rows are not
upgraded: unexpected preexisting content-manifest provenance aborts migration.
The discriminator cannot change in place, and strong rows also freeze their
task/config/source identity. Ordinary lease and lifecycle updates remain allowed.
The migration takes an upfront NOWAIT table lock and refuses downgrade while
any strong row remains, even if the old unique tuple would have no duplicates.

Application ensure now derives the discriminator from strict provenance and
creates distinct queued rows for distinct manifests, even when the old checksum
matches a ready image. Exact re-ensure preserves IDs. A strong identity with a
different frozen config, source or provenance fails instead of silently reusing
the prior snapshot; callers own the transaction and must roll it back on any
ensure error. Older insertion code carrying stronger provenance but omitting the
discriminator fails the database binding check before ON CONFLICT can reuse a
weak ready row. Current-catalog retention references match the full manifest
identity; historical nonterminal trial and execution references remain exact-ID
pins. No existing ready row is upgraded in place.

Python builders and trial workers verify a captured per-file manifest against
the registered digest before image lookup or runtime construction. Capture also
checks the legacy checksum and supplied mode provenance. Both main and sidecar
cache keys use a separate `bundle-manifest-sha256:<digest>` domain, while trial
metadata retains the original checksum. Verifying bytes and then looking up an
image by the legacy checksum would still permit incorrect cache reuse. These
checks require the downloaded directory to remain private and worker-owned until
use; they do not make a shared concurrently writable directory immutable. If a
materializer retained the transport mode sidecar, the worker validates its exact
canonical bytes against the verified file manifest and removes it from private
staging before any image lookup or runtime use. Bounded no-follow descriptor
reads require owned single-link regular files and unchanged descriptor/path
identity. A malformed or replaced sidecar rejects; unauthenticated transport
bytes cannot become extra Docker context under a verified cache key. This does
not modify the original source bundle or shared download cache, and it does not
infer missing executable modes: materialization must already have restored them.
Legacy bundles retain their original checks and cache keys. Strong execution
grants additionally validate the manifest-qualified materialization key.

The native plan reader now supports an explicitly versioned
`loom.task-image-build-plan.v2` carrying the mandatory registered content-manifest
digest and a content-qualified bundle prefix. V1 fields, defaults and wire bytes
remain unchanged; retained canonical hashes are not rewritten. Claim replay and
registry/publication/retirement readers preserve the version. Publication,
credential and retirement inventory readers require the plan discriminator to
match the materialization column and recompute the manifest-qualified key. The
restricted retirement snapshot includes that column for detached validation and
locked recheck. A successor session still cannot rewrite the original claim's
identity. Database consumer tests seed explicit strong receipts; they are not
evidence that native admission or content download is complete.

Native derivation still rejects strong provenance before changing a lease.
The production orchestrator selects the real registered Go downloader and
descriptor-owned executor, but manifest-bearing authority derivation and producer
orchestration remain unswitched. Inventory matching, V2 capability issuance and
tested data transfer do not open that admission boundary. Do not infer manifest
authority from mutable stored objects. Native admission remains closed until the
complete producer-to-downloader path and remaining activation boundaries are
verified.

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

The pinned runtime will emit opt-in exporter metadata under
`loom.task-image-base-resolution.v1`, requested only by the trusted supervisor
with `loom.capture-base-resolution=v1` and the fixed `dockerfile.v0` frontend.
Its JSON object has exactly `schema` (`loom.task-image-base-resolution/v1`),
`solve_ref`, `platform`, `output_digest`, and `observed_base_digests`. The runtime
collects successful image-source resolutions across that solve's provenance
bridges, even when the result has no filesystem reference, and emits a sorted
unique list of SHA-256 digests. Empty means observed no image sources in that
successful solve, not absence of a history record. The reserved response field
is installed after frontend metadata copying; request/frontend metadata cannot
choose its contents. Missing/unknown opt-in versions or unsupported frontends
do not yield this evidence. Invalid observed digests or exceeded bounds fail
the opted-in solve closed rather than dropping observations.

Frontend identity is checked against the solver's recorded frontend request,
not merely the outer client envelope: pinned `buildctl` intentionally clears
the outer frontend while invoking `dockerfile.v0` through its gateway API.
The empty gateway envelope alone is not evidence of an accepted frontend.
Absent, ambiguous or unsupported recorded frontend paths cannot produce this
record; tests must cover the real gateway-driven exporter flow.

The response uses BuildKit's base64 JSON carrier, so `--metadata-file` exposes
the record as a JSON object. Decoded metadata is bounded to 16 KiB, at most 128
unique image digests, 4,096 raw resolution observations and 4,096 visited
provenance bridges; solve references are 1–128 ASCII alphanumeric,
underscore or hyphen characters, starting alphanumeric. Platform is exactly
`linux/amd64` or `linux/arm64`. The supervisor must compare solve reference with
its own `--ref-file`, platform with its frozen plan, and output digest with its
exported OCI root. It captures bounded metadata through the existing contained
launcher before executor cleanup, emits no raw source/history/log/host-statistics
payload, and supplies same-attempt evidence to the authority's fixed operation.
This runtime extension and its consumers require real exporter tests and new
deterministic dual-architecture release hashes before composition.

### Versioned candidate evidence

The D2 candidate request uses an explicit version 2 and requires the complete
validated `base_resolution` object. Its output digest and platform must match
the candidate and the authority-derived frozen plan. The solve reference is
validated and retained as same-build provenance; it is not a new authorization
credential. Empty observations are accepted only as an explicit array inside a
valid record. Unknown fields, missing evidence and malformed bindings reject.

Persist that record atomically with the candidate in a version-2 canonical
acknowledgement using the existing `response_json` and `response_sha256` columns.
The stored acknowledgement binds both the archive facts and the metadata; every
replay and verification snapshot validates its schema, canonical digest and
row bindings. A changed record at the same operation or attempt/component is a
conflict. A second operation to attach metadata is unnecessary and would create
a partial-state interval. The evidence remains immutable input, not readiness.

Version-1 requests, responses and existing rows retain their original meaning.
They cannot satisfy D2 verification, acquire synthetic empty observations, or be
upgraded in place by replay. The new fixed guard/authority operation must carry
and acknowledge version-2 evidence explicitly, with no fallback to version 1.
No registry destination, signing authority or complete publication set becomes
caller-selected. The later durable job freezes only complete validated V2 sets.

Production signing authority resides behind a dedicated host signing service or
KMS/HSM; private keys do not enter the allocation or an authority HTTP request.
Test signers use generated keys only. Key records distinguish active,
verify-only and revoked. The future signed keyset and execution-start increment
must consume these exact envelopes and serialize against this durable epoch.
The [execution-trust contract](2026-09-10-task-image-execution-trust.md) now
implements bounded signed-keyset and one-component verification primitives;
authenticated distribution and complete online start composition remain closed.

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
The signed statement additionally binds `signing_key_id`,
`distributed_keyset_version` and `revocation_epoch` alongside signer-stamped
`issued_at`. A trusted distribution snapshot must bind current durable version,
epoch and key membership, and have whole-second UTC issue/expiry times valid at
signing and reply verification, with at most a fifteen-minute snapshot lifetime.
This freshness ceiling is distinct from the future signed keyset's lifetime.
Snapshot validity must also be bounded by the underlying authenticated keyset
and distribution evidence: re-stamping old evidence with fresh local timestamps
cannot extend trust, and refresh requires current valid authority.
It is the output of the future authenticated
signed-keyset/distribution adapter, never a caller assertion; no production
adapter is composed by D2. Replies allow at most five seconds of signer-clock
skew around the authority's request/receive interval, and signer I/O has a
bounded deadline (five seconds by default, never over ten).

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

### Dedicated signer transport

`publication_transport.HTTPSPublicationSigner` implements the existing signer
protocol over one fixed `POST /v1/publications/sign` operation. Trusted service
composition supplies an origin-only HTTPS URL, a dedicated server CA and a
client TLS certificate/key. The connection requires TLS 1.3, verifies the server
hostname, and presents the authority's client identity. The remote service must
authorize that identity using a dedicated client CA and constrain its operation
to the publication schema/domain. A certificate shared with node guards or
allocations is not suitable. The client's TLS authentication key is distinct
from publication and execution signing keys; neither signing key enters the
authority process or a task allocation.

The client rejects noncanonical or invalid unsigned publication input before
network I/O. Tasks cannot choose an endpoint, algorithm, credential or arbitrary
signing bytes. It does not use environment proxies, redirect, decompress or
retry. Each call owns one connection, with bounded concurrency and a monotonic
deadline that includes queue time (five seconds by default, at most ten). Raw
headers are bounded to 32 KiB by default (64 KiB maximum), total wire bytes to
1 MiB and the body to the caller's limit, never above the existing 128 KiB signer
reply ceiling. Raw framing checks reject duplicate/list content lengths,
transfer encoding combined with content length, and folded headers before HTTP
parser normalization. Informational responses, trailers, non-JSON and compressed
bodies are rejected. Close cancels and joins active and queued operations; cancellation,
deadline expiry and connect-handoff races retain socket disposal ownership.

Connection/deadline failures and HTTP 429/502/503/504 report transient failure to
the durable publication worker; authentication and malformed protocol responses
do not become catch-all retries. Errors contain neither response bodies nor
credentials. Successful transport returns untrusted envelope bytes: the existing
signature verifier still checks canonical identity, key, distribution snapshot,
epoch and clock, and the final transaction independently rechecks mutable state.
Real loopback mutual-TLS tests exercise that exact verifier path, identity refusal,
response limits and cancellation cleanup. They use generated disposable keys,
not a provisioned production signer.

This client does not activate publication. The authenticated signing service,
signed-keyset distribution adapter, worker envelope/keyset verification, one-use
start authority and protected native acceptance remain required before runtime
composition. No default adapter, signing key, distributed keyset counter or
provider switch is installed by this transport increment.

The publication migration can downgrade only an empty, inactive installation:
no keys, envelopes, jobs, registry credentials or candidates, and zero
keyset/revocation counters. A state-first, nonwaiting table-lock set precedes
any removal; a busy database refuses the downgrade and releases acquired locks
instead of risking deadlock with normal publication. Used authority and audit
history are retained during rollback; operational rollback is not schema reset.
Upgrade also acquires its complete preexisting DDL/FK table set without waiting.
If existing materialization, attempt or audit work holds incompatible locks,
the migration aborts for a later controlled retry rather than retaining audit
locks while waiting on an in-flight parent transaction.

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

The current rootless ready map has an explicit nullable publication-operation
binding, set atomically with verified completion, the exact ready timestamp and
immutable receipt. A restrictive composite foreign key binds that operation to
the same materialization. Existing Phase 1 rows retain NULL; upgrade does not
invent publication authority. A bound map, timestamp and operation cannot be
rewritten in place. Clearing ownership requires clearing the map and timestamp
and leaving ready state in the same update. Administrative retry does this while
retaining completed jobs and their historical receipts.

Legacy GC excludes bound rows before observations, recovery and leasing, even
when legacy image history coexists. It rejects pending materialization edits
before observing work and freshly reloads its selected row under lock, so a
cached rootless map cannot be returned after a concurrent Phase 1 rebuild.
Legacy evidence recording rejects grant-bound attempts before appending history;
the separate rootless authority owns their publication and failure lifecycle.
A subsequent actual Phase 1 completion after explicit retry can own a new
unbound map; historical rootless receipt replay
does not restore its old readiness. This binding is current ownership/provenance,
not signature verification, execution authority or permission to delete bytes.
Rootless retirement validates the exact immutable attempt, completed job,
image map and receipt under its database reference fence. A NULL pointer alone
does not prove arbitrary repository history safe for deletion.

Attempt repository discovery uses retained registry credentials, not only
candidate callbacks: a completed push or partial upload may outlive a lost
callback. A pure validator derives exact production attempt/component paths
from the frozen plan, validates every supplied credential's public schema,
canonical hash and row bindings, and collapses complete predecessor chains into
one repository inventory with the maximum expiry across all generations.
Registry service/issuer/key rotation and equal-second issuance timestamps do not
invalidate historical inventory. Every generation's public-binding digest stays
in the canonical evidence; no bearer or secret-store read is needed.

This inventory does not itself prove that all database rows were loaded, that
they are immutable, or that the attempt is unreferenced, retired or safe to
delete. The owned retirement transaction establishes database inventory and pin
observations; host maintenance and execution-start composition remain pending.
Claim replay shares materialization-before-attempt
locking with publication: its initial identity lookup is non-authoritative,
and both rows are freshly reloaded and revalidated after acquiring the locks.

Inventory preparation now uses an owned, READ COMMITTED read-only transaction to
load every credential generation for one attempt, with an explicit overflow bound. It loads
only the fields needed by public validation, not secret-store references. The
connection closes before CPU-heavy validation runs. The resulting immutable
evidence retains all parent/attempt identity inputs and the full bounded plan.
A separate recheck uses fresh SQL for those exact identities and counts at most
the prepared credential count plus one. Under active immutable-audit guards,
unchanged cardinality proves that no credential was appended; a mismatch requires
releasing the entire fence and preparing again outside it. Both phases reject
disabled, conditional or column-specific credential immutability guards, or
non-origin replication mode. They also require the permanent INSERT guard below:
an enabled, unconditional, nondeferrable AFTER ROW INSERT trigger bound to the
VOLATILE definer function with its fixed search-path and row-security settings.
Schema administration remains trusted across the interval between them.

Isolation is set on the checked-out connection before starting the owned session
transaction: inherited engine hooks can override derived-engine options. Tests
inspect PostgreSQL's actual isolation and read-only settings, including callers
configured for REPEATABLE READ or AUTOCOMMIT.

The recheck helper requires its caller to own the catalog/parent/attempt
fence and transaction deadlines. It does not acquire locks, inspect references,
retire an attempt, or independently make later raw database INSERT safe. Prepared
evidence is internal data, never a caller-supplied retirement authorization.

The unpublished publication migration adds database immutability for retained
credentials and candidates, including rows issued before the upgrade. Statement
triggers reject UPDATE, DELETE and TRUNCATE, including no-op updates; issuance
and read-only replay remain available. This protects the first-credential/no-
candidate inventory gap without changing published migration `0131` or rewriting
historical evidence. Application validation remains necessary; database-owner
fault injection is not prevented by triggers that such an owner can disable.
The owned retirement transaction establishes the complete inventory under its
shared fence. Execution-start references and registry quiescence remain separate
activation requirements.

An immediate AFTER INSERT row trigger permanently rejects credential rows for a
retired attempt, including raw SQL outside normal issuance. It checks the final
row identity after BEFORE-trigger transformations. Its exact attempt KEY SHARE
lock conflicts with retirement's FOR UPDATE fence without adding earlier
materialization, grant or session locks. A separate subsequent marker SELECT in
the VOLATILE function obtains a fresh READ COMMITTED snapshot after any wait:
retirement commit rejects the insertion; retirement rollback permits it. An
INSERT winner holds its attempt lock until transaction end, making retirement
fail NOWAIT and retry inventory preparation. One rejected row rolls back the
entire multirow statement.

Credential INSERT explicitly requires READ COMMITTED. REPEATABLE READ and
SERIALIZABLE fail closed because locking an unchanged attempt cannot refresh an
old snapshot of its separate retirement marker. Read-only credential replay does
not INSERT, but still requires the application retirement admission contract
below. Build-admitting HTTP transitions explicitly select READ COMMITTED on
their owned connection before the first query. Control and cleanup-only routes
retain the engine's SERIALIZABLE default; pooled connections restore that
default after admission. Switching the whole authority engine is unnecessary.
A narrow SECURITY DEFINER
function uses fully qualified tables,
`search_path=pg_catalog` and `row_security=off`; PUBLIC execution is revoked.
Restricted callers cannot hide retirement through search-path or RLS policies.
An owner itself subject to RLS fails closed rather than silently missing rows;
trusted schema/replication administration remains outside this protection.
Inactive downgrade removes this trigger/function before the retirement table.
This fence prevents new database credential audit rows, not registry requests
using previously issued bearer tokens or clock-based token revival.

Attempt retention now has a restrictive attempt foreign key and a durable
observation record. Observation time cannot regress or move to another attempt.
Retirement freezes a bounded inventory and its matching digest; UPDATE, DELETE
and TRUNCATE cannot discard or rewrite that evidence. Exact no-op replay remains
permitted. Inactive downgrade refuses any retired record and takes its complete
table-lock set, including the attempt parent, with NOWAIT before removing guards.
These storage checks do not certify inventory semantics or retirement eligibility.

Builder claim/input admission, fresh and replayed start/heartbeat, registry
credential issuance/replay/renewal, candidate admission and atomic publication
completion now consult fresh retirement state while holding the shared
materialization fence. Cached unretired observations and an older otherwise-live
clock cannot bypass retirement. Builder and publication admission reject pending
retention edits before autoflush; builder admission also rejects pending parent
and attempt edits before refreshed reads can discard caller-owned state.
Cleanup-only release/failure operations still require
their normal identity, session and lease checks; skipping the artifact-retirement
check does not grant build inputs or publication authority. Historical completion
receipt replay remains read-only and cannot restore readiness.

The shared marker lookup enforces READ COMMITTED and a non-null
`pg_catalog.pg_current_xact_id_if_assigned()` in the same SELECT. The preceding
parent FOR UPDATE assigns that transaction ID; AUTOCOMMIT releases it before
the later SELECT and is rejected. An assigned ID is a transaction-mode check,
not proof of exact parent lock ownership: callers must still acquire and retain
the documented shared parent fence. REPEATABLE READ and SERIALIZABLE are rejected
because locking an unchanged parent cannot refresh a separate marker's old
snapshot. Actual owned retirement followed by claim/plan/publication admission
is covered under all three modes, including deliberately old otherwise-live time.

HTTP claim, start/heartbeat (including replays), bundle, registry credential,
candidate V1/V2, and publication submit/poll own READ COMMITTED. Projection,
session control, revocation and cleanup-only operations retain SERIALIZABLE.
The verification worker independently selects READ COMMITTED for its final
completion transaction before any query, even with a fixed-snapshot session
factory; other worker transactions retain the supplied default. Direct store
callers own this transaction contract and must roll back on any failure.
Incompatible HTTP callers fail closed with a bounded 503. Historical receipt
reads remain non-admitting. These database/HTTP checks do not establish registry
token revocation, native containment, collector activation or safe physical GC.

`observe_or_retire_attempt` now owns one bounded, READ COMMITTED transaction per
attempt. Credential inventory and publication metadata are prepared in separate
read-only transactions; their connections close before bulk schema/canonical
validation runs off the event loop. The writer takes `tasks SHARE NOWAIT` before
materialization, exact attempt, publication job and retention-row `FOR UPDATE
NOWAIT` locks. It rechecks the full prepared identities, bounded credential count,
exact job/candidate values and completed envelope identities before mutation.
The five-second monotonic transaction deadline includes commit; one-second SQL
and idle-in-transaction deadlines additionally bound catalog-barrier ownership.
Before commit, contention, changed evidence, cancellation or timeout roll back
the whole transaction. A transport failure during commit can leave its outcome
unknown; retry recovers the immutable marker rather than assuming rollback.
A future collector must retry from preparation and observe skipped passes and
backlog age; catalog-wide locking does not guarantee progress under sustained
registration writes.

Pins include the exact live builder lease, queued/running publication before its
total deadline regardless of worker-lease expiry, the current ready owner with
a matching catalog checksum, and every completed attempt needed by a nonterminal
linked Trial or an execution lease without both deletion and completed cleanup.
The latter includes verifier leases and terminal Trials. Observing any pin resets
the grace observation. Otherwise the default grace is 168 hours for completed
publication and 24 hours for abandoned/no-envelope attempts. This is observation-
based grace, not proof of continuous absence between observations.

Retirement freezes the exact inventory and digest. Only the target attempt's
validated current ready operation/map/time is cleared, atomically with marking
the materialization retired; a newer lease or ready owner and legacy history are
preserved. Historical retirement replay does not rewrite evidence, including
when presented with an older clock. Later catalog registration or trial-link
ensure sees unavailable readiness and queues a rebuild. Earlier writers either
pin the images or make retirement fail promptly. Admission's scalar marker check
depends on this shared materialization fence, not a standalone racy lookup.

Publication preparation validates the stored canonical job/receipt, frozen-plan
identity, credential/candidate binding and complete envelope identity/time set.
It trusts the existing atomic completion writer and retained database metadata;
it does not reverify signatures or issue execution authority. Historical
cryptographic receipt replay remains independently implemented and unchanged.
The retirement entrypoint is deliberately not wired to a collector or runtime API.
Terminal verifier reservations can still add execution uses without reopening
a Trial or taking this fence, so the terminal-state invariant below is not a
substitute for rootless execution-start admission. Permanent registry-request
ingress denial, source lifecycle, authenticated offline maintenance, writer
quiescence and clock-revival protection remain requirements before deletion or
activation, even with the database credential INSERT fence in place.

The same unpublished migration prevents a terminal Trial from becoming
nonterminal through UPDATE, including changes made by BEFORE triggers. Its
row-local AFTER trigger raises a named constraint violation and rolls back the
whole statement. It permits nonterminal retries (including protected-pending),
metadata changes and terminal-to-terminal corrections. Installation and inactive
downgrade include `trials` in their upfront NOWAIT lock sets; runtime retention
does not gain a trials-table barrier. This closes later UPDATE reopening, not
deletion/reinsertion of Trial identities or later terminal-verifier admission.
The actual trial-link writer is covered by the shared materialization fence;
identity-recreation and execution-start safety remain activation requirements.

Reference ensure freshly reloads existing materializations under consistently
ordered row locks, so a cached ready object cannot survive committed retirement.
It rejects pending materialization edits before refreshing rather than discarding
caller-owned state. The returned architecture order is unchanged. This refresh
does not replace the task revision already supplied by a trial submission with
the catalog's newer revision; full registration/reference retirement fencing is
a separate pending contract.

Benchmark sync, local publication, catalog copy, adapter import, manifest
registration and taskset publication now ensure image prerequisites in their
Task-publication transactions. Unchanged sync and exact manifest re-registration
also recover retired prerequisites without replacing their identities. Sync
dry-run remains read-only; prebuilt-only tasks and empty catalog placeholders
do not gain image prerequisites. Updated Task rows are freshly returned before
ensuring, so a cached previous config/checksum is not reused. Taskset publication
retains its job/TaskSet lease fencing and atomically rolls back prerequisite or
publication failures.

Enqueuing is not rootless source admission. Before activation, source preparation
must reconcile external versus bundle-local task identities, retained file-metadata
digests, immutable object locations, and source retention for queued/historical
image prerequisites. Legacy adapter imports still use mutable instance prefixes;
taskset generation cleanup currently follows Task sources rather than image
prerequisites. These unresolved source-lifecycle boundaries are not cleared by
the registration tests or by a queued materialization alone.

### Strong-source preparation and recoverable writes

`prepare_task_bundle_registration` captures the content manifest, reads the exact
verified `task.toml` bytes, and records the relationship between authored and
catalog IDs without rewriting authored files. It preserves explicit architecture
choices; optional runtime fallback promotion reads the captured Dockerfile.
Parsed configuration and Dockerfile text each have a 1 MiB ceiling before the
verified full-byte read. The persisted `task_config_document` is a copied canonical
JSON view: set-valued network policies are sorted, while ordered steps/commands
remain ordered. Re-serializing its model view is not the persistence contract.
Cross-process hash-seed tests protect the normalized config fingerprint.

The source-storage primitives reuse `ObjectWriteResult` and add optional bounded
user metadata and required versioning to the existing object-store write API.
Legacy calls keep their existing requests and unversioned behavior. Strong writes
require an Enabled preflight and a non-null immutable version receipt. Metadata
is copied before asynchronous I/O; application and SDK retries carry the same
intent identity. A preflight cannot prevent an external versioning-policy change
or prove that an earlier timed-out request has stopped.

Each intended write therefore needs a **committed durable intent before storage
I/O**. Recovery lists only that intent's expected key prefix, reads exact versions
at the exact expected key, and verifies intent metadata, size and SHA-256. Equal
bytes from another intent do not establish ownership. `scan_batch` returns a
bounded verified batch and an intent-bound continuation for atomic journal
checkpointing. It preserves opaque MinIO continuation values without treating
them as object-key authority. An empty batch or observed end does not authorize
forgetting a tombstone: late versions require periodic fresh reconciliation.
Consumers must finish pagination before deleting its continuation-marker versions.

Disposable TLS MinIO tests cover disabled/suspended versioning, a delayed first
PUT arriving after its retry was deleted and an identical new publication was
created, exact-version cleanup preserving that new publication, and resumable
inventory across foreign versions and delete markers. These are storage-boundary
tests, not activation acceptance.

### Durable source publication and recovery

Migration `0137` adds an initially empty logical-source, upload-incarnation,
write-intent, exact-version and reference journal. It neither discovers historical
objects nor upgrades legacy authority. Logical identity uses the catalog ID and
captured manifest; data and auxiliary service-input manifest locations are stable
across generation replacement and upload retries, outside taskset generation
roots. The immutable source specification binds normalized config and provenance.
Physical upload IDs and object versions never enter materialization identity.
Data prefixes group by the SHA-256 of the catalog ID's first component, then
the full task-ID hash and manifest digest. Benchmark IDs cannot contain a slash,
so this yields a benchmark-scoped upstream locator without conflating task
identities. Auxiliary input manifests use the same grouping in their separate
namespace. Group membership is organizational, never read/delete authority.

The logical source row is the common publication/reference/retirement lock.
Callers acquire their catalog/trial/materialization locks first, then logical
sources in sorted order. Source retirement never acquires those caller locks in
reverse. All journal transitions use caller-owned database transactions with no
storage I/O. Each requires an explicit READ COMMITTED transaction: an unchanged
logical-source lock cannot refresh a fixed snapshot of separate incarnation and
reference rows. Separate transaction-ID assign/check statements reject AUTOCOMMIT
before ORM queries can flush caller-owned writes. Catalog publication and strong
image staging perform this preflight before their first flush or INSERT as well;
rejection must not leave a Task or queued image committed independently.
Publication needs a complete set of issued exact-version receipts
and attaches its reference atomically; competing complete uploads pin the first
available incarnation and retire only the losing upload. Retired incarnations
never revive. An available preparation ticket is not a reference: if retirement
wins before publication, the caller must roll back and prepare again, resolving
the current incarnation. Retries of an uploading incarnation preserve its
original deadline rather than extending its lifetime.

`TaskBundleSourcePublisher` commits intent issuance before verified file reads
and retrying PUTs, then commits each exact receipt. It returns a prepared ticket,
not a catalog publication. It caches immutable transport manifests and authored
key lookup, but rereads and verifies authored file descriptors on each upload.
Exceptions leave recoverable intent records, never prefix-delete compensation.

Recovery commits an inventory fence before first-page I/O, checkpoints receipts
and continuation atomically with an epoch comparison, and retains every observed
version. An explicit restart drops only the cursor and advances the epoch, making
older in-flight pages stale. Deletion claims wait for that incarnation's active
inventory passes; scans wait for its outstanding deletion claims. Shared keys
can also contain other sources' versions and markers: this is not a bucket-wide
snapshot or deletion fence. A pinned MinIO diagnostic resumed after deleting a
marker; production reconciliation must still support restart and periodic fresh
scans. An observed end is never proof that a timed-out writer has terminated.

`TaskBundleSourceRecovery` commits exact deletion claims before storage I/O and
records completion only after exact-version absence. A lost response can be
retried without deleting a newer publication at the same key. Global content
manifests have separately owned exact versions per upload; no source owns or
deletes their shared key. SQL identity/retirement guards reject mutation and
DELETE/TRUNCATE of recovery records; downgrade uses NOWAIT and refuses any
populated source journal. Compact tombstones are retained until a future explicit
writer-termination/compaction protocol can prove them unnecessary.

Real PostgreSQL and TLS MinIO composition tests cover committed intent-before-PUT,
publication rollback, competing publications, reference/retirement lock contention,
unversioned rejection, lost upload receipts, interrupted deletion, late writes,
new identical publications, and shared-manifest ownership. Migration tests cover
empty roundtrip, ORM parity and refusal to discard populated recovery authority.
Image ensure/revival and administrative retry now validate their complete frozen
snapshot against a registered available source and attach its materialization
reference while holding the image lock before the source lock. Merely supplying
a manifest-shaped digest cannot enqueue or revive a strong-source image. Exact
republication can restore the same logical source and image identities; retirement
of an old incarnation never does. These checks preserve the legacy path.

Bulk catalog publication locks persisted Task rows in ID order, stages all image
rows before taking any source lock, then locks the union of old/new sources in
source-ID order. It publishes prepared uploads, validates exact snapshots, and
pins the catalog and images in the caller's transaction. Replacing a catalog
entry releases only its previous catalog reference, never historical image pins.
The helper does no storage I/O and never commits. Callers must roll back their
whole transaction on failure. Administrative retry refreshes locked ORM state
before deciding eligibility, and refuses pending image edits rather than silently
overwriting them; cached failed/ready rows cannot requeue a concurrently claimed
or retiring materialization.

The local benchmark publisher has an explicitly selected `versioned-v1` Python
composition path. It stages compatibility copies without editing authored files,
uses verified registration with the existing architecture fallback policy, and
prepares all uploads before taking benchmark/catalog locks. The final transaction
publishes benchmark, Task, source and native-image admission together. Repeated
publication reuses available sources without PUTs. Preparation has a bounded
24-hour deadline, not a renewable upload lease; expiry rejects final publication.
Lost upload responses and aborted catalog transactions retain recovery records and
exact object versions rather than attempting prefix cleanup. Tests exercise this
real producer against PostgreSQL and TLS MinIO, including authored/catalog IDs,
two-architecture enqueue, reuse, upload failure and publication rollback.

Build claim, start and historical execution-grant lookup also validate and pin
the exact selected image source before returning authority. Locked reads refresh
cached ownership and refuse pending materialization edits. Initial reads suppress
autoflush until strong-source transaction preflight has succeeded. Queue claim
and registry-GC claim/completion require retained READ COMMITTED ownership even
for legacy images: queue-maintenance writes precede knowing the candidate's source
kind. Unsafe transaction modes are rejected before flushing unrelated caller Task
writes. These are normal-session APIs; the separately configured protected
SERIALIZABLE runtime still needs an explicit source-admission composition.

Completing legacy registry GC releases only the retired image's materialization
source pin in the same transaction. A raced catalog/trial reference requeues and
re-admits the source instead; catalog and trial pins remain independently owned.
Rollback restores both image state and its source reference. This is reference
release, not storage deletion or native-attempt retirement.

Delayed legacy publication remains cleanup evidence after image retirement;
retired rows with newly reported history reenter registry GC without reviving
their source. New history during an outstanding GC claim advances its epoch and
expires the claim, so an old acknowledgement cannot discard objects missing from
its deletion inventory. Exact repeated evidence does not invalidate an unchanged
claim. Publication reporting also refreshes locked state and requires retained
READ COMMITTED ownership. These fences cover reported artifacts, not proof that
an unreported writer can never finish; native writer-quiescence requirements are
unchanged.

`observe_unpublished_task_image_retirement` provides a separate, unscheduled
per-image retirement operation for strong-source queued/failed images with no
recorded publication. It owns a READ COMMITTED transaction, takes `tasks SHARE
NOWAIT` before the image lock, and applies one-second SQL/idle and five-second
transaction deadlines. Catalog contention aborts without observing or releasing
a pin. Current catalog references, nonterminal linked trials, execution leases
without positive deletion and cleanup, unretired native attempts and live build
leases reset the observation grace. The default grace is 24 hours; observations
must not move backward. Retirement advances the image lease epoch and releases
only its source pin atomically. Registry publication/history remains owned by
registry GC, and rootless attempts must pass their separate retirement fence.
The operation performs no object/registry I/O and neither schedules reconciliation
nor establishes later terminal-verifier/start admission safety. Those composition
gates remain necessary before enabling any source deletion.

The local CLI and Python default remain legacy. Adapter and taskset producer
integration, historical/prebuilt-only trial source snapshots and release, rootless
trial-start admission, both taskset GC paths, and scheduled reconciliation remain
required. Native builder source admission is implemented below. Taskset quota
accounting must include retained historical source objects outside generation
roots before that producer switches. These APIs do not activate native builders.

### Native registered-source composition

Native claims derive V2 plans only from an admitted registered source, checking
its manifest, full config/provenance, location, checksum and materialization key
together. The registered-source manifest column cannot silently fall back to a
V1 plan when provenance is missing. Historical V1 wire bytes and canonical hashes
remain unchanged; the synchronous V1 bundle provider still refuses V2 input.

Fresh/replayed claims, start/heartbeat operations including replay, live-plan
reads, and shared registry credential/candidate admission retain the source under
image → attempt → source locking. A retained READ COMMITTED transaction is checked
before authority reads can autoflush caller-owned state. Locked image and session
parent reads refresh cached ownership and reject pending edits before refresh.
Cleanup-only release and containment failure do not
require available input and retain their non-admitting SERIALIZABLE route.

Every continuing attempt compares its retained canonical claim against the
freshly admitted derivation. A self-consistent receipt hash alone cannot replace
the manifest, component paths or plan schema. Only live session identity and
authorization expiry may differ on renewal; a claim replay returns its original
receipt without extending that receipt's expiry or changing its session binding.

The configured asynchronous bundle path checks source authority both before
storage I/O and after reacquiring the transaction locks, including encrypted
capability replay. PostgreSQL and disposable TLS MinIO tests compose the real
registered publisher, HTTP projection/session/claim and configured bundle backend,
then download and hash returned signed objects. They exercise source loss before
I/O, during unlocked I/O and before replay, plus session renewal without rewriting
the claim. Projection/guard observations in those fixtures are synthetic: these
tests are not native Slurm, containment or activation acceptance.

Production providers remain disabled and source collection remains unscheduled.
Native session and registry authority still require production purpose; a genuine
shadow campaign must separately isolate queue membership, output authority and
readiness effects. This composition does not activate production to simulate a
shadow campaign or replace execution trust, capacity-fairness and rollback gates.

### Native protected-worker launch composition (inactive)

`OperatorLaunchProfileV2.native_execution` optionally binds the native protocol,
platform, environment and execution-root public bytes/lifetime into both the
launch-policy and approved-profile-set digests. The enclosing profile already
pins the worker image, launcher, launcher configuration and release. Legacy
profiles omit the new field from canonical serialization, preserving their
existing authority hashes; they do not acquire native eligibility. Rendering
refuses roots outside their validity at submission. Actual launch must recheck
validity after any Slurm queue delay.
The typed renderer applies the same pre-signing lifetime fence. Its policy set
also rejects native task-image execution profiles assigned to personal-development
build workers; those are a different purpose from application trial workers.

The native bootstrap codec transfers the bounded credential/root document through
a memory-only, EOF-terminated pipe, not container environment or retained files.
Preloading checks the pipe's actual capacity and atomic-write limit. Reading
requires canonical framing, a byte bound and a deadline; missing, delayed,
trailing or replayed input fails closed. Image tests exercise the installed
decoder through Docker stdin and a manual restart, and check that container
inspection/logs do not retain the credential. These prove transport semantics,
not actual worker registration or native execution authority.

The dedicated-process consumption function additionally sets and reads back
`RLIMIT_CORE=(0,0)` and Linux `PR_SET_DUMPABLE=0` before reading the secret. It
checks root validity against its own startup clock and replaces stdin with the
verified `/dev/null` device before returning. If replacement fails, fd 0 is
closed, including when hardening failed before any credential read. Subprocess
tests inject open, duplication and device-validation failures; installed-image
tests exercise dumpability, stdin detachment and restart refusal. These are
startup primitives, not yet the fixed launcher or worker settings composition.

The worker-side `loom_worker.native_main` entrypoint consumes this handoff before
constructing settings, metrics or the worker loop. Its optional canonical settings
subdocument is limited to 2 KiB inside the unchanged 4 KiB total frame ceiling;
frames without settings remain useful only for transport diagnostics and cannot
start a worker. Reserved credential/root fields and external-source overrides
are rejected. The native settings subclass uses initialization values only, with
dotenv disabled; credentials pass directly into secret-valued fields and the
public root stays in process settings, excluded from serialization. The ordinary
worker entrypoint and its environment configuration remain unchanged. Subprocess
tests exercise the production startup path with the worker-loop boundary replaced;
they are not evidence of actual protected registration.

The trusted launcher now has an opt-in native worker branch in its pinned config.
The candidate executable is the verified Docker executable snapshot and accepts
no command suffix. A fixed Unix endpoint and empty operator-owned Docker config
replace ambient CLI configuration. Image prefetch and actual digest/platform
readback precede capability exchange. Allocation-derived pool, hostname,
candidate, concurrency and resource limits replace caller settings; conflicting
settings fail before exchange. The complete bootstrap is size-checked with the
maximum credential length before consuming the launch marker. The host adapter
currently supports CPU-only trial allocations on OLDLAB and GB10, refusing GPU,
pipeline, singleton/sandbox and worker-vLLM modes until their equivalent native
admission paths exist. Phase 1 keeps its existing supported modes.

Native host launch requires Docker's `cgroupfs` driver on unified cgroup v2 and
the actual delegated Slurm job ancestor. The native allocation model refuses a
systemd slice, foreign job, traversal, or a batch-step path in place of that
aggregate ancestor. Daemon capability is checked before image prefetch or
bootstrap consumption. The same exact ancestor is passed to worker creation and
bound into its settings for trial/sidecar children. An independently capped
systemd sibling is not an alternative: assigning the full allocation limits to
both branches duplicates the aggregate budget and loses Slurm ancestry. Legacy
Phase 1's systemd bridge remains separately supported. No launcher changes daemon
configuration; compatible native runtime provisioning requires reviewed drained
node/deployment evidence without disrupting the existing service. Actual process
ancestry, cancellation and positive descendant cleanup still require live proof;
daemon/pull overhead is not made allocation-contained by container placement.

Before bootstrap exchange, the launcher opens the exact job ancestor through
descriptor-relative, no-follow traversal and retains its directory descriptor.
Bounded control-file reads require protected root ownership, finite positive
aggregate memory within the allocation, the exact PID ceiling, an effective CPU
set within the allocation, and zero swap allowance. Parent directory identities
and permissions are rechecked along with these controls after registration,
after consuming the launch marker, and after container creation before attachment.
Drift prevents startup; a known created container still receives exact-ID cleanup.
These checks establish launch-time readback, not cgroup preparation authority,
continuous enforcement, post-start process ancestry, or positive descendant cleanup.

The fixed container invocation uses an absolute isolated Python entrypoint, a
read-only root, a non-root user, explicit Docker parent/resource limits, no
restart or healthcheck, and only the Docker socket and per-launch scratch bind.
Image ENV is explicitly removed or replaced before Python starts. Control-command
output is incrementally bounded on both streams; attached worker output streams
without accumulating supervisor buffers. A native-only termination latch handles
SIGTERM, SIGHUP and SIGINT even while a synchronous Docker operation is reading;
repeated signals do not interrupt the subsequent bounded exact-ID cleanup.
SIGKILL and host loss still require administrator-owned cleanup. The scoped credential/root/settings
arrive only through the private EOF-terminated stdin pipe. After its marker is
consumed, launch is never retried. A known created container is removed by exact
ID with responsive-daemon absence readback, including attachment failures.
An uncertain create response still requires administrator-owned allocation
inventory/cleanup: instantaneous absence cannot exclude delayed creation.
Each invocation gets fresh private scratch with retained inode identities. A
proven pre-create failure removes only its still-matching empty directories,
allowing an unconsumed handoff to retry without reusing stale files. After a
possible create, scratch data is retained until allocation/runtime cleanup; worker-container
removal alone does not authorize deleting files still used by trial descendants.
Durable descendant-cleanup admission and scratch retirement remain part of the
unfinished native runtime/retention composition, not proof supplied by this launcher.

The complete native adapter remains incomplete. The new entrypoint advertises no
native capability and still uses the existing V1-only worker loop. Owner-projected
eligibility, authenticated native claims and one-use trial start remain unwired.
The checked-in legacy Slurm cgroup guard recognizes only
`loom-cgroup-v1:pids=<N>` comments, whereas the protected executor submits its
ownership token as the entire comment. That guard does not provision native
protected-worker parents. Its presence inventory now uses the all-state node
queue separately from its admission results: failed per-job readback, missing
resource facts, suspended/completing jobs, and unknown comments do not authorize
teardown of an existing slice. Positively terminal legacy jobs do not keep their
slices alive during Slurm's terminal-record retention interval. Only readable RUNNING legacy opt-ins acquire or
resize a slice. This prevents an admission failure from disrupting an existing
allocation; it neither authenticates a protected job nor proves positive runtime
cleanup or permits an independent slice creator. A coordinated owner-authenticated
guard adapter must delegate the real Slurm job subtree for native workers, not
create a substitute systemd slice. Pre-registration admission must verify the
current protected physical binding and unrevoked bootstrap plus fresh scheduler
incarnation/node/resource facts. Historical intent observation and the pre-submit
ownership signature are insufficient. Any purpose-specific signed delegation
must cover this post-bind authority; it grants containment preparation, never a
second worker registration or trial start. This adapter is still required before
host launch acceptance; neither the signed ownership
comment nor the legacy guard's opt-in format may be silently replaced.

`guard_0033` adds an executor-only unused-bootstrap observation. It matches the
entire persisted physical request, current local agent/candidate binding, prepared
bootstrap and latest protected bootstrap epoch, checks the database clock against
the original expiry, and refuses every later registration or terminal admission
event. It records no start or new admission event. The Python adapter requires an
idle SERIALIZABLE session and owns its short transaction through commit: reusing
an existing transaction could label a pre-withdrawal snapshot as current. Contended
admission locking fails immediately for caller-level retry. Historical binding
replay and historical intent observation retain their separate existing contracts.
This response is unsigned local snapshot evidence, not a containment lease; a
trusted issuer must still combine it with fresh manager/execution and scheduler
incarnation evidence, and the node guard must independently authenticate the
result. Direct SQL callers likewise own snapshot freshness. Containment renewal,
root delegation, and positive cleanup are not implemented by this observation.

Installed-image diagnostic tests cover transport, loader-environment clearing and
Docker client-loss cleanup, not actual protected worker registration acceptance.
The worker container and all descendants require verified allocation containment.
Docker client loss is not container termination. Unique in-memory execution
ownership, durable grant finalization/start consumption and positive owned
cleanup remain necessary before native readiness can reach any worker. No
profile parsing, successful bootstrap or signature alone enables execution.

The shared Docker-parent discovery also rejects a systemd slice whose memory
ceiling exceeds the live Slurm job ceiling. Both values must be finite positive
byte counts; an unverifiable job ceiling fails closed, and a stale slice may
converge only within the existing bounded wait. The job ceiling is reread on
every attempt. This is a trial-worker launch check, not proof that the sibling
systemd slice is a Slurm descendant or that its lifetime cleanup is correct.
The native rootless builder still requires its separate exact allocation-
descendant containment and positive cleanup proofs.

## Completion and subsequent activation

D2 acceptance requires real streamed-registry fixtures, PostgreSQL concurrency
and replay tests, signature mutation tests, expired-lease/attestation/job tests,
signer-rotation races, retention races, and a supervisor-to-authority flow.
Protected CI and an independent review precede the squash merge.

The full goal then requires worker keysets and one-use start authority, two
native shadow campaigns, architecture-capacity fences, separate OLDLAB and GB10
cutovers, task `4139e767` acceptance, soak and rollback proof. Phase 1 stays
operable throughout; a D2 merge is not completion of the activation goal.
