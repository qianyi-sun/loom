# Personal lifecycle connection to active membership

Status: dependent design, not implemented or runtime-certified. Execute after
the manager membership plan passes review. This is a required step toward the
multi-person development goal, not an alternative to live acceptance.

Dependencies: `2026-09-08-personal-dev-active-membership.md` and
`../architecture/personal-dev-active-membership.md`. Preserve the V2 shadow path,
shared `loom-dev`, owner namespaces, immutable base execution fence and common
owner account. Do not change historical zero-capacity acceptance certificates.

## Design decisions

Use an explicit `shadow-v1` / `membership-v1` mode, persisted on each operation.
Do not select a different mode on retry by looking at the manager's current state.
Legacy rows retain their exact global configuration-epoch semantics. Membership
records carry an independent execution/namespace/revision/head checkpoint and
never fill the old configuration-epoch columns with a membership revision.

Persist canonical, secret-free request and acknowledgement receipt bytes before
the network mutation. The existing projection carries reporter token hashes, not
bearer tokens; preserve that boundary. A digest alone is insufficient for exact
replay. A pending request survives reconciler lease takeover unchanged; its
receipt binds the originating operation/attempt and observation lease, while
the current lease independently fences local state writes. Do not re-attest the
request merely because a new reconciler now owns the lease.

An authenticated manager response may be lost after commit. Resend the stored
request and idempotency key first. Refresh only a typed stale-membership-revision
conflict in the same execution authority and namespace. Store the refreshed
request before sending it. Authentication, identity, idempotency and execution
fences are not revision conflicts. A changed execution namespace requires an
explicit re-attestation transition, not silent adoption.

Before that transition, resolve the old outcome through a read-only historical
operation-receipt lookup. Require a currently authorized, unbound `capacity:read`
management observer; a revoked old delegate credential never becomes valid again.
The query binds old execution/namespace, original actor, operation/idempotency key
and full request digest. Return the immutable exact committed receipt, or a typed
terminal-not-committed result only when the old epoch is irreversibly retired and
the consistent event-log lookup proves no matching event or conflicting key. A
missing row in an active/draining epoch is not final non-commit evidence. Conflicting
keys are integrity errors, not absence. Persist the resolution under the current
local lease. Record a historical commit without presenting it as current capacity
readiness; reconcile a surviving environment into the new authority through a
distinct explicitly attested operation. Never replace an unresolved old request.

The trusted management installer is the attestation boundary. It measures the
installed protected database/agent and binds the observation to the exact
operation, publication and prepared execution context. Authentication comes from
the delegated management principal and protected transport, not from hashing
caller-supplied fields. Do not give attestation or capacity-writer credentials to
personal source. Fresh installations truthfully attest legacy high-water zero;
do not fabricate a legacy freeze or reuse staging's positive-high-water ceremony.

Destroy's membership path must be `admission_disabled -> cleanup_pending ->
release_verified -> local_authority_sealed -> resource_deletion -> deleted`.
Existing pool executors and protected agents perform authenticated drain/release;
the lifecycle manager requests and observes that work, never marks it released.
Keep their namespace, database logins and history available through cleanup.
The release gate binds the exact subject incarnation, disabled generation and
all historical deployment/execution intents, reservations and attributed observed
commitments under the manager authority lock. Reuse the recreation release-set
validator and durable protected/terminal witnesses, including truthful empty and
never-accepted closure cases. Unknown or unreachable work remains charged and
keeps the operation deleting. Sealing/deleting APIs must require the persisted
release receipt, not merely a disabled-membership acknowledgement. Lease takeover
resumes cleanup; it cannot skip the release gate.

For `keep_data` recreation, retain the predecessor database sealed as historical
evidence and create an incarnation-specific fresh database and database roles.
Do not rename/reinitialize its protected singleton or restore its protected schema
into the successor. Persist an exact storage binding independently of the stable
environment/namespace name; provisioner, installer and cleanup must consume that
binding instead of re-deriving a database name on retry. Export a manifest-bound
allowlist of application table data from the sealed database and import into a
fresh candidate-migrated application schema using restricted application roles.
No old schema DDL, functions, triggers, grants, protected/agent/management tables
or runtime credential/session material may execute or transfer through this path.
Validate the application table inventory and candidate schema compatibility;
unsupported data stays preserved in the predecessor and blocks switching, not
silently discarded. Retain an immutable export/table inventory and import result
digest without logging row contents. Persist storage-created, data-imported,
data-verified and connection-published checkpoints. Only after verified import
switch the stable application Secret reference to the new storage binding, then
continue installation/admission. Retries identify the same successor database;
never overwrite the predecessor or create a new unnamed clone. Existing object
data stays owner-bound with new scoped access; preserve old evidence until a
separately authorized retention action. Test actual supported application data,
not an empty database. A crash must not cause old credentials or protected IDs
to be reused.

## Deliverable 1: Durable checkpoint and strict client

Affected files: `src/loom/personal_dev_environment.py`,
`personal_dev_environment_store.py`, `personal_dev_capacity.py`,
`src/loom/db/schema.py`, next lifecycle migration, existing capacity/environment
unit and PostgreSQL integration tests.

- Add a strict versioned local envelope for prepared request, observed receipt,
  request digest, expected manager checkpoint and optional exact result.
- Persist the envelope on the operation and the accepted checkpoint on the
  environment. Add explicit mode discrimination to schema constraints: forbid
  mixed shadow/membership epoch fields, partial request/result pairs, and success
  without the mode's matching accepted result. Keep old rows and old APIs valid.
  The environment has its own accepted-mode discriminator: while a membership
  operation is pending it retains its prior accepted shadow checkpoint. Switch
  the accepted mode/checkpoint atomically when the new result is recorded, retaining
  the old operation evidence. Existing succeeded `noop` rows remain valid without
  a fabricated mutation receipt. A requested mode migration is not a noop merely
  because source and capacity are unchanged; it requires explicit admission.
- Add lease-fenced prepare, revision-refresh and acknowledgement methods rather
  than weakening `prepare_capacity_projection`'s V1 contract. Adapt reservation
  records, completion, destroy evidence, cancellation and pre-activation abandonment
  predicates to require the correct mode's evidence; inspect every existing
  `capacity_configuration_epoch is not None` readiness shortcut.
- Add client methods for the manager's authenticated checkpoint and mutation
  response, strict typed conflict handling and bounded duplicate-rejecting JSON.
  Verify exact execution/namespace, original result head, subject incarnation,
  operation/configuration/deployment/reporter and full request binding.
- Tests first: shadow migration compatibility; invalid mixed/partial rows;
  two-owner concurrent revisions; committed response lost; exact replay after
  another owner advances the head; lease takeover; wrong acknowledgement; stale
  lease; changed execution namespace; corrupt saved request; typed versus unrelated
  conflicts. A timeout must neither re-attest nor acquire a new operation identity.
  Cover shadow-ready -> membership-pending -> membership-accepted and legacy noop.
  Cover commit/response-loss/retirement and changed-delegate historical recovery.

## Deliverable 2: Protected observation and reconciler integration

Affected files: `src/loom/personal_dev_capacity_runtime.py`,
`personal_dev_capacity.py`, `personal_dev_reconciler.py`, management runtime
configuration/wiring, focused unit and real PostgreSQL guard integration tests.

- Extend the trusted installer result with an observed execution receipt. Read
  back installed guard/agent identities and authenticated protected surface,
  verify runtime publication and credential separation, then produce the exact
  acknowledgement accepted by the manager. The receipt contains no credentials.
- Select membership mode only through explicit trusted runtime configuration
  compatible with the prepared V3 delegation. Never silently upgrade V2 operation
  records. New/pending operations follow their stored mode.
- On first admission, persist observation plus full request; on pending retry,
  send stored bytes. Read-only reconvergence checks may compare against persisted
  evidence, but must not mutate the pending request or rotate its credentials.
- Apply the same protocol to create, update, capacity and destroy. Keep physical
  predecessor work charged and preserve cleanup access. For update, test protected
  claim fencing before/after reporter/deployment rollover and authenticated old
  binding drain/release. Do not drain unrelated owners or staging.
- Implement the durable destroy release gate above, with restart/takeover tests
  and spies proving sealing/deletion cannot run while any predecessor work remains.
  Test old-generation and old-epoch work, not only the latest status response.
- Implement the incarnation-specific storage binding and keep-data transfer above
  as a separately verified subtask before enabling recreation. Include the actual
  provisioner/credential/Secret consumers and their retained-data schema inventory.
  Cover a crash after database creation, after import, and before/after connection
  switch and membership acknowledgement; prove data retained and protected identity
  fresh, with no import of obsolete authority or credentials.
- Verify the existing guard semantics, not an assumed global activation: migration
  guard_0009 preserves incarnation and advances generations; guard_0013 validates
  current deployment for increases but exempts exact drain/release from that check.
  Preserve those security boundaries. Recreate requires a fresh protected identity;
  never relax the singleton incarnation check to reuse an old authority.
- Tests first: forged/wrong publication or manifest receipt; changed installed
  reporter; stale observation lease; fresh legacy high-water zero; restart after
  persisted request; update with old charged work; destroy cleanup; deleted/recreate
  fresh incarnation; unaffected other-owner admission.

## Deliverable 3: Exact historical status and acceptance handoff

Affected files: manager API/execution status reader, personal capacity client/status
reader, matching integration tests, a new active acceptance runbook.

- Add a separate versioned exact subject-incarnation/deployment status query.
  Preserve V2 latest-only semantics. Resolve historical acknowledgement/bindings
  from immutable events; old status never means current worker readiness.
- Add the historical exact operation-receipt lookup and terminal outcome contract
  above. Keep it read-only, authenticated by current observer authorization and
  distinct from mutation replay. Test request/key substitution, conflicting events,
  rotated/revoked delegates, active-epoch ambiguity and retired-epoch non-commit.
- Prove old status/cleanup after update and recreation, including intents retained
  across execution epochs. Keep `keep_data` semantics: preserve requested application
  data, but do not reuse old protected capacity authority or reporter identity.
- Review the complete lifecycle diff, run covering migration/legacy/lifecycle/
  guard/API suites, then normal protected PR/CI. No source-only readiness claim.
- Prepare separately versioned active acceptance evidence for a CI-published
  release: explicit V2 retirement/V3 preparation boundary, two concurrent owners,
  unchanged staging/owner A while B joins, source variants, update/cancel/retry/
  cleanup/recreate, architecture-specific and neutral tasks, and scale-to-zero.
  Typed build-service grants, certified GB10 nodes 3–15 and held Slurm execution
  remain dependencies before running that live acceptance.
