# Staging data lifecycle and rollout checkpoints

Shared staging is a bounded validation environment, not an archival data lake.
Migration `0066` introduces typed lifecycle authority and the monotonic staging
mutation epoch used by garbage collection and rollout backup leases.

## Authority

Every newly tracked run, trial, event, artifact, and object-store key must bind
an environment, namespace, owning team, data class, logical owner, creation
time, and either an expiry or an explicit pin. Unclassified historical rows and
unknown object keys fail closed: inventory may report or quarantine them, but
GC may not delete them.

Execution writers obtain this authority through the transactional registry in
`loom.data_lifecycle_registry`. The runtime environment and namespace are
injected separately into each service; protected staging/production refuses to
derive a default namespace. Staging execution owners receive the bounded TTL
below, while non-staging owners are pinned until that environment has an
explicit reviewed retention policy. Retried registration reuses one unique
owner authority only when its team, scope, class, pin and retention facts still
match; conflicting authority fails the writer transaction.

Every service batch creation path, including failed-case reruns, Run Library
configuration clones, and artifact-derived runs, allocates its batch identity
and lifecycle authority in the same database transaction. Control-plane trial
submission binds the inserted trial to its authority before commit; an
idempotency conflict returns the already-bound trial without creating another
authority. A writer may not commit an execution owner with a null authority or
leave an authority whose owner insert rolled back.

Worker event ingestion and gateway LLM-call accounting share one event-stream
authority per trial. Before either child row is written, the registry locks and
validates the trial, lazily binds a pre-migration active trial when necessary,
and rejects a missing owner, team mismatch, or conflicting authority. Typed
trajectory artifacts receive a distinct artifact-class authority per exact
artifact identity; updates verify and reuse that authority rather than creating
an orphan or silently relabeling it.

Staging defaults are:

- ephemeral run, trial, event, and artifact data expires after 7 days;
- GC is required at 200,000 objects, 12 GiB, or less than 25% free disk/inodes;
- admission fails at 250,000 objects, 16 GiB, or less than 20% free disk/inodes;
- catalog, benchmark-input, bootstrap, system, and rollout-evidence data is
  pinned and separately classified.

These six numeric thresholds have one deterministic policy digest. Tier 0
`capacity.high-water` binds that digest and records object count, bytes, disk
free percentage, inode free percentage, the GC trigger decision, and the final
admission decision together. A missing inventory or policy-digest drift denies
admission before request publication or backup.

The object registry stores exact bucket, key, optional version, digest, and
size. A generic prefix is never sufficient deletion authority. Read-only
inventory uses one repeatable-read database snapshot and reports the count of
unclassified rows in every execution-history table; any nonzero count is bound
into the plan digest and blocks apply.

Application-owned delivery exports register both the archive payload and its
checksum sidecar in the artifact transaction. Unversioned objects require a
canonical SHA-256; versioned objects bind the exact object version. A retry may
reuse an existing registration only when authority, scope, version, digest,
size, creation time, and active state all remain exact.

The exact worker result projection carries SHA-256 and byte length for the
multipart trajectory and generated ATIF object, alongside the already exact
artifact collector evidence. Control-plane projection atomically creates or
verifies each artifact authority and exact object row. Missing, noncanonical,
or incomplete object evidence rejects the entire projection and rolls back the
trajectory-index update; finalization is not the first point at which a broad
prefix becomes deletion authority.

## Mutation epoch

Every protected staging mutation increments one database-backed epoch: rollout
cluster/schema changes, authorized GC, object rewrites/deletes, and rollback.
A backup lease binds the exact epoch, PostgreSQL snapshot identity, schema
revision, environment/namespace, and immutable object-inventory root. Lease
reuse fails closed unless those values and all component hashes remain exact.
The typed lease also binds its source request and manifest digest, requires a
completed restore-verification timestamp, and reports every mismatch together.
An expired lease, changed epoch, different DB snapshot/schema/inventory, or any
component/manifest drift makes reuse ineligible; the caller must create a fresh
candidate payload rather than weakening the comparison.

## Two-phase garbage collection

GC is staging-only and journaled:

1. Inventory and classify without mutation.
2. Mark exact authorities and object keys with one deletion token.
3. Delete only those exact keys.
4. Verify object absence.
5. Remove eligible metadata and increment the mutation epoch.

Runs are retryable and idempotent. Unknown or cross-environment objects are
quarantined/reported. DB references without objects and objects without DB
owners are reconciled in both directions; neither class is silently deleted.
The reusable executor in `loom.data_lifecycle_gc` binds a deterministic plan to
the current mutation epoch. An unversioned object is deletion-eligible only
when its exact SHA-256 is registered; a versioned object is addressed by its
exact version. Object deletion, absence verification, business-metadata
removal, and the epoch increment are distinct journaled transitions. A dry run
records its inventory digest but cannot call the object-store deleter.
The S3/MinIO adapter checks exact size and version before delete, and streams an
unversioned object to verify its registered SHA-256. Drift is detected before
the delete call; absence verification uses the same exact version identity.
After a failed apply, resume reconstructs the canonical epoch-bound plan from
the journal, verifies one exact deletion token across every item, and claims
the failed run with a compare-and-swap transition before doing more work. It
continues from each item's recorded phase, so an already deleted object is
verified rather than deleted again and already removed metadata is not replayed.
An active, dry-run, completed, mixed-token, or tampered-inventory run cannot be
resumed. A second resumer loses the claim and fails closed.

The supported operator entrypoint is
`scripts/ops/staging_data_lifecycle_gc.py`. It loads the normal control-plane
database and object-store settings from their existing secret-bearing
environment, never accepts secret values on argv, and supports four explicit
actions: `inventory`, `dry-run`, `apply`, and `resume`. `inventory` is purely
read-only and returns every blocker together. `apply` additionally requires the
operator to echo the exact live inventory digest and provide a mutation request
ID; a newly observed row, object, or epoch therefore invalidates authorization.
Evidence output can be written once to a new mode-0600 file. `resume` accepts
only an exact failed run ID and its request authority; it does not rebuild or
broaden the deletion plan from current prefixes.

## Rollout checkpoint versus disaster recovery

The synchronous rollout checkpoint contains only state the rollout can mutate
and cannot reproduce: a transactional PostgreSQL snapshot, required secret and
schema authority, and a signed immutable-object inventory. Ephemeral execution
objects are not recopied for every rollout. Full MinIO disaster recovery is an
asynchronous, versioned process on a separate failure domain.

Rollback payload rotation keeps exactly one active restorable lease at steady
state and at most two during replacement. The candidate is promoted only after
manifest/hash/provenance validation and a successful restore verification;
then every older unreferenced payload is removed while compact evidence is
retained.

If any proof is unavailable, the operator must fail closed to a fresh verified
checkpoint. A changed-epoch checkpoint plus restore must complete within 30
minutes before shared staging rollout may proceed.
