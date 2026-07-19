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
