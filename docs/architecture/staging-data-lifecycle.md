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

The object registry stores exact bucket, key, optional version, digest, and
size. A generic prefix is never sufficient deletion authority.

## Mutation epoch

Every protected staging mutation increments one database-backed epoch: rollout
cluster/schema changes, authorized GC, object rewrites/deletes, and rollback.
A backup lease binds the exact epoch, PostgreSQL snapshot identity, schema
revision, environment/namespace, and immutable object-inventory root. Lease
reuse fails closed unless those values and all component hashes remain exact.

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
