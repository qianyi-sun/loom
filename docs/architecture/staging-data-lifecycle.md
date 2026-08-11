# Staging Data Lifecycle

Shared staging has an explicit retention authority, capacity high-water mark,
mutation epoch, and journaled garbage collector. Unknown rows or object keys
are reported or quarantined; they are never inferred to be safe for deletion.

## Retention authority

Tracked run, trial, event, artifact, and object identities bind an environment,
namespace, team, data class, logical owner, creation time, and either an expiry
or an explicit pin. Staging execution data expires after seven days. Catalog
and system data is pinned. Non-staging data remains pinned unless that
environment has its own explicit policy.

Writers create or verify lifecycle authority in the same transaction as their
application row. Object registrations contain an exact bucket, key, optional
version, SHA-256, and byte size; a prefix is never deletion authority.

The dedicated lifecycle tools read `LOOM_LIFECYCLE_DB_URL` and, where object
access is required, `LOOM_LIFECYCLE_MINIO_*`. They do not load provider, JWT,
or unrelated application secrets and connect directly to Postgres rather than
through transaction-pooling PgBouncer.

## Capacity policy

| Decision | Object count | Stored bytes | Free disk and inodes |
| --- | ---: | ---: | ---: |
| Run GC | at least 200,000 | at least 12 GiB | either below 25% |
| Deny new staging runs | at least 250,000 | at least 16 GiB | either below 20% |

One policy digest binds all six thresholds. Capacity evidence expires after
five minutes. Missing, stale, or wrong-policy evidence denies staging run
admission.

The protected rollout's Tier 0 collector inventories the exact staging
trajectory and artifact buckets before backup or request publication. Its
fixed MinIO principal can inspect bucket/version state, list and read exact
object versions, and read server information; it cannot write or delete
objects or mutate server state.

Publish a single-node filesystem measurement with:

```bash
uv run --no-sync python scripts/ops/staging_data_lifecycle_capacity.py \
  --namespace loom-staging \
  --bucket trajectories \
  --bucket artifacts \
  --filesystem-path /data \
  --output /secure/evidence/staging-capacity.json
```

The scheduled maintenance workload and installed Tier 0 collector select the
capacity source from the rendered topology. Single-node host-path MinIO uses
the canonical backing filesystem. Distributed MinIO uses the bounded
`admin:ServerInfo` API because its ReadWriteOnce Longhorn PVCs cannot be
co-mounted by the maintenance pod; the least-free live drive determines byte
and inode headroom while object counts still come from exact S3 listings.
Missing drive authority fails closed, and the rollout host's unrelated local
filesystem is never treated as distributed MinIO capacity.

## Initialization and classification

The lifecycle schema requires one exact `staging/loom-staging` mutation-epoch
authority. `staging_data_lifecycle_bootstrap.py inventory` is read-only;
`apply` requires the reviewed inventory digest, request ID, and actor. The
preparation helper performs the same digest-bound checks when the database
also needs the canonical lifecycle migrations.

Rows and objects that lack lifecycle authority are handled by
`staging_data_lifecycle_classify.py`. Its inventory verifies exact ownership,
team relationships, object versions, hashes, sizes, and canonical key layouts.
Apply repeats the inventory under lock and binds only the reviewed digest. It
classifies data but does not delete it.

Every inventory/apply pair follows this pattern:

```text
... inventory --output PRIVATE_EVIDENCE
... apply --approved-inventory-digest SHA256 \
  --requested-by ACTOR --request-id REQUEST_ID
```

Any schema, row, object, epoch, source, or policy drift invalidates the
approval.

## Garbage collection

`scripts/ops/staging_data_lifecycle_gc.py` supports `inventory`, `dry-run`,
`apply`, `resume`, and the scheduled `auto` action. Manual `apply` requires the
exact live inventory digest and a mutation request ID.

GC is two-phase and journaled:

1. inventory eligible authorities and exact objects;
2. mark them with one deletion token;
3. verify hashes, sizes, and versions, then delete exact object identities;
4. verify absence;
5. remove eligible metadata; and
6. increment the mutation epoch.

The journal makes a failed apply resumable from its exact run ID. Resume does
not rebuild or widen the plan. Active, dry-run, completed, mixed-token, or
tampered runs are rejected. Object verification and deletion use bounded
groups and explicit S3 batches; partial external deletion remains recoverable.

The staging-only maintenance CronJob runs the same executor with least-authority
database and object-store credentials, no service-account token, a read-only
root filesystem, dropped capabilities, a fixed non-root UID/GID, and explicit
NetworkPolicy. Development and production renders omit this CronJob.

## Mutation epoch and rollout checkpoints

Protected staging mutations advance one database-backed epoch. Rollout backup
leases bind that epoch, schema revision, database snapshot, immutable object
inventory, environment, namespace, component hashes, source request, manifest,
and restore verification. A changed or expired binding requires a new verified
checkpoint.

Rollback-payload rotation keeps one active restorable lease and permits at
most one replacement candidate. Promotion requires immutable manifest and hash
validation plus a successful isolated restore. Retirements use exact
request-bound records and the protected backup-retention
`inventory`/`apply` command; operators must not delete backup roots with path
globs.

See [protected staging rollout](staging-rollout.md) and
[staging rollout preflight](staging-rollout-preflight.md) for the admission
checks that consume this evidence.
