# Staging data lifecycle and rollout checkpoints

Shared staging is a bounded validation environment, not an archival data lake.
Migration `0066` introduces typed lifecycle authority and the monotonic staging
mutation epoch used by garbage collection and rollout backup leases.

The migration is additive and deliberately does not guess which deployed
database is the protected staging namespace. After upgrading an existing
staging database, `scripts/ops/staging_data_lifecycle_bootstrap.py inventory`
produces a digest-bound, read-only proof that the lifecycle registry, journal,
classified links and epoch authority are all empty. A separate `apply` with
that reviewed digest initializes only `staging/loom-staging` at epoch zero. It
locks and rechecks the complete bootstrap inventory in one transaction; an
existing non-bootstrap epoch, event, registry row, classified execution row,
schema drift or concurrent publisher fails closed. The exact epoch-zero row is
an idempotent no-op. This explicit maintenance bootstrap is also the earliest
preflight predicate for cleanup; lifecycle tooling never relies on the rollout
final-apply path to create its authority implicitly.

The one-time deployed-`0065` maintenance bridge is
`scripts/ops/staging_data_lifecycle_prepare.py`. Its read-only `inventory`
binds the exact cumulative source SHA/tree/base, migration-policy digest,
the shared preflight migration-plan digest, canonical Alembic head, current
schema and absence of partial lifecycle
structures. A separately authorized `apply` must echo that digest. It rechecks
under one PostgreSQL advisory lock, runs only the canonical Alembic chain, and
then calls the same digest-approved epoch bootstrap used above. The transition
is fixed to `staging/loom-staging`; an earlier legacy revision, an ahead or
branched migration graph, a partial schema, source drift, another preparer or
any inventory drift fails closed. A crash never silently resumes: the operator
must inventory and approve the newly observed exact revision before another
apply. The Alembic script location is the absolute sealed-source migrations
directory and never depends on the invoking process's working directory. No
classification or deletion occurs in this step.

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

The rollout Tier 0 collector does not depend on migration `0067` already being
live. Its installed Kubernetes TokenRequest may open localhost transports only
to the exact `loom-minio-0` and `loom-postgres-0` pods. MinIO independently
authenticates a fixed list-only user whose one exact policy can read bucket
location/versioning and enumerate versions in only the staging trajectories
and artifacts buckets; it cannot read object payloads or write/delete anything.
The collector inventories those exact buckets and measures the canonical
`/data/loom-staging/minio` backing filesystem directly under the rollout
service account. This fresh snapshot is the single source for
`capacity.high-water` on both legacy and migrated staging. The `0067` row
remains runtime run-admission authority, but a missing legacy row can no longer
hide a real byte, object, disk, or inode blocker from rollout preflight.

Migration `0067` persists the same exact capacity authority for runtime run
admission. `scripts/ops/staging_data_lifecycle_capacity.py` enumerates every
explicitly allowlisted execution bucket, measures the configured staging data
filesystem, and atomically publishes object/byte/free-space counters with the
policy and evidence digests. Evidence is valid for five minutes. Every staging
batch creation path calls the shared lifecycle registry before inserting the
batch and fails closed on missing, stale, drifted, or hard-high-water evidence;
lazy binding of an already admitted pre-migration batch does not retroactively
reject its in-flight children. Non-staging environments retain their pinned,
non-destructive policy.

All lifecycle maintenance entry points share the dedicated
`loom.data_lifecycle_runtime` environment contract. Database-only bootstrap
requires only `LOOM_LIFECYCLE_DB_URL`; capacity, classification, GC, and the
scheduled maintenance worker additionally require the exact
`LOOM_LIFECYCLE_MINIO_*` endpoint and credentials. They never instantiate the
control-plane settings or require JWT/provider/application secrets. Secret
values are excluded from representations, output evidence, and validation
errors. The direct database URL is intentional: these transactional authority
operations do not run through transaction-pooling PgBouncer.

```bash
python scripts/ops/staging_data_lifecycle_capacity.py \
  --namespace loom-staging \
  --bucket trajectories \
  --bucket artifacts \
  --filesystem-path /data \
  --output /secure/evidence/capacity-$(date -u +%Y%m%dT%H%M%SZ).json
```

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

### Historical classification

Rows created before migration `0066` are never implicitly grandfathered into
GC. The supported classifier is
`scripts/ops/staging_data_lifecycle_classify.py`: `inventory` reads all five
execution tables in one repeatable-read snapshot, groups event/LLM rows under
their exact trial owner, and hashes every present artifact object through the
normal S3/MinIO credentials. Exact not-found responses become digest-bound
`verified_absent` evidence; authorization, transport, size, version, or digest
errors remain blockers. Inspection uses a bounded worker pool (32 by default,
never more than 64) and returns all failures together without unbounded future
submission. Missing owners, cross-team links, incomplete bucket/key metadata,
and digest/size/version drift are returned together as blockers. There is no
prefix fallback.

The same inventory also reconciles physical legacy objects that were written
outside an artifact row's single `storage` descriptor. Each accepted object is
still a separate bucket/key/version/digest/size registration: the classifier
never converts a directory into deletion authority. It recognizes only these
database-proven layouts:

- `<team>/<trial>/main/...` workspaces and exact
  `<team>/<trial>/{events.jsonl,atif.json}` trajectories, when both UUIDs match
  the trial row;
- delivery-export sidecars whose team, batch, and artifact UUIDs all match;
- family-state snapshots whose batch and complete family key match one
  `batch_family_state` row;
- user TaskSet roots whose team and slug match one `task_sets` row; these are
  pinned catalog authority, not execution-history deletion authority; and
- the two fixed pre-lifecycle sample/catalog roots, retained under pinned
  system authority.

Every key must use the strict canonical grammar, every S3 inventory entry must
carry a timezone-aware creation timestamp, and a second exact GET must reproduce
the enumerated version and size while producing its SHA-256. A malformed,
cross-team, unknown, changed, disappeared, or delete-marker identity remains a
blocker. This permits referential cleanup of legacy trial/family/export objects
without putting TaskSet, catalog, bootstrap, or system inputs into the purge.

A historical LLM-call row can legitimately outlive a trial because the legacy
schema did not enforce that reference. Such a row still carries an exact team,
row identity and capture time, so the classifier assigns it a distinct
ephemeral `event/orphan` authority rather than inventing a trial relationship
or blocking all unrelated cleanup. The authority owner ID is the exact LLM-call
UUID with a typed prefix. A trial event without any recoverable team authority,
an LLM call linked to a trial owned by another team, and any artifact whose
declared team conflicts with its linked batch/trial, remain blockers because
their deletion scope cannot be proven.

Legacy benchmark, catalog, bootstrap, and system artifacts follow the same
exact-object inspection path, but the classifier registers only those durable
data classes as pinned authorities with no expiry. A legacy
`shared_reusable` retention hint does not by itself turn a per-run evidence,
debug, trajectory, or ATIF artifact into permanent storage; those artifacts
remain subject to the seven-day execution-history policy. This keeps true
durable keys visible to referential reconciliation without letting a sharing
hint create unbounded retention. An unknown data class or an object whose
exact identity cannot be proven still fails closed.

The inventory digest excludes wall-clock report time, but binds every row
fingerprint, exact object identity, retention authority, scope, and current
mutation epoch. Operator output is a schema-v2 summary whose size is bounded
with respect to the successful row/object inventory: it contains that digest,
per-table/per-class counts, present-object bytes, exact-absence counts, and all
blockers, but does not duplicate a million-row applicable plan into stdout or a
second giant JSON file. `apply` rebuilds the complete live plan and requires
the operator to echo that digest and provide a request ID. It locks all
execution and supplemental-source tables in one fixed order, then locks the
epoch, proves the complete unclassified row set and all source fingerprints
are unchanged, installs deterministic authorities, binds exact objects and
rows, and advances the epoch in one transaction. A concurrent row or metadata
change blocks outside that transaction or makes the next inventory drift; it
cannot commit an unclassified row inside the approved publication window. This
classification step authorizes later inventory; it does not itself delete any
DB row or object.

An explicitly authorized one-time staging reset may additionally provide
`--expire-created-before <timezone-aware timestamp>`. The cutoff is included in
the inventory digest and authority metadata; only unpinned legacy owners
created before that exact instant receive an expiry no later than the cutoff.
Without this argument the normal seven-day TTL is unchanged. Classification
still performs no deletion, and durable benchmark/catalog/bootstrap/system
authorities remain pinned regardless of the cutoff.

The installed preflight database role has SELECT authority over these five
execution-history tables in addition to the small control-plane baseline. It
still has no TEMP, schema CREATE, write, sequence, secret, token, or provider
connection authority. This lets preflight and the cleanup planner inventory
legacy history before migration or deletion instead of discovering its shape
only after a protected mutation.

```bash
# Read-only; prints all blockers and the approval digest.
python scripts/ops/staging_data_lifecycle_classify.py inventory \
  --namespace loom-staging \
  --artifacts-bucket loom-staging-artifacts \
  --trajectories-bucket loom-staging-trajectories \
  --output /secure/evidence/legacy-inventory.json

# Separate, explicit mutation authority after reviewing that exact document.
python scripts/ops/staging_data_lifecycle_classify.py apply \
  --namespace loom-staging \
  --artifacts-bucket loom-staging-artifacts \
  --trajectories-bucket loom-staging-trajectories \
  --requested-by qianyi \
  --request-id req-legacy-<reviewed-id> \
  --approved-inventory-digest <sha256> \
  --output /secure/evidence/legacy-apply.json
```

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
Migration `0068` normalizes resume evidence for large runs: the run row retains
only the scope, epoch, canonical inventory digest and bounded counts, while
the authority-membership journal retains every exact authority (including
authorities with no object), and each object item retains its exact authority,
bucket, key, version, hash and size. Apply copies that full identity into a
temporary exact plan and compares every field while marking; changing object
metadata without changing its UUID therefore rolls back the run before any
external deletion. Resume reconstructs and re-hashes the canonical plan from
both journals. Legacy schema-v1 run
documents remain readable, but new runs no longer duplicate a hundreds-of-
thousands-object plan in both JSONB and item rows.
The operator requires every execution bucket as an explicit repeated
`--bucket` argument, enumerates non-versioned objects or every exact
version/delete marker as appropriate, and rejects registered-size drift. A
registered bucket outside that allowlist fails closed instead of disappearing
from evidence.
The reusable executor in `loom.data_lifecycle_gc` binds a deterministic plan to
the current mutation epoch. An unversioned object is deletion-eligible only
when its exact SHA-256 is registered; a versioned object is addressed by its
exact version. Object deletion, absence verification, business-metadata
removal, and the epoch increment are distinct journaled transitions. A dry run
records its inventory digest but cannot call the object-store deleter.
The S3/MinIO adapter checks exact size and version before delete, and streams an
unversioned object to verify its registered SHA-256. Drift is detected before
the delete call; absence verification uses the same exact version identity.
Large plans retain those predicates but execute in bounded phases: up to 32
object identities are verified concurrently, a fully verified group is sent
through explicit S3 `DeleteObjects` requests of at most 1,000 keys, and the
returned identities must match the requested set before the journal advances.
Journal state transitions use transaction-local COPY tables rather than an
unbounded SQL `IN` list. Each batch is then independently verified absent; a
partial external delete remains resumable from its exact marked identities.
After a failed apply, resume reconstructs the canonical epoch-bound plan from
the journal, verifies one exact deletion token across every item, and claims
the failed run with a compare-and-swap transition before doing more work. It
continues from each item's recorded phase, so an already deleted object is
verified rather than deleted again and already removed metadata is not replayed.
An active, dry-run, completed, mixed-token, or tampered-inventory run cannot be
resumed. A second resumer loses the claim and fails closed.

The supported operator entrypoint is
`scripts/ops/staging_data_lifecycle_gc.py`. It loads the dedicated lifecycle
database and object-store settings from the least-authority secret-bearing
environment, never accepts secret values on argv, and supports four explicit
actions: `inventory`, `dry-run`, `apply`, and `resume`. `inventory` is purely
read-only and returns every blocker together. `apply` additionally requires the
operator to echo the exact live inventory digest and provide a mutation request
ID; a newly observed row, object, or epoch therefore invalidates authorization.
The digest excludes the report-only `planned_at` wall clock, so a separately
reviewed inventory remains usable while its exact epoch, eligible owner/object
set, and blockers are unchanged; a newly eligible authority changes that set
and therefore changes the digest.
Evidence output can be written once to a new mode-0600 file. `resume` accepts
only an exact failed run ID and its request authority; it does not rebuild or
broaden the deletion plan from current prefixes.

```bash
python scripts/ops/staging_data_lifecycle_gc.py inventory \
  --namespace loom-staging \
  --requested-by qianyi \
  --bucket trajectories \
  --bucket artifacts
```

The staging-only `loom-staging-data-lifecycle` CronJob runs every five minutes
inside the protected staging namespace. It first publishes capacity from the
same exact bucket inventory, then uses the `auto` action: no eligible owners is
a journal-free no-op; any classification/reconciliation blocker makes the job
fail and alert; otherwise the ordinary epoch-bound two-phase executor performs
the deletion. The pod has no service-account token, uses a read-only root
filesystem, drops every Linux capability, and receives only its dedicated
lifecycle database and object-store credentials. A namespace-scoped
NetworkPolicy limits egress to the selected Postgres and MinIO pods. The
capacity PVC mounts are read-only and all configured data filesystems must be
measured; missing or duplicate filesystem authority fails closed. Development
and production renders omit the CronJob entirely. Automatic policy authority
does not weaken the manual digest-approved `apply` or exact failed-run `resume`
paths.

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

Each complete lease is published once under its evidence digest. The active and
candidate pointers live in a separate generation-numbered rotation record and
advance through compare-and-swap publication. A rotation record may reference
only an already-published byte-equivalent lease. Deletion is always a returned
post-publication action: a crash can retain an extra old payload for retry, but
cannot erase the current known-good payload or promote an unverified candidate.
Every post-publication deletion is also retained in that rotation record as an
exact retirement entry, including its request, timestamped bundle, reason and
manifest digest, until the idempotent payload remover succeeds and a second
compare-and-swap acknowledges it. Before deleting the large root, the worker
publishes compact immutable evidence and later a deletion receipt under the
private request store. The remover revalidates the non-`latest` root, request,
manifest digest, service ownership, modes, link counts, filesystem boundary and
bounded no-follow traversal immediately before removal. The installed worker
also resolves the sole active attempt back to its exact payload; it may not
retire a referenced rollback point. A pending or referenced retirement
blocks admission of another candidate, so repeated process crashes cannot lose
the payload identity or accumulate more generations. Immediately after a
promotion the physical bound is transiently two payloads; after retirement
acknowledgement it is one.
The persisted state distinguishes manifest verification from restore
verification: the former carries only the exact manifest digest, while the
latter is the first phase allowed to carry a complete lease.

The lease constructor accepts only a strictly revalidated schema-v2 checkpoint.
It binds the exact manifest and component hashes, identifies the PostgreSQL
snapshot by its dump digest, parses the content-addressed object inventory, and
requires its epoch, schema revision, environment, namespace, and clock to match.
An isolated restore report must repeat every one of those fields and bind its
own evidence digest before a lease can exist. A schema-v1 full-MinIO DR archive
or a manifest-only backup can never become rollout lease authority.

If any proof is unavailable, the operator must fail closed to a fresh verified
checkpoint. A changed-epoch checkpoint plus restore must complete within 30
minutes before shared staging rollout may proceed.

### One-time convergence of legacy payload generations

Payload roots created before the rotation record are converged with
`scripts/ops/staging_rollout_backup_retention.py`, never with `rm` or a prefix
glob. `inventory` binds the backup filesystem identity, exact `latest` target,
every complete timestamped root and manifest digest, plus every incomplete or
noncanonical directory that must remain untouched. A complete root is eligible
only when its private single-link manifest parses under the known schema,
matches `staging` and the configured namespace, carries the exact verified
component set, and keeps every recorded component path lexically inside that
root. The immutable plan records manifest bytes, component names, payload file
count and logical payload bytes for each protected or retiring root; operators
therefore review an exact deletion inventory rather than authorizing a path
prefix. `apply` requires the exact plan file and its separately approved digest.
It refuses an active rollout, re-inventories protected inputs, then retires only
exact listed complete roots through the same evidence-first no-follow remover
used by normal rotation.
Hard-linked regular payload files are safe to unlink from the selected root;
directories, symlinks, special files, ownership/mode drift, manifest drift and
cross-filesystem traversal remain fail closed. A crash-safe receipt makes the
same approved plan idempotently retryable. The current `latest` payload and all
manifest-less failed, partial or route-cutover evidence remain preserved.
