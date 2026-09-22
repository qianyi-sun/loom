# Historical staging data lifecycle

The shared-cluster rollout and its lifecycle operator tooling are retired and
unsupported. This record explains retained database and backup lineage, not a
procedure for current Nebius operations.

Historical lifecycle records bind runs, trials, events, artifacts and exact
object versions to an environment, namespace, team, owner, expiry or pin.
Object identity includes bucket, key, optional version, digest and size;
a prefix alone was never deletion authority. Published migrations retain these
records and their constraints so old databases and backups remain interpretable.

The previous garbage collector journaled deletion tokens, object verification,
absence checks and metadata removal. Its mutation epoch and rollout checkpoint
records tied backups to a schema revision, snapshot and immutable object
inventory. Restore and rollback readers must respect those identities rather
than infer safety from a revision number or an old retention duration.

The old capacity thresholds, CronJobs, systemd guards and rollout commands are
not current operational policy. Their exact implementation and procedures are
available in the
[pre-retirement source snapshot](https://github.com/qianyi-sun/loom/tree/archive/shared-cluster-final).
Retained schema does not authorize deleting historical rows or objects.

Use [Nebius deployment](../runbooks/nebius-deployment.md),
[restore verification](../runbooks/nebius-restore.md) and
[qualified database lineage conversion](../runbooks/nebius-lineage-conversion.md)
for supported operations. The [retirement record](shared-cluster-retirement-2026-09.md)
explains the repository-only scope and retained compatibility.
