# Historical Nebius database conversion

The database on the historical `codex/nebius-main` branch shares migrations
through `0132` with `dev`. Its `0133`–`0135` mean reward projection, zero-quota
observations, and native task-image attempt evidence. Those changes are
`0144`–`0146` on `dev`; `dev`'s own `0133`–`0143` must also run. Nebius later
added native resource usage at `0136`; dev retains its published migrations
through `0149` and appends native usage as `0150`.

`database/migrations/nebius_lineage.py` converts that fork to **0150**. Normal Alembic
upgrades continue to reject ambiguous historical revision numbers. The converter
checks the exact expected revision, absence of dev-only tables, the native-build
column definition, the historical quota constraint, and the complete native
resource-usage additions at `0136`. These markers are a
preflight, not proof of compatibility with every deployed database: qualify the
actual restored backup before applying it.

## Qualification and application

Use the independent Nebius operator authority and cluster identity checks from
[deployment](nebius-deployment.md). A legacy `loom-rollout` target does not grant
access to the independent Nebius database. Do not connect a developer checkout
directly to the live database or manually stamp its revision.

1. Read back the deployed candidate, cluster/namespace, exact database revision,
   current operations, and source schema. Preserve that candidate's image digests
   and rendered configuration. Resolve active deployment ownership before starting.
2. Retain an object-versioned backup and verify its checksum/byte count. Follow
   [restore verification](nebius-restore.md) to restore it into an isolated database
   and verify selected Trial, result, artifact, and usage identities. Retain that
   evidence and the restored database for conversion qualification.
3. Use a candidate service image containing this converter and the pinned dev
   migration history. In the isolated restore Job, set `LOOM_DB_URL` to the
   restored PostgreSQL instance and run the inspection command below. Add
   `--apply` to convert the restored database. Compare logical schema with a fresh
   database migrated to `0150`, and compare retained data, including native-build
   JSON, zero-quota observations, rewards, LLM calls/tokens, and object identities.
   Exercise application readback with the candidate. A passing synthetic test is
   not actual-backup qualification.
4. Stop admissions and quiesce all database writers, including service/control
   plane, Gateway, actuator, collectors, maintenance and scheduled Jobs. Record
   the previous replica counts and schedules for recovery. Drain active work;
   do not terminate unknown sessions or drop their locks. Create and verify the
   final backup after quiescence. Preserve backup-before-mutation ordering through
   the supported deployment path.
5. In the reviewed candidate `30-migrate.yaml`, prepend a conversion init container
   cloned from the migration container, preserving its image, namespace-local
   database Secret/CA mounts, resources, and security context. Set its name to
   `convert-nebius-lineage` and its command to the apply command below. Preserve
   any existing init containers and the ordinary database bootstrap container;
   remove probes and lifecycle hooks from the conversion init container.
   Use the normal deployer with this reviewed render, so backup completion and
   cluster checks remain required. Do not enable automatic Job retries.
6. Verify the conversion Job succeeded and the database reports `0150` before
   ordinary bootstrap/application rollout continues. Later dev revisions can then
   use ordinary Alembic upgrades. Restore writer counts/schedules only with the
   compatible candidate, then verify retained results, artifacts, accounting,
   native-build identities and HTTPS health. Retain sanitized phase evidence.

Inside the isolated restore or protected migration Job only:

```sh
# Substitute the source revision established by readback: 0133, 0134, 0135, or 0136.
python -m database.migrations.nebius_lineage --expected-revision 0136
python -m database.migrations.nebius_lineage --expected-revision 0136 --apply
```

The command takes the connection only from `LOOM_DB_URL`, never a command-line
credential. Inspection uses a read-only transaction. Application requires direct
PostgreSQL, READ COMMITTED and driver autocommit disabled. It locks all existing
public tables with `ACCESS EXCLUSIVE NOWAIT`, applies the missing dev migrations
through `0150` and updates `alembic_version` last. It skips only verified
preexisting additions: native-build `0146` for source `0135`/`0136`, and native
resource-usage `0150` for source `0136`. DDL and the revision transition commit
together.
Reward projection can fill a missing valid scalar; it preserves explicit results.
The zero-quota migration never restores a positive-only constraint. Existing
native-build JSON is never dropped or copied away.

## Failure and recovery

Busy writers, conflicting schema markers, missing migrations, or any migration
error abort the transaction. Locks are nonwaiting and statements have a 120-second
timeout. Retain the sanitized failure; diagnose before retrying. A repeated
conversion against `0150` is rejected rather than reinterpreting dev as Nebius;
confirm successful readback and use ordinary deployment continuation. A lost Job
acknowledgement requires revision/schema readback before replacing that Job.

After a committed conversion, rollback is restoration of the retained backup
with its compatible historical candidate and configuration. Do not run the dev
downgrade chain on historical native-build or zero-quota data. Restore into an
isolated target, verify retained identities, and follow the protected recovery
procedure for cutover. Never overwrite a database that has resumed accepting
writes without accounting for those writes.

## Local regression evidence

`tests/integration/test_nebius_lineage_conversion.py` reconstructs each historical
fork on disposable PostgreSQL, compares the converted logical schema with the
normal dev schema, and checks retained data and atomic failure. It supplements
the ordinary migration-lineage rejection test; neither test grants live
deployment authority or replaces actual-backup evidence.

The default fresh-install protected schema reference is pinned to `0152/guard_0035`,
independently provisioned for PostgreSQL 16 and 17. Historical references retain
their original recipes and fingerprints. Conversion qualification compares logical
schema and retained data; it does not prove a restored historical database matches
the fresh-install protected inventory, which also binds physical column positions.
The guarded ownership-handoff verifier must accept its exact admitted inventory
before any such handoff; conversion does not bypass that check.
