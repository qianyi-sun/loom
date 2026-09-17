# Keep Harbor90 on x86 without changing benchmark identity

`terminal-bench-2-harbor-90` is the existing 90-task Harbor catalog, licensed
Apache-2.0. Preserve that benchmark ID and all 90 task IDs. This migration changes
its persistent source and published configuration to x86_64. It does not rename
this historical catalog as canonical TB2.1, rewrite historical Trials, or claim
that all 90 tasks have passed native Nebius execution.

The retained September 14 source export contains the original catalog metadata
and complete task directories. Its upstream comparison against
[Terminal-Bench commit 2fd12b88](https://github.com/harbor-framework/terminal-bench-2/commit/2fd12b88aafdd04a52c298e3940bcb189f9766d6)
found 87 Dockerfiles identical apart from line endings, two different Dockerfiles
(`build-pov-ray`, `distribution-search`) and one local task absent upstream
(`file-archive-manifest`). That comparison is source context, not permission to
replace current tasks with upstream files. The live September 16 catalog audit
found a newer `distribution-search` bundle. Refresh changed task sources from
the current catalog before preparing this migration.

## Prepare the source locally

Keep a current read-only catalog export with `benchmark` metadata and `tasks`
rows (`id`, `checksum`, `license`, and configuration/provenance). Download each
changed source through its exact current immutable object prefix. The existing
source metadata restores file modes; do not infer executable bits from a bare
object download. Never overwrite the older export. Export/source comparison is
the boundary that prevents an old local snapshot reverting a newer task.

Provide the two already accepted native task directories and their read-only
`preparation-report.json`, whose `tasks` entries record `name` and
`current_task_checksum` from the existing frozen TaskSet. The utility accepts
exactly that reviewed 20-task cohort. It preserves their entire bundle bytes and
modes; it does not synthesize a new generic verifier implementation.

```sh
python scripts/ops/prepare_nebius_harbor90.py \
  --source-root /protected/current90-source \
  --catalog-metadata /protected/current90-catalog-metadata.json \
  --native-overlay /protected/baseline10/tasks \
  --native-overlay /protected/additional10/tasks \
  --overlay-metadata /protected/preparation-report.json \
  --runtime-profile /protected/current-runtime-profile.json \
  --output /protected/harbor90-x86

loom datasets validate-local /protected/harbor90-x86 --json
```

The same inputs produce the same task contents. An existing output directory is
refused; retain the reviewed output or choose a new directory. Input trees are
never modified. The utility checks the existing 90 IDs/license/current revisions,
rejects unexpected or duplicate overlays, and protects all step instruction
files and test assertions/data. Four accepted overlays already replaced the
bootstrap-only `tests/test.sh` with their native verifier bridge; only those
known omissions are permitted. New changes to assertions are rejected. The same Dockerfile compatibility
preflight used by `publish-local` runs for every task before output is written;
do not use `--compat-flatten-environment` to bypass a source-layout problem.

For the other 70 tasks, the tool edits only the environment architecture
declaration. It leaves deadlines, limits, Dockerfiles, verifier commands and
network settings unchanged. An executable `arm64`/`aarch64` Dockerfile reference
requires task-specific review; comments and guest MIPS/x86 emulation remain
intact. There is no broad string replacement and no automatic removal of
package installation or other legitimate verifier setup.

`migration-report.json` records the current source and prepared checksums, the
active runtime version, preserved files/modes, and per-task native admission
blockers. The approved 20 must pass the current native compatibility and
1-CPU/2-GiB/2-GiB request checks. The remaining 70 may still require explicit
resource limits, gateway-only runtime support and native verifier preparation.
For example, some verifier scripts install OCR tools, download model weights or
copy fixture data. Architecture compatibility does not make those operations
work under a nonroot, offline verifier. Preserve them until each task has an
explicit reviewed adaptation.

## Publish and retain the x86 source

Review the generated artifact before invoking the existing supported publisher:

```sh
loom datasets publish-local /protected/harbor90-x86 \
  --db-url env:LOOM_DB_URL \
  --minio-access-key env:LOOM_MINIO_ACCESS_KEY \
  --minio-secret-key env:LOOM_MINIO_SECRET_KEY \
  --bucket "$LOOM_CATALOG_BUCKET" \
  --minio-region "$LOOM_MINIO_REGION" \
  --imported-by nebius-harbor90-x86-migration
```

Use the existing catalog bucket. Publication defaults to object operations only,
so a Nebius `storage.object-editor` identity does not need `HeadBucket` or bucket
creation permissions. Do not use `--create-bucket` for this migration; that flag
is only for explicit bootstrap with a bucket-capable identity. Actual object
write failures still abort the database transaction.

Both `publish-local` and `audit --verify-bundles` take `--minio-region`, defaulting
to `LOOM_MINIO_REGION`, then `LOOM_SVC_MINIO_REGION`, then `us-east-1`. Set the
actual Nebius region. When invoking the CLI in the control-plane container,
map `LOOM_CP_MINIO_REGION` along with endpoint/access/secret environment values
to the corresponding `LOOM_MINIO_*` names without printing their values.

Use the established protected operator identity and its object-store endpoint;
credentials must not appear as literal arguments. This is a catalog/database and
object-store mutation, separate from local preparation. The publisher updates
the existing rows and writes new content-addressed task revisions plus native
input manifests. It does not edit or delete old object revisions. Frozen Trials
and their archived input/output references remain historical records.

Retain the complete prepared directory, migration report and publication receipt
as the source for subsequent catalog publishing. Export/download the published
catalog through the ordinary supported CLI and compare IDs, task contents and
executable modes with that prepared source. Read back all 90 task configurations
as x86_64 and verify the ordinary catalog identifies the same 90-task benchmark.
Do not republish the older ARM export. Use the generated report to state which
20 tasks are native-compatible and which 70 still have native blockers; catalog
registration or successful publication is not all-task runtime acceptance.

Changed architecture/checksums produce new task-image materialization keys.
Those keys include task ID, checksum and architecture; native persisted BuildKit
cache prefixes use the same key. First execution of these benchmark revisions
can require cold image builds even when equivalent TaskSet tasks ran earlier.
Measure image preparation separately from execution packing. Use the separately
bounded 20-task real acceptance to verify runtime behavior; do not launch 90
model calls as part of this catalog migration.
