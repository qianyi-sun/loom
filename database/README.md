# Retained database migration packages

This directory groups the published migration histories of Loom's retired
shared-cluster capacity system. They are compatibility resources, not supported
execution services or deployment options. The active application migration
chain remains in [`../migrations/`](../migrations/).

| Package | Historical responsibility | Published head |
| --- | --- | --- |
| `capacity_migrations` | Global management database: allocations, grants, work queues, membership and release evidence | `capacity_0023` |
| `capacity_guard_migrations` | Per-environment `loom_capacity_guard` schema: protected trial/worker admission, fencing and release records | `guard_0035` |
| `capacity_build_guard_migrations` | `loom_capacity_build_guard` schema: task-image build authorization, assignments, publication and release evidence | `build_guard_0032` |

## Why these remain

Published revisions preserve database lineage and the interpretation of retained
records. Disposable schema reconstruction, historical migration tests and
qualified recovery still consume these packages. Their presence does not enable
retired controllers, workers or autoscalers. See the
[retirement record](../docs/historical/shared-cluster-retirement-2026-09.md).

The source directories are grouped here while Python package names and installed
wheel paths remain unchanged. Revisions can continue importing historical SQL
helpers by those names. Each `alembic.ini` locates its scripts relative to itself;
repository tooling uses `database/<package>/alembic.ini`. Migration environment
and role checks still apply. Moving source does not run migrations.

## Change policy

Do not rewrite published revisions, combine these independent chains, or infer
that retained schema grants permission to execute retired workloads. Correct
schema changes use forward migrations and preserve required data. Keep packaging,
image inputs, CI ownership and reconstruction tests aligned with source moves.

A clean Nebius baseline or an exporter/importer would be a separate change. It
requires an explicit retained-data contract, a proven conversion against an
isolated restored backup, and a supported route for historical recovery. A Git
tag alone is not proof that a backup remains restorable. No database reset,
schema deletion or live conversion is part of this directory reorganization.
