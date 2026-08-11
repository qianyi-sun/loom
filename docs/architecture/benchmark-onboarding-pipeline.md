# Benchmark Catalog and Onboarding

Loom's native benchmark catalog is operator-managed. Catalog entries identify
an adapter, immutable source revision, task-manifest location, compatibility
metadata, readiness state, and activation profile. Runnable task rows and
their bundles are persisted in Postgres and object storage.

User-owned collections use [TaskSets](user-brought-tasksets.md) instead of
creating native benchmark rows.

## Sources of truth

- `config/benchmarks.toml` declares repository-known benchmark adapters and
  local fixtures.
- Adapter code under `src/loom/benchmarks/` converts upstream examples into
  Loom `TaskConfig` bundles.
- Registered `benchmarks` and `tasks` rows are the service catalog used by the
  SPA and evaluation APIs.
- Immutable object-store or Hugging Face manifests bind task IDs, checksums,
  bundle locations, source revision, adapter version, and profile identity.

Registration never treats an unversioned mutable upstream dataset as runnable
authority. Task IDs are normalized and unique within a benchmark, bundle
hashes are verified, and profile activation is atomic.

## Operator commands

The `loom datasets` group provides the current lifecycle:

```text
loom datasets sync-config
loom datasets import
loom datasets publish
loom datasets register
loom datasets audit
loom datasets activate
loom datasets verify
loom datasets provision-catalog
```

- `sync-config` reconciles `config/benchmarks.toml` with catalog rows.
- `import` converts tasks, uploads bundles, and inserts rows directly.
- `publish` converts and publishes an immutable dataset manifest for the Loom
  catalog.
- `register` verifies a published manifest, upserts benchmark/task rows, and
  can mirror bundles into internal object storage.
- `audit` reports catalog, manifest, bundle, task-config, verifier, adapter,
  architecture, and readiness blockers.
- `activate` selects a fully audited immutable profile in one transaction.
- `verify` samples registered tasks and runs the oracle path end to end.
- `provision-catalog` copies selected runnable benchmark, task, agent, and
  bundle state between controlled environments.

Credentials are read from environment or credential-source files. Do not put
database, object-store, provider, or Hugging Face tokens in command arguments
or catalog files.

## Readiness

Catalog presence does not imply runnable support. A profile becomes active
only when its manifest and source are immutable, every selected task has a
valid canonical `TaskConfig`, referenced bundles match their checksums, the
adapter and verifier are available, and the target architecture is supported.

The SPA and service expose these readiness states. Submission routes enforce
the same checks, so a client cannot bypass a blocked catalog entry by calling
the API directly.

Terminal-Bench-shaped `task.toml` bundles and `environment/` subtrees are
normalized by the current adapter/materializer path. Repository fixtures and
tests under `tests/fixtures/benchmarks/`, `tests/unit/`, and `tests/system/`
exercise manifest validation, conversion, registration, audit, activation,
and sample execution.

For third-party adapter authoring, see
[Benchmark adapter](benchmark-adapter.md). For operator command examples, see
the [operator runbook](../runbooks/operator-runbook.md).
