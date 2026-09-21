# Shared-cluster retirement — September 2026

The repository retirement decision of 2026-09-17 makes Nebius the only supported
hosted platform. OLDLAB, GB10 and Slurm execution are retired: they are not
fallbacks, expansion targets or release prerequisites. The purpose is to remove
parallel infrastructure and execution paths from the supported product.

The preceding architecture combined Kubernetes platform services with remote
Docker workers, Slurm allocations, host cgroup guards, separate task-image
builders and shared development-fleet capacity services. It also offered
ephemeral OLDLAB GitHub Actions runners. Git history preserves that source;
retired implementations and tests are deleted, not archived here.

The pre-retirement `dev` snapshot is
`504600cbcee21e4dfd10619ef34d64f455e21813`. This identifies the source baseline,
not a verified deployment or a guarantee that every legacy capability worked.
The architectural context is recorded in issues #1536, #1548 and #1553;
workload parity and live acceptance have separate ownership in #1550 and #1538.

## Replacement and gaps

Nebius hosts platform services, Kubernetes execution, database, object storage,
registry, backups and monitoring. Native execution uses durable attempts and
leases, Kubernetes Job identities and fenced output publication. Native image
build reconciliation lives in `loom_execution_actuator.task_image_controller`.
Hosted deployment uses `scripts/ops/deploy_nebius_platform.py`, preserving
its target, backup and migration checks. The old broker entrypoint is retired;
`loom cluster up` refuses hosted targets. Cluster release evidence no longer
requires external worker pools, Slurm authority, GB10 status or GB10 mirror probes.
CI, native AMD64/ARM64 image builds and publication use GitHub-hosted runners.
Local development, disposable tests and user-selected external inference APIs
remain supported.

Retirement does not implement missing workload replacements. The baseline
compatibility inventory marks desktop/GUI and Behavior GPU service workloads
unsupported; several pipeline and task classes still require conversion.
Hosted pipeline creation/retry and the shared-cluster Stage 1 smoke endpoints
are removed. Historical run reads, artifact access and cancellation remain;
explicit local development keeps the pipeline submission path. The generated
compatibility report lists only `nebius-cpu`, with 66 conversion-required classes
and three unsupported classes at retirement.
These gaps remain explicit. Removing a hosted path does not certify native
parity, and local/domain functionality and retained results must remain usable.

## Historical schema and data

Published application migrations, including the legacy worker, pool, build
grant and provenance tables, remain immutable. Their historical identifiers
can remain in schema models, migration fixtures and retained records without
authorizing new execution on those backends. No tables or retained data have
been dropped as part of the repository retirement.

The application chain at the source baseline ends at `0150`. Earlier Nebius
and `dev` branches used revisions `0133`–`0136` for different changes. Preserve
the [qualified lineage conversion](../ops/nebius-lineage-conversion.md);
revision labels alone cannot identify the correct database history. Preserve
separate published capacity and guard migration chains and the helpers needed
to load and validate them.

Trial and attempt identities, generation fencing, rewards, trajectories,
artifacts, usage and provenance remain durable. Historical claim/refund and
grant-revocation constraints continue to matter when reading or restoring old
data. Migration tests use retained row fixtures instead of requiring retired
controllers to create that history.

## Operational boundary

This is a repository retirement. It neither performs nor proves infrastructure
shutdown, credential revocation, DNS changes, live data migration or cleanup.
Those operations require their own scoped authority and recovery evidence.
Ordinary CI is not proof of completed live migration or workload acceptance.
