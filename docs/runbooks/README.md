# Runbooks

Current procedures for Loom operators and developers. Start with the master
operator runbook; use the narrower runbooks for the named environment or
deployment workflow.

## Cluster and release operations

- **[Nebius deployment](../ops/nebius-deployment.md)** — hosted deployment, backup
  protection, migration and readiness checks.
- **[Nebius restore](../ops/nebius-restore.md)** — isolated restore verification.

- **[Operator runbook](operator-runbook.md)** — deployment, upgrades,
  rollback, credentials, storage, capacity, monitoring, and incident response.
- **[Staging release validation](staging-launch.md)** — candidate-bound checks
  required before production promotion.

## Local development

- **[Local development](local-dev-workflow.md)** — local Docker Compose stack and
  pre-push checks.

Create a separate runbook only for a repeatable procedure with distinct safety,
rollback, or coordination requirements. Put component behavior in architecture
docs and one-off migration records in the
[runbook archive](../../archive/docs/runbooks/).
