# Runbooks

Current procedures for Loom operators and developers. Start with the master
operator runbook; use the narrower runbooks for the named environment or
capacity workflow.

## Cluster and release operations

- **[Operator runbook](operator-runbook.md)** — deployment, upgrades,
  rollback, credentials, storage, capacity, monitoring, and incident response.
- **[Staging release validation](staging-launch.md)** — candidate-bound checks
  required before production promotion.
- **[Multi-node staging on k3s](deploy-staging-k3s.md)** — topology,
  prerequisites, authorized host helper, verification, and recovery.

## Capacity and development environments

- **[Remote worker pool](remote-worker-pool.md)** — external Docker workers,
  Slurm-backed capacity, tunnels, and recovery.
- **[Developer sandboxes](developer-sandboxes.md)** — candidate-bound shared
  development sandboxes.
- **[Shared sandbox capacity](shared-sandbox-capacity-broker.md)** — disabled
  compatibility ledger, offline request/lease checks, and recovery evidence.
- **[Local development](local-dev-workflow.md)** — local kind deployment and
  pre-push checks.

Create a separate runbook only for a repeatable procedure with distinct safety,
rollback, or coordination requirements. Put component behavior in architecture
docs and one-off migration records in `archive/docs/runbooks/`.
