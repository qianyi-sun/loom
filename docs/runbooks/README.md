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
- **[Personal-development management-plane shadow](personal-dev-management-plane-shadow.md)**
  — exact render, deploy, readiness, rollback, and stop conditions for the
  inert shared management foundation in `loom-dev`.
- **[Personal-development zero-capacity acceptance](personal-dev-zero-capacity-acceptance.md)**
  — plan-bound enablement, two-owner candidate lifecycle acceptance, exact
  zero-ceiling observation, manager-first teardown, and byte-reviewed rollback.
- **[Executable global-capacity bridge rehearsal](executable-global-capacity-bridge-rehearsal.md)**
  — separately gated zero-ceiling manager and two-pool preparation evidence.
- **[Global fleet pool-executor dry run](global-fleet-pool-executor-dry-run.md)**
  — non-executable reservation, permit, inventory, journaling, fencing, and
  protected-release rehearsal for physical pool controllers.
- **[Local development](local-dev-workflow.md)** — local kind deployment and
  pre-push checks.

Create a separate runbook only for a repeatable procedure with distinct safety,
rollback, or coordination requirements. Put component behavior in architecture
docs and one-off migration records in the
[runbook archive](../../archive/docs/runbooks/).
