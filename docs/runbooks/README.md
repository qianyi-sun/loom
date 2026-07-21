# Runbooks

Operator-facing procedures for running Loom in production or shared-dev
environments. `docs/index.md` at the repo docs root is the task-router.

## Reading order

1. **[operator-runbook.md](operator-runbook.md)** — the master runbook.
   Deployment, upgrade/rollback, rate-card management, token rotation, alarm
   response, backup/restore, capacity planning. Everything else here is
   scoped by phase or scenario; the master runbook covers the steady state.

2. **[first-prod-release-runbook.md](first-prod-release-runbook.md)** — the
   single operator path for the first `main`-based production release.
   First-prod bootstrap, temporary staging capacity leases, frontend route
   checks, production release gate, rollback preparation, emergency staging
   drain. Read this before doing the first production cut; retire it for
   normal production ops.

3. **[staging-launch.md](staging-launch.md)** — release-owner checklist for
   promoting `dev` to `main`. Deployment, onboarding, Run Library, security,
   and smoke evidence gates.

4. **[staging-migration-runbook.md](staging-migration-runbook.md)** — live
   migration from the legacy `public-beta` name to `staging`. Run this when
   the pilot cluster is drained.

5. **[full-max-slot-canary-runbook.md](full-max-slot-canary-runbook.md)** —
   preparation artifact for the unified staging canary. Wait for coordinating
   thread `GO` before actually submitting.

6. **[remote-worker-pool.md](remote-worker-pool.md)** — join extra
   Docker-capable hosts to an existing Loom control node for shared-dev or
   staging capacity before moving to full Kubernetes cluster mode.

7. **[local-dev-workflow.md](local-dev-workflow.md)** — developer-facing
   setup for pre-push testing on a laptop. Single-node kind, in-cluster
   `k8s_worker` by default with external Slurm as an advanced option. Local is
   not a formal environment; it is not in the identity contract or on
   `yylx.world`.

## When to open a new runbook

For a repeated procedure that spans more than a few commands, has hazards
(rollback / drain / dual-write), or coordinates multiple operators. Ad-hoc
troubleshooting stays inside the master `operator-runbook.md` under the
matching section.
