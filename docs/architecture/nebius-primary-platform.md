# Full Nebius platform integration

Status: owner-approved target; development integration requested 2026-09-14. Implementation
and live acceptance remain open under #1536 and #1538.

## Branch and delivery contract

The owner requested integration of `codex/nebius-main` into `dev` on 2026-09-14.
`dev` is the pre-main development branch for Nebius and other project work;
`main` remains reserved for validated release promotion from `dev`.
The earlier isolated branch remains historical source and release evidence.
New feature branches start at current `origin/dev`, and PRs target `dev`.

```bash
git fetch origin dev
git switch -c feature/nebius-<issue>-<change> origin/dev
gh pr create --base dev
```

The four strict, app-bound current-head checks remain merge authority:
`repository-checks`, `images-gate`, `cluster-smoke-gate`, and
`staging-smoke-gate`. Use native squash auto-merge, with no protection bypass.
The isolated branch's single aggregate and legacy-test exclusions do not apply
to `dev`. Existing capacity, signed task-image, rollout, dual-architecture and
release controls remain covered alongside Nebius tests. Credential-free
Kubernetes checks include the disposable native execution/platform tests.
Nebius candidate publication remains GitHub-hosted and protected by the
`nebius-integration` Environment. See [publication](../ops/nebius-candidate.md).

This integration is source delivery, not a deployment or a claim that all
workload, recovery, cost or retirement acceptance has passed. Open acceptance
remains owned by #1536 and its component issues. No legacy infrastructure is
retired by this merge. The target runtime architecture is Nebius; existing
platform code remains available during the separately qualified migration.

## Database lineage when moving from the isolated branch

The branches independently used revisions `0133`–`0135` for different changes.
`dev` keeps its published history through `0142`; the Nebius reward projection,
zero-quota observation and native-build observation migrations are appended as
`0143`, `0144` and `0145`. Fresh databases and existing `dev` databases upgrade
through that single chain.

An existing isolated-branch Nebius database at `0133`, `0134` or `0135` is **not**
a database at the corresponding `dev` revision. Do not run this checkout's
normal upgrade against it or stamp it to a `dev` revision: that could skip
required schema changes. Moving an existing Nebius deployment requires a
separately prepared and tested lineage conversion, with backup/restore evidence,
before selecting this `dev` candidate for deployment. Keep the previous
branch-bound candidate for that deployment until the conversion is qualified.

## Publication and acceptance boundaries

Development publication records the Git commit and immutable image references
once. Rendering and deployment reuse that record. Runtime image admissions
remain verified at their owning boundary; backup and transfer integrity checks
retain their distinct purpose. Existing release records from the isolated branch
remain readable for rollback. Hosted checks do not prove live Nebius acceptance.

## Terminal architecture

Deployed Loom services and workload resources run on Nebius: web/API, control
plane and schedulers, model Gateway, every supported worker pool, database,
registry, object storage, backups, retention, logs and monitoring. CI and
candidate image builds remain on GitHub-hosted runners under the current owner
exception; this architecture does not require separate Nebius runner or builder
infrastructure. Deployment commands may run from GitHub-hosted jobs or an
explicitly authorized local operator environment, while the deployed resources
and their steady-state operation remain on Nebius. User-selected external
inference APIs remain supported; moving the platform does not require hosting
every model on Nebius.

Start with one region and the existing managed-cluster foundations. Separate
system services from elastic execution node pools. Keep environment identities,
database/storage scopes and resource policies separate. Shared-cluster failure
domain limits must be explicit; another cluster requires a concrete requirement.

The owner's 2026-09-10 priority is stable operation in the primary region
(`eu-north1`). Cross-region acceptance is deferred until primary resources are
insufficient; it does not gate the initial single-region service. Keep secondary
routing disabled while paused. Existing regional resources and implementation
are retained for a later decision, not silently deleted or treated as accepted.
Focus current validation on ordinary-user submission, progress, cancellation,
durable results, native scale-to-zero, upgrades and recovery in the primary
region. #1897 owns the command-consumption and cancellation defects found during
the bounded rollout; #1884 retains the deferred regional expansion work.

Execution commands belong to one target. Each actuator, including callers of
`POST /admin/service-execution/commands/claim`, supplies its `target_id`; the
claim transaction selects only leases for that target before locking commands.
There is no default global consumer. Cancellation also covers an attempt that
never created a Job: authoritative namespace reconciliation must finish its
existing cleanup path without treating an active create as deleted.

Legacy worker heartbeat and stale-claim requeues exclude Trials whose current
attempt belongs to a service-execution lease, including revoked leases awaiting
cleanup. The service scheduler never selects a cancel-requested queued Trial.
Retry-exhaustion sweeps leave cancellation-requested records to cancellation
authority, while ordinary exhausted retries still become failed.
Ordinary cancellation replay can settle historical queued cancellation records
without creating work or changing a deleted lease; the original request time is
preserved. If provider cleanup is still pending, its admission, provisioning and
cost reservations remain held until the actuator confirms resource absence.

Browser-session cancellation carries the session and CSRF credentials to the
control plane, which independently validates the caller's submit scope and team.
Bearer-token cancellation retains the same authority checks. Neither path may
substitute an administrator credential for the ordinary user.

Expose a stable public HTTPS application endpoint with DNS, TLS, authentication
and team permissions. Browser, CLI and SDK users on an ordinary internet
connection must not need OLDLAB access, a VPN, SSH tunnels or port forwarding.
Keep database and worker management on private networking. Configure outbound
internet access according to the workload contract. Interactive task previews
and terminals use authenticated task-scoped proxy routes where supported.

Use Kubernetes Jobs for compatible workloads. Any temporarily necessary
Docker/host-compatible VM worker must itself run on Nebius and have a bounded
compatibility contract. Do not silently weaken tasks to fit the current narrow
CPU compiler. Inventory and validate actual Harbor/Terminus, multi-step,
sidecar/verifier, browser, GPU, ARM64 and host-specialized requirements. A needed
workload with no validated replacement blocks complete retirement.

Preserve durable Trial/attempt identity, single authoritative execution,
generation fencing, results, numeric verifier reward, complete trajectories,
artifacts and usage. Keep immutable commit/acknowledgement semantics when
simplifying same-cloud storage transfers. Kubernetes completion alone is not
Loom success.

## Work packages and ownership

| Existing issue | Revised responsibility |
| --- | --- |
| #1536 | Full Nebius umbrella, sequencing and retirement decision |
| #1548 | Full-platform architecture and production workload compatibility inventory |
| #1543 | Reproducible platform, public ingress, private dependencies, database/storage and environment bindings |
| #1540 | Provider-independent durable control plane with no required legacy-pool authority |
| #1549 | Nebius execution actuation, reconciliation and recovery |
| #1550 | Real workload parity across agents, runtime, sidecars and verification |
| #1551 | Reuse completed security baseline; validate changed public/private boundaries |
| #1552 | Reuse capacity implementation; validate Nebius-only admission and actual capacity |
| #1765 | Canonical data/output migration, integrity, retention and restore |
| #1766 | Public web/API/CLI operation and complete result delivery |
| #1798 | Exact-branch artifacts, GitHub-hosted builds and independent Nebius deployment |
| #1538 | End-to-end pure Nebius acceptance and recovery evidence |
| #1553 | Drain and retire all Loom OLDLAB/GB10 dependencies and obsolete paths |
| #1547 | Keep outstanding retired-provider credential/resource retirement separate |

Completed child implementation is not invalidated automatically. Expanded live
acceptance stays open in the parent/owning issues. Do not close a series issue
merely because its code merges to this branch.

## Execution sequence

1. Establish branch protection and contribution routing. Implement locked,
   digest-bound artifacts and deployment/CI execution independent of OLDLAB.
2. Provision/converge the independent Nebius platform and public application
   entry. Verify database roles, storage, secrets, certificates and backups.
3. Achieve parity for the actual supported workload inventory; validate complete
   user-visible outcomes and runtime recovery on Nebius resources.
4. Migrate data with one authoritative writer and verified counts/digests.
   Preserve rollback evidence; do not point the new candidate at a shared live
   database as a shortcut.
5. Run exact-candidate pure Nebius acceptance with old-pool routes ineligible.
   Prove platform dependencies through resource identity and network/readback
   evidence; a selected `backend=nebius` flag alone is insufficient.
6. Drain and retire Loom-owned old dependencies after migration acceptance.
   Preserve unrelated users' resources and retained data. Record revocation,
   cleanup and removal of obsolete deployment/test paths.
7. Only then evaluate promotion into `dev` with the owner and current CI.

## Pure Nebius acceptance

- From an external machine without private configuration: authenticate, access
  the catalog/upload inputs, submit, observe live progress, cancel/retry and
  download complete results through HTTPS browser and CLI/API surfaces.
- Prove deployed platform services, active workers, databases, object stores
  and steady-state maintenance resources operate on Nebius. GitHub-hosted CI
  and candidate image builds are explicitly permitted; deployment commands may
  run from GitHub-hosted jobs or an explicitly authorized local operator
  environment. Do not require Nebius runner or builder infrastructure for these
  activities. Normal operation, upgrades and recovery must require no
  OLDLAB/GB10 route, secret, SSH account, mount, broker, runner or scheduled
  process.
- Execute the agreed real workload inventory, including multi-step agent and
  verifier cases where part of the supported product. Record explicit gaps;
  do not substitute a direct-completion fixture for full workload parity.
- Verify numeric reward, complete native/canonical trajectories, artifacts,
  provider usage, delivery/download after compute release, and retained data
  after source cleanup.
- Exercise cancellation, retry, worker/node loss, actuator/control-plane
  restart, database permissions, output publication recovery and upgrade/restore
  from a previous supported state. Prove no duplicate authoritative execution.
- Use measured current-quota stages with real simultaneous execution, resource
  observations, cleanup and execution-pool scale-to-zero. Do not reinstate an
  arbitrary 200-concurrency or quota-increase prerequisite.
- Preserve candidate SHA, artifact digests, Terraform/config identity, public
  endpoint verification, commands, timestamps and results in an acceptance
  bundle. Ordinary CI, old hybrid batches and configured capacity are not this
  acceptance evidence.

This charter authorizes the repository direction and integration route. It
does not execute paid infrastructure changes, migrate live writes, change DNS,
or stop shared hosts. Carry separately authorized live operations through their
specific target and recovery boundaries when implementation is ready.
