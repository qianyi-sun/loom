# Full Nebius platform integration

Status: owner-approved target and integration policy, 2026-09-08. Implementation
and live acceptance remain open under #1536 and #1538.

## Branch and delivery contract

`codex/nebius-main` is the integration branch for the complete Nebius series.
It starts from `dev` commit `722ca66a1bb003377667b46c3ddfb93d094b2efb`.
Create feature branches from its latest remote head and target all series PRs
to `codex/nebius-main`, not `dev` or `main`. Examples:

```bash
git fetch origin codex/nebius-main
git switch -c codex/nebius-<issue>-<change> origin/codex/nebius-main
gh pr create --base codex/nebius-main
```

The branch requires one direct GitHub Actions result (app 15368):
`repository-checks`. One CI workflow plans changed paths and aggregates all
selected jobs, including reusable image, Kubernetes and system checks. Failure,
cancellation or an unexpected skip blocks this result. Require the current
head and current integration base, strict checks, PR-only squash merges,
linear history, no bypass, no force push, and no branch deletion. A trusted
collaborator enables GitHub-native auto-merge; there is no custom merge
controller or extra approval gate.

The initial seed used the inherited four checks. This CI simplification changes
only the Nebius integration ruleset after the single aggregate is verified.

`dev` remains the repository default and the lane for unrelated work and
existing-service repairs. Its protection and `main` promotion policy are not
changed. Already merged Nebius foundations are inherited, not reverted from
`dev`. Necessary upstream fixes enter the new branch through explicit, tested
PRs; do not repeatedly merge or rebase the shared integration branch itself.

Do not open or enable an integration-to-dev merge until exact-candidate pure
Nebius acceptance passes. Passing acceptance makes that merge eligible for an
owner decision; it does not automatically authorize or perform the merge.
Any later approved PR into `dev` must satisfy its then-current admission policy.

## Superseded design

This decision replaces the earlier terminal architecture of permanent
Nebius/OLDLAB/GB10 coexistence in #1536, #1548, and #1553. Hybrid implementation
and historical evidence remain useful migration inputs. They are not the
target architecture or sufficient evidence for this branch's acceptance.

Old deployment instructions and baked-in `dev` allowlists describe the legacy
lane. Do not merely broaden their authority to deploy this experimental branch
onto existing staging. Implement candidate-bound Nebius build/publication and
deployment against the new branch's independently identified target first.
Branch creation does not claim that this deployment wiring already exists.

## CI scope and remaining delivery work

CI uses explicit GitHub-hosted runners temporarily; #1798 owns Nebius-hosted
runner provisioning and immutable candidate publication/deployment. Hosted CI
passing is not proof that all CI compute already runs on Nebius. PR validation
has no OLDLAB/GB10 runner leases, localhost package mirrors, Slurm smoke,
personal-dev builders, capacity-manager/executor images or legacy rollout
checks. The image reusable workflow builds and scans; it cannot publish. Server image and Linux locked-install
validation target the current Nebius AMD64 execution classes. GB10-motivated
ARM64 server builds are removed; the independent macOS client lock check stays.
This does not claim ARM64 workload parity: any such required workload remains
part of the explicit compatibility inventory and acceptance under #1550.

`config/component-ownership.toml` is the shared source for active test paths,
image matrices and payload cases. `ci_ignored_paths` also filters executable
lane selection. Mixed Python suites mark only retired-pool cases `legacy_pool`;
CI excludes that marker. Generic worker, durable data, migration, API, provider,
frontend, benchmark and Nebius rejection-boundary coverage remains active.
Mypy keeps strict checking. Frontend coverage thresholds remain unchanged;
Python total coverage is an opt-in diagnostic, not a merge threshold. Active
imports of historical modules are still type-checked until runtime decoupling.

Ordinary Python runs do not instrument coverage or exchange coverage artifacts.
Use `ci:coverage-summary` for a report. Removing old-platform suites changes
the measured population, so the inherited aggregate 70% floor is not retained
or made green by excluding uncovered active code. Functional test failures
still block admission.

The Kubernetes lane exercises real disposable Kubernetes API operations and
execution; system smoke retains the local Compose user flow. Both are
credential-free checks, not live Nebius end-to-end acceptance. Deployment and
public-endpoint acceptance remain separate work under #1798 and #1538.

## Terminal architecture

All Loom-operated resources run on Nebius: web/API, control plane and
schedulers, model Gateway, every supported worker pool, database, registry,
object storage, backups, builders/CI runners, deployment executors, retention,
credential maintenance, logs and monitoring. GitHub remains the source and CI
orchestrator. User-selected external inference APIs remain supported; moving
the platform does not require hosting every model on Nebius.

Start with one region and the existing managed-cluster foundations. Separate
system services from elastic execution node pools. Keep environment identities,
database/storage scopes and resource policies separate. Shared-cluster failure
domain limits must be explicit; another cluster requires a concrete requirement.

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
| #1798 | Exact-branch artifacts, independent deployment/CI route and acceptance runner |
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
- Prove every platform component, active worker, builder, deployment executor,
  database, object store and scheduled maintenance dependency is on Nebius.
  Normal operation, upgrades and recovery must require no OLDLAB/GB10 route,
  secret, SSH account, mount, broker, runner or scheduled process.
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
