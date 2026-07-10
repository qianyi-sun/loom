# Loom v1.0 Release-Ready Program Design

Status: design
Date: 2026-07-09
Owner: Qianyi Sun
Execution branch: `codex/v1-release-ready-program`
Canonical repository: `qianyi-sun/loom`

## Goal

Make Loom ready to promote from `dev` to `main` as v1.0. Readiness means the
candidate is evidence-backed, reviewable, and safe to promote; it does not
authorize merging the promotion PR, creating the final `v1.0.0` tag, changing
production desired state, or running a production rollout.

The program closes only when all pre-promotion requirements in this document
are proven against one immutable candidate SHA and no unresolved release
blocker remains.

## Ready-to-Promote Definition

All of the following must be true at the same time:

1. The v1.0 acceptance matrix has a single executable source of truth in
   GitHub issue #715, while #39 remains the release umbrella.
2. Required `dev` merge checks cannot become green while a path- or label-
   selected validation is pending, skipped unexpectedly, cancelled, or failed.
3. Production-facing GitHub Environments have branch restrictions and required
   approval rules appropriate to their secrets and deployment authority.
4. The v1.0 workload trust contract is explicit and machine-enforced:
   user-supplied TaskSet transforms are disabled, untrusted workload isolation
   is not claimed, and enabling either unsupported capability fails preflight or
   the release gate.
5. Release-blocking TaskSet materialization ownership is fenced so a stale
   executor cannot overwrite a newer executor's result.
6. Every pre-promotion row of #715 has fresh staging or deterministic dry-run
   evidence linked from the issue. Evidence must identify the candidate SHA,
   environment, actor/team, provider/model, batch/trial ids where applicable,
   and sanitized artifact checksums.
7. The Devansh-owned clean TB2/Harbor chain #744 -> #745 -> #746 -> #743 is
   complete and validated on fresh staging data. This program monitors that
   dependency and does not duplicate its implementation.
8. A draft `dev` -> `main` promotion PR exists with immutable candidate SHA,
   image digests, migration head, release manifest, open-risk statement,
   rollback target, and links to the complete pre-promotion evidence package.
9. The worktree, repository, GitHub Project, issues, milestones, owners, native
   dependencies, and release documentation agree on the same current state.

The final `main` merge, tag, production deployment, prod E2E, and post-deploy
smoke are release execution, not readiness work. They remain under explicit
production authority and keep #39 open until the release itself is complete.

## Current Baseline

The design baseline is `origin/dev` commit
`312ae20e961ca748640376ed3e79eef93062d4d9`.

Existing accepted evidence includes:

- detached staging rollout
  `/data/loom-staging/rollouts/20260709t181114z-staging-312ae20`;
- release-gate and admin-on-behalf smoke success for that rollout;
- all 15 GB10 hosts inspected for the HF token boundary;
- realistic 100-task user-uploaded TaskSet evidence from the 5003-style source
  under issue #718;
- explicit ownership handoff of #743-#746 to `Devansh8321`.

This evidence remains useful only where its scope matches a matrix row. It does
not substitute for a fresh final-candidate matrix run.

## Program Structure

The program uses independently reviewable PRs rather than one stabilization
branch containing unrelated risk domains. Each PR has its own issue, test
cycle, documentation update, CI labels, acceptance evidence, and rollback.

### Workstream A: Release Governance and CI Integrity

Issue #700 is the first implementation workstream because every later PR relies
on correct merge gating.

Every PR and merge-group candidate reports four stable required contexts:
`repository-checks`, `images-gate`, `cluster-smoke-gate`, and
`staging-smoke-gate`. The shared planner computes the validation work required
by changed paths and labels, and each gate enforces its selected result. A
selected job that is cancelled, skipped unexpectedly, or absent is a failure.
Labels may add validation but cannot remove path-inferred validation. Docs-only
PRs keep a bounded fast path while the stable contexts still report.

Codex enables squash auto-merge immediately after opening each normal `dev`
PR. GitHub queues the merge until every required gate is visible and successful
on the current head SHA and any applicable repository protection passes.
Promotion PRs from `dev` to `main` remain explicitly owner-managed and do not
use the routine `dev` auto-merge path. This is a Codex operational rule, not a
contributor-specific review policy.

The design covers at least:

- `ci:integration`;
- `ci:integration-docker`;
- `ci:images`;
- `cluster-smoke`;
- `staging-smoke`;
- coverage summary where selected.

Changed-path rules provide the minimum required set automatically. Labels may
add checks but may not remove checks inferred from changed paths. The PR
template and contributor guidance describe the rule, while a repository test
prevents workflow-to-documentation drift.

GitHub settings are part of the acceptance evidence:

- `dev` requires the final aggregate but no human approval; Codex-authored
  routine `dev` PRs auto-merge only after the required current-head CI is
  successful;
- conversation resolution is required;
- production-capable environments restrict deployment branches;
- `production` requires owner approval;
- secret-bearing `ci-aws` and `huggingface-publish` environments no longer have
  empty protection policy.

### Workstream B: Workload Trust Contract

The durable decision record is
[`adr/v1-workload-trust-contract.md`](adr/v1-workload-trust-contract.md).

Loom v1.0 is an internal, trusted-workload release. It does not claim that
arbitrary uploaded Python, verifier code, or task container images are securely
multi-tenant.

The release contract records explicit fields equivalent to:

```text
workload_trust_mode = "internal_trusted"
taskset_transforms_enabled = false
taskset_transform_network_isolated = false
untrusted_workload_isolation = false
```

The exact storage location follows the existing generated config and release
manifest patterns. Production and staging preflight must reject unsupported
combinations before any user transform code runs. In particular, setting
`taskset_transforms_enabled=true` is not allowed for a v1.0 candidate even if
`taskset_transform_network_isolated=true`, because the current transform
subprocess does not isolate filesystem, PID, UID, mount, output, or service
secrets and the checked-in service image cannot call `os.unshare`.

The existing TaskSet feature documentation must label transforms as unavailable
for v1.0 deployment. A focused post-v1 isolation issue owns the real one-shot
OCI/gVisor/Kata implementation and its adversarial tests. Disabling a feature
outside v1.0 scope is a product contract, not evidence that the sandbox is
fixed.

Worker sandbox startup must also be semantically explicit: if an environment
claims enforced isolation, failure to start the isolation boundary is fatal. A
trusted legacy environment may continue only when its release manifest says it
is operating in trusted mode.

### Workstream C: TaskSet Materialization Ownership

The TaskSet materializer runs in multiple service replicas. A fixed
`claimed_at` timeout without a lease generation allows executor A to continue
after executor B reclaims the same job and then overwrite B's state or task
rows.

The durable design adds an integer lease generation and heartbeat:

- claim or reclaim increments `lease_epoch` atomically;
- the owner refreshes `claimed_at` while materialization is active;
- every state transition and final task-row promotion uses a compare-and-set on
  `(job_id, lease_epoch, claimed_by)`;
- loss of ownership stops the old executor from publishing rows or terminal
  state;
- S3 output is written under a generation-specific prefix;
- only the winning generation promotes the manifest/pointer;
- abandoned generation objects remain eligible for bounded GC.

The HTTP upload path must not run blocking boto3 transfer on the event loop.
The first slice moves blocking transfer to a bounded thread or existing upload
worker without changing the public API. Multipart/presigned upload is a later
scalability improvement unless final-candidate testing proves it release
blocking.

Acceptance requires two-replica fault tests covering a job longer than the
claim TTL, stale owner recovery, old-owner completion after reclaim, process
crash, DB failure after object upload, and idempotent retry.

### Workstream D: GitHub Execution Surface

Issue #39 is the release umbrella and must not duplicate an executable matrix.
Issue #715 is the single pre-promotion matrix. Its rows are split into:

- pre-promotion evidence required for Ready-to-Promote;
- explicit release-owner dispositions, including the deferred post-v1 #88
  killed-driver drill;
- post-promotion production validation that keeps #39 open but does not
  redefine readiness retroactively.

The Project is made trustworthy before it is used as a WIP view:

- every open release issue is in the Project;
- title prefix and Project Status agree;
- P0/P1 work has an owner or an explicit handoff/blocker;
- milestone and priority labels match title semantics;
- #744 -> #745 -> #746 -> #743 -> #715 -> #39 is represented with native
  dependencies or sub-issues;
- completed implementation issues do not remain stale `[WIP]`;
- decision-only issues close or become non-execution records after their child
  work exists;
- all changes include an issue comment explaining the evidence and state
  transition.

No issue closes merely because code merged. Live-acceptance issues stay open as
`[Needs validation]` until the required staging evidence exists.

### Workstream E: Acceptance and Promotion Package

The final matrix is executed against one immutable candidate from the fixed
`platform-dev` runner. It includes:

- canonical Terminal-Bench 2.1 Harbor Hub revision 6, SkillLearnBench, and the
  already-scoped realistic user-uploaded TaskSet lane;
- representative agent/model cells from #35;
- operator-free frontend and CLI/API submit, monitor, detail/debug, usage,
  artifact, trajectory, and export paths;
- provider sharing, actor/represented-owner attribution, usage and cost
  visibility, and permission-negative cases;
- route-safe staging downloads;
- deterministic prod/staging isolation and capacity-policy dry runs;
- rollout resilience disposition;
- clean TB2/Harbor v2 export evidence from the Devansh dependency.

A realistic multi-task result with all-zero or all-full reward is not
score-valid evidence until root-caused. A single-task smoke proves connectivity
only.

The promotion package is generated from the candidate, not assembled by hand.
It includes:

- candidate and parent SHAs;
- container image names and digests;
- migration head;
- release manifest and config hash;
- acceptance row -> evidence link mapping;
- unresolved risk list, which must be empty for release blockers;
- rollback commit/tag target and operator commands;
- secret-scan result;
- draft promotion PR URL.

## Evidence Model

Each release gate has these fields:

```text
gate_id
candidate_sha
environment
scope
preconditions
command_or_workflow_run
result
artifact_or_evidence_uri
sanitized_checksums
verified_at
verifier
```

Evidence is append-only for a candidate. A new candidate invalidates evidence
whose code, image, migration, or configuration input changed. The final package
must distinguish reused evidence from fresh candidate evidence and explain why
reuse is valid.

## Failure Handling

Every failed gate follows the Loom root-cause process:

1. preserve the failing command, candidate, environment, ids, and sanitized
   evidence;
2. identify the concrete code, contract, config, infrastructure, provider,
   benchmark, or task root cause;
3. register or update a focused issue before using a mitigation;
4. implement a durable fix with regression coverage;
5. rerun the original failing gate on the new candidate;
6. invalidate dependent evidence when candidate inputs changed.

The program rejects rerun-until-green, manual DB/object-store edits, hidden
failure reclassification, reduced benchmark scope, skipped tasks, and host-only
repairs that are not captured in repository automation or issue history.

## Testing Strategy

Every code PR uses red-green-refactor TDD and runs its focused tests plus the
repository-required gate. The program-level verification includes:

- Python unit, contract, property, CLI, and applicable integration tiers;
- Web unit tests and production build;
- Go tests;
- workflow contract tests for selected-check aggregation;
- adversarial configuration tests for the workload trust contract;
- two-replica TaskSet lease/fencing fault tests;
- config codegen and generated-file clean-tree checks;
- Ruff, strict mypy, secret scanning, and documentation-link checks;
- cluster/staging smoke where changed paths require them;
- a final full candidate release-gate run.

Test success proves only the scope exercised by the test. Staging/live-worker
acceptance is not replaced with repository-side tests.

## Documentation and Decision Records

This program adds or updates:

- an ADR for v1.0 workload trust and unsupported untrusted execution;
- `docs/agent/domain-model.md` entries for Release Candidate,
  Ready-to-Promote, Release, and Workload Trust Mode;
- the first-prod and operator runbooks;
- TaskSet transform capability documentation;
- CI and contribution guidance;
- #39/#715 and focused implementation issues;
- `MEMORY.md` only when current coordination state changes.

The family-runs design PR #671 must be reconciled against the implementation
already in `dev`; it is not merged unchanged as part of this program.

## Explicit Deferrals

The following findings are important but do not block v1.0 unless the final
matrix reproduces their failure class:

- immutable persisted `BatchPlan`;
- complete trajectory durable outbox/high-water-mark redesign;
- autoscaler external-side-effect outbox;
- package import-boundary refactor;
- generated OpenAPI TypeScript client;
- broad complexity/module-size reduction;
- unified S3 client factory follow-up;
- official benchmark score parity and mixed-architecture evidence already
  assigned to later milestones.

Each deferred systemic finding must have a focused issue with owner, priority,
milestone, root-cause evidence, and acceptance. Deferral does not mean closure
or acceptance of the current design.

## Rollback and Safety

Repository and GitHub governance changes are applied incrementally and verified
after each mutation. Required CI gates are never weakened while a replacement
gate is unproven; routine `dev` approval policy follows the owner governance
decision. Production secrets are referenced, never printed. Staging capacity
changes use leases and preserve in-flight work. The #88 killed-driver drill is
post-v1; no `main` merge, tag creation, production rollout, or production
capacity mutation occurs without explicit authority.

## Completion Audit

Before declaring Ready-to-Promote, the owner must inspect current authoritative
state rather than relying on issue prose or this design:

1. fetch the candidate and confirm the draft promotion PR head equals the
   immutable candidate SHA;
2. verify branch protection and Environment settings through GitHub APIs;
3. verify every #715 pre-promotion row against its linked evidence artifact;
4. confirm all selected CI and staging checks are terminal and successful;
5. run release manifest, environment isolation, capacity, migration, config,
   secret, and rollback validations;
6. confirm #743-#746 are complete with real v2 staging evidence;
7. confirm there are no unowned or untracked release blockers;
8. confirm the working tree and generated files are clean;
9. publish the final readiness comment on #715 and the draft promotion PR;
10. leave #39 open for the separately authorized release and post-deploy
    production validation.

Only after all ten checks pass is the Goal eligible to be marked complete.
