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

Current operational amendment: #822 temporarily removes `trt-gb10-7` from the
staging rollout target. Fresh candidate evidence must cover all 14 active hosts
and 140 slots, while preserving node 7 as stopped/unreachable; the historical
15-host HF-boundary evidence above remains historical rather than a current
reachability claim.

## Program Structure

The program uses independently reviewable PRs rather than one stabilization
branch containing unrelated risk domains. Each PR has its own issue, test
cycle, documentation update, CI labels, acceptance evidence, and rollback.

### Workstream A: Release Governance and CI Integrity

Issue #700 is the first implementation workstream because every later PR relies
on correct merge gating.

Every merge-group candidate and relevant non-draft PR reports four stable
required contexts:
`repository-checks`, `images-gate`, `cluster-smoke-gate`, and
`staging-smoke-gate`. The shared planner computes the validation work required
by changed paths and labels, and each gate enforces its selected result. A
selected job that is cancelled, skipped unexpectedly, or absent is a failure.
Labels may add validation but cannot remove path-inferred validation. Docs-only
PRs keep a bounded location-and-format fast path. Drafts and unrelated metadata
events report distinct filtered contexts. Runtime Markdown, executable files under `docs/`, and unknown
non-document paths are not docs-only; unknown runtime paths select every heavy
lane until a canonical owner is declared.

A default-branch-trusted publisher owns exactly one authoritative CheckRun for each of
the four protected names on a candidate head. Validation workflows publish
generation-scoped `*-attempt` aggregates. As soon as the publisher observes a
relevant same-head event, it returns all four authoritative checks to
`in_progress`; only the newest full generation may finalize its matching check. Superseded attempts
cannot overwrite a newer generation; the current generation fails closed on a
failed, timed-out, cancelled, skipped, or missing aggregate. Each generation
also carries a fixed-order six-bit snapshot of every validation-relevant label.
The marker also carries the PR number (`none` for merge groups and non-PR runs),
and the controller binds it to the latest ordered GitHub issue-event ID for
validation-changing PR activity. A mask-aware source occurrence distinguishes
observable add/remove ABA sequences even when their final label snapshot is
identical. Once every trusted occurrence is represented, byte-identical markers
form one generation and the highest run ID/attempt is its replacement
execution. A relevant trusted webhook resets an existing terminal check before
fallible history reads; a shorter history snapshot cannot lower the persisted
event watermark. Before and immediately after publishing a terminal result,
the controller re-reads the live PR, event epoch, exact run attempt, and latest
source-run inventory. If authority changed across the non-atomic CheckRun API
write, it compensates by returning that same CheckRun to `in_progress`.
Comments and unrelated labels are excluded from the epoch and do not
invalidate an otherwise equivalent generation. Relevant source runs and
background metadata runs also use separate concurrency lanes, so GitHub's
single pending slot cannot let an unrelated filtered run evict the
authoritative replacement.

The implementation bootstraps without a protection gap: until the publisher is
present on the target base, source workflows keep their legacy protected names.
When an already-completed publisher CheckRun is reopened, its PATCH changes the
status to `in_progress` and omits `conclusion` and `completed_at`; GitHub's
update schema rejects JSON `null` for those terminal fields. Publisher contract
v2 records that live-verified behavior. Bases that contain only v1 keep the
legacy protected source names until v2 merges, so a broken publisher cannot
strand its own repair. The earlier one-time upgrade from base
`28aa5257927a3468ebc35ec7f245fecaf3226dbf` uses GitHub's whitespace
`run-name` fallback so that the pre-fix publisher still sees the fixed workflow
identity while the PR title carries a generation marker ordered after the
synchronize snapshot. That exception is bound to the exact historical base and
therefore expires automatically after the repair merges.
The exact same-repository `dev`-to-`main` promotion is the one exception because
the trusted publisher already lives on the default `dev` branch. Merge-group
heads use the same publisher contract. Push and manual runs report distinct
`*-push` and `*-manual` aggregate names so a `dev` push check cannot collide
with the protected custom check on a later `dev`-to-`main` promotion head. Push
workflow-run events and non-authoritative workflow ID/name pairs are also
rejected by the publisher. Each head/context pair has one publisher lane and
never cancels its in-progress invocation. This matches the SHA-scoped CheckRun
identity and prevents concurrent events from racing the same external ID;
delayed invocations for an old head never write the live head. Pending
invocations may coalesce, so every invocation derives authority from live PR
state, ordered issue events, and the exact source-run attempt instead of relying
on delivery order. If one head is associated with multiple open PRs, the
publisher fails closed because one SHA-level check cannot represent two PR
metadata generations.

Changes to this publisher require a disposable pull-request acceptance pass in
addition to contract tests. Open the probe as Draft, make it Ready with one
deliberately failing repository test, and confirm the current authoritative
generation remains red with auto-merge blocked. Push a real fix, then rapidly
add or remove validation-relevant labels on that same head. The final label
generation must own exactly one GitHub Actions App (`15368`) CheckRun for each
protected context; cancelled superseded attempts must not remain merge
authoritative. Enable squash auto-merge throughout the non-draft portion and
let the final all-green generation merge naturally, without an empty or
tree-identical re-anchor commit. Record the probe PR, exact heads, source runs,
and protected CheckRun IDs on the tracking issue.
Keep a durable documentation or test improvement in the probe's final tree so
removing the deliberate failure is itself a real fix rather than a
tree-identical re-anchor.

Manual dispatch remains available, but aggregate jobs report
`repository-checks-manual`, `images-gate-manual`,
`cluster-smoke-gate-manual`, and `staging-smoke-gate-manual`. Those names
cannot satisfy the protected PR contexts. Pull-request `edited` events rerun
the planner so a base retarget cannot reuse a plan computed against the old
base.

The checked-in image lane separates validation from publication. Pull requests,
merge groups, and manual dispatches use read-only permissions, never log in to
GHCR, and do not use a publication cache; manual dispatch is build-only. Only
the `publish` job on a push to `dev` or `main` requests job-scoped
`packages: write` authority. This protects the normal workflow path, but it is
not a hard ceiling for a same-repository writer because branch workflow code
runs under the PR-controlled definition. A fork-only autonomous-agent boundary or an external trusted
workflow/App is still required for that stronger guarantee.
The required `staging-smoke-gate` is likewise credential-free and depends only
on the kind smoke. Real AWS validation belongs to a separately protected,
trusted post-merge/release lane and a skipped cloud run cannot count as cloud
evidence.

The trusted base-branch controller enables squash auto-merge for every eligible
non-draft PR without using author or reviewer identity. GitHub queues each
candidate until every required gate is visible and successful on the current
head SHA. These four strict,
GitHub-Actions-app-bound checks are
the only merge authority: `dev` requires no human approval, no CODEOWNER
approval, and no conversation resolution. Governance changes still select full
CI.

`main` accepts only a same-repository production release promotion from `dev`.
The same controller enables squash auto-merge after release evidence is
attached; the four current-head CI gates are its only merge authority.

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

The machine-readable ownership authority lives at
`config/component-ownership.toml`. Its fail-closed validator owns the schema,
all tracked Dockerfile and test-path assignments, allowed CI lanes, component
owner registries, runtime-payload execution policies, and rollout image identity
checks. The images workflow derives its eleven-image release matrix, build
contexts, release names, and changed-path selection directly from the authority.
Ordinary pytest lanes and the isolated runtime-payload lane derive their exact
inputs from the same source. Runtime payloads execute one file per read-only, networkless,
resource-limited container, using a digest-pinned image and a per-file minimal
synthetic passing-workspace fixture case;
that result is verifier-conformance evidence, not real task or trial success.
Rollout build/load and expected-image evidence reuse a nine-image matrix generated
from the fixed candidate worktree's explicitly tagged seven primary and two
auxiliary components. The two sandbox conformance images remain outside the
rollout matrix.

GitHub settings are part of the acceptance evidence:

- `dev` requires the four strict, app-bound current-head checks and no other
  merge authority: no human approval, no CODEOWNER approval, and no
  conversation resolution;
- `main` retains the four strict checks, admin-enforcement, and linear-history
  controls, with no human, CODEOWNER, or conversation-resolution gate;
- production-capable environments restrict deployment branches;
- `production` requires owner approval;
- secret-bearing `ci-aws` and `huggingface-publish` environments no longer have
  empty protection policy.

The four same-repository workflow checks do not form an ideal
autonomous-agent trust root. An organization-required workflow or separate
GitHub App with a distinct app identity should eventually emit merge authority
so a PR cannot redefine its own judge. The manifest's
`release_owner_approval` evidence and Production Environment approval are
separate controls and are not interchangeable with CI merge authority.

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
- TB2.1 physical profile `terminal-bench-2@tb2.1-r6` activated only after a
  fresh 89/89 audit of Hub digests, bundles, verifier assets, checksums,
  architecture, and the private-verifier-driver boundary; no TB2.0 fallback or
  reduced task count is allowed;
- an explicit audited TB2.1 physical task ID supplied to every full-cluster
  rollout via `--smoke-task-id`; only `current-gb10` retains the Loom-owned
  `loom-smoke/gb10-oracle-hello-world` default;
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
only. Numeric verifier reward `0` is still a platform-successful scored result;
missing, malformed, or non-finite reward evidence is a platform/verifier
failure. Benchmark profile/Hub provenance and agent-runtime/image provenance
must be recorded independently.

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
gate is unproven; routine `dev` uses CI-only auto-merge with no human,
CODEOWNER, or conversation-resolution gate. Production secrets are referenced,
never printed. Staging capacity
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
10. confirm squash auto-merge is enabled and only the current-head protected
    CI gates can complete the promotion;
11. leave #39 open for the separately authorized release and post-deploy
    production validation.

Only after all eleven checks pass is the Goal eligible to be marked complete.
