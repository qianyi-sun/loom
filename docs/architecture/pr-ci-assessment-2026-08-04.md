# PR CI assessment — 2026-08-04

This assessment advances issue #1130. It separates merged repository behavior,
live read-only observations, this Draft implementation, and proposals that still
need acceptance or rollout authority.

## Scope and method

- Repository baseline: `qianyi-sun/loom` `origin/dev` at
  `595b5963faa60176224983a0244b6d8d1e50f333`.
- Publisher window: `2026-08-02T08:36:37Z` through
  `2026-08-04T17:09:21.071703Z`, capped at the newest 300 runs. The observed
  records span `2026-08-03T15:08:32Z` through `2026-08-04T17:08:33Z`, so the
  result is a truncated recent sample rather than a complete two-day census.
- The sample contains 60 distinct source attempts, 15 for each required source
  workflow. All 300 run names were instrumented and valid. All 339 publisher job
  records were classified: 338 had terminal API-call metrics and one was a
  normal workflow-level skip. There were no log errors or uncovered jobs.
- Live runner and Actions-variable inspection was read-only. No runner route,
  repository variable, ruleset, environment, or deployment state was changed.
- The issue's earlier PR-level baseline remains useful for the end-to-end view:
  428 workflow runs, about 850 jobs, and 5.1 runner-hours for six representative
  PRs. The fixed-window publisher sample below is the repeatable subsystem
  baseline added by this slice.

Reproduce the publisher sample with:

```bash
GITHUB_TOKEN="$(gh auth token)" python scripts/ops/authoritative_gate_metrics.py \
  --repository qianyi-sun/loom \
  --since 2026-08-02T08:36:37Z \
  --until 2026-08-04T17:09:21.071703Z \
  --max-runs 300 \
  --workers 16
```

## Publisher amplification baseline

| Source workflow | Attempts | Publisher runs/jobs | API calls | Runs/jobs per attempt | API calls per attempt |
| --- | ---: | ---: | ---: | ---: | ---: |
| CI | 15 | 82 | 1,109 | 5.467 | 73.933 |
| images | 15 | 65 | 823 | 4.333 | 54.867 |
| cluster-smoke | 15 | 71 | 1,054 | 4.733 | 70.267 |
| staging-smoke | 15 | 68 | 902 | 4.533 | 60.133 |
| **Source-workflow total** | **60** | **286** | **3,888** | **4.767** | **64.800** |

The same window also contains 14 `pull_request_target` invalidation runs, 53
publisher job records, 52 executed jobs, and 661 API calls. Across both trigger
types, the publisher made 4,549 measured GitHub API calls and had zero failed
publisher workflow runs. That workflow-run result is transport evidence only;
it does not prove every late-downgrade or stale-delivery acceptance case.

The dominant amplification is first-attempt `workflow_run.in_progress`: 174
publisher runs/jobs and 2,526 API calls. That is 60.8% of source publisher jobs
and 65.0% of source publisher API calls in this sample. The initial
`workflow_run.requested` event already performs the fail-closed invalidation.
This Draft therefore filters only the redundant first-attempt `in_progress`
publisher job. It preserves:

- first-attempt `requested` invalidation;
- every terminal `completed` reconciliation; and
- `in_progress` invalidation for `run_attempt > 1`.

The last condition is required because GitHub documents that the `requested`
activity type does not occur when a workflow is rerun. Removing `in_progress`
globally would create a rerun invalidation gap. See GitHub's
[workflow-run event documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run).

The filter is job-level: GitHub will still create the lightweight publisher
workflow record, but it will avoid checkout, publisher execution, and the
measured API calls for the redundant delivery. Based on this fixed window, the
expected steady-state reduction is 174 executed jobs and 2,526 API calls per
300 publisher runs. A post-merge window is still required to verify the actual
reduction and that rerun invalidation remains intact.

## Post-merge Track 2 acceptance

The fixed post-merge window from `2026-08-04T19:05:53Z` through
`2026-08-04T20:20:00Z` contains 403 publisher workflow records for 88 observed
source attempts. The sample is not truncated. Two attempts were still active or
had not yet received a `completed` delivery at the cutoff, so the acceptance
ratios use only the 86 terminal source attempts. This prevents an active tail
from making job or API-call amplification look artificially low.

All 86 terminal attempts had both a fail-closed `requested` or `in_progress`
delivery and complete publish-job metrics. There were no terminal attempts with
a missing invalidation, missing terminal metrics, publisher transport failure,
or cancelled publisher. Seven publisher workflow records concluded `failure`,
but all seven contained the publisher's terminal metrics record and are
classified as `authoritative_result`: the workflow correctly published a red
authoritative outcome rather than failing to transport the outcome.

| Terminal-attempt metric | Before | After | Reduction |
| --- | ---: | ---: | ---: |
| Executed publisher jobs per source attempt | 4.767 | 1.849 | 61.213% |
| Publisher API calls per source attempt | 64.8 | 20.256 | 68.741% |
| Publisher workflow records per source attempt | 4.767 | 4.395 | 7.804% |

The Track 2 runtime-amplification target is met for executed publisher jobs and
API calls without omitting terminal authority. The remaining workflow records
are lightweight skipped deliveries: 206 first-attempt `in_progress` records in
this window executed zero publish jobs and made zero publisher API calls. Their
record-count reduction remains an explicit residual rather than being presented
as a 40% workflow-record reduction.

Reproduce the fail-closed acceptance decision with:

```bash
GITHUB_TOKEN="$(gh auth token)" python scripts/ops/authoritative_gate_metrics.py \
  --repository qianyi-sun/loom \
  --since 2026-08-04T19:05:53Z \
  --until 2026-08-04T20:20:00Z \
  --max-runs 1000 \
  --workers 16 \
  --baseline-executed-publish-jobs-per-attempt 4.767 \
  --baseline-api-calls-per-attempt 64.8 \
  --baseline-publisher-runs-per-attempt 4.767 \
  --minimum-terminal-source-attempts 30 \
  --minimum-reduction-percent 40 \
  --require-acceptance
```

The command exits with status 3 if the minimum terminal sample, delivery/log
coverage, publisher transport integrity, executed-job reduction, or API-call
reduction fails. An authoritative red result does not count as publisher
transport failure. Rollback is removal of the acceptance-only reporting fields
and CLI switches; this slice does not alter lifecycle triggers, protected
contexts, concurrency, runner routing, or live capacity.

## Track 5 reusable-build and metadata fast-path contract

The pre-Track-5 audit found 428 filtered source workflows in a sample of 850.
Those filtered records accumulated 18,433 seconds (5.12 hours) of workflow wall
time: median 27 seconds, p90 74 seconds, and maximum 555 seconds. The four
required source workflows previously entered a hosted planner job and checked
out the repository before classifying several draft, irrelevant-label, and
non-base edit events. Their event metadata is already sufficient to classify
those cases. The Track 5 fast path therefore runs one shell classifier as the
first planner step, emits `gate_mode=filtered`, and ends without checkout,
planner execution, heavy jobs, or an aggregate gate job. Events that add or
remove a CI-controlling label, change the base, or otherwise affect path
inference still enter the full planner and remain fail closed.

The reusable image contract removes the second native image build after a
same-repository PR is merged:

1. Each PR image/architecture job remains `contents: read` with no registry
   credentials. Fork PRs, merge groups, and manual runs only validate builds;
   eligible same-repository PRs additionally export a local Docker archive.
2. The gzip-compressed OCI archive is uploaded as a one-day Actions artifact.
   Its record contains
   a deterministic image/architecture artifact name, byte size, and SHA-256.
   Archive, record, and index names include the source run attempt so GitHub's
   immutable artifact namespace cannot collide across reruns. No PR job can
   write an image, tag, or cache to GHCR.
3. A separate hosted job aggregates one archive record for every selected
   image/architecture. The index binds repository, PR number, head, base, Git
   tree, source run ID, source run attempt, exact planner matrix, artifact name,
   size, and checksum. Missing, duplicate, mixed-attempt, cross-image, or extra
   records fail closed.
4. A trusted `dev` or `main` push accepts an index only when the pushed commit
   maps to one unique merged same-repository PR, the PR head tree equals the
   pushed tree, the source images workflow is successful, and every identity
   field matches. The resolver also requires every attempt-bound archive to
   exist and remain unexpired. Each native publish job downloads its exact source-run
   archive, verifies size and SHA-256 before loading it, verifies architecture
   and revision labels after loading, and publishes the merge-SHA build tag
   without executing `docker buildx build` again.
5. Direct pushes, fork PRs, merge commits whose trees differ, base advancement,
   cancelled or failed source attempts, expired artifacts, missing candidate
   archives, checksum or size mismatches, and any ambiguous or malformed
   provenance take the existing trusted push rebuild path or fail before
   publishing. Candidate absence is an optimization miss, never an authorization
   signal.

The candidate archives, records, and index are retained for one day. The full
11-image, two-architecture registry baseline contains about 8.24 GiB of
compressed layers; the short retention bounds artifact storage while making
archive expiry an explicit trusted-rebuild fallback. Downstream
manifests reference only the trusted merge-SHA build tags and the existing
short-SHA/branch tags; PR candidates create no registry tags.
Cancellation cannot make an incomplete candidate promotable because only a
successful exact source run with a complete index is eligible. Base advancement
changes the provenance identity. A rerun deliberately rebuilds and generates a
new run/attempt-bound archive set; it never silently reuses a cancelled or
partially uploaded attempt.

Security boundary: the entire PR build job retains only `contents: read`; it has
no registry token, package-write permission, remote cache write, or secret.
Only the trusted push jobs can read source-run artifacts and write packages.
They verify provenance, archive bytes, architecture, and revision before login
and publication. Rollback is removal of the candidate archive/index and
resolver jobs, restoration of the always-rebuild trusted publish matrix, and
removal of the four pre-checkout classifiers. It requires no runner route,
timer, host, environment, or deployment mutation.

Live acceptance remains distinct from this repository contract. It requires a
Draft metadata event demonstrating planner-only execution, a Ready exact-head
run producing the complete candidate index, and a merge push demonstrating 22
verified archive publications, 11 two-platform manifests, zero image rebuilds, and no
change to the four app-bound required context names.

## Six-track assessment

| Track | Current evidence | Assessment and next gate |
| --- | --- | --- |
| 1. Authoritative correctness | PR #1131 is merged. The fixed sample has no failed publisher workflows and complete metrics coverage. | The state machine is substantially hardened, but issue acceptance still requires a disposable PR that proves fail-closed behavior, label churn, one app-15368 CheckRun per protected context, and exact-source rerun invalidation. Keep this track open. |
| 2. Lifecycle amplification | In 86 terminal post-merge attempts, executed publisher jobs fell 61.213% and API calls fell 68.741%; all terminal attempts retained complete authority evidence. | Runtime amplification passes the 40% target. Lightweight workflow records fell only 7.804% and remain a separately reported residual; do not weaken rerun invalidation merely to suppress those records. |
| 3. Capacity isolation | Live snapshot: 10 registered oldlab ephemeral KVM runners, 9 online and busy, 1 offline. `LOOM_CI_ACCELERATOR_RUNS_ON` routes accelerator work to the shared oldlab-5 pool. | Saturation is observable, while class reservation and light/heavy separation are absent. Design and rehearse a separate light-lane capacity policy before any live route mutation. This Draft makes no route change. |
| 4. Architecture-native coverage | Locked-environment validation has a native `ubuntu-24.04-arm` job. PR image builds still request `linux/amd64,linux/arm64` on x64 oldlab runners through QEMU. | Native ARM coverage exists for one boundary, but image-build cost and failure attribution are not architecture-isolated. Split native x86/ARM image validation only after runner inventory, cache ownership, and required-context aggregation are specified. |
| 5. Duplicate work and metadata fast paths | Earlier six-PR baseline: 428 filtered workflows of 850, 18,433 seconds (5.12 hours) of cumulative wall time. The Track 5 repository contract adds pre-checkout metadata classification and exact archive provenance for same-repository PR image builds. | Live acceptance must prove planner-only filtered events, a complete exact-head candidate index, and post-merge archive publication without rebuild. Every identity mismatch must retain the trusted rebuild fallback. |
| 6. Upgrades and retries | Workflow actions are SHA-pinned. Recent source-run samples include failures and substantial cancellation, but retry reasons are not classified and almost all runs remain attempt 1. | Add normalized cause categories for superseded, infrastructure, flaky-test, product-test, and operator rerun. Version upgrades should use a fixed canary matrix and explicit rollback rather than inferred success from aggregate cancellation rates. |

## Recommended execution order

1. Preserve the accepted first-attempt `in_progress` job filter and repeat its
   terminal-attempt acceptance report when publisher behavior changes.
2. Classify source-workflow cancellations and reruns before changing retry policy; otherwise
   superseded PR generations can be mistaken for instability.
3. Move cheap metadata/path decisions ahead of checkout and establish reusable
   plan artifacts.
4. Specify light/heavy runner classes, reservation thresholds, rollback, and a
   shadow-routing rehearsal. Live Actions-variable mutation is a separate
   authorization boundary.
5. Split native architecture lanes after capacity isolation exists, then measure
   cache hit rate, queue time, and per-architecture failure attribution.
6. Run the issue's disposable late-downgrade acceptance matrix and record exact
   heads, source runs, CheckRun IDs, generations, and app identity.

Publisher concurrency remains serialized by context and candidate head using
`cancel-in-progress: false` and `queue: max`. GitHub's default concurrency model
allows only one pending item and replaces an older pending item; the explicit
max queue prevents that replacement for this correctness-sensitive publisher.
The queue is bounded at 100 pending jobs/runs. See GitHub's
[concurrency documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)
and [Actions limits](https://docs.github.com/en/actions/reference/limits).

## Rollback and acceptance boundaries

- Repository rollback for this slice is removal of the two-clause
  first-attempt filter; it does not require runner or environment mutation.
- A metrics regression must fail visibly as incomplete coverage. Normal skipped
  jobs are zero-cost classified records, not missing logs.
- Do not make a publisher-changing PR Ready until it is rebased onto the exact
  current `origin/dev` and all four app-15368 required checks pass on that head.
- Green CI is not full correctness acceptance. The disposable PR sequence in
  `v1-release-ready-program.md` remains mandatory immediately after the
  default-branch workflow is activated; a `workflow_run` PR cannot execute its
  candidate publisher definition before merge.
- Capture disposable-PR evidence from the commit CheckRuns endpoint, not only
  the rendered PR checks list. For the final probe head, record each protected
  context's CheckRun ID, GitHub App ID, status, conclusion, generation marker,
  and source run/attempt. This distinguishes one stable authoritative CheckRun
  being updated from duplicate same-name checks that can look equivalent in the
  PR UI.
- Runner-pool isolation, route-variable changes, merge, and deployment are
  separate authorities. None is authorized by this assessment.
