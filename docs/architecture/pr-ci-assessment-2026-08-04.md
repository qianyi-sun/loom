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

## Six-track assessment

| Track | Current evidence | Assessment and next gate |
| --- | --- | --- |
| 1. Authoritative correctness | PR #1131 is merged. The fixed sample has no failed publisher workflows and complete metrics coverage. | The state machine is substantially hardened, but issue acceptance still requires a disposable PR that proves fail-closed behavior, label churn, one app-15368 CheckRun per protected context, and exact-source rerun invalidation. Keep this track open. |
| 2. Lifecycle amplification | 4.767 publisher jobs and 64.8 API calls per source attempt; first-attempt `in_progress` accounts for 174 jobs and 2,526 calls. | This Draft implements the highest-confidence reduction and adds repeatable measurement. Verify the post-merge delta before considering broader trigger or API changes. |
| 3. Capacity isolation | Live snapshot: 10 registered oldlab ephemeral KVM runners, 9 online and busy, 1 offline. `LOOM_CI_ACCELERATOR_RUNS_ON` routes accelerator work to the shared oldlab-5 pool. | Saturation is observable, while class reservation and light/heavy separation are absent. Design and rehearse a separate light-lane capacity policy before any live route mutation. This Draft makes no route change. |
| 4. Architecture-native coverage | Locked-environment validation has a native `ubuntu-24.04-arm` job. PR image builds still request `linux/amd64,linux/arm64` on x64 oldlab runners through QEMU. | Native ARM coverage exists for one boundary, but image-build cost and failure attribution are not architecture-isolated. Split native x86/ARM image validation only after runner inventory, cache ownership, and required-context aggregation are specified. |
| 5. Duplicate work and metadata fast paths | Earlier six-PR baseline: 428 workflows, about 850 jobs, 5.1 runner-hours. Superseded-generation cancellations are common. Planner workflows still start and check out before deciding some metadata-only cases; cross-workflow artifacts are not reused comprehensively. | After publisher dedupe, prioritize pre-checkout planning and durable plan/artifact reuse. Preserve fail-closed path inference and keep label changes that add coverage authoritative. |
| 6. Upgrades and retries | Workflow actions are SHA-pinned. Recent source-run samples include failures and substantial cancellation, but retry reasons are not classified and almost all runs remain attempt 1. | Add normalized cause categories for superseded, infrastructure, flaky-test, product-test, and operator rerun. Version upgrades should use a fixed canary matrix and explicit rollback rather than inferred success from aggregate cancellation rates. |

## Recommended execution order

1. Merge and measure the first-attempt `in_progress` job filter after its Draft
   CI and disposable correctness acceptance pass.
2. Classify cancellations and reruns before changing retry policy; otherwise
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
