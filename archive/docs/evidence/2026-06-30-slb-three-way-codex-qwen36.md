# SkillLearnBench three-way alignment — `codex × qwen3.6-35b-a3b × yibuapi`

> Archived dated benchmark report. It is not current user documentation.

Date: 2026-06-30
Refs: #6 (superseded history), #32 (current alignment owner), #82, #83, #100,
#106, #107, #110

## Status and authority

This is frozen historical Layer 3 evidence, not a current release or worker-
architecture gate. It demonstrates a controlled comparison and records where
the three runs agreed; it does not prove strict full100 parity, production
x86 acceptance, or an architecture-caused score difference.

Current authority is split deliberately:

- #32 owns deterministic verifier/artifact alignment and statistical matched
  live-run analysis;
- #715 requires a platform-valid, diagnosable SkillLearnBench user flow with a
  numeric reward, not official-harness full100 equality;
- #49/#715 validate x86_64 and arm64 through independent operator-only portable
  Terminal-Bench-like canaries, never by adding coverage trials or pool choices
  to a user batch.

## Scope

Three execution legs, joined per `task_id` across all 100 SkillLearnBench tasks:

1. **Official adjusted baseline** — `runtime_clean_42_adjusted_full100_vs_loom_secret_safe.csv` from #106, which stitches the original `evaluation_log_yibu_qwen36_codex_full100_srun_20260627T042747Z` raw run with the 37 non-GH runtime-clean reruns from #83 and the 5 GH-token reruns from #106. This is the canonical baseline used by the published Layer 3 report; the raw run on its own carried 41 `agent_transport_failure` zeros and 1 `agent_timeout` that mixed live model variance with infra failure.
2. **Loom ARM** — batch `f18efd76-ad3b-490d-9c27-5dee14d35630` on public-beta `gb10-arm64` workers (`trt-gb10-*`). Pre-existing, used as the existing Layer 3 evidence point.
3. **Loom x86 (local-dev)** — fresh batch sequence on the local x86 cluster of this audit:
   - Original batch `23c2da8e-b80d-49ee-86b2-36d79344443d` (100 trials, 52 succeeded, 48 platform-failed mid-run).
   - Recovery batch `a72cbe3a-3b60-4e03-899d-eba62803cb7f` (48 trials, the failed task_ids re-run after a missing migration was applied; 48/48 succeeded).
   - Comparator picks the latest succeeded trial per `task_id` across both batches via `DISTINCT ON (task_id) ORDER BY task_id, state='succeeded' DESC, started_at DESC` — so the recovery's clean retry wins over the original's pre-fix failure.
   - x86 cluster is the historical local dev cluster on this host
     (single-node, x86_64). This leg remains labeled `local-dev`; it must not be
     relabeled as production or current release evidence.

## Aggregate

- N tasks joined: **100**
- Official: **18.0 / 100** (0.180 mean) — matches the published Layer 3 baseline exactly.
- Loom ARM: **11.0 / 100** (0.110 mean) — matches the published number exactly.
- Loom x86: **18.0 / 100** (0.180 mean) — same aggregate as official.

## Concordance buckets (one row per task)

| Bucket | Count | Meaning |
|---|---:|---|
| three_way_match | 74 | all three sources agree |
| loom_agrees_official_dissents | 11 | both Loom legs say the same reward; official differs |
| arm_dissents | 6 | official + x86 agree; ARM differs |
| x86_dissents | 9 | official + ARM agree; x86 differs |
| **Total** | **100** | |

## Pairwise reward match rates

| Pair | Matches / Total | Match rate |
|---|---:|---:|
| arm_vs_official | 83 / 100 | 83.00% (matches published 83/100) |
| x86_vs_official | 80 / 100 | 80.00% |
| arm_vs_x86 | 85 / 100 | 85.00% |

## What this validates

1. **The Loom × SLB end-to-end pipeline produces real, comparable scores on x86 as well as ARM.** The 18/100 aggregate matches the official adjusted baseline exactly. The 11/100 ARM baseline already in the issue chain is reproduced.
2. **The two historical Loom legs agree on 85/100 tasks.** A single live run
   per architecture cannot attribute the remaining 15 differences to CPU
   architecture; repeated matched runs or frozen-artifact replay are required.
3. **Both Loom legs disagree with official on the same 11 tasks.** This is a
   replay priority, not proof that the official runner or Loom is wrong. Each
   row must be classified from frozen verifier inputs and required artifacts.

## What this does NOT validate

- **Current release or production x86 acceptance.** This x86 leg ran on local
  dev. Current #49 acceptance uses a separate operator-only portable
  Terminal-Bench-like canary; it does not require repeating this SkillLearnBench
  full100 comparison.
- **Per-task identity of the 11 loom-agrees-official-dissents tasks.** Need #110-style replay to confirm whether each one is the "missing expected output artifact" pattern or a different cause. Out of scope for this report.

## Repo-only dissent replay plan

Use the checked-in CSV as the source of truth for deterministic triage before
any future live replay. This command performs no provider, Docker, database,
artifact-store, or infrastructure calls:

```bash
uv run python scripts/alignment/skilllearnbench_three_way_replay_plan.py \
    --three-way-csv docs/evidence/2026-06-30-slb-three-way-codex-qwen36.csv \
    --out-json /tmp/slb-three-way-replay-plan.json \
    --out-md /tmp/slb-three-way-replay-plan.md
```

The current checked-in CSV yields 26 historical triage rows: 8
`likely_verifier_artifact_replay_needed`, 3
`official_semantics_drift_candidate`, and 15
`architecture_specific_rerun_needed`. These are replay-plan labels, not
root-cause conclusions. The architecture label is not current #49 release
scope; later #32 validation still needs task-level verifier
inputs, artifacts, stdout/stderr, required-files manifests, reward JSON, and
safe provider/model metadata.

## Per-task table

See `2026-06-30-slb-three-way-codex-qwen36.csv` (sibling file). Mismatches summary table for quick triage:

| Task | Official | Loom ARM | Loom x86 | Concordance |
|---|---:|---:|---:|---|
| anthropic-poster-design/anthropic-poster-design-1 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| anthropic-poster-design/anthropic-poster-design-3 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| anthropic-poster-design/anthropic-poster-design-5 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| chinese-poem-generator/chinese-poem-generator-2 | 0.0 | 1.0 | 0.0 | arm_dissents |
| chinese-poem-generator/chinese-poem-generator-3 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| chinese-poem-generator/chinese-poem-generator-4 | 0.0 | 0.0 | 1.0 | x86_dissents |
| chinese-poem-generator/chinese-poem-generator-5 | 1.0 | 0.0 | 1.0 | arm_dissents |
| court-form-filling/court-form-filling-2 | 0.0 | 1.0 | 1.0 | loom_agrees_official_dissents |
| court-form-filling/court-form-filling-5 | 0.0 | 0.0 | 1.0 | x86_dissents |
| dbscan-parameter-tuning/dbscan-parameter-tuning-2 | 1.0 | 0.0 | 1.0 | arm_dissents |
| dependency-vulnerability-check/dependency-vulnerability-check-1 | 1.0 | 1.0 | 0.0 | x86_dissents |
| earthquake-plate-calculation/earthquake-plate-calculation-3 | 0.0 | 1.0 | 0.0 | arm_dissents |
| enterprise-information-search/enterprise-information-search-1 | 0.0 | 1.0 | 1.0 | loom_agrees_official_dissents |
| enterprise-information-search/enterprise-information-search-4 | 0.0 | 1.0 | 1.0 | loom_agrees_official_dissents |
| enterprise-information-search/enterprise-information-search-5 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| financial-analysis/financial-analysis-3 | 0.0 | 0.0 | 1.0 | x86_dissents |
| github-repo-analytics/github-repo-analytics-3 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| nlp-paper-reproduction/nlp-paper-reproduction-2 | 1.0 | 1.0 | 0.0 | x86_dissents |
| python-scala-translation/python-scala-translation-2 | 0.0 | 0.0 | 1.0 | x86_dissents |
| schedule-planning/schedule-planning-4 | 0.0 | 1.0 | 0.0 | arm_dissents |
| temperature-simulation/temperature-simulation-1 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| travel-planning/travel-planning-2 | 0.0 | 0.0 | 1.0 | x86_dissents |
| travel-planning/travel-planning-3 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |
| video-object-counting/video-object-counting-3 | 1.0 | 1.0 | 0.0 | x86_dissents |
| video-object-counting/video-object-counting-4 | 0.0 | 1.0 | 0.0 | arm_dissents |
| weighted-gdp-calculation/weighted-gdp-calculation-4 | 1.0 | 0.0 | 0.0 | loom_agrees_official_dissents |

_(74 three-way-matched rows omitted from this preview; full table in the CSV.)_

## Reproduction

```bash
# Official adjusted baseline (publicly accessible at the path below).
OFFICIAL=/shared_work/qianyi/skilllearnbench-official/issue106_gh5_fixed_redaction_20260628T220730Z_oldlab5/runtime_clean_42_adjusted_full100_vs_loom_secret_safe.csv

# ARM Loom comparison CSV (from the prior two-way work).
ARM=/shared_work/qianyi/skilllearnbench-official/loom_full100_compare_f18efd76_20260627T162612Z/comparison.csv

# x86 Loom: pass both batch ids comma-separated so the latest
# successful trial per task_id wins.
X86_BATCHES=23c2da8e-b80d-49ee-86b2-36d79344443d,a72cbe3a-3b60-4e03-899d-eba62803cb7f

uv run python scripts/alignment/skilllearnbench_three_way_compare.py \
    --official-adjusted-csv "$OFFICIAL" \
    --arm-comparison-csv "$ARM" \
    --x86-loom-batch-id "$X86_BATCHES" \
    --out-csv docs/evidence/2026-06-30-slb-three-way-codex-qwen36.csv \
    --out-md docs/evidence/2026-06-30-slb-three-way-codex-qwen36.md
```

The x86 batches were submitted via:

```bash
# Original
loom eval batch create --agent codex \
    --provider qa-relay --model qwen3.6-35b-a3b \
    --benchmark skilllearnbench \
    --name slb-x86-localdev-codex-qwen36-full100

# Recovery (after applying migration 0047 typed artifact registry)
loom eval batch create --agent codex \
    --provider qa-relay-hongjian --model qwen3.6-35b-a3b \
    --task-filter @recovery-48-task-ids.json \
    --name slb-x86-localdev-codex-qwen36-recovery48
```

## Cluster notes (one-time setup gotchas)

These bit me during this audit; capturing them for the next operator:

- **Local dev cluster image drift.** The dev cluster's images were 26 commits behind `origin/dev`, missing migrations 0042-0048. CP wouldn't start until `alembic upgrade head` was applied and images were rebuilt. If you bring up the local dev cluster after a multi-day break, run `git pull && docker compose build` first.
- **Migration 0047 (typed artifact registry).** Without this migration the `artifacts` table is missing and every trial's terminal `PATCH /trials/.../trajectory_index` returns 500 → trial recorded as `trajectory_flush_failed`. 48 of my original 100 trials lost this way; the recovery batch covers them.
- **Account-based CLI auth replaces team tokens.** Submitting batches now requires a user account session (or user-owned API token). Legacy team tokens get `legacy team token cannot submit user-facing work`. Set a user's password via direct DB UPDATE using dollar-quoted SQL literals so psql doesn't eat the argon2 hash's `$` separators.
