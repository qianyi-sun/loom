# Harbor Adapter Snapshot — 2026-06-25

**Repo:** https://github.com/harbor-framework/harbor
**Commit:** `2ead3f1f2462f6f7260aca5ef2377cd7e309ff06`
**Pulled:** 2026-06-25
**Purpose:** Pinned reference for canonical #32 Layer 2 evidence correction. The original Layer 2 batches (#499/#513/#514/#517) mistakenly identified `coder-harbor-cloud` (Huawei platform) as "Harbor"; historical archive issue carinrc/loom#538 preserves that correction context. The actual reference target is the harbor-framework/harbor agent-evaluation framework. This snapshot fixes the adapter inventory at a single commit so later manifest entries point at a stable target.

## v1.0 benchmark → Harbor adapter mapping

| Loom v1.0 benchmark | Harbor adapter directory | Notes |
|---|---|---|
| aime-24 | `adapters/aime` | One Harbor adapter covers AIME; per-year task-set split (24 vs 25) is a benchmark-config concern, not separate adapters. |
| aime-25 | `adapters/aime` | Same adapter as aime-24; year selection happens inside the adapter's task set. |
| gpqa | `adapters/gpqa-diamond` | Harbor ships the GPQA-Diamond split only. Parity vs. Loom's `gpqa` depends on Loom also using the Diamond split — flag for the manifest entry. |
| livecodebench | `adapters/livecodebench` | Direct match. |
| swe-bench-verified | `adapters/swebench` | Direct match for the canonical SWE-bench adapter. Variants `swebenchpro`, `multi-swe-bench`, `swebench_multilingual` also ship in this snapshot (all four present and verified) but are separate benchmarks, not Verified-split substitutes. |
| humaneval | none | No `humaneval` directory. Only `humanevalfix` (the bug-fixing variant) ships — NOT a parity target for plain HumanEval. |
| mbpp | none | No `mbpp` directory and no MBPP-family variant in this snapshot. |
| math-500 | none | No `math-500` / `math500` / `math_500` directory. Closest math-family adapters are `ineqmath` and `omnimath` — different benchmarks, not parity targets. |
| mmlu-pro | none | No `mmlu-pro` / `mmlu_pro` directory. `mmmlu` (Multilingual MMLU) ships but is a distinct benchmark, NOT a parity target for MMLU-Pro. |
| skillflow | none | Loom-specific benchmark; Harbor does not adapt it. |
| skilllearnbench | none | Loom-specific benchmark; Harbor does not adapt it. |
| terminal-bench-2 | none | No `terminal-bench` / `tb2` directory. Terminal-Bench 2 is the host harness, not an adapted benchmark in Harbor's adapter inventory. |

**Summary:** Of the 12 v1.0 benchmark entries (counting aime-24/25 separately), 5 have direct Harbor adapter parity (aime ×2, gpqa-diamond, livecodebench, swebench), and 7 have no Harbor adapter at this commit (humaneval, mbpp, math-500, mmlu-pro, skillflow, skilllearnbench, terminal-bench-2). Variant adapters (`humanevalfix`, `mmmlu`, `ineqmath`, `omnimath`) are explicitly called out as non-parity to avoid future confusion.

## Full adapter list (82 entries)

aa-lcr
abc-bench
ace-bench
adebench
aider_polyglot
aime
algotune
arc_agi_2
autocodebench
bfcl
bigcodebench_hard
bird_bench
bixbench
clbench
codepde
compilebench
cooperbench
crmarena
crustbench
cybergym
dabstep
dacode
deepsynth
deveval
devopsgym
ds1000
evoeval
featbench
featurebench
financeagent
frontier-cs-algorithm
gaia
gaia2
gdb
gpqa-diamond
gso
hle
humanevalfix
ineqmath
kramabench
kumo
labbench
lawbench
livecodebench
llmsr_bench
medagentbench
ml_dev_bench
mlgym-bench
mmau
mmmlu
multi-swe-bench
omnimath
pixiu
qcircuitbench
quixbugs
reasoning-gym
refav
replicationbench
research-code-bench
rexbench
satbench
scicode
scienceagentbench
seal0
simpleqa
sldbench
spider2-dbt
spreadsheetbench-verified
strongreject
swebench
swebench_multilingual
swebenchpro
swegym
swelancer
swesmith
swtbench
tau3-bench
textarena
theagentcompany
usaco
webgen_bench
widesearch
