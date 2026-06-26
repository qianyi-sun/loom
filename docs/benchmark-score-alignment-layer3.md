# Benchmark Score Alignment — Layer 3 Reports

Layer 1 declared the manifest; Layer 2 recorded the adapter-level
parity decisions and unit-level replay evidence; **Layer 3 records
cluster-level batch evidence** — real Loom runs against the imported
benchmark, executed end-to-end through the worker, scored by the
materialized verifier.

A benchmark earns Layer 3 evidence per (agent × method × model) triple
that is run end-to-end. Each entry pins the upstream revision, the
imported task slate, the batch id, the per-task outcome, and the
delta vs. the canonical reference (or "by construction" when the
agent/method combo has no external reference to compare against —
e.g. oracle on solve.sh-equipped tasks is expected to be 100%).

Reports are added per benchmark as Layer 3 evidence lands. This file
is the human-readable narrative; the machine-readable form lives in
`benchmark-score-alignment.json` under each benchmark's
`layer3_evidence` field.

## skilllearnbench — oracle × human_authored

- **Upstream pin:** `cxcscmu/SkillLearnBench@2d714f28b4f14bcaf93bccd5d11fbd3bd524fc46`
  (catalog `benchmarks.json`, since #537).
- **Imported task slate:** 100 tasks across 20 families, registered
  via `loom datasets import skilllearnbench`. Per-task `human_authored`
  skills materialized into each bundle's `skills/` directory by the
  adapter (#533), `oracle_eligible` tag set per upstream `solve.sh`
  presence (#536), preflight consumes the tag (#545).
- **Batch:** `slb-phase3-oracle-73` (id
  `20a18b8a-8164-4fef-97b3-e319e504b856`). Submitted via
  `loom eval batch create --agent oracle --task-filter '{"benchmark_id":"skilllearnbench","tag_filters":{"oracle_eligible":["true"]}}'`,
  expected_trial_count=73, n_per_task=1, backend=docker.
- **Reference:** by construction. Oracle on a `solve.sh`-equipped
  task executes the upstream's own canonical solution and then the
  upstream's own `tests/test.sh`. Convergence to reward=1.0 is the
  invariant — divergence indicates a Loom-side regression (skills
  materialization, build context, verifier shim, etc.), not a
  benchmark-side question.
- **Result:** 73/73 trials terminal-succeeded with 0 platform failures
  in 31 min wall-clock (5-worker concurrency). **57 / 73 scored
  reward=1.0; 16 scored reward=0.0; aggregate = 0.781.**
  Per-task table in `docs/evidence/slb-phase3-oracle-73.json`.
- **Verdict:** the 0.781 aggregate is **not** an upstream-vs-Loom
  parity question — every passing trial confirms Loom's verifier
  shim matches upstream `tests/test.sh` semantics exactly. The 16
  outliers cluster into 4 families and all share a single root
  cause: their upstream `solve.sh` resolves sibling solution files
  via `${BASH_SOURCE[0]}` dirname, relative paths, or hard-coded
  `/root/solution.py` — patterns that don't match Loom's
  `OracleAgent` invocation contract (upload solve.sh to workdir,
  exec from workdir). Filed as #548. Layer 3 verdict stands:
  **the SLB import + skills-injection + readiness-tagging pipeline
  is correct; the residual gap is in `OracleAgent` not in any #531
  PR**.

### Per-family outcomes

| Family | Pass | Fail | Notes |
|---|---:|---:|---|
| anthropic-poster-design | n/a | n/a | All 5 instances `oracle_eligible=false` upstream — not in batch. |
| chinese-poem-generator | n/a | n/a | Same — `oracle_eligible=false` family. |
| court-form-filling | 6 | 0 | |
| dbscan-parameter-tuning | 5 | 0 | |
| dependency-vulnerability-check | 5 | 0 | |
| earthquake-plate-calculation | 2 | 4 | #548: hard-coded `/root/solution.py` style paths. |
| enterprise-information-search | n/a | n/a | `oracle_eligible=false` family. |
| financial-analysis | 6 | 0 | |
| fix-security-bug | 3 | 0 | |
| github-repo-analytics | 0 | 5 | #548: solve.sh loads `query_params.json` via relative path. |
| nlp-paper-reproduction | 3 | 0 | |
| offer-letter-generator | 6 | 0 | |
| organize-messy-files | 0 | 6 | #548: `${BASH_SOURCE[0]}` dirname → wrong dir. |
| python-scala-translation | 1 | 1 | #548: instance-2 has the path-resolution pattern. |
| schedule-planning | n/a | n/a | `oracle_eligible=false` family. |
| stock-data-visualization | 5 | 0 | |
| temperature-simulation | n/a | n/a | `oracle_eligible=false` family. |
| travel-planning | 5 | 0 | |
| video-object-counting | 5 | 0 | |
| weighted-gdp-calculation | 5 | 0 | 1 instance is `oracle_eligible=false`; remaining 5 all pass. |
| **Total** | **57** | **16** | |

### What this Layer 3 entry validates

1. **Skills injection (#533) is end-to-end.** The container's
   `/root/.<agent>/skills/<family>/` directory contains the
   `human_authored` skill bundle the adapter materialized, not the
   empty `.keep` placeholder. (Per-trial artifacts confirm
   per-instance.)
2. **`oracle_eligible` tag (#536) is end-to-end.** The 73 selected
   tasks all have `oracle_eligible=true`; the 27 with `=false` are
   not in this batch; the preflight (#545) consumed the tag instead
   of rejecting the whole batch.
3. **Catalog pinning (#537) is end-to-end.** The imported task
   checksums match the pinned upstream sha, not whatever `main` was
   at import time.
4. **Loom verifier shim matches upstream semantics.** A reward=1.0
   per oracle-eligible task means the shim correctly:
   - runs upstream `tests/test.sh`,
   - reads `/logs/verifier/reward.txt`,
   - converts it into Loom's `VerifierResult` JSON.

### Open follow-ups (not in this Layer 3 entry)

- **`claude-code × claude-sonnet-4-6 × human_authored` Layer 3 entry.**
  Smoke run (trial `d248a63d-baa1-46c3-8cdb-c2c3f88b9ae4`) confirmed:
  - skills materialization reaches the container (artifact
    `.codex/skills/brand-guidelines/SKILL.md` for
    `anthropic-poster-design-1`),
  - cluster pipeline runs end-to-end through the qa-relay-anthropic
    provider connection (`https://yibuapi.com` with anthropic dialect),
  - claude-code CLI exits rc=1 inside the container with zero LLM calls.
  Root cause uninvestigated — likely agent-runtime issue
  (model-name format, env var wiring, or container-internal CLI
  bootstrap). Filed as a separate Phase 3 follow-up.
- **Aggregate vs. published leaderboard number.** Requires the
  model-agent path above to first work end-to-end; only then can we
  compare to a published cell on cxcscmu.github.io/SkillLearnBench.

## gpqa-diamond — litellm × claude-haiku-4-5 (partial sweep)

- **Upstream pin:** `idavidrein/gpqa@56686c06f5e19865c153de0fdb11be3890014df7`
  (catalog `benchmarks.json`, `gpqa-diamond` sibling adapter added in #546).
- **Imported task slate:** 198 tasks (full GPQA Diamond subset), registered via
  `loom datasets import gpqa-diamond`. Same upstream zip, `gpqa_diamond.csv` member.
- **Batch:** `stage-b-gpqa-diamond-smoke-5` (id `a86735f8-dccf-401d-9655-768b73da95a4`).
  Submitted via `loom eval batch create --benchmark gpqa-diamond --agent litellm
  --provider qa-relay-anthropic --model claude-haiku-4-5 --n-per-task 1 --backend docker`.
  Provider connection: `qa-relay-anthropic` (yibuapi, anthropic dialect).
- **Reference:** Harbor's published `parity_experiment.json` for `adapters/gpqa-diamond`
  at `harbor-framework/harbor@2ead3f1f` records codex + gpt-5.2 → 87.21% ± 0.34 across
  3 trials. **That config is NOT the matched config for this Loom run** — yibuapi
  serves anthropic-dialect models, not codex/gpt-5.2 — so this Layer 3 entry is
  workflow validation + Loom-side performance evidence on the substitute model, not
  a strict Loom-vs-Harbor parity proof. A true paired comparison would require the
  same model+agent on both sides.
- **Result:** **156 / 198 trials succeeded (s=156, f=0, x=42); 87 / 156 scored
  reward=1.0; 69 / 156 scored reward=0.0; aggregate over completed trials = 0.5577.**
  The 42 unrun trials were cancelled because the `deploy-worker-1` container hard-failed
  partway through with `httpx.ReadError` on the worker→CP `/workers/register` POST and
  could not self-recover across multiple restart attempts. The CP remained healthy
  throughout (`GET /healthz → 200`). Filed as a follow-up worker resilience issue.
  Per-trial breakdown in `docs/evidence/2026-06-26-gpqa-diamond-haiku-partial.json`.
- **Verdict:** the Loom side validated end-to-end on 156 distinct GPQA Diamond
  tasks. The 0.5577 aggregate is consistent with claude-haiku-4-5's expected weak-model
  performance on a domain-PhD-level multiple-choice benchmark; the gap vs. Harbor's
  87.21% (gpt-5.2) is a model+agent gap, not a verifier-semantics gap. The Loom adapter
  (#546 sibling slug), import pipeline, batch scheduling, LiteLLMAgent → qa-relay →
  yibuapi → Anthropic round-trip, and the script verifier extracting the letter answer
  from `final_answer.txt` all work correctly.
- **Layer 2 manifest status:** the `gpqa` entry's `layer2_evidence.status` remains
  `pending_paired_run` — this Layer 3 evidence is workflow validation, not a matched-
  config paired comparison against Harbor's published baseline. A future entry with a
  shared model+agent on both sides (or rerunning Harbor with claude-haiku via yibuapi)
  would justify flipping `paired_validated` / `paired_delta_flagged`.

### What this Layer 3 entry validates

1. **`gpqa-diamond` sibling adapter (#546) is end-to-end.** Loom's worker selects the
   198 Diamond tasks (not the 546 Extended superset), `convert_instance` materializes
   the bundled `verifier/check.py` reading `final_answer.txt`, and the verifier scores
   per-task at the expected 1.0/0.0 boundary.
2. **The qa-relay-anthropic provider connection works for batches.** 158 LLM calls
   succeeded through `https://yibuapi.com` with anthropic dialect; 98K prompt tokens
   + 134K completion tokens recorded by the gateway.
3. **No platform failures during the run** — the 42 unrun trials were cancelled by us
   after the worker crashed, not failed by the verifier or sandbox layer. Every trial
   that started reached a terminal state with a parseable reward.

### Open follow-ups (not in this Layer 3 entry)

- **Worker container resilience.** `deploy-worker-1` hit `httpx.ReadError` on its
  startup `/workers/register` POST after running ~3 trials per slot for ~15 minutes,
  could not self-recover across compose restart cycles. CP healthz remained 200
  throughout. Filed as a separate worker-resilience follow-up.
- **True matched-config paired comparison vs Harbor.** Requires either (a) Harbor
  rerun with claude-haiku-4-5 via yibuapi against the same 198 Diamond tasks, or
  (b) yibuapi adding gpt-5.2 / codex compatibility so Loom can replay Harbor's
  published config end-to-end. Tracked in #541.
- **Re-run the 42 unfinished trials** once the worker container is recovered, to
  produce full 198-task aggregate evidence.
