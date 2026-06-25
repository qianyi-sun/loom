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
