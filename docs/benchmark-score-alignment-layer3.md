# Benchmark Score Alignment — Layer 3 Reports

> **Cross-repo issue/PR refs:** bare `#N` and `carinrc/loom#N` refer to
> the pre-2026-06-26 archive tracker. Canonical follow-up work lives on
> `qianyi-sun/loom` (see [`repo-migration.md`](repo-migration.md)); use
> full-URL form when it matters which repo.

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

For SkillLearnBench Codex reference runs, Layer 3 reports must also
attach effective request-parameter evidence. Generate it offline with:

```
python scripts/alignment/skilllearnbench_effective_params.py \
  --official-plan-json <redacted-official-plan.json> \
  --loom-debug-json <redacted-loom-debug.json> \
  --out-json <effective-params.json> \
  --out-md <effective-params.md>
```

The official-plan input records the agent id, template id, computed
`extra_flags`, command template, and rendered command. The helper emits
only a redacted command summary; prompt bodies, env values, paths,
tokens, and secret-looking option values are omitted. If the selected
template does not contain `{extra_flags}`, computed settings such as
`--settings '{"temperature":0}'` are classified as
`provider_defaults_extra_flags_not_consumed`, so reports do not confuse
computed runner flags with effective model request params. The Loom
debug input records sanitized `trial_config.request_params` and
gateway/provider `request_params` audit summaries, and the output
classifies each task as aligned by provider defaults, aligned by
explicit params, or mismatched.

## skilllearnbench — oracle × human_authored

- **Upstream pin:** `cxcscmu/SkillLearnBench@2d714f28b4f14bcaf93bccd5d11fbd3bd524fc46`
  (catalog `benchmarks.json`, since #537).
- **Imported task slate:** 100 tasks across 20 families, registered
  via `loom datasets import skilllearnbench`. Per-task `human_authored`
  skills materialized into each bundle's `skills/` directory by the
  adapter (#533), `oracle_eligible` tag set per upstream `solve.sh`
  presence (#536), preflight consumes the tag (#545).
- **Slate evolution since this batch:** the eligibility logic was
  later tightened in #552 (`_UPSTREAM_BAD_ORACLE_INSTANCE_IDS` for
  upstream `solve.sh` files that don't actually solve their task, +
  docker-compose external-env gating for tasks that need credentials
  like `GH_TOKEN` that the platform doesn't supply to oracle runs).
  Current production slate is **58 oracle_eligible=true / 42 false**,
  not the 73/27 captured in this batch. A re-run on current `dev`
  (`slb-548-verify-58`, batch `901100ab-acd6-4078-84b7-3f727cc9d062`)
  scored 56/58 reward=1.0 — the 2 outliers
  (`python-scala-translation-1`/`-2`) hit a host
  `fs.inotify.max_user_instances` limit at sbt-compile time, not a
  Loom regression. This file preserves the original 73-batch
  evidence as a snapshot; for current eligibility see
  `packages/loom-benchmarks/loom_benchmarks/adapters/skilllearnbench.py`.
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

## gpqa-diamond — paired Loom vs Harbor on claude-haiku-4-5

Historical archive issue carinrc/loom#541 closed with this evidence. This is
the matched-config paired evidence under canonical #32: both Loom and
Harbor ran the same 198-task GPQA Diamond subset with the same model (claude-haiku-4-5
via yibuapi, anthropic dialect) through the same upstream verifier semantics (exact
letter match on the canonical Diamond answer). The agent runtime differs on each side:
Loom's `litellm` agent is single-shot (one model call per task) whereas Harbor's
`terminus-2` agent runs a tool-use loop. We keep both numbers and attribute the delta
to the agent-runtime difference, not to verifier divergence.

- **Upstream pin (shared):** `idavidrein/gpqa@56686c06f5e19865c153de0fdb11be3890014df7`.
- **Imported task slate (shared):** 198 GPQA Diamond tasks. Loom uses the `gpqa-diamond`
  sibling adapter from #546 (reads `gpqa_diamond.csv` from the same upstream zip).
  Harbor uses `adapters/gpqa-diamond` at `harbor-framework/harbor@2ead3f1f`.
- **Verifier (shared by construction):** exact letter match (A/B/C/D) of the agent's
  final answer against the canonical answer for each Diamond row. Loom emits a script
  verifier (`verifier/check.py`) that reads `final_answer.txt`; Harbor's adapter emits
  the same shape via `tests/test.sh`. Both score 1.0 on match, 0.0 otherwise.

### Loom side

- **Batch:** `stage-b-gpqa-diamond-full-198` (id `4128a78a-2bfa-4e02-a700-2aba54252f5d`).
- **Invocation:**
  ```
  uv run python -m loom_cli eval batch create \
    --benchmark gpqa-diamond --agent litellm \
    --provider qa-relay-anthropic --model claude-haiku-4-5 \
    --n-per-task 1 --backend docker
  ```
- **Provider connection:** `qa-relay-anthropic` (`https://yibuapi.com`, anthropic
  dialect, `rate_card=anthropic`).
- **Result: 198/198 trials succeeded, 0 failures. 101 correct + 97 incorrect →
  aggregate 0.5101 (51.01%).** 204 LLM calls (some retries), 144,221 prompt + 182,523
  completion tokens. Per-trial breakdown in
  `docs/evidence/2026-06-26-gpqa-diamond-loom-full-198.json`.

### Harbor side

- **Job:** `stage-b-paired-haiku-198-v4` (id `438da02b-8403-4453-bb1c-dd3d1e747e1c`).
- **Invocation:**
  ```
  ANTHROPIC_API_KEY=<qa-relay key> uv run harbor job start \
    -p datasets/gpqa-diamond -a terminus-2 -m anthropic/claude-haiku-4-5 \
    --ak api_base=https://yibuapi.com \
    --ae ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY --ae ANTHROPIC_API_BASE=https://yibuapi.com \
    --n-concurrent 5
  ```
- **Result: 198/198 trials reached a verifier reward (1 AgentTimeoutError still
  produced a 0.0). 115 correct + 83 incorrect → aggregate 0.5808 (58.08%).** 14.0M input
  + 649K output tokens, $9.73 reported by Harbor. Per-trial breakdown in
  `docs/evidence/2026-06-26-gpqa-diamond-harbor-full-198.json`.

### Paired comparison

| | Loom | Harbor | Delta |
|---|---|---|---|
| Agent | litellm (single-shot) | terminus-2 (loop) | runtime differs |
| Model | claude-haiku-4-5 | claude-haiku-4-5 | identical |
| Tasks (Diamond) | 198 | 198 | identical |
| Verifier | letter-match | letter-match | identical by construction |
| Aggregate | 51.01% | 58.08% | **+7.07 pp Harbor** |

The 7.07 pp gap is consistent with the agent-runtime difference: terminus-2 can
re-read the question, call shell tools, and re-attempt before committing an answer,
whereas litellm answers in one shot. **Verifier semantics agree by construction** —
both sides apply identical letter-match logic against the same canonical answer row,
so any per-task disagreement reduces to model stochasticity given the agent runtime.

The Harbor parity_experiment baseline at the pinned commit (codex + gpt-5.2 → 87.21%
across 3 trials) is a different (agent, model) combination and isn't the matched
comparison run here; reproducing it would require yibuapi (or another relay) serving
codex / gpt-5.2.

### What this Layer 3 entry validates

1. **`gpqa-diamond` sibling adapter (#546) is end-to-end.** Loom's worker selects the
   198 Diamond tasks (not the 546 Extended superset), `convert_instance` materializes
   the bundled `verifier/check.py` reading `final_answer.txt`, and the verifier scores
   per-task at the expected 1.0/0.0 boundary.
2. **The qa-relay-anthropic provider connection works for batches** at the 198-task
   scale and routes through `https://yibuapi.com` with anthropic dialect.
3. **No verifier-semantics divergence between Loom and Harbor** on the Diamond slate —
   both score by exact letter match against the same canonical row, and the aggregate
   gap is fully attributable to agent-runtime differences.
4. **End-to-end Loom platform health** at the 198-task scale after #553 was resolved
   (root cause: `docker compose` not loading root `.env` from `deploy/` project dir;
   fixed via the `deploy/.env → ../.env` symlink in #555).

### Open follow-ups (not in this Layer 3 entry)

- **Symmetric agent-runtime comparison.** To remove the agent-runtime contribution from
  the delta, a follow-up entry could run Loom with `claude-code` (Loom's agent-loop
  runtime, currently filed as a separate Phase 3 follow-up for SLB) against the same
  198 Diamond tasks, or run Harbor with a single-shot agent if one ships. Either would
  produce verifier-purer parity evidence.
- **Per-task cross-reference.** Both runs produced per-trial JSONs; cross-referencing
  task IDs (Loom keys by upstream Airtable `Record ID`, Harbor keys by row index) and
  comparing per-task rewards would surface any individual-task verifier disagreement
  that aggregates hide.

## aime — paired Loom (aime-24 + aime-25) vs Harbor (aime) on claude-haiku-4-5

Historical archive issue carinrc/loom#540 closed with this evidence. Matched-config
paired evidence on AIME 2024 + 2025 (60 tasks total, 30
per year). Same model, same provider, same verifier semantics (exact integer match
on the canonical answer). Loom uses two separate dataset slugs (`aime-24`, `aime-25`)
sharing the `_AIMEYearBase` adapter; Harbor's `adapters/aime` covers both years in a
single 60-task slate. The agent runtime differs as in the GPQA paired run above:
Loom's
`litellm` is single-shot, Harbor's `terminus-2` is a tool-use loop. AIME's
math-reasoning surface amplifies the agent-runtime effect compared to GPQA Diamond's
multiple-choice surface.

- **Upstream pin (Loom):** `loom_benchmarks.adapters.aime` adapter reads the AIME 2024
  + 2025 problems from huggingface dataset rows pinned in the catalog
  `benchmarks.json` (`aime-aimo-validation` family with `params.year`).
- **Upstream pin (Harbor):** `harbor-framework/harbor@2ead3f1f` `adapters/aime`.
- **Harbor parity_experiment.json baseline:** none published at the pinned commit
  (`gh api .../parity_experiment.json → 404`). This Layer 3 entry establishes the
  cross-system baseline ourselves.
- **Verifier (shared by construction):** Loom emits a script verifier
  (`packages/loom-benchmarks/loom_benchmarks/adapters/aime.py` →
  `verifier/run.sh` + `verifier/check.py`) extracting the last integer from
  `final_answer.txt`. Harbor's adapter scores via `tests/test.sh` doing the same
  exact-integer extraction. Both pipe through the same upstream canonical answer.

### Loom side

- **Batches (two slugs, ran sequentially via 5-worker concurrency):**
  - `stage-b-aime-24-haiku-full-30` (id `5f417465-ecf6-4535-8f5c-f0f302b537f9`,
    30/30 succeeded, 6 correct → 20.00%)
  - `stage-b-aime-25-haiku-full-30` (id `0fc16ea6-7368-414a-a7ff-f80b03d17eaa`,
    30/30 succeeded, 8 correct → 26.67%)
- **Combined Loom AIME:** 60/60 succeeded, 0 failures. 14 correct + 46 incorrect →
  aggregate **0.2333 (23.33%)**. 60 LLM calls, 360,263 prompt + 211,811 completion
  tokens. Per-trial JSONs in
  `docs/evidence/2026-06-26-aime-24-loom-full-30.json` and
  `docs/evidence/2026-06-26-aime-25-loom-full-30.json`.
- **Invocation (both, identical except slug + name):**
  ```
  uv run python -m loom_cli eval batch create \
    --benchmark aime-24 \                # or aime-25
    --agent litellm \
    --provider qa-relay-anthropic --model claude-haiku-4-5 \
    --n-per-task 1 --backend docker
  ```

### Harbor side

- **Job:** `stage-b-aime-haiku-60` (id from `result.json`).
- **Result:** 60/60 trials reached a verifier reward (1 AgentTimeoutError → 0.0).
  32 correct + 28 incorrect → aggregate **0.5333 (53.33%)**. 15.18M input + 492K
  output tokens, $5.91. Per-trial JSON in
  `docs/evidence/2026-06-26-aime-harbor-full-60.json`.
- **Invocation:**
  ```
  ANTHROPIC_API_KEY=<qa-relay key> uv run harbor job start \
    -p datasets/aime -a terminus-2 -m anthropic/claude-haiku-4-5 \
    --ak api_base=https://yibuapi.com \
    --ae ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY --ae ANTHROPIC_API_BASE=https://yibuapi.com \
    --n-concurrent 5
  ```

### Paired comparison

| | Loom (24+25) | Harbor (aime) | Delta |
|---|---|---|---|
| Agent | litellm (single-shot) | terminus-2 (loop) | runtime differs |
| Model | claude-haiku-4-5 | claude-haiku-4-5 | identical |
| Tasks | 60 (30+30) | 60 | identical slate |
| Verifier | last-integer-match | last-integer-match | identical by construction |
| Aggregate | 23.33% | 53.33% | **+30.00 pp Harbor** |

The 30 pp gap is far larger than the GPQA Diamond gap (+7 pp), reflecting AIME's
reasoning surface — math problems benefit substantially from an agent loop that
can produce intermediate work, retry, and self-correct, whereas multiple-choice
GPQA tolerates a single-shot answer better. **Verifier semantics still agree by
construction** — both systems extract the last integer from `final_answer.txt` and
compare it to the canonical row. The runtime difference fully explains the delta.

### What this Layer 3 entry validates

1. **The `_AIMEYearBase` per-year adapter pattern works end-to-end.** Each year
   slug materializes to its own 30-task slate with the year tag, and both slugs
   share the same script verifier code path.
2. **Loom CLI handles back-to-back batch submissions** without interfering — both
   AIME batches ran cleanly under the same worker container without restart.
3. **Verifier semantics align across systems** even on a reasoning surface where
   answers are integers extracted from free-form text, not multiple-choice letters.

### Open follow-ups (not in this Layer 3 entry)

- **No published Harbor baseline at the pinned commit.** Means we can only compare
  Loom vs Harbor at the same config (which we did here), not Loom vs the literature
  number. The 53.33% Harbor result here is the only AIME terminus-2 + haiku-4-5
  data point we know of at this Harbor commit; future Harbor parity_experiment
  updates may add a reference number.
- **Same symmetric-agent-runtime follow-up as the GPQA entry** — running Loom with
  `claude-code` against AIME would isolate verifier parity from agent-runtime
  contribution.

## terminal-bench-2 — preliminary claude-code × claude-haiku-4-5

This entry preserves the checked-in qianyi-sun/loom#222 public-beta evidence for
Claude Haiku 4.5 on Terminal-Bench 2, but it is **preliminary** and **not canonical
acceptance**. Canonical #222 acceptance still requires a Terminus-2 rerun/live
validation against the same 86-task slate.

Status: preliminary and not canonical acceptance.

- **Upstream pin:** `terminal-bench-core` v0.1.1, commit `91e10457`.
- **Imported task slate:** full Terminal-Bench 2 task set, 86 tasks.
- **Batch:** `81d3f790-a426-4c64-97aa-2ddf5a08a563`, captured from public-beta
  `https://yylx.world` on 2026-06-30.
- **Agent / model:** Loom `claude-code` adapter with `claude-haiku-4-5` through
  `yibuapi-anthropic-pb`.
- **Per-task evidence:** `docs/evidence/issue-222/per-task-results.json`.

### Preliminary result

| Metric | Value |
|---|---:|
| Full denominator | 86 tasks |
| Reward-positive rows | 35/86 = 40.70% |
| Clean `state=succeeded, reward=1.0` rows | 33/86 = 38.37% |
| Reward-positive rows that ended `trajectory_flush_failed` | 2 |
| `state=failed` system failures | 12 system failures |
| Upstream Haiku reference | ~40.2% |
| Historical reward-positive delta vs upstream | +0.50 pp |

The historical 35/86 reward-positive headline is close to Anthropic's published
Claude Haiku 4.5 Terminal-Bench reference of ~40.2% (non-thinking mode,
Terminus-2 scaffold, 11-run average). The checked-in JSON also shows that only
33/86 rows are clean platform-successful `state=succeeded, reward=1.0` outcomes:
two reward-positive rows ended as `trajectory_flush_failed`, and the batch has
12 system failures overall. Those platform-failure rows are part of the evidence,
not something to smooth over in the acceptance status.

### What this preliminary entry validates

1. **The full 86-task slate ran through Loom public beta with a real model.** This
   is stronger than replay-only Layer 2 evidence and should remain linked from
   the manifest so future readers do not lose it.
2. **The reward-positive headline is in the expected upstream range.** Counting
   all reward-positive rows gives 40.70%, a +0.50 pp delta vs the upstream ~40.2%
   Haiku reference.
3. **The result is not final acceptance.** The run uses Loom `claude-code` while
   the upstream reference uses Terminus-2, it is a single run vs an 11-run
   reference average, and the 12 system failures include two
   `trajectory_flush_failed` reward-positive rows.

### Open follow-ups (not in this preliminary entry)

- **Terminus-2 rerun for canonical #222 acceptance.** Re-run the full 86-task
  Terminal-Bench 2 slate with `--agent terminus-2 --model claude-haiku-4-5` and
  record clean live acceptance evidence before changing the issue to final
  Layer 3 accepted status.
- **Failure taxonomy review.** The 12 system failures should remain visible in
  the evidence; a future run should either eliminate them or account for them as
  platform failures outside model-quality scoring.
