# Benchmark Score Alignment — Layer 2 Reports

> **Cross-repo issue/PR refs:** bare `#N` and `carinrc/loom#N` refer to
> the pre-2026-06-26 archive tracker. Canonical follow-up work lives on
> `qianyi-sun/loom` (see [`repo-migration.md`](repo-migration.md)); use
> full-URL form when it matters which repo. Numbering is fresh on the
> new tracker, so bare `#N` on the archive is likely a different issue
> from `qianyi-sun/loom#N`.

Layer 1 (see [`benchmark-score-alignment.md`](benchmark-score-alignment.md))
declares the manifest: each v1.0 benchmark's canonical reference, score
semantics, and replay-case definitions. Layer 2 records the actual
alignment evidence for each benchmark: the parity decision (Harbor vs.
upstream canonical), the replay tests that prove Loom's verifier matches
the canonical scorer, and any observed deltas or known limitations.

This file is the human-readable narrative. The machine-readable form
lives in [`benchmark-score-alignment.json`](benchmark-score-alignment.json)
under each benchmark's `layer2_evidence` field; the `harbor_reference`
block at the top of that manifest pins the Harbor repo and commit this
document is written against.

## Correction note (2026-06-25)

The first four Layer 2 batches (PRs #499, #513, #514, #517) mistakenly
named `coder-harbor-cloud` (a Huawei agent platform) as the Harbor
parity target. The real reference is `harbor-framework/harbor` at
commit `2ead3f1f2462f6f7260aca5ef2377cd7e309ff06`. The correction is
tracked under canonical #32, with historical archive sub-issue
carinrc/loom#538 preserving the original correction context. The pinned
adapter inventory at
that commit is captured in
[`docs/research/harbor-adapter-snapshot-2026-06-25.md`](research/harbor-adapter-snapshot-2026-06-25.md).
All sections below reference the real Harbor.

## Reference target

The pinned Harbor reference is `harbor-framework/harbor`
(<https://github.com/harbor-framework/harbor>) at commit
`2ead3f1f2462f6f7260aca5ef2377cd7e309ff06` (2026-06-25). At that
commit, Harbor's `adapters/` directory contains adapters for five of
the twelve v1.0 benchmarks; the remaining seven collapse to
upstream-canonical equivalence because Harbor ships no parity target
for them.

| Benchmark | Harbor parity | Adapter at pinned commit | Layer 2 evidence |
|---|---|---|---|
| `aime-24` | supported | [`adapters/aime`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/aime) | paired-validated with `aime-25`: Loom 23.33% vs Harbor 53.33% |
| `aime-25` | supported | [`adapters/aime`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/aime) (shared with aime-24) | paired-validated with `aime-24`: Loom 23.33% vs Harbor 53.33% |
| `gpqa` | supported (Diamond) | [`adapters/gpqa-diamond`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/gpqa-diamond) | paired-validated on Diamond: Loom 51.01% vs Harbor 58.08% |
| `livecodebench` | supported | [`adapters/livecodebench`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/livecodebench) | pending paired run |
| `swe-bench-verified` | supported | [`adapters/swebench`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/swebench) | pending paired run |
| `humaneval` | not supported | (`adapters/humanevalfix` is a different task set) | replay-validated |
| `mbpp` | not supported | — | replay-validated |
| `math-500` | not supported | (`aime`, `ineqmath`, `omnimath` cover different task sets) | replay-validated |
| `mmlu-pro` | not supported | (`adapters/mmmlu` is the multilingual variant, different question pool) | replay-validated |
| `terminal-bench-2` | not supported | (TB-2 is Harbor's host framework, not an adapted benchmark) | replay-validated |
| `skillflow` | not supported | — | replay-validated |
| `skilllearnbench` | not supported | — | replay-validated |

## Benchmarks with Harbor parity

These five benchmarks have a real Harbor adapter at the pinned commit.
Loom's adapter and Harbor's adapter implement the same parity target
via independent code paths, so end-to-end matched-config paired runs
are the appropriate evidence — stronger than the by-construction
claims used by the earlier (incorrect) Layer 2 batches.

Three of the five now have paired validation evidence: AIME 2024/2025
share one 60-task Harbor slate, and GPQA is validated on Harbor's
198-task Diamond subset. LiveCodeBench and SWE-Bench Verified remain
pending paired runs tracked by canonical #21 and #20.

The replay tests listed below remain the Layer 2 evidence that Loom's
verifier semantics are well-defined and behave as documented; they
are necessary but no longer sufficient where Harbor ships a parallel
implementation.

### aime-24

- **Harbor adapter:** [`adapters/aime`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/aime) at `harbor-framework/harbor@2ead3f1f`.
- **Published Harbor baseline:** none at the pinned commit; the paired run below establishes the same-config Harbor baseline.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/aime.py`.
- **Layer 2 status:** `paired_validated` through the combined 60-task AIME 2024/2025 paired run.
- **Paired result:** Loom `litellm` + `claude-haiku-4-5` scored 14/60 (23.33%) across `aime-24` and `aime-25`; Harbor `terminus-2` + the same model scored 32/60 (53.33%). The +30.00 pp Harbor delta is attributed to agent-runtime differences; both systems use last-integer exact match against the canonical answer.
- **Replay tests (verifier semantics):**
  - `packages/loom-benchmarks/tests/test_aime_adapter.py::test_aime_run_sh_is_self_contained_and_writes_verifier_result`
  - `packages/loom-benchmarks/tests/test_aime_adapter.py::test_aime_checker_rejects_wrong_answer`
  - `packages/loom-benchmarks/tests/test_aime_adapter.py::test_aime_checker_picks_last_integer`
- **Evidence:** historical archive issue [carinrc/loom#540](https://github.com/carinrc/loom/issues/540) closed with paired evidence; narrative and per-trial files are in [`benchmark-score-alignment-layer3.md`](benchmark-score-alignment-layer3.md#aime--paired-loom-aime-24--aime-25-vs-harbor-aime-on-claude-haiku-4-5).

### aime-25

- **Harbor adapter:** [`adapters/aime`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/aime) — shared with aime-24; Harbor's adapter covers both years through one verifier.
- **Published Harbor baseline:** none at the pinned commit; the paired run below establishes the same-config Harbor baseline.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/aime_2025.py` (shares script-verifier infrastructure with Loom's aime-24 adapter, mirroring Harbor's single-adapter approach).
- **Layer 2 status:** `paired_validated` through the combined 60-task AIME 2024/2025 paired run.
- **Paired result:** Loom `litellm` + `claude-haiku-4-5` scored 14/60 (23.33%) across `aime-24` and `aime-25`; Harbor `terminus-2` + the same model scored 32/60 (53.33%). The +30.00 pp Harbor delta is attributed to agent-runtime differences; both systems use last-integer exact match against the canonical answer.
- **Replay tests (verifier semantics):**
  - `packages/loom-benchmarks/tests/test_aime_adapter.py::test_aime_2025_emits_script_path`
  - `packages/loom-benchmarks/tests/test_aime_adapter.py::test_aime_checker_extracts_last_integer`
- **Evidence:** historical archive issue [carinrc/loom#540](https://github.com/carinrc/loom/issues/540) closed with paired evidence; narrative and per-trial files are in [`benchmark-score-alignment-layer3.md`](benchmark-score-alignment-layer3.md#aime--paired-loom-aime-24--aime-25-vs-harbor-aime-on-claude-haiku-4-5).

### gpqa

- **Harbor adapter:** [`adapters/gpqa-diamond`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/gpqa-diamond) — covers the Diamond subset (198 tasks) only.
- **Published Harbor baseline:** codex + gpt-5.2, 3 trials, 198 Diamond tasks — Harbor 87.21% ± 0.34 vs. `XuandongZhao/gpqa-harbor-adapter` original 87.88% ± 0.58.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/gpqa.py` — currently emits the **Extended** subset.
- **Subset note:** Harbor ships Diamond only, while Loom's primary `gpqa` score semantics cover the 546-task Extended slate. The paired evidence uses Loom's `gpqa-diamond` sibling adapter on the same 198 Diamond tasks; exact-letter verifier semantics are identical across Diamond and Extended, so the Diamond evidence validates the verifier path. An Extended-vs-Extended paired run would be a future entry.
- **Layer 2 status:** `paired_validated` on GPQA Diamond.
- **Paired result:** Loom `litellm` + `claude-haiku-4-5` scored 101/198 (51.01%); Harbor `terminus-2` + the same model scored 115/198 (58.08%). The +7.07 pp Harbor delta is attributed to agent-runtime differences, not verifier divergence.
- **Replay tests (verifier semantics):**
  - `packages/loom-benchmarks/tests/test_gpqa_adapter.py::test_gpqa_verifier_scores_correct_letter`
- **Evidence:** historical archive issue [carinrc/loom#541](https://github.com/carinrc/loom/issues/541) closed with paired evidence; narrative and per-trial files are in [`benchmark-score-alignment-layer3.md`](benchmark-score-alignment-layer3.md#gpqa-diamond--paired-loom-vs-harbor-on-claude-haiku-4-5).

### livecodebench

- **Harbor adapter:** [`adapters/livecodebench`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/livecodebench).
- **Published Harbor baselines:**
  - terminus-2 + gpt-5-mini, 4 trials, 100 release_v6 tasks — TB adapter 76.50% ± 0.50 (Harbor n/a) vs. `audreycs/terminal-bench` original 77.25% ± 0.48.
  - claude-code@2.0.32 + claude-haiku-4-5, 4 trials, 100 release_v6 tasks — Harbor 53.25% ± 0.95 vs. TB adapter 54.50% ± 1.50.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/livecodebench.py`.
- **Layer 2 status:** `pending_paired_run`. Match Harbor's claude-haiku-4-5 baseline for Stage B.
- **Replay tests (verifier semantics):**
  - `packages/loom-benchmarks/tests/test_livecodebench_adapter.py::test_livecodebench_solution_passes_subprocess_run`
  - `packages/loom-benchmarks/tests/test_livecodebench_adapter.py::test_livecodebench_decodes_compressed_private_cases`
  - `packages/loom-benchmarks/tests/test_livecodebench_adapter.py::test_livecodebench_functional_cases_call_solution_method`
- **Stage B paired run:** tracked in canonical [#21](https://github.com/qianyi-sun/loom/issues/21); old [carinrc/loom#542](https://github.com/carinrc/loom/issues/542) is archive provenance only.

### swe-bench-verified

- **Harbor adapter:** [`adapters/swebench`](https://github.com/harbor-framework/harbor/tree/2ead3f1f2462f6f7260aca5ef2377cd7e309ff06/adapters/swebench).
- **Published Harbor baselines:**
  - terminus-2 + Claude-Sonnet-4-5, 1 run, 500 tasks — Harbor 68.6% vs. TB adapter 70.0%.
  - mini-swe-agent@2.1.0 + gpt-5-mini, 3 runs, 499 comparable tasks (Daytona) — Harbor 54.5% ± 0.7 vs. SWE-Bench leaderboard 56.3%.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/swe_bench_verified.py` — emits the upstream SWE-Bench harness (`solve.sh` applies the canonical patch; `tests/test_swebench.py` shells out to `pytest -x -q` against the union of `FAIL_TO_PASS` + `PASS_TO_PASS` node ids) inside the per-instance `swebench/sweb.eval.x86_64.<slug>` Docker image.
- **Layer 2 status:** `pending_paired_run`. Match Harbor's Claude-Sonnet-4-5 or gpt-5-mini baseline for Stage B.
- **Replay tests (verifier semantics):**
  - `packages/loom-benchmarks/tests/test_swe_bench_verified_adapter.py::test_convert_emits_valid_task`
  - `packages/loom-benchmarks/tests/test_swe_bench_verified_adapter.py::test_empty_test_node_lists_emit_script_verifier_reward_zero` (guards the #388 false-positive: empty node-id lists must NOT silently pass)
  - `packages/loom-benchmarks/tests/test_swe_bench_verified_adapter.py::test_image_slug_replaces_double_underscore` (per-instance image-name encoding pins the runtime to the upstream eval image)
- **Known limitation:** no CI test pulls the multi-GB `swebench/sweb.eval.x86_64.<slug>` image to execute `solve.sh` + pytest end-to-end. Image size × image count (500) makes per-instance CI execution impractical. Mitigation: operator smoke — submit an oracle batch with N=5 SWE-Bench Verified instances and verify all 5 reach `state=succeeded` with the bundled patch.
- **Stage B paired run:** tracked in canonical [#20](https://github.com/qianyi-sun/loom/issues/20); old [carinrc/loom#543](https://github.com/carinrc/loom/issues/543) is archive provenance only.

## Benchmarks without a Harbor adapter (upstream-canonical equivalence)

These seven benchmarks have no parity target in
`harbor-framework/harbor` at the pinned commit. Parity therefore
collapses to the **upstream canonical** evaluator — the official
paper/leaderboard/test harness — and Loom's verifier IS that
canonical scorer by construction. Equivalence is proven by replay,
not by paired runtime comparison.

### humaneval

- **Closest Harbor variant:** `adapters/humanevalfix` (HumanEval+ bugfix variant). Different task set + prompts, NOT a parity target.
- **Canonical parity target:** OpenAI HumanEval `check(candidate)`.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/humaneval.py` — emits the upstream `check(candidate)` function verbatim as a pytest-discoverable test next to the bundled `_reference.py`.
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmarks/tests/test_humaneval_adapter.py::test_convert_instance_solution_runs_after_oracle_copy` (bundled reference passes the upstream check, reward 1.0)
  - `packages/loom-benchmarks/tests/test_humaneval_adapter.py::test_convert_instance_pytest_fails_without_oracle_copy` (stub solution fails, guarding the #388 false-positive class)
- **Verdict:** credible for user-facing reporting.

### mbpp

- **Closest Harbor variant:** none.
- **Canonical parity target:** Google sanitized-MBPP test strings under pytest.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/mbpp.py` — writes the upstream sanitized-MBPP test strings verbatim into the bundled `tests/test_mbpp_*.py` files; verifier is `pytest` running those tests.
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmarks/tests/test_mbpp_adapter.py::test_mbpp_solution_runs_against_tests_after_oracle_copy` (bundled reference scores reward 1.0)
  - `packages/loom-benchmarks/tests/test_mbpp_adapter.py::test_mbpp_solution_fails_pytest_when_stub_not_replaced` (stub fails, guarding the #388 false-positive class)
- **Verdict:** credible for user-facing reporting.

### math-500

- **Closest Harbor variants:** `adapters/aime`, `adapters/ineqmath`, `adapters/omnimath` — all cover different task sets, none is a MATH-500 parity target.
- **Canonical parity target:** `HuggingFaceH4/MATH-500` boxed-answer equivalence (inherited from the original Hendrycks MATH evaluator).
- **Loom adapter:** `MATH500Adapter` in `packages/loom-benchmarks/loom_benchmarks/adapters/hendrycks_math.py` (inherits from `HendrycksMATHAdapter`). Script verifier extracts the boxed final answer from `final_answer.txt` and compares via the boxed-answer math-equivalence routine (`\frac{1}{3}` ≡ `1/3`, etc.) that the MATH paper uses.
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmarks/tests/test_hendrycks_math_adapter.py::test_math500_lists_public_500_problem_test_split` (pinned `@6e4ed1a` upstream revision matches the canonical 500-row test set)
  - `packages/loom-benchmarks/tests/test_hendrycks_math_adapter.py::test_math500_convert_writes_math500_task_id`
  - `packages/loom-benchmarks/tests/test_hendrycks_math_adapter.py::test_hendrycks_math_verifier_accepts_equivalent_boxed_output`
  - `packages/loom-benchmarks/tests/test_hendrycks_math_adapter.py::test_hendrycks_math_extracts_nested_boxed_answer`
- **Verdict:** credible for user-facing reporting.

### mmlu-pro

- **Closest Harbor variant:** `adapters/mmmlu` (M-MMLU multilingual variant). Different question pool with multilingual prompts, NOT a parity target.
- **Canonical parity target:** TIGER-Lab MMLU-Pro exact-letter match against the dataset row's canonical answer.
- **Loom adapter:** `packages/loom-benchmarks/loom_benchmarks/adapters/mmlu_pro.py` — script verifier extracts the last standalone letter from `final_answer.txt` and compares to the dataset row's `answer` field.
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmarks/tests/test_mmlu_pro_adapter.py::test_mmlu_pro_verifier_scores_last_standalone_letter` (pins the extraction rule against multi-letter reasoning bodies — e.g. "Reasoning mentions A. Final answer: D" → D)
- **Verdict:** credible for user-facing reporting.

### terminal-bench-2

- **Closest Harbor variant:** none — Terminal-Bench 2 is Harbor's host framework, not an adapted external benchmark. Harbor's `adapters/` directory ships nothing for TB-2.
- **Canonical parity target:** upstream Terminal-Bench 2 (laude-institute) test runner.
- **Loom adapter:** `packages/loom-benchmark-terminal-bench-2/` — emits a verifier shim that wraps the upstream TB-2 `tests/` directory verbatim (`TEST_DIR` defaults to `/app/environment/tb2-tests`).
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmark-terminal-bench-2/tests/test_adapter_convert_instance.py::test_convert_copies_reference_solution_for_oracle_smoke`
  - `packages/loom-benchmark-terminal-bench-2/tests/test_adapter_convert_instance.py::test_convert_renders_solution_yaml_for_oracle_smoke` (the YAML-solution oracle path EXECUTES end-to-end in CI via `subprocess.run(['bash', solve.sh], check=True)` plus an output-file assertion — stronger live-runtime evidence than the other by-construction benchmarks here)
  - `packages/loom-benchmark-terminal-bench-2/tests/test_adapter_convert_instance.py::test_generated_verifier_shim_emits_loom_verifier_result`
  - `packages/loom-benchmark-terminal-bench-2/tests/test_adapter_convert_instance.py::test_convert_writes_verifier_shim`
- **Verdict:** credible for user-facing reporting.

### skillflow

- **Closest Harbor variant:** none — SkillFlow is a Loom-supported external benchmark, not in Harbor's catalog.
- **Canonical parity target:** the upstream SkillFlow task bundle's pre-baked solution + pytest tests.
- **Loom adapter:** passes the upstream task bundle through unchanged; the bundled solution + tests run under pytest end-to-end.
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmarks/tests/test_skillflow_and_skilllearnbench_adapter.py::test_skill_solution_runs[SkillFlowAdapter-skillflow]`
- **Verdict:** credible for user-facing reporting.

### skilllearnbench

- **Closest Harbor variant:** none — SkillLearnBench is a Loom-supported external benchmark, not in Harbor's catalog.
- **Canonical parity target:** the upstream SkillLearnBench task bundle (same construction as SkillFlow).
- **Loom adapter:** same adapter family as SkillFlow; passes the upstream bundle through unchanged.
- **Parity kind:** `upstream_canonical_by_construction`.
- **Replay tests:**
  - `packages/loom-benchmarks/tests/test_skillflow_and_skilllearnbench_adapter.py::test_skill_solution_runs[SkillLearnBenchAdapter-skilllearnbench]`
- **Verdict:** credible for user-facing reporting.
