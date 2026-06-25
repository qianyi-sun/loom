# Benchmark Score Alignment — Layer 2 Reports

Layer 1 (PR #477) declared the manifest — per-benchmark canonical
reference, score semantics, and replay-case definitions. Layer 2
records the actual alignment evidence: for each benchmark, the
parity decision (Harbor vs. upstream), the replay tests that prove
Loom's verifier matches the canonical scorer, and any observed
deltas or known limitations.

Reports are added per benchmark as Layer 2 evidence lands. This
file is the human-readable narrative; the machine-readable form
lives in `benchmark-score-alignment.json` under each benchmark's
`layer2_evidence` field.

## Harbor support determination

`coder-harbor-cloud` (the platform the gate names as the first-choice
parity target) ships the `voyager` subsystem with bespoke benchmark
tasks (fizzbuzz, hello-world, bugfix-sum, npu-smoke). It does **not**
ship native verifiers for any v1.0 academic benchmark: AIME, GPQA,
MATH-500, HumanEval, MBPP, LiveCodeBench, MMLU-Pro, or SWE-Bench. So
for all 12 v1.0 benchmarks the parity target collapses to **upstream
canonical** — the official paper/leaderboard/test harness for the
benchmark.

This determination is reflected in each completed entry's
`harbor_support.status="not_supported"` with the justification
inline; new entries should follow the same convention until or
unless Harbor ships a verifier for that benchmark.

## Batch 1 (this PR) — mbpp, humaneval, aime-24, aime-25

### mbpp

- **Parity kind:** by construction. Loom's MBPP adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/mbpp.py`)
  writes the upstream sanitized-MBPP test strings verbatim into the
  bundle's `tests/test_mbpp_*.py` files (one pytest file per test
  case). The verifier is `pytest` running those bundled tests. The
  canonical MBPP scorer is also `pytest` running the same tests, so
  the verifier and canonical scorer are the same code path.
- **Replay validation:** two tests in
  `packages/loom-benchmarks/tests/test_mbpp_adapter.py` —
  `test_mbpp_solution_runs_against_tests_after_oracle_copy`
  exercises a known-passing solution (the bundled `_reference.py`),
  asserts pytest returns 0 (reward=1.0). The complementary
  `test_mbpp_solution_fails_pytest_when_stub_not_replaced` proves
  the inverse path: the stub solution must NOT silently pass
  (guards against the #388 false-positive).
- **Known deltas:** none. Loom reports one-output reward as a
  pass@1 surrogate; the manifest's `score_semantics.task_reward`
  documents this explicitly.
- **Verdict:** credible for user-facing reporting.

### humaneval

- **Parity kind:** by construction. Loom's HumanEval adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/humaneval.py`)
  emits the upstream `check(candidate)` function verbatim as a
  pytest-discoverable test next to the bundled `_reference.py`.
- **Replay validation:** two tests in
  `packages/loom-benchmarks/tests/test_humaneval_adapter.py` —
  `test_convert_instance_solution_runs_after_oracle_copy` proves
  the canonical reference passes the bundled check; the inverse
  `test_convert_instance_pytest_fails_without_oracle_copy` proves
  the stub fails the same check.
- **Known deltas:** none. Same one-output / pass@1 convention as
  mbpp.
- **Verdict:** credible for user-facing reporting.

### aime-24

- **Parity kind:** replay-validated. Loom's adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/aime.py`)
  emits a script verifier (`verifier/run.sh` + `verifier/check.py`)
  that extracts the last integer from `final_answer.txt` and
  compares to the canonical `answer` field from the AIME 2024
  dataset row. The canonical scorer is exact integer match against
  the same dataset field.
- **Replay validation:** three tests in
  `packages/loom-benchmarks/tests/test_aime_adapter.py` —
  `test_aime_run_sh_is_self_contained_and_writes_verifier_result`
  (correct answer → reward 1.0 with structured `{got, expected}`),
  `test_aime_checker_rejects_wrong_answer` (wrong answer → reward
  0.0), `test_aime_checker_picks_last_integer` (pins the
  extraction rule against ambiguous final-line shapes).
- **Known deltas:** the extraction rule is "last integer on the
  final line" — well-defined for AIME's single-integer-answer
  convention but does NOT handle boxed-LaTeX, multi-answer rows,
  or non-integer answers. None of those shapes appear in AIME.
- **Verdict:** credible for user-facing reporting.

### aime-25

- **Parity kind:** replay-validated by inheritance. The aime-25
  adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/aime_2025.py`)
  emits the same script-verifier infrastructure as aime-24; the
  integer-extraction logic in `verifier/check.py` is identical.
- **Replay validation:** `test_aime_2025_emits_script_path` proves
  the verifier wiring is shared with aime-22..24; the shared
  extraction tests
  (`test_aime_checker_extracts_last_integer`,
  `test_aime_checker_picks_last_integer`,
  `test_aime_checker_rejects_wrong_answer`) cover the same code
  path the aime-25 task bundle invokes.
- **Known deltas:** same as aime-24.
- **Verdict:** credible for user-facing reporting.

## Batch 2 — gpqa, mmlu-pro, livecodebench, skillflow, skilllearnbench

### gpqa

- **Parity kind:** replay-validated. The adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/gpqa.py`)
  emits a script verifier that extracts the answer letter from
  `final_answer.txt` and compares to the canonical letter stored
  in `answer_key.json` (derived from the GPQA Extended dataset
  row). The canonical scorer is exact letter match against the
  same dataset field.
- **Replay validation:**
  `packages/loom-benchmarks/tests/test_gpqa_adapter.py::test_gpqa_verifier_scores_correct_letter`
  — correct letter → reward `{score: 1.0}`.
- **Known deltas:** extraction rule is "letter after 'Final
  answer:' on the last line"; rejects ambiguous outputs (no
  partial credit, by design).
- **Verdict:** credible for user-facing reporting.

### mmlu-pro

- **Parity kind:** replay-validated. Same shape as gpqa — script
  verifier extracts the last standalone letter and compares to the
  dataset row's `answer` field.
- **Replay validation:**
  `packages/loom-benchmarks/tests/test_mmlu_pro_adapter.py::test_mmlu_pro_verifier_scores_last_standalone_letter`
  — pins the extraction rule against multi-letter reasoning bodies
  (e.g. "Reasoning mentions A. Final answer: D" → D).
- **Known deltas:** same convention as gpqa.
- **Verdict:** credible for user-facing reporting.

### livecodebench

- **Parity kind:** by construction. The adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/livecodebench.py`)
  decodes the compressed IO/functional test cases from the
  upstream dataset row and emits them verbatim as pytest test
  files. Loom's pytest verifier IS the canonical LiveCodeBench
  scorer — same code path.
- **Replay validation:** three tests in
  `packages/loom-benchmarks/tests/test_livecodebench_adapter.py` —
  `test_livecodebench_solution_passes_subprocess_run` (end-to-end
  pass against decoded cases),
  `test_livecodebench_decodes_compressed_private_cases` (guards
  the decode step against upstream-format drift),
  `test_livecodebench_functional_cases_call_solution_method` (the
  functional invocation matches the upstream evaluator's shape).
- **Known deltas:** one-output pass@1 surrogate, same convention
  as mbpp/humaneval.
- **Verdict:** credible for user-facing reporting.

### skillflow

- **Parity kind:** by construction. The adapter passes through
  the upstream SkillFlow task bundle unchanged. The bundled
  pre-baked solution + tests pass under pytest end-to-end. Loom's
  pytest verifier IS the canonical scorer.
- **Replay validation:**
  `packages/loom-benchmarks/tests/test_skillflow_and_skilllearnbench_adapter.py::test_skill_solution_runs[SkillFlowAdapter-skillflow]`
  — bundled solution passes pytest after `convert_instance`.
- **Known deltas:** none for the parity claim; reward semantics
  are the task bundle's own (mean task reward over bundled
  sub-checks).
- **Verdict:** credible for user-facing reporting.

### skilllearnbench

- **Parity kind:** by construction by inheritance — same adapter
  family as skillflow, same parametrized test
  (`[SkillLearnBenchAdapter-skilllearnbench]` variant).
- **Replay validation:** same `test_skill_solution_runs` test,
  parametrized by adapter class.
- **Known deltas:** same as skillflow.
- **Verdict:** credible for user-facing reporting.

## Batch 3 — swe-bench-verified

### swe-bench-verified

- **Parity kind:** by construction. The adapter
  (`packages/loom-benchmarks/loom_benchmarks/adapters/swe_bench_verified.py`)
  emits the official SWE-Bench evaluation harness verbatim:
  - `solve.sh` runs `git apply --3way` of the canonical patch
    inside the per-instance image's `/testbed` checkout.
  - `tests/test_swebench.py` shells out to `pytest -x -q` with the
    union of the upstream `FAIL_TO_PASS` + `PASS_TO_PASS` node ids
    and asserts rc=0.
  - The container image is the upstream
    `swebench/sweb.eval.x86_64.<slug>` per-instance evaluation
    image. Loom's verifier code path IS the upstream harness, so
    parity is by construction — same story as mbpp/humaneval where
    Loom's pytest verifier executes the canonical test code.
- **Replay validation:** three tests in
  `packages/loom-benchmarks/tests/test_swe_bench_verified_adapter.py` —
  `test_convert_emits_valid_task` pins the canonical structure
  (solve.sh + tests + image),
  `test_empty_test_node_lists_emit_script_verifier_reward_zero`
  guards the degenerate path (no test ids → reward=0, not a
  silent pytest-collects-zero "pass"; covers the #388 false-
  positive class for SWE-Bench),
  `test_image_slug_replaces_double_underscore` pins the per-
  instance image-name encoding so the runtime pulls the correct
  upstream eval image.
- **Known limitation — live runtime not unit-tested:** No CI test
  pulls the multi-GB `swebench/sweb.eval.x86_64.<slug>` image to
  execute solve.sh + pytest end-to-end. Image size × image count
  (500 in SWE-Bench Verified) makes per-instance CI execution
  impractical. The construction claim above is the verifier-
  semantics parity proof; live-runtime parity is observed when
  operators submit oracle batches against SWE-Bench Verified and
  the trials reach succeeded with reward 1.0. **Mitigation:**
  operator smoke — submit an oracle batch with N=5 SWE-Bench
  Verified instances and verify all 5 reach state=succeeded with
  the bundled patch. Any verifier-semantics regression would
  surface as reward!=1.0 on the oracle path.
- **Known deltas:** none on verifier semantics. The displayed
  metric (resolved rate = fraction of instances where all node
  ids pass) matches the upstream SWE-Bench-Verified leaderboard
  definition.
- **Verdict:** credible for user-facing reporting, with the
  live-runtime gap noted in operator-facing docs.

## Batch 4 — math-500, terminal-bench-2 (final)

### math-500

- **Parity kind:** replay-validated. The `MATH500Adapter` (in
  `packages/loom-benchmarks/loom_benchmarks/adapters/hendrycks_math.py`,
  inherits from `HendrycksMATHAdapter`) emits a script verifier
  that extracts the boxed final answer from `final_answer.txt`
  and compares it to the canonical `answer` field on the
  `HuggingFaceH4/MATH-500` row via the boxed-answer math-
  equivalence routine the MATH paper uses (`\frac{1}{3}` ≡
  `1/3`, etc.).
- **Replay validation:** four tests in
  `packages/loom-benchmarks/tests/test_hendrycks_math_adapter.py` —
  `test_math500_lists_public_500_problem_test_split` (the pinned
  `@6e4ed1a` upstream revision matches the canonical 500-row
  test set), `test_math500_convert_writes_math500_task_id`
  (task ids namespaced under `math-500/...`),
  `test_hendrycks_math_verifier_accepts_equivalent_boxed_output`
  (shared verifier code path scores equivalent answers as 1.0),
  `test_hendrycks_math_extracts_nested_boxed_answer` (extraction
  handles nested boxed structures and picks the last one).
- **Known deltas:** none. The math-equivalence routine matches
  the upstream MATH paper's `is_equiv` algorithm.
- **Verdict:** credible for user-facing reporting.

### terminal-bench-2

- **Parity kind:** by construction. The TB2 adapter (in
  `packages/loom-benchmark-terminal-bench-2/`) emits a verifier
  shim that wraps the upstream TB2 `tests/` directory verbatim —
  `TEST_DIR` defaults to `/app/environment/tb2-tests` where the
  upstream test suite is staged in the per-task image. Loom's
  verifier code path IS the upstream TB2 test runner.
- **Replay validation:** four tests in
  `packages/loom-benchmark-terminal-bench-2/tests/test_adapter_convert_instance.py` —
  `test_convert_copies_reference_solution_for_oracle_smoke`
  (upstream `reference.sh` is staged + wired into oracle's
  `solve.sh`), `test_convert_renders_solution_yaml_for_oracle_smoke`
  (the YAML-solution oracle path actually EXECUTES end-to-end in
  CI — `subprocess.run(['bash', solve.sh], check=True)` plus
  asserts on the resulting output file; this is stronger
  live-runtime evidence than any other v1.0 benchmark in this
  suite), `test_generated_verifier_shim_emits_loom_verifier_result`
  (shim emits a well-formed VerifierResult JSON), and
  `test_convert_writes_verifier_shim` (TEST_DIR points at the
  upstream test location).
- **Known deltas:** none on verifier semantics. The shim's
  result-shape mapping is well-defined and tested.
- **Verdict:** credible for user-facing reporting. Note: the
  TB2 in-CI oracle execution provides stronger live-runtime
  evidence than any other v1.0 benchmark here.

## Coverage summary — all 12 v1.0 benchmarks Layer 2-validated

| Batch | PR | Benchmarks |
|---|---|---|
| 1 | #499 | aime-24, aime-25, humaneval, mbpp |
| 2 | #513 | gpqa, mmlu-pro, livecodebench, skillflow, skilllearnbench |
| 3 | #514 | swe-bench-verified |
| 4 | (this PR) | math-500, terminal-bench-2 |

All 12 v1.0-supported benchmarks now have:
- `harbor_support.status = "not_supported"` with justification
- `layer2_evidence` citing the adapter test(s) that prove the
  verifier-semantics parity claim
- A `Verdict` section in this document marking the benchmark as
  credible for user-facing reporting

The two known gaps recorded in earlier batches remain:
- **swe-bench-verified live-runtime parity** — no in-CI execution
  of the multi-GB per-instance Docker image. Mitigation: operator
  smoke (oracle batch against N=5 instances).
- **`hendrycks-math`** is cataloged but explicitly non-v1; it's
  the broader MATH dataset, and `math-500` is the v1-scoped
  subset (see codex's #480).
