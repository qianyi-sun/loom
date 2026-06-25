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

## Remaining work — Batches 3..N

3 v1.0 benchmarks remain. Each is genuinely blocked rather than
"more of the same":

- **`math-500`** — manifest lists it, but Loom has no adapter
  module. The benchmark cannot run end-to-end, so there's nothing
  to validate. Tracked as a separate "missing-adapter" issue (see
  PR description for batch 2). Layer 2 should run once the
  adapter ships.
- **`swe-bench-verified`** — patch-replay against the official
  SWE-Bench harness is substantial new test infrastructure. The
  parity story is replay-validated (apply a known-good patch,
  observe pass/fail via the official `swebench` evaluator). Will
  be its own batch.
- **`terminal-bench-2`** — TB2 evaluator integration is in flight
  on the `codex/issue426-tb2-followup` branch under #426. Layer 2
  for TB2 lands cleanly once that work merges and there's a
  stable replay surface.

Each subsequent batch should follow the same shape: confirm Harbor
support (almost certainly `not_supported`), enumerate the replay
tests that prove Loom's verifier matches the canonical scorer,
record any deltas, and append a section here.
