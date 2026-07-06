# Benchmark Score Alignment

This document defines the Layer 1 score-credibility gate for the v1.0 benchmark
support set.

Layer 1 is model-independent. It answers: given the same final answer, patch,
or task artifact, does Loom assign the same task-level reward and aggregate
metric semantics as the canonical reference?

Layer 2 is end-to-end. It runs Loom and Harbor, or Loom and an upstream
canonical runner, with matched benchmark-specific configs and compares
task-level rewards plus aggregate scores. Layer 2 belongs in run evidence, not
in the static manifest.

## Manifest

The machine-readable contract lives in
[`docs/benchmark-score-alignment.json`](benchmark-score-alignment.json). Every
v1.0-supported benchmark must have:

- a canonical reference source;
- Harbor support status and parity decision;
- score semantics: task set, denominator, task reward, aggregation, displayed
  metric, and partial-credit rules;
- at least one same-output replay or golden-case definition for Layer 1.

Run the local gate before accepting a benchmark as score-credible:

```bash
python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/benchmark-score-alignment.json
```

The gate intentionally does not call model-provider APIs. It validates that the
score-alignment evidence contract is complete enough to drive replay against
Harbor or the upstream canonical evaluator.

## Current v1.0 Scope

| Benchmark | Canonical reference | Displayed metric | Layer 1 parity target |
|---|---|---|---|
| `aime-24` | AI-MO AIME 2024 rows | accuracy | Upstream exact-integer scorer unless Harbor support is confirmed |
| `aime-25` | MathArena AIME 2025 I rows | accuracy | Upstream exact-integer scorer unless Harbor support is confirmed |
| `gpqa` | GPQA Extended | accuracy | Upstream answer-key scorer unless Harbor support is confirmed |
| `math-500` | MATH-500 | accuracy | Canonical final-answer scorer unless Harbor support is confirmed |
| `humaneval` | OpenAI HumanEval | pass@1 accuracy | Harbor if supported, otherwise OpenAI HumanEval harness |
| `livecodebench` | LiveCodeBench code generation lite | pass@1 accuracy | Upstream LiveCodeBench evaluator unless Harbor support is confirmed |
| `mbpp` | Google MBPP sanitized | pass@1 accuracy | Harbor if supported, otherwise MBPP canonical tests |
| `mmlu-pro` | TIGER-Lab MMLU-Pro | accuracy | Upstream answer-key scorer unless Harbor support is confirmed |
| `skillflow` | SkillFlow task bundle | mean task reward | Upstream SkillFlow verifier unless Harbor support is confirmed |
| `skilllearnbench` | SkillLearnBench | mean task reward | Upstream SkillLearnBench verifier unless Harbor support is confirmed |
| `swe-bench-verified` | SWE-Bench Verified | resolved rate | Harbor if supported, otherwise official SWE-Bench harness |
| `terminal-bench-2` | Terminal-Bench 2.0 | accuracy / mean task reward | Upstream Terminal-Bench evaluator unless Harbor support is confirmed |

## Evidence Rules

Layer 1 evidence should use identical outputs on both sides of the comparison.
Examples:

- final answer replay for AIME, GPQA, MATH-500, and MMLU-Pro;
- code artifact replay for HumanEval, MBPP, and LiveCodeBench;
- patch replay for SWE-Bench Verified;
- task artifact replay for SkillFlow, SkillLearnBench, and Terminal-Bench 2.0.

If live model outputs differ during Layer 2, replay one side's output through
both verifier paths before calling the score delta a Loom scoring mismatch.
Layer 2 parity baselines must also exclude terminal model-backed trials whose
API/debug evidence reports `llm_evidence_status=no_calls_invalid` or
`partial_no_calls` unless a supplemental retry succeeds and records
`calls_observed`. In particular, `no_call_reason=codex_high_demand_no_call`
means Codex exited before any Loom Gateway request, so the reward row is not
clean model/provider evidence and cannot satisfy #6 score-alignment or #85
request-parameter audit baselines by itself.

The manifest may record `harbor_support.status="unknown"` while support is not
confirmed. That is intentionally explicit: the parity target is the upstream
canonical evaluator until Harbor support is proven.
