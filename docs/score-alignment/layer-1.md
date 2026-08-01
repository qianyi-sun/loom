# Benchmark Score Alignment

This document defines the Layer 1 score-credibility contract for Loom's
benchmark catalog. #32 owns this post-v1 alignment program.

Layer 1 is model-independent. It answers: given the same final answer, patch,
or task artifact, does Loom assign the same task-level reward and aggregate
metric semantics as the canonical reference?

Layer 2 is end-to-end. It runs Loom and Harbor, or Loom and an upstream
canonical runner, with matched benchmark-specific configs and compares
task-level rewards plus aggregate scores. Layer 2 belongs in run evidence, not
in the static manifest.

## Manifest

The machine-readable contract lives in
[`docs/score-alignment/manifest.json`](manifest.json). Every benchmark that
makes a user-facing score-credibility claim must have:

- a canonical reference source;
- Harbor support status and parity decision;
- score semantics: task set, denominator, task reward, aggregation, displayed
  metric, and partial-credit rules;
- at least one same-output replay or golden-case definition for Layer 1.

Run the local gate before accepting a benchmark as score-credible:

```bash
python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/score-alignment/manifest.json
```

The gate intentionally does not call model-provider APIs. It validates that the
score-alignment evidence contract is complete enough to drive replay against
Harbor or the upstream canonical evaluator.

## Catalog score-contract scope

The table is broader than first-production native support. #715 currently owns
the v1.0 platform-valid execution gates for canonical Terminal-Bench 2.1 rev 6
and SkillLearnBench. Other rows remain post-v1 score-alignment work unless they
are explicitly promoted into #715.

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
| `terminal-bench-2` | Terminal-Bench 2.1 Harbor Hub revision 6 | accuracy / mean task reward | Locked native rev-6 verifier contract |

## Evidence Rules

Layer 1 evidence should use identical outputs on both sides of the comparison.
Examples:

- final answer replay for AIME, GPQA, MATH-500, and MMLU-Pro;
- code artifact replay for HumanEval, MBPP, and LiveCodeBench;
- patch replay for SWE-Bench Verified;
- task artifact replay for SkillFlow, SkillLearnBench, and the immutable
  Terminal-Bench 2.1 rev-6 physical profile.
- effective request-parameter evidence for SkillLearnBench Codex alignment reports,
  generated with
  `python scripts/alignment/skilllearnbench_effective_params.py` from redacted
  official-plan and Loom debug JSON. This records computed official
  `extra_flags`, whether the selected agent template consumed them, sanitized
  Loom `trial_config.request_params`, observed gateway/provider request params,
  and an explicit default-vs-explicit alignment classification.

If live model outputs differ during Layer 2, replay one side's output through
both verifier paths before calling the score delta a Loom scoring mismatch.
Layer 2 parity baselines must also exclude terminal model-backed trials whose
API/debug evidence reports `llm_evidence_status=no_calls_invalid` or
`partial_no_calls` unless a supplemental retry succeeds and records
`calls_observed`. In particular, `no_call_reason=codex_high_demand_no_call`
means Codex exited before any Loom Gateway request, so the reward row is not
clean model/provider evidence and cannot satisfy #32 score-alignment or #85
request-parameter audit baselines by itself.

For SkillLearnBench, deterministic same-artifact replay and live agent
comparison have different acceptance semantics:

- replaying one frozen output/artifact set through Loom and upstream verifier
  paths must produce identical task rewards and aggregate semantics;
- matched live Codex/model runs are statistical evidence and must classify
  platform failures, runtime failures, artifact-production gaps, verifier
  disagreements, and expected model-output variance separately;
- architecture coverage is an independent operator-only #49/#715 gate and is
  never injected into a normal user evaluation batch.

The manifest may record `harbor_support.status="unknown"` while support is not
confirmed. That is intentionally explicit: the parity target is the upstream
canonical evaluator until Harbor support is proven.
