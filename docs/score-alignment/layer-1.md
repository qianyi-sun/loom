# Benchmark Score Contract

Layer 1 is Loom's model-independent scoring contract. Given the same final
answer, patch, program, or task artifact, Loom must produce the same task-level
reward and aggregate metric semantics as the declared canonical evaluator.

The machine-readable [manifest](manifest.json) covers every benchmark in the
current supported catalog. Each entry declares:

- the exact upstream or Harbor Hub reference and revision;
- whether current Harbor has a matching adapter;
- the task set and denominator;
- task reward, aggregation, displayed metric, and partial-credit semantics;
- at least one identical-output replay case.

Harbor support is not assumed from a similar benchmark name. For example,
Harbor's `gpqa-diamond` adapter is not a matched reference for Loom's GPQA
Extended rows, and `humanevalfix` is not the OpenAI HumanEval contract.

## Current catalog

| Benchmark | Canonical evaluator | Metric |
| --- | --- | --- |
| `aime-24`, `aime-25` | exact normalized integer; Harbor `aime` is a matching replay target | accuracy |
| `gpqa` | GPQA Extended answer key | accuracy |
| `math-500` | pinned MATH-500 answer scorer | accuracy |
| `humaneval` | OpenAI HumanEval tests | pass@1 accuracy |
| `livecodebench` | pinned LiveCodeBench evaluator; Harbor adapter is a matching replay target | pass@1 accuracy |
| `mbpp` | Google MBPP sanitized tests | pass@1 accuracy |
| `mmlu-pro` | pinned MMLU-Pro answer key | accuracy |
| `skillflow`, `skilllearnbench` | task-native upstream-compatible verifier | mean task reward |
| `swe-bench-verified` | official or matching Harbor SWE-Bench verifier | resolved rate |
| `terminal-bench-2@tb2.1-r6` | locked Harbor Hub revision-6 native verifier | mean task reward |
| `terminal-bench-2` | active physical revision-6 profile | mean task reward |

Validate the manifest without provider or live-service access:

```bash
uv run --no-sync python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/score-alignment/manifest.json
```

The gate checks catalog coverage, exact Harbor repository identity and pinned
commit shape, required scoring fields, duplicate/unexpected rows, and replay
case completeness. It does not claim live model-quality parity.

Dated paired runs, score deltas, incident caveats, and provider observations
belong in the archive or external run evidence, not in this current contract.
