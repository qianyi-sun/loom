# Benchmark score contract

Loom's supported benchmark catalog has a model-independent scoring contract:
given the same final answer, patch, or task artifact, Loom must assign the same
task-level reward and aggregate metric semantics as the declared canonical
reference.

- **[Layer 1 contract](layer-1.md)** explains the fields and update rules.
- **[Machine-readable manifest](manifest.json)** contains the supported
  benchmark set, canonical references, reward semantics, and replay cases.

Validate the contract with:

```bash
uv run --no-sync python scripts/benchmark_score_alignment_gate.py manifest \
  --manifest docs/score-alignment/manifest.json
```

Live-run comparisons and dated adapter reports are evidence, not current
product behavior; preserved reports live in
[`../../archive/docs/score-alignment/`](../../archive/docs/score-alignment/).
