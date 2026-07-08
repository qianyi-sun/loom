# Benchmark Score Alignment

The score-credibility manifest for Loom's v1.0 benchmark support set. Three
layers of increasing depth, each answering a different question about
whether Loom's numeric task rewards are trustworthy relative to the
canonical reference implementation.

## Layers

- **[layer-1.md](layer-1.md)** — model-independent gate. Given the same final
  answer/patch/artifact, does Loom assign the same task-level reward and
  aggregate metric semantics as the canonical reference? Layer 1 is the
  reward-contract manifest for every supported benchmark.
- **[layer-2.md](layer-2.md)** — adapter-level reports. Per-benchmark
  evidence that Loom's parsing, verifier, and reward math match the upstream
  contract.
- **[layer-3.md](layer-3.md)** — paired-run alignment reports. Direct
  numeric-agreement runs between Loom and Harbor/upstream references on the
  same model + task set, so any residual delta is attributable.

## Data

- **[manifest.json](manifest.json)** — machine-readable Layer 1 manifest.
  Consumed by `scripts/benchmark_score_alignment_gate.py`.

## When to update

Update Layer 1 whenever a supported benchmark's reward semantics change
(new benchmark added, verifier rewritten, aggregate changed). Layer 2 and
Layer 3 append new evidence as it lands; don't rewrite prior reports.
