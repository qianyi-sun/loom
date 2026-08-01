# Benchmark Score Alignment

This directory is the canonical post-v1 score-alignment contract tracked by
#32. It preserves historical evidence from superseded benchmark-specific
tickets such as #6 without making official-harness parity a v1.0 release gate.

First-production acceptance remains in #715: supported benchmarks must run
through ordinary user-visible surfaces, terminate with diagnosable evidence,
and produce a valid numeric verifier reward. That release requirement is
separate from the deeper canonical-harness comparisons documented here.

## Layers

- **[layer-1.md](layer-1.md)** — model-independent gate. Given the same final
  answer/patch/artifact, does Loom assign the same task-level reward and
  aggregate metric semantics as the canonical reference? Layer 1 is the
  reward-contract manifest for every supported benchmark.
- **[layer-2.md](layer-2.md)** — adapter-level reports. Per-benchmark
  evidence that Loom's parsing, verifier, and reward math match the upstream
  contract.
- **[layer-3.md](layer-3.md)** — matched live-run reports. These quantify
  agreement and variance after pins and effective request settings are aligned;
  they do not require stochastic agent runs to produce identical task outputs.

## Data

- **[manifest.json](manifest.json)** — machine-readable Layer 1 manifest.
  Consumed by `scripts/benchmark_score_alignment_gate.py`.

## When to update

Update Layer 1 whenever a supported benchmark's reward semantics change
(new benchmark added, verifier rewritten, aggregate changed). Preserve frozen
Layer 2/3 evidence, but correct its authority and interpretation when a later
decision supersedes an old release or architecture claim.
