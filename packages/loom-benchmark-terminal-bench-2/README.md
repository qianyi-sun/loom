# loom-benchmark-terminal-bench-2

Canonical Terminal-Bench-2.0 adapter for the Loom evaluation runtime.

Pinned upstream: terminal-bench-core v0.1.1
(commit `91e10457b5410f16c44364da1a34cb6de8c488a5`).

## Install

```bash
pip install loom-benchmark-terminal-bench-2
```

`loom datasets list` will then surface `terminal-bench-2` via the
`loom.benchmarks` entry point.

## Run

```bash
loom run --dataset terminal-bench-2 --agent claude-code \
         --tb2-report ./tb2-results.json
```

The native Loom ATIF lands in `./runs/<trial>/atif.json`; the TB-2
canonical result JSON (matching `terminal_bench.harness_models.BenchmarkResults`)
lands at the `--tb2-report` path.

## License

Apache-2.0 (mirrors TB-2 upstream).
