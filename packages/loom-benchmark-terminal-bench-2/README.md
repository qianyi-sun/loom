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

For platform onboarding or CI smoke, import a small slice first:

```bash
loom datasets import terminal-bench-2 --instance-id hello-world ...
loom datasets audit terminal-bench-2 --db-url "$LOOM_DB_URL"
```

Converted TB-2 tasks set `[environment].workdir = "/app"` because upstream
instructions, tests, and Docker images use `/app` as the terminal workspace.
The adapter materializes the upstream `tests/` tree under
`/app/environment/tb2-tests` and uses a script verifier at
`/app/verifier/run.sh` that executes upstream `run-tests.sh` with `bash` and
emits Loom `VerifierResult` JSON through `LOOM_VERIFIER_OUTPUT`.

## License

Apache-2.0 (mirrors TB-2 upstream).
