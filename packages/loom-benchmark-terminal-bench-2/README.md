# loom-benchmark-terminal-bench-2

Canonical Terminal-Bench-2.0 adapter for the Loom evaluation runtime.

Pinned upstream: terminal-bench-core v0.1.1
(commit `91e10457b5410f16c44364da1a34cb6de8c488a5`).
The pinned official task set contains 86 tasks.

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

For platform onboarding, publish and register the full pinned task set:

```bash
loom datasets publish terminal-bench-2 --hf-org "$LOOM_HF_ORG"
loom datasets register terminal-bench-2 --hf-org "$LOOM_HF_ORG" \
  --db-url "$LOOM_DB_URL"
loom datasets audit terminal-bench-2 --db-url "$LOOM_DB_URL"
```

Converted TB-2 tasks set `[environment].workdir = "/app"` because upstream
instructions, tests, and Docker images use `/app` as the terminal workspace.
The adapter stages each upstream Docker client build context under
`.loom-build/client`, which service-mode workers use to build a deterministic
task image without uploading build-only assets such as `protected/` into the
agent workspace. It materializes the upstream `tests/` tree under
`/app/environment/tb2-tests`, sets `TEST_DIR` for the primary container, and
uses a script verifier at `/app/verifier/run.sh` that executes upstream
`run-tests.sh` with `bash` and emits Loom `VerifierResult` JSON through
`LOOM_VERIFIER_OUTPUT`.

For deterministic oracle smokes, the adapter stages upstream reference
solutions under `solution/`. TB-2 tasks may ship either `solution.sh` or
`solution.yaml`; Loom wraps both into `solution/solve.sh` so the generic oracle
can run them. The wrapper is best-effort and exits `0`, matching upstream
Terminal-Bench semantics where the verifier, not the reference command exit
code, determines the task reward.

Three pinned tasks require auxiliary services. Their compose side services are
represented as `environment.sidecars` and are started by Docker-backed workers
on the same per-trial network as the primary sandbox container:
`security-vulhub-minio`, `simple-sheets-put`, and `simple-web-scraper`.

## License

Apache-2.0 (mirrors TB-2 upstream).
