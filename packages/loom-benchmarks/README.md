# loom-benchmarks

Benchmark adapters for the [Loom](../../README.md) evaluation runtime.
Each adapter pairs a `BenchmarkAdapter` Protocol implementation with the
upstream-source kind (`huggingface`, `git`, `https-tarball`) so the
`loom_benchmark_tool` CLI can fetch, convert, and ingest benchmark
instances into the Loom Postgres + MinIO state.

See `docs/architecture/benchmark-adapter.md` for the framework
reference (Protocol, canonical task layout, fetchers, how to add a
new adapter). 16 adapters ship today.

## BFCL output contract

The BFCL adapter targets the upstream v4 task layout under
`berkeley-function-call-leaderboard/bfcl_eval/data`. It publishes every
JSONL task row with either the matching `possible_answer/` ground truth or
the official relevance/irrelevance call-presence objective. Current upstream
v4 coverage is 4696 tasks.

BFCL tasks ask the selected agent to write `agent_output.json` rather than
free-form prose. Single-turn tasks use:

```json
{"calls": [{"name": "function_name", "arguments": {"arg": "value"}}]}
```

If no function should be called, write `{"calls": []}`. Multi-turn tasks use
`{"turns": [[...turn 1 calls...], [...turn 2 calls...]]}`. The bundled
script verifier is self-contained in each task bundle and scores ordinary
function-call matches, relevance/irrelevance call presence, and multi-turn
call sequences without requiring `/opt/bfcl/evaluator.py` in the sandbox
image.
