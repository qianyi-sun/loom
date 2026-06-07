# loom-benchmarks

Benchmark adapters for the [Loom](../../README.md) evaluation runtime.
Each adapter pairs a `BenchmarkAdapter` Protocol implementation with the
upstream-source kind (`huggingface`, `git`, `https-tarball`) so the
`loom_benchmark_tool` CLI can fetch, convert, and ingest benchmark
instances into the Loom Postgres + MinIO state.

See `docs/superpowers/specs/2026-06-07-loom-benchmark-integrations-design.md`
for the integration spec; Plan 14 ships this package's core (Protocol,
util, fetchers, HumanEval reference adapter); Plan 15 fills out the
remaining adapters; Plan 16 ships `verify`.
