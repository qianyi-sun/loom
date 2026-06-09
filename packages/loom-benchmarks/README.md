# loom-benchmarks

Benchmark adapters for the [Loom](../../README.md) evaluation runtime.
Each adapter pairs a `BenchmarkAdapter` Protocol implementation with the
upstream-source kind (`huggingface`, `git`, `https-tarball`) so the
`loom_benchmark_tool` CLI can fetch, convert, and ingest benchmark
instances into the Loom Postgres + MinIO state.

See `docs/architecture/benchmark-adapter.md` for the framework
reference (Protocol, canonical task layout, fetchers, how to add a
new adapter). 14 adapters ship today.
