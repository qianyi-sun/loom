# Changelog

## [Unreleased]

### Added
- `loom-benchmark-terminal-bench-2` sibling package ships the canonical
  Terminal-Bench-2.0 adapter via the `loom.benchmarks` entry point.
  Pinned upstream commit `91e10457b5410f16c44364da1a34cb6de8c488a5`
  (terminal-bench-core v0.1.1). See plan
  `docs/plans/2026-06-08-loom-plan-25-terminal-bench-2-adapter.md`.

### Notes
- BenchmarkAdapter Protocol unchanged. Plan 25 deliberately ships
  with no Protocol extension — the TB-2 task schema fits the existing
  surface. Future TB-2 versions that require new primitives must
  extend the Protocol additively in a follow-up plan.
