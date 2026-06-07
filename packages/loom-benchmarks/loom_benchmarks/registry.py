"""Name → adapter instance map. v1 slate (HumanEval shipped Plan 14;
remaining 11 ship Plan 15)."""

from __future__ import annotations

from loom_benchmarks.adapters.humaneval import HumanEvalAdapter
from loom_benchmarks.adapters.swe_bench import SWEBenchAdapter
from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import BenchmarkAdapter

REGISTRY: dict[str, BenchmarkAdapter] = {
    "humaneval": HumanEvalAdapter(),
    "swe-bench-verified": SWEBenchVerifiedAdapter(),
    "swe-bench": SWEBenchAdapter(),
}
