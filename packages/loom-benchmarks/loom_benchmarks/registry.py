"""Name → adapter instance map. Plan 15 adds the rest of the slate."""

from __future__ import annotations

from loom_benchmarks.adapters.humaneval import HumanEvalAdapter
from loom_benchmarks.base import BenchmarkAdapter

REGISTRY: dict[str, BenchmarkAdapter] = {
    "humaneval": HumanEvalAdapter(),
}
