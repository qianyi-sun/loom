"""Name → adapter instance map. v1 slate (HumanEval shipped Plan 14;
remaining 11 ship Plan 15)."""

from __future__ import annotations

from loom_benchmarks.adapters.bfcl import BFCLAdapter
from loom_benchmarks.adapters.gaia import GAIAAdapter
from loom_benchmarks.adapters.humaneval import HumanEvalAdapter
from loom_benchmarks.adapters.livecodebench import LiveCodeBenchAdapter
from loom_benchmarks.adapters.mbpp import MBPPAdapter
from loom_benchmarks.adapters.osworld import OSWorldAdapter
from loom_benchmarks.adapters.swe_bench import SWEBenchAdapter
from loom_benchmarks.adapters.swe_bench_multimodal import SWEBenchMultimodalAdapter
from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.adapters.webarena import WebArenaAdapter
from loom_benchmarks.base import BenchmarkAdapter

REGISTRY: dict[str, BenchmarkAdapter] = {
    "humaneval": HumanEvalAdapter(),
    "swe-bench-verified": SWEBenchVerifiedAdapter(),
    "swe-bench": SWEBenchAdapter(),
    "swe-bench-multimodal": SWEBenchMultimodalAdapter(),
    "osworld": OSWorldAdapter(),
    "webarena": WebArenaAdapter(),
    "mbpp": MBPPAdapter(),
    "bfcl": BFCLAdapter(),
    "gaia": GAIAAdapter(),
    "livecodebench": LiveCodeBenchAdapter(),
}
