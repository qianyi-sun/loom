"""Terminal-Bench-2.0 adapter — module-level `adapter` instance for entry-point discovery."""

from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter
from loom_benchmark_terminal_bench_2.upstream import UPSTREAM_REVISION

adapter = TerminalBench2Adapter()

__all__ = ["UPSTREAM_REVISION", "TerminalBench2Adapter", "adapter"]
