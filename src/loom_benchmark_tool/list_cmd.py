"""`python -m loom_benchmark_tool list` — registered benchmark catalog."""

from __future__ import annotations

from loom_benchmarks.registry import REGISTRY


def run_list() -> str:
    lines: list[str] = []
    for name, adapter in sorted(REGISTRY.items()):
        lines.append(
            f"{name:30s} {adapter.display_name:30s} {adapter.license_spdx:12s}",
        )
    return "\n".join(lines)
