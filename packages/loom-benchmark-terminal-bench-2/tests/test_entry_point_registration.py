"""Entry-point discovery finds the TB-2 adapter once the package is
installed in editable mode. Plan 24 consumes this via importlib.metadata
to populate `loom datasets list`.
"""

from __future__ import annotations

from importlib.metadata import entry_points


def test_terminal_bench_2_entry_point_registered() -> None:
    eps = entry_points(group="loom.benchmarks")
    by_name = {ep.name: ep for ep in eps}
    assert "terminal-bench-2" in by_name, sorted(by_name)
    loaded = by_name["terminal-bench-2"].load()
    assert loaded.name == "terminal-bench-2"
    assert loaded.license_spdx == "Apache-2.0"
