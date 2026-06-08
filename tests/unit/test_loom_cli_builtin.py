"""builtin.py reads entry_points(group='loom.benchmarks')."""

from __future__ import annotations

import pytest

from loom_cli.builtin import load_builtin_entries


def test_load_returns_all_thirteen_shipped_adapters() -> None:
    entries = load_builtin_entries()
    slugs = {e.slug for e in entries}
    assert "humaneval" in slugs
    assert "swe-bench-verified" in slugs
    assert "aime" in slugs
    assert len(entries) >= 13


def test_each_entry_has_display_name_and_license() -> None:
    entries = load_builtin_entries()
    he = next(e for e in entries if e.slug == "humaneval")
    assert he.display_name == "HumanEval"
    assert he.license_spdx == "MIT"
    assert he.source == "builtin"


def test_broken_entry_point_does_not_crash_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli import builtin

    class _Boom:
        name = "broken"

        def load(self) -> object:
            raise ImportError("no such module")

    def _fake_eps(group: str) -> list[_Boom]:
        return [_Boom()]

    monkeypatch.setattr(builtin, "_entry_points", _fake_eps)
    out = builtin.load_builtin_entries()
    assert out == []
