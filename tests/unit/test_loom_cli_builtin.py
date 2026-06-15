"""builtin.py reads entry_points(group='loom.benchmarks')."""

from __future__ import annotations

import pytest

from loom_cli.builtin import load_builtin_entries


def test_load_returns_all_shipped_adapters() -> None:
    """PR-2 (per-year AIME split): aime ships as aime-22/aime-23/aime-24/
    aime-25 — one slug per exam year so users can pick AIME-24 / AIME-25
    / both in a single click. `swe-bench-verified` restored as a peer
    of `swe-bench` and `swe-bench-multimodal`. Floor bumps to 14."""
    entries = load_builtin_entries()
    slugs = {e.slug for e in entries}
    assert "humaneval" in slugs
    assert "aime-22" in slugs
    assert "aime-23" in slugs
    assert "aime-24" in slugs
    assert "aime-25" in slugs
    assert "swe-bench" in slugs
    assert "swe-bench-verified" in slugs
    assert "swe-bench-multimodal" in slugs
    assert len(entries) >= 14


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
