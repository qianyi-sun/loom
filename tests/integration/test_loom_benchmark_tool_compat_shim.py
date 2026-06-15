"""Plan 14's `loom_benchmark_tool list` keeps working: REGISTRY
now reads entry-points but presents the same dict surface."""

from __future__ import annotations

from loom_benchmarks.registry import REGISTRY

from loom_benchmark_tool.list_cmd import run_list


def test_registry_exposes_all_shipped_adapters_via_entry_points() -> None:
    """PR-2 (per-year AIME): aime ships as aime-22/aime-23/aime-24/
    aime-25, all under series=aime. swe-bench-verified restored as a
    sibling of swe-bench + swe-bench-multimodal. Floor bumps to ≥ 14."""
    assert "humaneval" in REGISTRY
    assert "swe-bench" in REGISTRY
    assert "swe-bench-verified" in REGISTRY
    assert "swe-bench-multimodal" in REGISTRY
    for year_slug in ("aime-22", "aime-23", "aime-24", "aime-25"):
        assert year_slug in REGISTRY
    assert REGISTRY["humaneval"].display_name == "HumanEval"
    assert len(REGISTRY) >= 14


def test_run_list_still_prints_each_adapter() -> None:
    out = run_list()
    assert "humaneval" in out
    assert "HumanEval" in out
    assert "MIT" in out
    assert out.count("\n") + 1 >= 13


def test_registry_is_iterable_like_a_dict() -> None:
    slugs = sorted(REGISTRY)
    assert "humaneval" in slugs
    # PR-1 (series): slug `aime` is now `aime-22` so
    # year-specific siblings (aime-25) can coexist in the series.
    assert "aime-22" in slugs


def test_registry_copy_returns_plain_dict() -> None:
    snap = REGISTRY.copy()
    assert isinstance(snap, dict)
    assert "humaneval" in snap
    assert len(snap) >= 13
    # Mutating the snapshot does not touch REGISTRY.
    snap["humaneval"] = "not-an-adapter"  # type: ignore[assignment]
    assert REGISTRY["humaneval"].display_name == "HumanEval"
