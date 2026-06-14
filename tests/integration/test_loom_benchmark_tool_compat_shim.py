"""Plan 14's `loom_benchmark_tool list` keeps working: REGISTRY
now reads entry-points but presents the same dict surface."""

from __future__ import annotations

from loom_benchmarks.registry import REGISTRY

from loom_benchmark_tool.list_cmd import run_list


def test_registry_exposes_all_shipped_adapters_via_entry_points() -> None:
    """PR-1 (series): swe-bench-verified slug dropped (verified is now
    a tag on swe-bench); aime renamed to aime-aimo-validation with
    aime-2025 as a sibling under the aime series. Floor stays ≥ 13."""
    assert "humaneval" in REGISTRY
    assert "swe-bench" in REGISTRY
    assert "aime-aimo-validation" in REGISTRY
    assert "aime-2025" in REGISTRY
    assert "swe-bench-verified" not in REGISTRY
    assert REGISTRY["humaneval"].display_name == "HumanEval"
    assert len(REGISTRY) >= 13


def test_run_list_still_prints_each_adapter() -> None:
    out = run_list()
    assert "humaneval" in out
    assert "HumanEval" in out
    assert "MIT" in out
    assert out.count("\n") + 1 >= 13


def test_registry_is_iterable_like_a_dict() -> None:
    slugs = sorted(REGISTRY)
    assert "humaneval" in slugs
    # PR-1 (series): slug `aime` is now `aime-aimo-validation` so
    # year-specific siblings (aime-2025) can coexist in the series.
    assert "aime-aimo-validation" in slugs


def test_registry_copy_returns_plain_dict() -> None:
    snap = REGISTRY.copy()
    assert isinstance(snap, dict)
    assert "humaneval" in snap
    assert len(snap) >= 13
    # Mutating the snapshot does not touch REGISTRY.
    snap["humaneval"] = "not-an-adapter"  # type: ignore[assignment]
    assert REGISTRY["humaneval"].display_name == "HumanEval"
