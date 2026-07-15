"""Fail-closed task-filter identity checks for release canaries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_ALLOWED_CANARY_FILTER_KEYS = frozenset(
    {
        "benchmark_id",
        "benchmark_ids",
        "license",
        "n",
        "seed",
        "subset_kind",
        "tag_filters",
        "task_ids",
    }
)


def task_filter_targets_only_benchmark(value: Any, benchmark_id: str) -> bool:
    """Return whether a filter can select tasks only from ``benchmark_id``.

    The service resolves benchmark and TaskSet source selectors with OR
    semantics, and ``benchmark_ids`` is a multi-select. Release evidence must
    therefore reject filters that merely mention the target alongside another
    source. Unknown keys also fail closed so future selector semantics cannot
    silently broaden an old gate.
    """

    if not isinstance(value, Mapping) or not benchmark_id:
        return False
    if not set(value).issubset(_ALLOWED_CANARY_FILTER_KEYS):
        return False

    has_singular_benchmark = "benchmark_id" in value
    has_multiple_benchmarks = "benchmark_ids" in value
    if has_singular_benchmark and has_multiple_benchmarks:
        return False

    task_ids = value.get("task_ids")
    has_task_ids = "task_ids" in value
    if has_task_ids and (
        not isinstance(task_ids, list)
        or not task_ids
        or not all(
            isinstance(task_id, str)
            and (task_id == benchmark_id or task_id.startswith(f"{benchmark_id}/"))
            for task_id in task_ids
        )
    ):
        return False

    if has_singular_benchmark:
        return value.get("benchmark_id") == benchmark_id
    if has_multiple_benchmarks:
        return value.get("benchmark_ids") == [benchmark_id]
    return has_task_ids


__all__ = ["task_filter_targets_only_benchmark"]
