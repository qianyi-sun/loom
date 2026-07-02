"""Unit tests for upstream row iterators (#242 sub-plan 3)."""

from __future__ import annotations

from loom.models.taskset import TaskSetSource
from loom.taskset.upstream_rows import iter_upstream_rows


def test_jsonl_inline_yields_rows() -> None:
    source = TaskSetSource(
        type="jsonl-inline",
        locator='{"id":"1","question":"q1"}\n{"id":"2","question":"q2"}\n',
    )
    rows = list(iter_upstream_rows(source, cache_root=__import__("pathlib").Path("/tmp")))
    assert len(rows) == 2
    assert rows[0]["id"] == "1"
