"""Unit tests for TaskSet instance mapping (#242 sub-plan 3)."""

from __future__ import annotations

import pytest

from loom.taskset.instance_mapping import MappingError, resolve_mapping


def test_resolve_mapping_dotted_paths() -> None:
    row = {"question": "q1", "nested": {"id": "abc"}}
    instance = resolve_mapping(row, {
        "prompt": "row.question",
        "task_id": "row.nested.id",
    })
    assert instance == {"prompt": "q1", "task_id": "abc"}


def test_missing_mapping_path_raises() -> None:
    with pytest.raises(MappingError, match="missing_field"):
        resolve_mapping({"a": 1}, {"missing_field": "row.missing"})
