"""Family-key extractor plugins."""

from __future__ import annotations

from dataclasses import dataclass

from loom.family_run.keys import InstanceIdPrefixExtractor


@dataclass
class _Task:
    id: str
    tags: dict[str, str] | None = None


def test_prefix_default_depth_1():
    ext = InstanceIdPrefixExtractor()
    key = ext.key_for(_Task(id="skillflow/Compensation-Scenario-Modeling/task-1"))
    assert key == "skillflow"


def test_prefix_depth_2():
    ext = InstanceIdPrefixExtractor()
    ext.default_params = {"depth": 2}
    key = ext.key_for(_Task(id="skillflow/Compensation-Scenario-Modeling/task-1"))
    assert key == "skillflow/Compensation-Scenario-Modeling"


def test_prefix_depth_greater_than_segments_returns_full_id():
    ext = InstanceIdPrefixExtractor()
    ext.default_params = {"depth": 10}
    key = ext.key_for(_Task(id="a/b"))
    assert key == "a/b"
