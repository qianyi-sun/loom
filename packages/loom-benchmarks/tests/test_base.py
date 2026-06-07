"""BenchmarkAdapter Protocol + value type contract (Plan 14 Task 2)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, is_dataclass

import pytest
from loom_benchmarks.base import (
    BenchmarkAdapter,
    BenchmarkInstance,
    ConvertedTask,
    UpstreamSource,
)


def test_upstream_source_is_frozen_dataclass() -> None:
    src = UpstreamSource(
        kind="huggingface", locator="openai_humaneval", revision="abc",
    )
    assert is_dataclass(src)
    with pytest.raises(FrozenInstanceError):
        src.revision = "xyz"  # type: ignore[misc]


def test_benchmark_instance_carries_raw() -> None:
    inst = BenchmarkInstance(
        instance_id="HumanEval/0", split="test", raw={"prompt": "p"},
    )
    assert inst.raw["prompt"] == "p"


def test_converted_task_warnings_is_tuple() -> None:
    ct = ConvertedTask(
        task_id="t", checksum="cs", license_spdx="MIT", warnings=(),
    )
    assert ct.warnings == ()


def test_protocol_has_required_attrs_and_methods() -> None:
    # Protocol attributes live in __annotations__, not dir().
    assert "name" in BenchmarkAdapter.__annotations__
    assert "display_name" in BenchmarkAdapter.__annotations__
    assert "upstream_source" in BenchmarkAdapter.__annotations__
    assert "license_spdx" in BenchmarkAdapter.__annotations__
    assert "splits" in BenchmarkAdapter.__annotations__
    assert hasattr(BenchmarkAdapter, "list_instances")
    assert hasattr(BenchmarkAdapter, "convert_instance")
