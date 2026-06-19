from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom_benchmark_tool.register_cmd import task_config_from_manifest_entry


def _valid_task_config(task_id: str = "fake-bench/task-001") -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": "Fake task"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def test_manifest_entry_with_valid_task_config_returns_config() -> None:
    cfg = _valid_task_config()
    assert task_config_from_manifest_entry({"task_config": cfg}) == cfg


def test_manifest_entry_without_task_config_remains_legacy_placeholder() -> None:
    assert task_config_from_manifest_entry({"task_id": "fake-bench/task-001"}) == {}


def test_manifest_entry_with_invalid_task_config_raises_validation_error() -> None:
    with pytest.raises(ValidationError):
        task_config_from_manifest_entry({"task_config": {"task": {"id": "broken"}}})


def test_manifest_entry_rejects_mismatched_task_config_id() -> None:
    cfg = _valid_task_config("fake-bench/different")
    with pytest.raises(ValueError, match=r"task_config\.task\.id"):
        task_config_from_manifest_entry(
            {
                "task_id": "fake-bench/task-001",
                "task_config": cfg,
            }
        )
