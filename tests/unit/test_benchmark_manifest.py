from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom_benchmark_tool.manifest import load_task_config_from_bundle

_VALID_TASK_TOML = """\
schema_version = "1"

[task]
id = "fake-bench/task-001"
name = "Fake task"

[environment]
os = "linux"
docker_image = "python:3.12-slim"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
"""


def test_load_task_config_from_bundle_returns_valid_raw_config(tmp_path) -> None:
    (tmp_path / "task.toml").write_text(_VALID_TASK_TOML)
    cfg = load_task_config_from_bundle(tmp_path)
    assert cfg["task"]["id"] == "fake-bench/task-001"
    assert cfg["environment"]["docker_image"] == "python:3.12-slim"


def test_load_task_config_from_bundle_rejects_invalid_task_toml(tmp_path) -> None:
    (tmp_path / "task.toml").write_text("[task]\nid = 'broken'\n")
    with pytest.raises(ValidationError):
        load_task_config_from_bundle(tmp_path)
