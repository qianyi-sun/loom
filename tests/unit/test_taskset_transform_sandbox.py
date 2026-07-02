"""Unit tests for transform sandbox (#242 sub-plan 4)."""

from __future__ import annotations

import pytest

from loom.taskset.transform_sandbox import (
    TransformSandboxConfig,
    TransformSandboxError,
    run_transform,
)

_ENABLED = TransformSandboxConfig(
    enabled=True,
    network_isolated=True,
    wall_timeout_sec=5,
    cpu_limit_sec=2,
    memory_limit_mb=64,
)


def test_run_transform_happy_path() -> None:
    script = b"""
def transform(row):
    return {**row, "extra": 1}
"""
    out = run_transform(
        transform_script=script,
        row={"id": "1", "question": "hi"},
        config=_ENABLED,
        manifest_timeout_s=None,
    )
    assert out == {"id": "1", "question": "hi", "extra": 1}


def test_run_transform_user_exception() -> None:
    script = b"""
def transform(row):
    raise ValueError("boom")
"""
    with pytest.raises(TransformSandboxError) as exc_info:
        run_transform(
            transform_script=script,
            row={"id": "1"},
            config=_ENABLED,
            manifest_timeout_s=None,
        )
    assert exc_info.value.code == "transform_error"
    assert "boom" in exc_info.value.message


def test_run_transform_timeout() -> None:
    script = b"""
import time
def transform(row):
    time.sleep(10)
    return row
"""
    config = TransformSandboxConfig(
        enabled=True,
        network_isolated=True,
        wall_timeout_sec=1,
        cpu_limit_sec=2,
        memory_limit_mb=64,
    )
    with pytest.raises(TransformSandboxError) as exc_info:
        run_transform(
            transform_script=script,
            row={"id": "1"},
            config=config,
            manifest_timeout_s=None,
        )
    assert exc_info.value.code == "transform_limit_exceeded"


def test_run_transform_returns_non_dict() -> None:
    script = b"""
def transform(row):
    return []
"""
    with pytest.raises(TransformSandboxError) as exc_info:
        run_transform(
            transform_script=script,
            row={"id": "1"},
            config=_ENABLED,
            manifest_timeout_s=None,
        )
    assert exc_info.value.code == "transform_error"
    assert "dict" in exc_info.value.message


def test_run_transform_missing_function() -> None:
    script = b"""
def other(row):
    return row
"""
    with pytest.raises(TransformSandboxError) as exc_info:
        run_transform(
            transform_script=script,
            row={"id": "1"},
            config=_ENABLED,
            manifest_timeout_s=None,
        )
    assert exc_info.value.code == "transform_error"
    assert "transform" in exc_info.value.message.lower()


def test_run_transform_respects_manifest_timeout_cap() -> None:
    script = b"""
import time
def transform(row):
    time.sleep(10)
    return row
"""
    config = TransformSandboxConfig(
        enabled=True,
        network_isolated=True,
        wall_timeout_sec=30,
        cpu_limit_sec=2,
        memory_limit_mb=64,
    )
    with pytest.raises(TransformSandboxError) as exc_info:
        run_transform(
            transform_script=script,
            row={"id": "1"},
            config=config,
            manifest_timeout_s=1,
        )
    assert exc_info.value.code == "transform_limit_exceeded"
