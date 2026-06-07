"""upload_task_dir + import_cmd guard against poisoned upstream
records that could leak files outside the benchmark namespace or
smuggle path-traversal into the S3 prefix (Plan 14 audit follow-ups)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import _validate_instance_id
from loom_benchmark_tool.upload import upload_task_dir


async def test_upload_task_dir_rejects_empty_prefix(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    with pytest.raises(ValueError, match="non-empty prefix"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b", prefix="", task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_traversal_prefix(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    with pytest.raises(ValueError, match="traversal"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b", prefix="humaneval/../escape/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_rejects_absolute_prefix(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    with pytest.raises(ValueError, match="traversal or absolute"):
        await upload_task_dir(
            store=FakeObjectStore(),
            bucket="b", prefix="/escape/",
            task_dir=tmp_path,
        )


async def test_upload_task_dir_normal_prefix_works(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("x = 1\n")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "s.py").write_text("pass\n")
    store = FakeObjectStore()
    n = await upload_task_dir(
        store=store, bucket="b",
        prefix="humaneval/HumanEval/0/", task_dir=tmp_path,
    )
    assert n == 2
    assert ("b", "humaneval/HumanEval/0/task.toml") in store.objects
    assert ("b", "humaneval/HumanEval/0/solution/s.py") in store.objects


def test_validate_instance_id_accepts_normal_ids() -> None:
    _validate_instance_id("HumanEval/0")
    _validate_instance_id("inst-1")
    _validate_instance_id("swe-bench-verified/django__django-12345")
    _validate_instance_id("MMLU/abstract_algebra/0")
    _validate_instance_id("v1.0+r2")


def test_validate_instance_id_rejects_traversal() -> None:
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("..")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("foo/../bar")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("foo/./bar")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("/leading-slash")
    with pytest.raises(ValueError, match=r"empty / \.\. / \. segments"):
        _validate_instance_id("trailing-slash/")


def test_validate_instance_id_rejects_specials() -> None:
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("foo bar")  # space
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("foo;rm")
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id('id"quote')
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("id\nnewline")
    with pytest.raises(ValueError, match="characters outside"):
        _validate_instance_id("id\x00nul")
