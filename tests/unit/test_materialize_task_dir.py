"""_materialize_task_dir pulls bundle.source content from MinIO when
source is an s3:// URL; leaves dir empty for fixture://, git+, None
(Plan 13 Task 3)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from loom.trajectory.storage import FakeObjectStore
from loom_worker.main_loop import _materialize_task_dir


async def test_s3_source_pulls_content() -> None:
    store = FakeObjectStore()
    store.objects[("loom-benchmarks", "swe/inst-1/task.toml")] = b"toml"
    store.objects[("loom-benchmarks", "swe/inst-1/solution/solve.sh")] = b"sh"
    store.objects[("loom-benchmarks", "swe/inst-1/tests/test_x.py")] = b"py"

    task_dir = await _materialize_task_dir(
        bundle={
            "id": "swe-bench-verified/inst-1",
            "checksum": "x" * 64,
            "config": {},
            "source": "s3://loom-benchmarks/swe/inst-1/",
        },
        object_store=store,  # type: ignore[arg-type]
        trial_id=uuid4(),
    )
    assert (task_dir / "task.toml").read_bytes() == b"toml"
    assert (task_dir / "solution/solve.sh").read_bytes() == b"sh"
    assert (task_dir / "tests/test_x.py").read_bytes() == b"py"


async def test_fixture_source_leaves_dir_empty() -> None:
    store = FakeObjectStore()
    store.objects[("any", "any-key")] = b"should-not-appear"
    task_dir = await _materialize_task_dir(
        bundle={
            "id": "hand-authored",
            "checksum": "0" * 64,
            "config": {},
            "source": "fixture://hand-authored",
        },
        object_store=store,  # type: ignore[arg-type]
        trial_id=uuid4(),
    )
    assert task_dir.exists()
    assert list(task_dir.iterdir()) == []


async def test_none_source_leaves_dir_empty() -> None:
    store = FakeObjectStore()
    task_dir = await _materialize_task_dir(
        bundle={"id": "x", "checksum": "0", "config": {}, "source": None},
        object_store=store,  # type: ignore[arg-type]
        trial_id=uuid4(),
    )
    assert task_dir.exists()
    assert list(task_dir.iterdir()) == []


async def test_malformed_s3_source_warns_and_empty() -> None:
    """An s3:// URL with no key prefix is logged but doesn't crash."""
    store = FakeObjectStore()
    task_dir = await _materialize_task_dir(
        bundle={"id": "x", "checksum": "0", "config": {},
                "source": "s3://bucket-only"},
        object_store=store,  # type: ignore[arg-type]
        trial_id=uuid4(),
    )
    assert task_dir.exists()
    assert list(task_dir.iterdir()) == []


_ = pytest
