"""FakeObjectStore.download_prefix contract (Plan 13 Task 2)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.trajectory.storage import FakeObjectStore


async def test_downloads_every_key_under_prefix(tmp_path: Path) -> None:
    store = FakeObjectStore()
    store.objects[("benchmarks", "swe/instance-1/task.toml")] = b"toml a"
    store.objects[("benchmarks", "swe/instance-1/solution/solve.sh")] = b"sh"
    store.objects[("benchmarks", "swe/instance-1/tests/test_x.py")] = b"py"
    store.objects[("benchmarks", "swe/instance-2/task.toml")] = b"toml b"
    store.objects[("other-bucket", "swe/instance-1/sneak.txt")] = b"x"

    count = await store.download_prefix(
        bucket="benchmarks", prefix="swe/instance-1/", out_dir=tmp_path,
    )
    assert count == 3
    assert (tmp_path / "task.toml").read_bytes() == b"toml a"
    assert (tmp_path / "solution/solve.sh").read_bytes() == b"sh"
    assert (tmp_path / "tests/test_x.py").read_bytes() == b"py"
    # The other instance + other bucket aren't pulled.
    assert not (tmp_path / "instance-2").exists()
    assert not (tmp_path / "sneak.txt").exists()


async def test_creates_out_dir(tmp_path: Path) -> None:
    store = FakeObjectStore()
    store.objects[("bench", "x/file.txt")] = b"hi"
    target = tmp_path / "nested" / "dir"
    assert not target.exists()
    count = await store.download_prefix(
        bucket="bench", prefix="x/", out_dir=target,
    )
    assert count == 1
    assert (target / "file.txt").read_bytes() == b"hi"


async def test_empty_prefix_returns_zero(tmp_path: Path) -> None:
    store = FakeObjectStore()
    count = await store.download_prefix(
        bucket="bench", prefix="nothing/here/", out_dir=tmp_path,
    )
    assert count == 0


async def test_prefix_only_key_skipped(tmp_path: Path) -> None:
    """A key that IS the prefix (no suffix) doesn't map to a real file."""
    store = FakeObjectStore()
    store.objects[("bench", "x/")] = b"placeholder"
    store.objects[("bench", "x/file.txt")] = b"hi"
    count = await store.download_prefix(
        bucket="bench", prefix="x/", out_dir=tmp_path,
    )
    assert count == 1
    assert (tmp_path / "file.txt").read_bytes() == b"hi"


_ = pytest
