"""Verify list_instances enumerates every <slug>/task.yaml under tasks/."""

from __future__ import annotations

from pathlib import Path

import pytest
from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter


def test_adapter_declares_series_metadata() -> None:
    assert TerminalBench2Adapter.series == "tool-use"


@pytest.fixture
def source_root(tmp_path: Path, fixtures_dir: Path) -> Path:
    """Lay out a mock cloned upstream: tasks/<slug>/task.yaml ..."""
    src = tmp_path / "repo"
    tasks = src / "tasks"
    tasks.mkdir(parents=True)
    target = tasks / "hello-world"
    target.mkdir()
    fixture = fixtures_dir / "tb2-task-hello-world"
    for child in fixture.rglob("*"):
        if child.is_dir():
            (target / child.relative_to(fixture)).mkdir(
                parents=True, exist_ok=True,
            )
        else:
            dest = target / child.relative_to(fixture)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(child.read_bytes())
    return src


def test_list_instances_enumerates_each_task_dir(source_root: Path) -> None:
    adapter = TerminalBench2Adapter()
    found = list(adapter.list_instances(source_dir=source_root, split="test"))
    assert [i.instance_id for i in found] == ["hello-world"]
    only = found[0]
    assert only.split == "test"
    assert only.raw["instruction"].startswith("Create a file called hello.txt")
    assert only.raw["parser_name"] == "pytest"
    assert only.raw["__source_path"] == str(source_root / "tasks" / "hello-world")


def test_list_instances_accepts_fetch_cache_root(source_root: Path) -> None:
    cache_root = source_root.parent
    adapter = TerminalBench2Adapter()
    found = list(adapter.list_instances(source_dir=cache_root, split="test"))
    assert [i.instance_id for i in found] == ["hello-world"]


def test_list_instances_skips_files_at_tasks_root(source_root: Path) -> None:
    (source_root / "tasks" / "README.md").write_text("not a task")
    adapter = TerminalBench2Adapter()
    found = list(adapter.list_instances(source_dir=source_root, split="test"))
    assert [i.instance_id for i in found] == ["hello-world"]
