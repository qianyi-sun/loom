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


def _copy_fixture(fixture_root: Path, dest: Path) -> None:
    for child in fixture_root.rglob("*"):
        target = dest / child.relative_to(fixture_root)
        if child.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(child.read_bytes())


def test_list_instances_emits_oracle_eligible_true_when_solution_sh_present(
    tmp_path: Path, fixtures_dir: Path,
) -> None:
    """The hello-world fixture ships an upstream `solution.sh`, so the
    adapter must tag it `oracle_eligible="true"` — the precise marker
    the cap check at `src/loom_service/task_compat.py` consults instead
    of the legacy `terminal-bench-2/` prefix backstop."""
    src = tmp_path / "repo"
    target = src / "tasks" / "hello-world"
    target.mkdir(parents=True)
    _copy_fixture(fixtures_dir / "tb2-task-hello-world", target)
    found = list(TerminalBench2Adapter().list_instances(
        source_dir=src, split="test",
    ))
    assert found[0].tags["oracle_eligible"] == "true"


def test_list_instances_emits_oracle_eligible_false_when_no_solution(
    tmp_path: Path, fixtures_dir: Path,
) -> None:
    """The chess-best-move fixture ships no `solution.sh`/`solution.yaml`,
    so the adapter must tag it `oracle_eligible="false"`. Without this
    explicit tag the legacy task-id prefix would over-grant oracle
    capability and the oracle agent would fail at runtime for tasks
    lacking an upstream reference solution."""
    src = tmp_path / "repo"
    target = src / "tasks" / "chess-best-move"
    target.mkdir(parents=True)
    _copy_fixture(fixtures_dir / "tb2-task-chess-best-move", target)
    found = list(TerminalBench2Adapter().list_instances(
        source_dir=src, split="test",
    ))
    assert found[0].tags["oracle_eligible"] == "false"


def test_list_instances_emits_oracle_eligible_true_when_solution_yaml_present(
    tmp_path: Path, fixtures_dir: Path,
) -> None:
    """`solution.yaml` is the other upstream solution shape; the
    adapter wraps both into `solution/solve.sh`, so either should
    produce `oracle_eligible="true"`."""
    src = tmp_path / "repo"
    target = src / "tasks" / "yaml-only"
    target.mkdir(parents=True)
    _copy_fixture(fixtures_dir / "tb2-task-chess-best-move", target)
    (target / "solution.yaml").write_text("- command: echo done\n")
    found = list(TerminalBench2Adapter().list_instances(
        source_dir=src, split="test",
    ))
    assert found[0].tags["oracle_eligible"] == "true"
