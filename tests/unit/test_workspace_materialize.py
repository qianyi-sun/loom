"""materialize_workspace — recursive upload of the task bundle into the
sandbox (#186)."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from loom.driver.fake import FakeDriver
from loom.trial.workspace import materialize_workspace


async def test_uploads_nested_files_preserving_layout(tmp_path: Path) -> None:
    (tmp_path / "instruction.md").write_text("do the thing")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "solution.py").write_text("def f(): pass\n")
    (tmp_path / "solution" / "__init__.py").write_text("from .solution import f\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_x.py").write_text("def test_ok(): pass\n")
    (tmp_path / "tests" / "conftest.py").write_text("import sys\n")
    (tmp_path / "task.toml").write_text("schema_version = \"1\"\n")

    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=tmp_path, dst=PurePosixPath("/workspace"),
    )
    # 5 files uploaded; task.toml skipped.
    assert count == 5
    fs = d.filesystem
    assert fs[PurePosixPath("/workspace/instruction.md")] == b"do the thing"
    assert fs[PurePosixPath("/workspace/solution/solution.py")] == b"def f(): pass\n"
    assert fs[PurePosixPath("/workspace/solution/__init__.py")].startswith(b"from")
    assert fs[PurePosixPath("/workspace/tests/test_x.py")] == b"def test_ok(): pass\n"
    assert fs[PurePosixPath("/workspace/tests/conftest.py")] == b"import sys\n"
    # task.toml is host-side metadata and must not be uploaded.
    assert PurePosixPath("/workspace/task.toml") not in fs


async def test_empty_task_dir_uploads_nothing(tmp_path: Path) -> None:
    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=tmp_path, dst=PurePosixPath("/workspace"),
    )
    assert count == 0
    assert d.filesystem == {}


async def test_missing_task_dir_is_silent_noop(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    d = FakeDriver()
    await d.start()
    count = await materialize_workspace(
        driver=d, task_dir=missing, dst=PurePosixPath("/workspace"),
    )
    assert count == 0
