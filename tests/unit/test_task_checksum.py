import builtins
from pathlib import Path

import pytest
from loom_benchmarks.util import sha256_of_dir

from loom.models.task_checksum import task_checksum

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BUILDER = _REPO_ROOT / "src" / "loom_worker" / "task_image_builder.py"


@pytest.fixture
def task_dir(tmp_path: Path) -> Path:
    """Build a minimal valid task directory."""
    d = tmp_path / "task"
    d.mkdir()
    (d / "task.toml").write_text("schema_version = \"1\"\n")
    (d / "instruction.md").write_text("Do the thing.\n")
    return d


def test_checksum_is_hex_string(task_dir: Path):
    h = task_checksum(task_dir)
    assert isinstance(h, str)
    assert len(h) == 64                                 # sha256 hex
    int(h, 16)                                          # parses as hex


def test_checksum_is_deterministic(task_dir: Path):
    assert task_checksum(task_dir) == task_checksum(task_dir)


def test_checksum_changes_when_content_changes(task_dir: Path):
    before = task_checksum(task_dir)
    (task_dir / "instruction.md").write_text("Do the OTHER thing.\n")
    after = task_checksum(task_dir)
    assert before != after


def test_checksum_changes_when_file_added(task_dir: Path):
    before = task_checksum(task_dir)
    (task_dir / "new.txt").write_text("new\n")
    after = task_checksum(task_dir)
    assert before != after


def test_task_checksum_matches_adapter_sha256_of_dir_including_dotfiles(
    task_dir: Path,
) -> None:
    """publish-local and adapter registration must agree (#1463)."""
    (task_dir / ".gitignore").write_text("*.pyc\n")
    assert task_checksum(task_dir) == sha256_of_dir(task_dir)


def test_task_checksum_runtime_does_not_import_benchmark_adapters(
    task_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Service/TaskSet hashing must work without the optional adapter package."""
    original_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "loom_benchmarks" or name.startswith("loom_benchmarks."):
            raise AssertionError("runtime checksum imported benchmark adapters")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    assert len(task_checksum(task_dir)) == 64


def test_runtime_and_adapter_consumers_share_one_checksum_function() -> None:
    """Every ingestion/build/audit path must bind the neutral implementation."""
    from loom_bundle_checksum import sha256_of_dir as canonical_sha256_of_dir

    from loom_cli import benchmark_readiness
    from loom_worker import main_loop, task_image_builder

    assert sha256_of_dir is canonical_sha256_of_dir
    assert task_image_builder.sha256_of_dir is canonical_sha256_of_dir
    assert main_loop.sha256_of_dir is canonical_sha256_of_dir
    assert benchmark_readiness.sha256_of_dir is canonical_sha256_of_dir
