from pathlib import Path

import pytest

from loom.models.task_checksum import task_checksum
from loom_benchmarks.util import sha256_of_dir

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


def test_task_image_builder_imports_shared_sha256_of_dir() -> None:
    """The exclusive builder must not keep a second directory-hash copy."""
    text = _BUILDER.read_text(encoding="utf-8")
    assert "from loom_benchmarks.util import sha256_of_dir" in text
    assert "def sha256_of_dir" not in text
