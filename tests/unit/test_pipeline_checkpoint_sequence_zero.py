from __future__ import annotations

from pathlib import Path

import pytest

from loom_worker.pipeline_attempt_workspace import AttemptWorkspaceError
from loom_worker.pipeline_checkpoint_watcher import (
    PipelineCheckpointWatcherError,
    scan_completed_checkpoints,
)


def test_missing_checkpoint_root_has_no_implicit_sequence(tmp_path: Path) -> None:
    assert scan_completed_checkpoints(tmp_path / "missing") == []


def test_first_completed_directory_must_be_exact_zero(tmp_path: Path) -> None:
    root = tmp_path / "checkpoints"
    (root / "000000000001").mkdir(parents=True)
    with pytest.raises(PipelineCheckpointWatcherError, match="checkpoint_contract_mismatch"):
        scan_completed_checkpoints(root)


def test_partial_zero_is_ignored(tmp_path: Path) -> None:
    (tmp_path / "checkpoints" / ".partial" / "000000000000").mkdir(parents=True)
    assert scan_completed_checkpoints(tmp_path / "checkpoints") == []


def test_unexpected_future_or_non_numeric_directory_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "checkpoints"
    (root / "future").mkdir(parents=True)
    with pytest.raises(PipelineCheckpointWatcherError, match="checkpoint_contract_mismatch"):
        scan_completed_checkpoints(root)
    # Keep the import tied to the sole inner-writer error family expected by callers.
    assert issubclass(AttemptWorkspaceError, ValueError)
