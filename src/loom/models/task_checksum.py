"""Content-addressed identity for a task directory (spec §4.1)."""

from __future__ import annotations

from pathlib import Path

from dirhash import dirhash  # type: ignore[import-untyped]


def task_checksum(task_dir: Path) -> str:
    """SHA-256 hex digest of every file in the task directory.

    Stored on every TrialResult and `trials` row for reproducibility.
    """
    return str(dirhash(task_dir, "sha256"))
