"""Content-addressed identity for a task directory (spec §4.1)."""

from __future__ import annotations

from pathlib import Path

from loom_bundle_checksum import sha256_of_dir


def task_checksum(task_dir: Path) -> str:
    """SHA-256 hex digest of every file in the task directory.

    Canonical algorithm is ``loom_bundle_checksum.sha256_of_dir``
    (NUL-delimited relative path + content). Stored on ``tasks.checksum``
    and copied onto task-image materializations.
    """
    return sha256_of_dir(task_dir)
