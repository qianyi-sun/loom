"""Filesystem checks for local deployment artifacts."""

import stat
from pathlib import Path


def require_real_file(
    path: Path,
    *,
    label: str,
    expected_owner_uid: int | None = None,
) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular file, not a symlink")
    if expected_owner_uid is not None and metadata.st_uid != expected_owner_uid:
        raise ValueError(f"{label} must be service-owned")
