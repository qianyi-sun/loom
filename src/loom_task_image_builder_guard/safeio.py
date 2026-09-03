"""Race-resistant bounded reads used by the guard trust boundary."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from loom_task_image_builder_guard.errors import GuardError


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def read_stable_file(
    path: Path,
    *,
    uid: int,
    gid: int,
    mode: int,
    maximum: int,
) -> bytes:
    """Read one exact regular file without following or racing a path."""

    if (
        not isinstance(path, Path)
        or not path.is_absolute()
        or type(uid) is not int
        or uid < 0
        or type(gid) is not int
        or gid < 0
        or type(mode) is not int
        or mode < 0
        or mode > 0o7777
        or type(maximum) is not int
        or maximum <= 0
    ):
        raise GuardError("safe_file_arguments_invalid")
    descriptor: int | None = None
    try:
        lexical = os.lstat(path)
        if (
            not stat.S_ISREG(lexical.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or lexical.st_nlink != 1
            or lexical.st_uid != uid
            or lexical.st_gid != gid
            or stat.S_IMODE(lexical.st_mode) != mode
        ):
            raise GuardError("safe_file_invalid")
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(lexical):
            raise GuardError("safe_file_invalid")
        chunks: list[bytes] = []
        total = 0
        while total <= maximum:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > maximum:
            raise GuardError("safe_file_too_large")
        if _identity(os.fstat(descriptor)) != _identity(opened):
            raise GuardError("safe_file_changed")
        return b"".join(chunks)
    except GuardError:
        raise
    except OSError as exc:
        raise GuardError("safe_file_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


__all__ = ["read_stable_file"]
