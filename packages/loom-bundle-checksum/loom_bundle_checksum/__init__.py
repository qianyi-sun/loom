"""Canonical, dependency-free identity for Loom task bundles."""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_of_dir(directory: Path) -> str:
    """Hash the relative path and content of every file in ``directory``.

    Paths are sorted and NUL-delimited so that different bundle layouts cannot
    produce the same byte stream. Dotfiles are included by ``Path.rglob``.
    """
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_dir():
            continue
        relative_path = path.relative_to(directory).as_posix().encode("utf-8")
        digest.update(b"\x00" + relative_path + b"\x00")
        digest.update(path.read_bytes())
    return digest.hexdigest()


__all__ = ["sha256_of_dir"]
