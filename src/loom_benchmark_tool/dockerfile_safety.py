"""Compatibility wrapper for task Dockerfile preflight checks."""

from __future__ import annotations

from pathlib import Path

from loom.task_bundle_compat import (
    validate_dockerfile_compatibility,
    validate_task_dir_compatibility,
)


def validate_task_dir_dockerfiles(task_dir: Path) -> None:
    """Reject converted task bundles with known-fragile Dockerfile patterns."""

    validate_task_dir_compatibility(task_dir)


def validate_dockerfile_text(text: str, *, path: Path | None = None) -> None:
    validate_dockerfile_compatibility(text, path=path)
