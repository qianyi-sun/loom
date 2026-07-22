from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from loom.data_lifecycle_prepare import (
    LifecyclePrepareError,
    LifecycleSourceIdentity,
    verify_lifecycle_source,
)


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def test_exact_clean_source_identity_is_required(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "Test")
    _git(root, "config", "user.email", "test@example.invalid")
    tracked = root / "tracked.txt"
    tracked.write_text("base\n", encoding="utf-8")
    _git(root, "add", "tracked.txt")
    _git(root, "commit", "-qm", "base")
    base = _git(root, "rev-parse", "HEAD")
    tracked.write_text("candidate\n", encoding="utf-8")
    _git(root, "commit", "-qam", "candidate")
    identity = LifecycleSourceIdentity(
        candidate_sha=_git(root, "rev-parse", "HEAD"),
        candidate_tree=_git(root, "rev-parse", "HEAD^{tree}"),
        approved_base_sha=base,
    )

    verify_lifecycle_source(root, identity)

    tracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(LifecyclePrepareError, match="source identity drifted"):
        verify_lifecycle_source(root, identity)


def test_source_identity_rejects_noncanonical_digests() -> None:
    with pytest.raises(ValueError, match="source identity is invalid"):
        LifecycleSourceIdentity(
            candidate_sha="A" * 40,
            candidate_tree="1" * 40,
            approved_base_sha="2" * 40,
        )
