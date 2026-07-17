from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_sealed_source as sealed


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "Sealed Test",
            "GIT_AUTHOR_EMAIL": "sealed@example.invalid",
            "GIT_COMMITTER_NAME": "Sealed Test",
            "GIT_COMMITTER_EMAIL": "sealed@example.invalid",
        },
    ).stdout.strip()


def _source(repo: Path) -> sealed.SealedSource:
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "remote", "add", "origin", sealed.APPROVED_REMOTE_URL)
    (repo / "value.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "value.txt")
    _git(repo, "commit", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "value.txt").write_text("first\n", encoding="utf-8")
    _git(repo, "commit", "-am", "first")
    (repo / "value.txt").write_text("second\n", encoding="utf-8")
    _git(repo, "commit", "-am", "second")
    commit = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    _git(repo, "checkout", "--detach", commit)
    return sealed.SealedSource(repo, commit, tree, base)


def test_sealed_source_accepts_only_exact_clean_linear_detached_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path / "repo")
    monkeypatch.setattr(sealed, "_validate_authority", lambda *_args, **_kwargs: None)

    sealed.validate_sealed_source(source)

    with pytest.raises(sealed.SealedSourceError, match="tree identity"):
        sealed.validate_sealed_source(
            sealed.SealedSource(source.path, source.commit_sha, "f" * 40, source.base_sha)
        )


def test_sealed_source_rejects_branch_checkout_dirty_tree_and_remote_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path / "repo")
    monkeypatch.setattr(sealed, "_validate_authority", lambda *_args, **_kwargs: None)
    _git(source.path, "switch", "-c", "mutable")
    with pytest.raises(sealed.SealedSourceError, match="detached HEAD"):
        sealed.validate_sealed_source(source)

    _git(source.path, "checkout", "--detach", source.commit_sha)
    (source.path / "untracked").write_text("drift\n", encoding="utf-8")
    with pytest.raises(sealed.SealedSourceError, match="clean"):
        sealed.validate_sealed_source(source)

    (source.path / "untracked").unlink()
    _git(source.path, "remote", "set-url", "origin", "https://example.invalid/loom.git")
    with pytest.raises(sealed.SealedSourceError, match="origin is not approved"):
        sealed.validate_sealed_source(source)


def test_sealed_source_rejects_git_indirection_before_identity_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path / "repo")
    monkeypatch.setattr(sealed, "_validate_authority", lambda *_args, **_kwargs: None)
    alternates = source.path / ".git/objects/info/alternates"
    alternates.write_text("/tmp/objects\n", encoding="utf-8")

    with pytest.raises(sealed.SealedSourceError, match="indirection"):
        sealed.validate_sealed_source(source)
