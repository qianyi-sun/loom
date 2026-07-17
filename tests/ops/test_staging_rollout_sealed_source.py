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
    (repo / ".gitignore").write_text("/ignored-link\n", encoding="utf-8")
    (repo / "deploy").mkdir()
    os.symlink("../.env", repo / "deploy/.env")
    (repo / "value.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", ".gitignore", "deploy/.env", "value.txt")
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


def _trust_tmp_parents(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sealed,
        "_validate_parent_authority",
        lambda *_args, **_kwargs: None,
    )


def _validate_real_checkout(source: sealed.SealedSource) -> None:
    sealed.validate_sealed_source(
        source,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )


def _commit_symlink(source: sealed.SealedSource, path: str, target: str) -> sealed.SealedSource:
    destination = source.path / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(target, destination)
    _git(source.path, "add", "--", path)
    _git(source.path, "commit", "-m", f"add {path}")
    commit = _git(source.path, "rev-parse", "HEAD")
    tree = _git(source.path, "rev-parse", "HEAD^{tree}")
    return sealed.SealedSource(source.path, commit, tree, source.base_sha)


def test_sealed_source_accepts_only_exact_clean_linear_detached_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path / "repo")
    _trust_tmp_parents(monkeypatch)

    _validate_real_checkout(source)

    with pytest.raises(sealed.SealedSourceError, match=r"Git validation|tree identity"):
        sealed.validate_sealed_source(
            sealed.SealedSource(source.path, source.commit_sha, "f" * 40, source.base_sha),
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
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
    _trust_tmp_parents(monkeypatch)
    alternates = source.path / ".git/objects/info/alternates"
    alternates.write_text("/tmp/objects\n", encoding="utf-8")

    with pytest.raises(sealed.SealedSourceError, match="indirection"):
        _validate_real_checkout(source)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("untracked", "checkout tree is unsafe"),
        ("ignored-untracked", "checkout tree is unsafe"),
        ("git", "checkout tree is unsafe"),
        ("tracked-payload", "payload does not match exact tree"),
        ("tracked-type", "tracked symlink authority is unsafe"),
        ("regular-type", "checkout tree is unsafe"),
        ("hardlink", "checkout tree is unsafe"),
        ("special", "checkout tree is unsafe"),
        ("unsafe-mode", "checkout tree is unsafe"),
    ),
)
def test_real_checkout_rejects_untracked_drifted_and_type_changed_symlinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    source = _source(tmp_path / "repo")
    _trust_tmp_parents(monkeypatch)
    if mutation == "untracked":
        os.symlink("value.txt", source.path / "untracked")
    elif mutation == "ignored-untracked":
        os.symlink("value.txt", source.path / "ignored-link")
    elif mutation == "git":
        os.symlink("/tmp", source.path / ".git/hooks/untracked-link")
    elif mutation == "tracked-payload":
        (source.path / "deploy/.env").unlink()
        os.symlink("../value.txt", source.path / "deploy/.env")
    elif mutation == "tracked-type":
        (source.path / "deploy/.env").unlink()
        (source.path / "deploy/.env").write_text("../.env", encoding="utf-8")
    elif mutation == "regular-type":
        (source.path / "value.txt").unlink()
        os.symlink("deploy/.env", source.path / "value.txt")
    elif mutation == "hardlink":
        os.link(source.path / "value.txt", source.path / "hardlink")
    elif mutation == "special":
        os.mkfifo(source.path / "fifo")
    elif mutation == "unsafe-mode":
        (source.path / "value.txt").chmod(0o664)
    else:  # pragma: no cover - the parameter list is exhaustive
        raise AssertionError(mutation)

    with pytest.raises(sealed.SealedSourceError, match=message):
        _validate_real_checkout(source)


def test_real_checkout_rejects_symlink_index_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path / "repo")
    _git(source.path, "rm", "--cached", "--", "deploy/.env")
    _trust_tmp_parents(monkeypatch)

    with pytest.raises(sealed.SealedSourceError, match="index does not match exact tree"):
        _validate_real_checkout(source)


@pytest.mark.parametrize(
    ("path", "target", "message"),
    (
        ("absolute-link", "/etc/passwd", "target is unsafe"),
        ("deploy/escape-link", "../../outside", "escapes checkout"),
        ("directory-link", "deploy", "target is unsafe"),
    ),
)
def test_real_checkout_rejects_exact_tracked_unsafe_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    target: str,
    message: str,
) -> None:
    source = _commit_symlink(_source(tmp_path / "repo"), path, target)
    _trust_tmp_parents(monkeypatch)

    with pytest.raises(sealed.SealedSourceError, match=message):
        _validate_real_checkout(source)


def test_real_checkout_rejects_exact_tracked_symlink_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path / "repo")
    os.symlink("loop-b", source.path / "loop-a")
    os.symlink("loop-a", source.path / "loop-b")
    _git(source.path, "add", "--", "loop-a", "loop-b")
    _git(source.path, "commit", "-m", "add loop")
    source = sealed.SealedSource(
        source.path,
        _git(source.path, "rev-parse", "HEAD"),
        _git(source.path, "rev-parse", "HEAD^{tree}"),
        source.base_sha,
    )
    _trust_tmp_parents(monkeypatch)

    with pytest.raises(sealed.SealedSourceError, match="target is unsafe"):
        _validate_real_checkout(source)
