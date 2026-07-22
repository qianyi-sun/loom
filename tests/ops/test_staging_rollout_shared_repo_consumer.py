from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_shared_repo_consumer as consumer


def _run(*argv: str, cwd: Path) -> str:
    result = subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _normalize(path: Path) -> None:
    raw_index = subprocess.run(
        ["/usr/bin/git", "-C", str(path), "ls-files", "--stage", "-z"],
        check=True,
        capture_output=True,
    ).stdout
    index_modes = {
        entry.partition(b"\t")[2].decode("utf-8"): entry.split(b" ", 1)[0].decode("ascii")
        for entry in raw_index.split(b"\0")
        if entry
    }
    for directory, directories, files in os.walk(path, topdown=False):
        for name in files:
            candidate = Path(directory) / name
            if not candidate.is_symlink():
                relative = candidate.relative_to(path).as_posix()
                candidate.chmod(0o750 if index_modes.get(relative) == "100755" else 0o640)
        for name in directories:
            (Path(directory) / name).chmod(0o750)
    path.chmod(0o750)


def _checkout(tmp_path: Path) -> tuple[Path, Path, str]:
    root = tmp_path / "worker-repos"
    repo = root / "loom-remote-worker-test"
    repo.mkdir(parents=True)
    _run("/usr/bin/git", "init", "--quiet", cwd=repo)
    _run("/usr/bin/git", "config", "user.email", "test@example.com", cwd=repo)
    _run("/usr/bin/git", "config", "user.name", "Test", cwd=repo)
    (repo / "README.md").write_text("fixed candidate\n", encoding="utf-8")
    (repo / "other.txt").write_text("other tracked payload\n", encoding="utf-8")
    (repo / "current-link").symlink_to("README.md")
    script = repo / "scripts" / "run.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)
    _run("/usr/bin/git", "add", ".", cwd=repo)
    _run("/usr/bin/git", "commit", "--quiet", "-m", "fixture", cwd=repo)
    sha = _run("/usr/bin/git", "rev-parse", "HEAD", cwd=repo)
    (repo / ".git" / "config").write_bytes(consumer._CANONICAL_GIT_CONFIG)
    _normalize(repo)
    root.chmod(0o2750)
    return root, repo, sha


@pytest.fixture
def deny_consumer_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        consumer.os,
        "access",
        lambda _path, mode, **_kwargs: not bool(mode & os.W_OK),
    )


def test_consumer_accepts_exact_immutable_checkout(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, sha = _checkout(tmp_path)

    evidence = consumer.verify(
        root=root,
        repo=repo,
        sha=sha,
        owner_uid=os.geteuid(),
        shared_gid=os.getegid(),
        consumer_uid=os.geteuid(),
    )

    assert evidence["head"] == sha
    assert evidence["tracked_entries"] == 4
    assert len(str(evidence["index_sha256"])) == 64
    assert len(str(evidence["tree_content_sha256"])) == 64
    assert evidence["probe_file_sha256"] == hashlib.sha256(b"fixed candidate\n").hexdigest()


def test_consumer_rejects_empty_untracked_directory(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    (repo / "untracked-empty").mkdir(mode=0o750)

    with pytest.raises(consumer.ConsumerVerificationError):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_rejects_exact_index_mode_or_head_drift(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    readme = repo / "README.md"
    readme.chmod(0o660)

    with pytest.raises(consumer.ConsumerVerificationError):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )

    readme.chmod(0o640)
    with pytest.raises(consumer.ConsumerVerificationError):
        consumer.verify(
            root=root,
            repo=repo,
            sha="f" * 40,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_metadata_is_strict_after_git_operations(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    assert stat.S_IMODE((repo / ".git").stat().st_mode) == 0o750
    (repo / ".git" / "config").chmod(0o660)

    with pytest.raises(consumer.ConsumerVerificationError):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_fails_closed_when_root_cannot_be_opened(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root = tmp_path / "missing-worker-repos"

    with pytest.raises(
        consumer.ConsumerVerificationError,
        match="failed safely",
    ):
        consumer.verify(
            root=root,
            repo=root / "loom-remote-worker-test",
            sha="a" * 40,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


@pytest.mark.parametrize("hidden_flag", ("--assume-unchanged", "--skip-worktree"))
def test_consumer_rejects_hidden_non_probe_content_drift(
    tmp_path: Path,
    deny_consumer_writes: None,
    hidden_flag: str,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    _run("/usr/bin/git", "update-index", hidden_flag, "other.txt", cwd=repo)
    (repo / "other.txt").write_text("hidden drift\n", encoding="utf-8")
    _normalize(repo)

    with pytest.raises(consumer.ConsumerVerificationError, match="content drifted"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


@pytest.mark.parametrize("drift", ("mode", "object-id"))
def test_consumer_rejects_index_entry_drift_from_commit_tree(
    tmp_path: Path,
    deny_consumer_writes: None,
    drift: str,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    if drift == "mode":
        _run("/usr/bin/git", "update-index", "--chmod=-x", "scripts/run.sh", cwd=repo)
        (repo / "scripts" / "run.sh").chmod(0o640)
    else:
        payload = b"index-only object\n"
        object_id = (
            subprocess.run(
                ["/usr/bin/git", "-C", str(repo), "hash-object", "-w", "--stdin"],
                input=payload,
                check=True,
                capture_output=True,
            )
            .stdout.decode("ascii")
            .strip()
        )
        _run(
            "/usr/bin/git",
            "update-index",
            "--cacheinfo",
            f"100755,{object_id},scripts/run.sh",
            cwd=repo,
        )
        (repo / "scripts" / "run.sh").write_bytes(payload)
        (repo / "scripts" / "run.sh").chmod(0o750)
    _normalize(repo)

    with pytest.raises(consumer.ConsumerVerificationError, match="index drifted"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_rejects_symlink_payload_drift(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    (repo / "current-link").unlink()
    (repo / "current-link").symlink_to("other.txt")

    with pytest.raises(consumer.ConsumerVerificationError, match="symlink drifted"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_ignores_replace_ref_and_rejects_replacement_tree_checkout(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, candidate_sha = _checkout(tmp_path)
    _run("/usr/bin/git", "config", "user.email", "test@example.com", cwd=repo)
    _run("/usr/bin/git", "config", "user.name", "Test", cwd=repo)
    (repo / "other.txt").write_text("hostile replacement payload\n", encoding="utf-8")
    _run("/usr/bin/git", "add", "other.txt", cwd=repo)
    _run("/usr/bin/git", "commit", "--quiet", "-m", "replacement", cwd=repo)
    replacement_sha = _run("/usr/bin/git", "rev-parse", "HEAD", cwd=repo)
    _run("/usr/bin/git", "replace", candidate_sha, replacement_sha, cwd=repo)
    _run("/usr/bin/git", "reset", "--hard", candidate_sha, cwd=repo)
    (repo / ".git" / "config").write_bytes(consumer._CANONICAL_GIT_CONFIG)
    _normalize(repo)
    assert (repo / "other.txt").read_text(encoding="utf-8") == ("hostile replacement payload\n")

    with pytest.raises(consumer.ConsumerVerificationError, match="index drifted"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=candidate_sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_ignores_supported_legacy_grafts(
    tmp_path: Path,
    deny_consumer_writes: None,
) -> None:
    root, repo, candidate_sha = _checkout(tmp_path)
    _run("/usr/bin/git", "config", "user.email", "test@example.com", cwd=repo)
    _run("/usr/bin/git", "config", "user.name", "Test", cwd=repo)
    (repo / "other.txt").write_text("unrelated commit\n", encoding="utf-8")
    _run("/usr/bin/git", "add", "other.txt", cwd=repo)
    _run("/usr/bin/git", "commit", "--quiet", "-m", "graft-parent", cwd=repo)
    replacement_parent = _run("/usr/bin/git", "rev-parse", "HEAD", cwd=repo)
    _run("/usr/bin/git", "reset", "--hard", candidate_sha, cwd=repo)
    grafts = repo / ".git" / "info" / "grafts"
    grafts.write_text(f"{candidate_sha} {replacement_parent}\n", encoding="ascii")
    grafts.chmod(0o640)
    supported = subprocess.run(
        ["/usr/bin/git", "-C", str(repo), "rev-parse", f"{candidate_sha}^"],
        check=False,
        capture_output=True,
        text=True,
    )
    if supported.returncode != 0 or supported.stdout.strip() != replacement_parent:
        pytest.skip("installed Git no longer supports legacy grafts")
    (repo / ".git" / "config").write_bytes(consumer._CANONICAL_GIT_CONFIG)
    _normalize(repo)

    with pytest.raises(consumer.ConsumerVerificationError, match="legacy graft"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=candidate_sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )


def test_consumer_rejects_malicious_git_config_before_any_git_command(
    tmp_path: Path,
    deny_consumer_writes: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    sentinel = tmp_path / "must-not-execute"
    (repo / ".git" / "config").write_text(
        "[core]\n\tfsmonitor = !touch " + str(sentinel) + "\n"
        '[filter "sentinel"]\n\tsmudge = touch ' + str(sentinel) + "\n",
        encoding="utf-8",
    )
    (repo / ".git" / "config").chmod(0o640)
    calls = 0

    def forbidden_git(*_args: object, **_kwargs: object) -> bytes:
        nonlocal calls
        calls += 1
        raise AssertionError("Git must not run before canonical config validation")

    monkeypatch.setattr(consumer, "_git", forbidden_git)

    with pytest.raises(consumer.ConsumerVerificationError, match="configuration"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )

    assert calls == 0
    assert not sentinel.exists()


def test_consumer_rejects_external_common_git_authority_before_any_git_command(
    tmp_path: Path,
    deny_consumer_writes: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    external_common = tmp_path / "external-common.git"
    shutil.copytree(repo / ".git", external_common, symlinks=True)
    commondir = repo / ".git" / "commondir"
    commondir.write_text(str(external_common) + "\n", encoding="utf-8")
    commondir.chmod(0o640)
    assert _run("/usr/bin/git", "rev-parse", "--git-common-dir", cwd=repo) == str(external_common)
    sentinel = tmp_path / "must-not-execute"
    (external_common / "config").write_text(
        "[core]\n\tfsmonitor = !touch " + str(sentinel) + "\n",
        encoding="utf-8",
    )
    calls = 0

    def forbidden_git(*_args: object, **_kwargs: object) -> bytes:
        nonlocal calls
        calls += 1
        raise AssertionError("Git must not run before common authority validation")

    monkeypatch.setattr(consumer, "_git", forbidden_git)

    with pytest.raises(consumer.ConsumerVerificationError, match="common authority"):
        consumer.verify(
            root=root,
            repo=repo,
            sha=sha,
            owner_uid=os.geteuid(),
            shared_gid=os.getegid(),
            consumer_uid=os.geteuid(),
        )

    assert calls == 0
    assert not sentinel.exists()


def test_consumer_accepts_distinct_owner_and_runtime_consumer_identity(
    tmp_path: Path,
    deny_consumer_writes: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, repo, sha = _checkout(tmp_path)
    owner_uid = os.geteuid()
    consumer_uid = owner_uid + 10_000
    monkeypatch.setattr(consumer.os, "geteuid", lambda: consumer_uid)

    evidence = consumer.verify(
        root=root,
        repo=repo,
        sha=sha,
        owner_uid=owner_uid,
        shared_gid=os.getegid(),
        consumer_uid=consumer_uid,
    )

    assert evidence["head"] == sha


def test_git_verification_uses_exact_safe_directory_argv_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "worker-repos" / "loom-remote-worker-test"
    captured: dict[str, object] = {}

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        captured["command"] = command
        captured["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, stdout=b"sha1\n", stderr=b"")

    monkeypatch.setattr(consumer.subprocess, "run", run)

    assert consumer._git(repo, "rev-parse", "--show-object-format") == b"sha1\n"
    assert captured["command"] == [
        "/usr/bin/git",
        "--git-dir",
        str(repo / ".git"),
        "--work-tree",
        str(repo),
        "-c",
        f"safe.directory={repo}",
        "-c",
        f"core.worktree={repo}",
        "-c",
        "core.bare=false",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.attributesFile=/dev/null",
        "-c",
        "core.excludesFile=/dev/null",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "submodule.recurse=false",
        "-c",
        "fetch.recurseSubmodules=false",
        "-c",
        "protocol.file.allow=never",
        "-c",
        "credential.helper=",
        "-c",
        "core.sshCommand=/usr/bin/false",
        "rev-parse",
        "--show-object-format",
    ]
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs == {
        "check": False,
        "capture_output": True,
        "timeout": 15,
        "env": {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_PROTOCOL_FROM_USER": "0",
            "GIT_PAGER": "cat",
            "GIT_EXTERNAL_DIFF": "/usr/bin/false",
            "GIT_SSH_COMMAND": "/usr/bin/false",
            "HOME": "/nonexistent",
            "XDG_CONFIG_HOME": "/nonexistent",
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": "/usr/bin:/bin",
        },
    }
