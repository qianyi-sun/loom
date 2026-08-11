"""Deterministic, no-follow personal-development source sealing."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

from loom_cli.personal_dev_source import (
    PersonalDevSourceError,
    create_personal_dev_source_snapshot,
    verify_personal_dev_source_snapshot,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "snapshot@example.test")
    _git(repo, "config", "user.name", "Snapshot Test")
    (repo / ".gitignore").write_text("ignored.txt\n.env\n", encoding="utf-8")
    (repo / "app.py").write_text("print('base')\n", encoding="utf-8")
    executable = repo / "run.sh"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o755)
    (repo / "deploy").mkdir()
    os.symlink("../.env", repo / "deploy" / ".env")
    _git(repo, "add", ".gitignore", "app.py", "run.sh")
    _git(repo, "add", "-f", "deploy/.env")
    _git(repo, "commit", "-qm", "base")
    return repo


def test_snapshot_seals_committed_modified_and_permitted_untracked_source(
    tmp_path: Path,
) -> None:
    repo = _repo(tmp_path)
    (repo / "app.py").write_text("print('feature')\n", encoding="utf-8")
    (repo / "new_module.py").write_text("VALUE = 3\n", encoding="utf-8")
    (repo / "ignored.txt").write_text("not a build input\n", encoding="utf-8")
    (repo / ".env").write_text("TOKEN=must-not-leak\n", encoding="utf-8")
    (repo / ".env.local").write_text("TOKEN=also-must-not-leak\n", encoding="utf-8")
    (repo / "private.pem").write_text("not-a-real-key\n", encoding="utf-8")
    first_archive = tmp_path / "first.tar"
    second_archive = tmp_path / "second.tar"

    first = create_personal_dev_source_snapshot(repo, first_archive)
    second = create_personal_dev_source_snapshot(repo, second_archive)

    assert first == second
    assert first.manifest.attestation_scope == "personal-dev-only"
    assert first.manifest.source_commit == _git(repo, "rev-parse", "HEAD")
    assert first.manifest.dirty is True
    assert tuple(item.path for item in first.manifest.files) == (
        ".gitignore",
        "app.py",
        "new_module.py",
        "run.sh",
    )
    assert first.manifest.file_count == 4
    assert first.manifest.total_bytes == sum(item.size for item in first.manifest.files)
    assert first.manifest.excluded_sensitive_paths == (
        ".env.local",
        "deploy/.env",
        "private.pem",
    )
    assert first.source_digest == second.source_digest
    assert first.archive_sha256 == second.archive_sha256
    assert first_archive.read_bytes() == second_archive.read_bytes()
    assert b"must-not-leak" not in first_archive.read_bytes()
    with tarfile.open(first_archive, "r:") as archive:
        members = archive.getmembers()
        assert [member.name for member in members] == [
            "SOURCE-MANIFEST.json",
            ".gitignore",
            "app.py",
            "new_module.py",
            "run.sh",
        ]
        assert all(member.mtime == member.uid == member.gid == 0 for member in members)
        assert archive.extractfile("app.py").read() == b"print('feature')\n"  # type: ignore[union-attr]
        assert archive.getmember("run.sh").mode == 0o755

    assert (
        verify_personal_dev_source_snapshot(
            first_archive,
            expected_source_digest=first.source_digest,
            expected_archive_sha256=first.archive_sha256,
        )
        == first.manifest
    )


def test_snapshot_rejects_any_nonexcluded_symlink_or_special_file(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    os.symlink("app.py", repo / "linked.py")
    with pytest.raises(PersonalDevSourceError, match="symbolic link"):
        create_personal_dev_source_snapshot(repo, tmp_path / "symlink.tar")

    (repo / "linked.py").unlink()
    os.mkfifo(repo / "pipe")
    with pytest.raises(PersonalDevSourceError, match="regular file"):
        create_personal_dev_source_snapshot(repo, tmp_path / "fifo.tar")


def test_snapshot_rejects_checkout_change_between_copy_and_verification(
    tmp_path: Path,
) -> None:
    repo = _repo(tmp_path)

    def mutate() -> None:
        (repo / "app.py").write_text("print('raced')\n", encoding="utf-8")

    output = tmp_path / "raced.tar"
    with pytest.raises(PersonalDevSourceError, match="changed while it was being sealed"):
        create_personal_dev_source_snapshot(repo, output, _between_passes=mutate)
    assert not output.exists()


@pytest.mark.parametrize("context", ("../escape", "/absolute", ".git", "missing"))
def test_snapshot_rejects_unsafe_or_unavailable_context(tmp_path: Path, context: str) -> None:
    repo = _repo(tmp_path)
    with pytest.raises(PersonalDevSourceError, match="context"):
        create_personal_dev_source_snapshot(
            repo,
            tmp_path / "context.tar",
            contexts=(context,),
        )


def test_snapshot_enforces_file_and_byte_limits_before_publication(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    output = tmp_path / "limited.tar"
    with pytest.raises(PersonalDevSourceError, match="file-count limit"):
        create_personal_dev_source_snapshot(repo, output, max_files=2)
    assert not output.exists()

    with pytest.raises(PersonalDevSourceError, match="byte limit"):
        create_personal_dev_source_snapshot(repo, output, max_total_bytes=4)
    assert not output.exists()


def test_snapshot_output_must_not_be_inside_mutable_source_tree(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    with pytest.raises(PersonalDevSourceError, match="outside the source repository"):
        create_personal_dev_source_snapshot(repo, repo / "snapshot.tar")


def test_snapshot_verifier_rejects_digest_drift_and_archive_path_traversal(
    tmp_path: Path,
) -> None:
    repo = _repo(tmp_path)
    output = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, output)

    with pytest.raises(PersonalDevSourceError, match="archive digest"):
        verify_personal_dev_source_snapshot(
            output,
            expected_source_digest=snapshot.source_digest,
            expected_archive_sha256="0" * 64,
        )

    malicious = tmp_path / "malicious.tar"
    manifest_payload = json.dumps(
        {
            "attestation_scope": "personal-dev-only",
            "contexts": ["."],
            "deleted_tracked_paths": [],
            "dirty": False,
            "excluded_sensitive_paths": [],
            "file_count": 1,
            "files": [
                {
                    "mode": 420,
                    "path": "../escape",
                    "sha256": hashlib.sha256(b"bad").hexdigest(),
                    "size": 3,
                }
            ],
            "schema_version": 1,
            "source_commit": "a" * 40,
            "total_bytes": 3,
            "worktree_state_sha256": hashlib.sha256(b"").hexdigest(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    with tarfile.open(malicious, "w", format=tarfile.USTAR_FORMAT) as archive:
        info = tarfile.TarInfo("SOURCE-MANIFEST.json")
        info.size = len(manifest_payload)
        info.mode = 0o600
        archive.addfile(info, io.BytesIO(manifest_payload))
        payload = tarfile.TarInfo("../escape")
        payload.size = 3
        payload.mode = 0o644
        archive.addfile(payload, io.BytesIO(b"bad"))
    malicious_bytes = malicious.read_bytes()
    with pytest.raises(PersonalDevSourceError, match=r"manifest|unsafe path"):
        verify_personal_dev_source_snapshot(
            malicious,
            expected_source_digest=hashlib.sha256(manifest_payload).hexdigest(),
            expected_archive_sha256=hashlib.sha256(malicious_bytes).hexdigest(),
        )


def test_snapshot_verifier_rejects_noncanonical_trailer_bytes(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    output = tmp_path / "source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, output)

    appended = tmp_path / "appended.tar"
    appended.write_bytes(output.read_bytes() + b"\0" * tarfile.RECORDSIZE)
    with pytest.raises(PersonalDevSourceError, match="canonical tar size"):
        verify_personal_dev_source_snapshot(
            appended,
            expected_source_digest=snapshot.source_digest,
            expected_archive_sha256=hashlib.sha256(appended.read_bytes()).hexdigest(),
        )

    nonzero = tmp_path / "nonzero-padding.tar"
    payload = bytearray(output.read_bytes())
    payload[-1] = 1
    nonzero.write_bytes(payload)
    with pytest.raises(PersonalDevSourceError, match="trailer"):
        verify_personal_dev_source_snapshot(
            nonzero,
            expected_source_digest=snapshot.source_digest,
            expected_archive_sha256=hashlib.sha256(payload).hexdigest(),
        )
