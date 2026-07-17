from __future__ import annotations

import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import staging_rollout_shared_repo as helper


def _portable_rename_noreplace(
    source_fd: int,
    source_name: str,
    destination_fd: int,
    destination_name: str,
) -> None:
    try:
        os.stat(destination_name, dir_fd=destination_fd, follow_symlinks=False)
    except FileNotFoundError:
        os.rename(
            source_name,
            destination_name,
            src_dir_fd=source_fd,
            dst_dir_fd=destination_fd,
        )
        return
    raise FileExistsError("destination exists")


def test_consumer_identity_keeps_distinct_primary_and_shared_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        helper.pwd,
        "getpwnam",
        lambda name: SimpleNamespace(pw_uid=2005, pw_gid=2005) if name == "qianyi" else None,
    )
    monkeypatch.setattr(helper.os, "getgrouplist", lambda _name, _gid: [2005, 2007])

    identity = helper._identity("qianyi")

    assert identity.uid == 2005
    assert identity.gid == 2005
    assert identity.groups == (2005, 2007)


def test_ensure_child_publishes_private_temp_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper, "_rename_noreplace", _portable_rename_noreplace)
    parent = helper._open_absolute(tmp_path)
    try:
        child, created = helper._ensure_child(
            parent,
            "worker-repos",
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o750,
        )
        try:
            assert created is True
            assert child.path == tmp_path / "worker-repos"
            assert stat.S_IMODE(os.fstat(child.fd).st_mode) == 0o750
        finally:
            os.close(child.fd)
        assert not list(tmp_path.glob(".worker-repos.tmp-*"))
    finally:
        os.close(parent.fd)


def test_ensure_child_never_takes_over_concurrent_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = helper._open_absolute(tmp_path)

    def inject_concurrent_destination(
        _source_fd: int,
        _source_name: str,
        _destination_fd: int,
        destination_name: str,
    ) -> None:
        os.mkdir(destination_name, 0o700, dir_fd=parent.fd)
        raise FileExistsError("destination exists")

    monkeypatch.setattr(helper, "_rename_noreplace", inject_concurrent_destination)
    try:
        with pytest.raises(helper.AuthorityError, match="metadata"):
            helper._ensure_child(
                parent,
                "worker-repos",
                uid=os.geteuid(),
                gid=os.getegid(),
                mode=0o750,
            )
        assert stat.S_IMODE((tmp_path / "worker-repos").stat().st_mode) == 0o700
        assert not list(tmp_path.glob(".worker-repos.tmp-*"))
    finally:
        os.close(parent.fd)


def test_ensure_child_rejects_existing_metadata_without_mutation(tmp_path: Path) -> None:
    target = tmp_path / "worker-repos"
    target.mkdir(mode=0o700)
    parent = helper._open_absolute(tmp_path)
    try:
        with pytest.raises(helper.AuthorityError, match="metadata"):
            helper._ensure_child(
                parent,
                target.name,
                uid=os.geteuid(),
                gid=os.getegid(),
                mode=0o750,
            )
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
    finally:
        os.close(parent.fd)
