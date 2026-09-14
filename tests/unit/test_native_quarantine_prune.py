"""Metadata-only pruning is bounded to an already fenced, quarantined inode."""

import os
from contextlib import contextmanager
from importlib import import_module

import pytest


@contextmanager
def fixture(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_quarantine_prune")
    root = tmp_path / "quarantined"
    root.mkdir(mode=0o700)
    (root / "recovery.json").write_bytes(b"retained-locator")
    monkeypatch.setattr(module, "_require_initial_root", lambda: None)
    uid, gid = os.getuid() or 24850, os.getgid() or 24851
    if os.getuid() == 0 or os.getgid() == 0:
        from types import SimpleNamespace

        actual_stat, actual_fstat = os.stat, os.fstat

        def mapped(metadata):
            fields = {name: getattr(metadata, name) for name in dir(metadata) if name.startswith("st_")}
            fields["st_uid"] = uid if metadata.st_uid == 0 else metadata.st_uid
            fields["st_gid"] = gid if metadata.st_gid == 0 else metadata.st_gid
            return SimpleNamespace(**fields)
        monkeypatch.setattr(module.os, "stat", lambda *args, **kwargs: mapped(actual_stat(*args, **kwargs)))
        monkeypatch.setattr(module.os, "fstat", lambda *args, **kwargs: mapped(actual_fstat(*args, **kwargs)))
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(fd)
        identity = module.NativeQuarantineIdentity(device=metadata.st_dev, inode=metadata.st_ino,
            mount_id=module._mount_id(fd), uid_ranges=((uid, 1),), gid_ranges=((gid, 1),))
        yield module, root, fd, identity
    finally:
        os.close(fd)


def test_prune_keeps_locator_and_never_follows_links_or_reads_files(tmp_path, monkeypatch):
    with fixture(tmp_path, monkeypatch) as (module, root, fd, identity):
        outside = tmp_path / "foreign"
        outside.write_bytes(b"unrelated-data")
        (root / "link").symlink_to(outside)
        (root / "hardlink").hardlink_to(outside)
        (root / "dir").mkdir()
        (root / "dir/data").write_bytes(b"payload")
        (root / "dir/data").chmod(0o000)
        os.mkfifo(root / "fifo")
        module.prune_native_quarantine(fd, identity=identity)
        assert [item.name for item in root.iterdir()] == ["recovery.json"]
        assert outside.read_bytes() == b"unrelated-data" and outside.stat().st_nlink == 1


@pytest.mark.parametrize("changed", ["device", "inode", "mount_id", "uid_ranges", "gid_ranges"])
def test_prune_rejects_changed_root_before_deletion(tmp_path, monkeypatch, changed):
    from dataclasses import replace

    with fixture(tmp_path, monkeypatch) as (module, root, fd, identity):
        (root / "data").write_bytes(b"kept")
        value = ((999999, 1),) if changed.endswith("ranges") else getattr(identity, changed) + 1
        with pytest.raises(ValueError, match=r"identity|ownership|mount"):
            module.prune_native_quarantine(fd, identity=replace(identity, **{changed: value}))
        assert (root / "data").read_bytes() == b"kept"


def test_foreign_mount_is_never_traversed(tmp_path, monkeypatch):
    with fixture(tmp_path, monkeypatch) as (module, root, fd, identity):
        (root / "mounted").mkdir()
        (root / "mounted/data").write_bytes(b"kept")
        original = module._mount_id
        foreign_inode = (root / "mounted").stat().st_ino
        monkeypatch.setattr(module, "_mount_id", lambda descriptor: identity.mount_id + 1
            if os.fstat(descriptor).st_ino == foreign_inode else original(descriptor))
        with pytest.raises(ValueError, match="mount"):
            module.prune_native_quarantine(fd, identity=identity)
        assert (root / "mounted/data").read_bytes() == b"kept"


def test_foreign_owned_descendant_is_retained(tmp_path, monkeypatch):
    from types import SimpleNamespace

    with fixture(tmp_path, monkeypatch) as (module, root, fd, identity):
        (root / "foreign").write_bytes(b"kept")
        inode = (root / "foreign").stat().st_ino
        actual_fstat = os.fstat

        def foreign(descriptor):
            metadata = actual_fstat(descriptor)
            if metadata.st_ino != inode:
                return metadata
            return SimpleNamespace(**{name: (999999 if name == "st_uid" else getattr(metadata, name))
                for name in dir(metadata) if name.startswith("st_")})
        monkeypatch.setattr(module.os, "fstat", foreign)
        with pytest.raises(ValueError, match="ownership"):
            module.prune_native_quarantine(fd, identity=identity)
        assert (root / "foreign").read_bytes() == b"kept"


def test_prune_budget_retains_locator_and_can_resume(tmp_path, monkeypatch):
    with fixture(tmp_path, monkeypatch) as (module, root, fd, identity):
        for index in range(5):
            (root / str(index)).write_bytes(b"data")
        monkeypatch.setattr(module, "_MAX_ENTRIES", 2)
        for expected_remaining in (3, 1):
            with pytest.raises(ValueError, match="bound"):
                module.prune_native_quarantine(fd, identity=identity)
            assert len(list(root.iterdir())) == expected_remaining + 1
        assert (root / "recovery.json").read_bytes() == b"retained-locator"
        module.prune_native_quarantine(fd, identity=identity)
        assert [item.name for item in root.iterdir()] == ["recovery.json"]


def test_prune_does_not_change_modes(tmp_path, monkeypatch):
    with fixture(tmp_path, monkeypatch) as (module, root, fd, identity):
        (root / "data").write_bytes(b"data")
        monkeypatch.setattr(module.os, "chmod", lambda *args, **kwargs: pytest.fail("chmod is forbidden"))
        monkeypatch.setattr(module.os, "fchmod", lambda *args, **kwargs: pytest.fail("fchmod is forbidden"))
        monkeypatch.setattr(module.os, "chown", lambda *args, **kwargs: pytest.fail("chown is forbidden"))
        module.prune_native_quarantine(fd, identity=identity)
        assert [item.name for item in root.iterdir()] == ["recovery.json"]
