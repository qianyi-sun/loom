"""Protected rootfs bytes must not become unconstrained host archive extraction."""

import hashlib
import io
import os
import tarfile
from importlib import import_module

import pytest


def rootfs_archive(path, *, fault=None):
    with tarfile.open(path, "w") as archive:
        for name, kind, data in (
            ("usr", tarfile.DIRTYPE, b""),
            ("usr/bin", tarfile.DIRTYPE, b""),
            ("usr/bin/tool", tarfile.REGTYPE, b"trusted tool"),
            ("bin", tarfile.SYMTYPE, b"/usr/bin"),
        ):
            member = tarfile.TarInfo(name)
            member.type, member.uid, member.gid, member.mode = kind, os.getuid(), os.getgid(), 0o755
            if member.isfile():
                member.size = len(data)
            if member.issym():
                member.linkname = data.decode()
            archive.addfile(member, io.BytesIO(data) if member.isfile() else None)
        if fault:
            member = tarfile.TarInfo({"duplicate": "usr/bin/tool", "traversal": "../outside",
                "absolute": "/outside", "symlink-parent": "bin/escaped"}.get(fault, "extra"))
            member.uid, member.gid = os.getuid(), os.getgid()
            if fault == "device":
                member.type = tarfile.CHRTYPE
            elif fault == "hardlink":
                member.type, member.linkname = tarfile.LNKTYPE, "usr/bin/tool"
            elif fault == "escaping-link":
                member.type, member.linkname = tarfile.SYMTYPE, "../outside"
            archive.addfile(member)
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("fault", [None, "digest", "size", "unpacked-limit", "entry-limit", "duplicate",
    "traversal", "absolute", "symlink-parent", "device", "hardlink", "escaping-link", "reused"])
def test_bounded_rootfs_preserves_guest_links_without_following_them(tmp_path, fault):
    module = import_module("loom_capacity_executor.native_rootfs_archive")
    archive = tmp_path / "rootfs.tar"
    digest = rootfs_archive(archive, fault=fault)
    destination = tmp_path / "rootfs"
    if fault == "reused":
        destination.mkdir()
        (destination / "keep").write_text("foreign")
    arguments = dict(archive=archive, destination=destination,
        expected_sha256="f" * 64 if fault == "digest" else digest,
        expected_size_bytes=archive.stat().st_size + (1 if fault == "size" else 0),
        max_unpacked_bytes=1 if fault == "unpacked-limit" else 1024,
        max_entries=1 if fault == "entry-limit" else 100)
    if fault is None:
        result = module.unpack_native_rootfs_archive(**arguments)
        assert result.unpacked_bytes == len(b"trusted tool")
        assert result.entries == 4
        assert (destination / "usr/bin/tool").read_bytes() == b"trusted tool"
        assert os.readlink(destination / "bin") == "/usr/bin"
        assert (destination / "usr/bin/tool").stat().st_mode & 0o777 == 0o755
    else:
        with pytest.raises((ValueError, OSError)):
            module.unpack_native_rootfs_archive(**arguments)
        if fault == "reused":
            assert (destination / "keep").read_text() == "foreign"
        else:
            assert not destination.exists(), "invalid material created a destination"


def test_partial_rootfs_failure_does_not_remove_replacement_directory(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_rootfs_archive")
    archive = tmp_path / "rootfs.tar"
    digest = rootfs_archive(archive)
    destination, moved = tmp_path / "rootfs", tmp_path / "moved"
    original = module.os.write
    replaced = False

    def failed_write(fd, data):
        nonlocal replaced
        if not replaced:
            replaced = True
            destination.rename(moved)
            destination.mkdir()
            (destination / "keep").write_text("foreign")
            raise OSError("fixture copy failed")
        return original(fd, data)

    monkeypatch.setattr(module.os, "write", failed_write)
    with pytest.raises(OSError, match="fixture copy failed"):
        module.unpack_native_rootfs_archive(archive=archive, destination=destination,
            expected_sha256=digest, expected_size_bytes=archive.stat().st_size,
            max_unpacked_bytes=1024, max_entries=100)
    assert (destination / "keep").read_text() == "foreign"


@pytest.mark.parametrize("replacement", ["root", "file"])
def test_rootfs_rejects_replaced_created_material(tmp_path, monkeypatch, replacement):
    module = import_module("loom_capacity_executor.native_rootfs_archive")
    archive = tmp_path / "rootfs.tar"
    digest = rootfs_archive(archive)
    destination = tmp_path / "rootfs"
    changed = False
    original_open, original_sync = module.os.open, module.os.fsync

    def opened(path, flags, *args, **kwargs):
        nonlocal changed
        if replacement == "root" and path == "rootfs" and flags & os.O_DIRECTORY and not changed:
            changed = True
            destination.rename(tmp_path / "retained-root")
            destination.mkdir(mode=0o700)
            (destination / "keep").write_text("foreign")
        return original_open(path, flags, *args, **kwargs)

    def synced(fd):
        nonlocal changed
        original_sync(fd)
        target = destination / "usr/bin/tool"
        if replacement == "file" and not changed and target.exists():
            changed = True
            target.rename(tmp_path / "retained-file")
            target.write_bytes(b"foreign file")

    monkeypatch.setattr(module.os, "open", opened)
    monkeypatch.setattr(module.os, "fsync", synced)
    with pytest.raises(ValueError, match="changed"):
        module.unpack_native_rootfs_archive(archive=archive, destination=destination,
            expected_sha256=digest, expected_size_bytes=archive.stat().st_size,
            max_unpacked_bytes=1024, max_entries=100)
    if replacement == "root":
        assert list(destination.iterdir()) == [destination / "keep"]
        assert destination.stat().st_mode & 0o777 == 0o700
    else:
        assert (destination / "usr/bin/tool").read_bytes() == b"foreign file"


def test_rootfs_failure_cleans_owned_readonly_directories(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_rootfs_archive")
    archive = tmp_path / "rootfs.tar"
    with tarfile.open(archive, "w") as output:
        for name, kind in (("private", tarfile.DIRTYPE), ("private/file", tarfile.REGTYPE)):
            member = tarfile.TarInfo(name)
            member.type, member.uid, member.gid, member.mode = kind, os.getuid(), os.getgid(), 0o555
            output.addfile(member)
    destination = tmp_path / "rootfs"

    def failed_verification(*args):
        raise OSError("fixture verification read failed")

    monkeypatch.setattr(module, "_verify_members", failed_verification)
    with pytest.raises(OSError, match="fixture verification"):
        module.unpack_native_rootfs_archive(archive=archive, destination=destination,
            expected_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), expected_size_bytes=archive.stat().st_size,
            max_unpacked_bytes=1024, max_entries=100)
    assert not destination.exists()


@pytest.mark.parametrize("fault", ["metadata-limit", "changed-during-copy"])
def test_rootfs_rejects_excessive_metadata_and_mutating_archive(tmp_path, monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_rootfs_archive")
    archive = tmp_path / "rootfs.tar"
    if fault == "metadata-limit":
        with tarfile.open(archive, "w", format=tarfile.PAX_FORMAT) as output:
            member = tarfile.TarInfo("file")
            member.pax_headers = {"comment": "a" * (1024**2 + 1)}
            output.addfile(member)
    else:
        rootfs_archive(archive)
        original = module.os.fsync
        changed = False

        def mutate(fd):
            nonlocal changed
            original(fd)
            if not changed:
                changed = True
                with archive.open("ab") as output:
                    output.write(b"changed")

        monkeypatch.setattr(module.os, "fsync", mutate)
    destination = tmp_path / "rootfs"
    with pytest.raises(ValueError):
        module.unpack_native_rootfs_archive(archive=archive, destination=destination,
            expected_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), expected_size_bytes=archive.stat().st_size,
            max_unpacked_bytes=1024, max_entries=100)
    assert not destination.exists()
