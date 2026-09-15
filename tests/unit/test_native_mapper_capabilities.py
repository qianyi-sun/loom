"""Only fixed mapped-root helpers regain their published capabilities."""

import errno
import io
import os
from importlib import import_module

import pytest


@pytest.mark.parametrize("fault", [None, "existing", "unexpected-capability", "symlink", "mode", "unmapped",
    "replace-root", "replace-bin", "replace-helper", "second-write"])
def test_mapper_capabilities_are_fixed_validated_and_read_back(tmp_path, monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_mapper_capabilities")
    root = tmp_path / "rootfs"
    (root / "usr/bin").mkdir(parents=True)
    for directory in (root, root / "usr", root / "usr/bin"):
        directory.chmod(0o755)
    for name in ("newuidmap", "newgidmap"):
        target = root / "usr/bin" / name
        target.write_bytes(b"trusted mapped helper")
        target.chmod(0o755)
    expected = {"newuidmap": bytes.fromhex("0100000280000000000000000000000000000000"),
        "newgidmap": bytes.fromhex("0100000240000000000000000000000000000000")}
    values = dict(expected) if fault == "existing" else {}
    if fault == "unexpected-capability":
        values["newgidmap"] = b"not-published"
    if fault == "symlink":
        (root / "usr/bin/newgidmap").unlink()
        (root / "usr/bin/newgidmap").symlink_to("newuidmap")
    if fault == "mode":
        (root / "usr/bin/newgidmap").chmod(0o777)
    writes = []
    original = module.os.fstat

    def mapped_metadata(fd):
        fields = list(original(fd))
        fields[4:6] = [0, 0]
        return os.stat_result(fields)

    def helper_name(fd):
        return os.readlink(f"/proc/self/fd/{fd}").rsplit("/", 1)[-1]

    def getcap(fd, name):
        assert name == "security.capability"
        if helper_name(fd) not in values:
            raise OSError(errno.ENODATA, "no capability")
        return values[helper_name(fd)]

    def setcap(fd, name, value):
        assert name == "security.capability"
        if fault == "second-write" and helper_name(fd) == "newgidmap":
            raise OSError("fixture second write failed")
        values[helper_name(fd)] = value
        writes.append(helper_name(fd))
        if fault == "replace-root" and len(writes) == 1:
            root.rename(tmp_path / "retained-root")
            root.mkdir(mode=0o755)
            (root / "keep").write_text("foreign")
        if fault == "replace-bin" and len(writes) == 1:
            (root / "usr/bin").rename(tmp_path / "retained-bin")
            (root / "usr/bin").mkdir(mode=0o755)
            (root / "usr/bin/keep").write_text("foreign")
        if fault == "replace-helper" and len(writes) == 1:
            helper = root / "usr/bin/newuidmap"
            helper.rename(tmp_path / "retained-helper")
            helper.write_bytes(b"foreign")

    if fault != "unmapped":
        monkeypatch.setattr(module, "_require_mapped_root", lambda: None)
    monkeypatch.setattr(module.os, "fstat", mapped_metadata)
    monkeypatch.setattr(module.os, "getxattr", getcap)
    monkeypatch.setattr(module.os, "setxattr", setcap)
    if fault in {None, "existing"}:
        module.restore_native_mapper_capabilities(root)
        assert values == expected
        assert writes == ([] if fault == "existing" else ["newuidmap", "newgidmap"])
    else:
        message = {"unexpected-capability": "capability changed", "mode": "helper metadata changed",
            "unmapped": "mapped root", "replace-root": "changed", "replace-bin": "directory changed",
            "replace-helper": "helper changed", "second-write": "fixture second write"}.get(fault)
        with pytest.raises((ValueError, RuntimeError, OSError), match=message):
            module.restore_native_mapper_capabilities(root)
        if fault == "replace-root":
            assert list(root.iterdir()) == [root / "keep"]
        elif fault == "replace-bin":
            assert list((root / "usr/bin").iterdir()) == [root / "usr/bin/keep"]
        elif fault == "replace-helper":
            assert (root / "usr/bin/newuidmap").read_bytes() == b"foreign"
        elif fault == "second-write":
            assert writes == ["newuidmap"]
        else:
            assert writes == [], "all helper metadata must be validated before any mutation"


@pytest.mark.parametrize("mapping,valid", [
    (b"0 24850 1\n1 100000 65536\n", True),
    (b"0 0 4294967295\n", False),
    (b"0 24850 65536\n", False),
    (b"1 24850 1\n", False),
    (b"0 invalid 1\n", False),
    (b"", False),
])
def test_capabilities_require_single_noninitial_root_mapping(monkeypatch, mapping, valid):
    module = import_module("loom_capacity_executor.native_mapper_capabilities")
    monkeypatch.setattr(module.os, "geteuid", lambda: 0)
    monkeypatch.setattr(module.os, "getegid", lambda: 0)
    monkeypatch.setattr(module.Path, "open", lambda *args: io.BytesIO(mapping))
    if valid:
        module._require_mapped_root()
    else:
        with pytest.raises(RuntimeError):
            module._require_mapped_root()
