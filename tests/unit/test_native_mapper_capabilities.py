"""Only fixed mapped-root helpers regain their published capabilities."""

import errno
import os
from importlib import import_module

import pytest


@pytest.mark.parametrize("fault", [None, "existing", "unexpected-capability", "symlink", "mode", "unmapped"])
def test_mapper_capabilities_are_fixed_validated_and_read_back(tmp_path, monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_mapper_capabilities")
    root = tmp_path / "rootfs"
    (root / "usr/bin").mkdir(parents=True)
    root.chmod(0o755)
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
        values[helper_name(fd)] = value
        writes.append(helper_name(fd))

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
        with pytest.raises((ValueError, RuntimeError, OSError)):
            module.restore_native_mapper_capabilities(root)
        assert writes == [], "all helper metadata must be validated before any mutation"
