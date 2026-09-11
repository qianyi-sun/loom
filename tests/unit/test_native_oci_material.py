"""Mapped material assembly creates only fixed, fresh runtime resources."""

import hashlib
import json
import os
from importlib import import_module

import pytest

from loom_capacity_executor.native_oci_bundles import NativeOciBundlePolicy
from tests.unit.test_native_rootless_runtime import spec_file


@pytest.mark.parametrize("fault", [None, "reused-output", "reused-bundles", "write-error",
    "replace-root", "replace-role", "replace-config", "replace-output", "short-write", "replace-before-open"])
def test_mapped_oci_material_is_fixed_readonly_and_failure_scoped(tmp_path, monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_oci_material")
    _runtime, spec, _path, _digest = spec_file(tmp_path)
    rootfs = tmp_path / "rootfs"
    rootfs.mkdir(mode=0o755)
    bundle_root = tmp_path / "bundles"
    workspace = tmp_path / "work"
    (workspace / "input").mkdir()
    (workspace / "input/keep").write_text("staged source")
    seccomp = b'{"defaultAction":"SCMP_ACT_ERRNO","syscalls":[{"names":["read"],"action":"SCMP_ACT_ALLOW"}]}'
    policy = NativeOciBundlePolicy(rootfs=rootfs, workspace=workspace,
        client_seccomp=seccomp, client_seccomp_sha256=hashlib.sha256(seccomp).hexdigest(),
        tmp_bytes=1024**2, buildkit_state_bytes=1024**2)
    foreign = workspace / "output" if fault == "reused-output" else bundle_root
    if fault in {"reused-output", "reused-bundles"}:
        foreign.mkdir()
        (foreign / "keep").write_text("foreign")
    original_fstat = module.os.fstat
    owners = {}

    def metadata(fd):
        result = original_fstat(fd)
        fields = list(result)
        fields[4:6] = owners.get((result.st_dev, result.st_ino), (0, 0))
        return os.stat_result(fields)

    def chown(fd, uid, gid):
        result = original_fstat(fd)
        owners[(result.st_dev, result.st_ino)] = (uid, gid)

    monkeypatch.setattr(module, "_require_mapped_root", lambda: None)
    monkeypatch.setattr(module.os, "fstat", metadata)
    monkeypatch.setattr(module.os, "fchown", chown)
    original_write = module.os.write
    replacement = None
    if fault == "replace-before-open":
        original_open = module.os.open
        replacement = workspace / "output"
        replaced = False

        def replace_before_open(path, flags, *args, **kwargs):
            nonlocal replaced
            if path == "output" and not replaced:
                replaced = True
                replacement.rename(workspace / "output-original")
                replacement.mkdir(mode=0o755)
                (replacement / "keep").write_text("foreign")
            return original_open(path, flags, *args, **kwargs)

        monkeypatch.setattr(module.os, "open", replace_before_open)
    elif fault is not None and fault.startswith("replace-"):
        replacement = {"replace-root": bundle_root, "replace-role": bundle_root / "pause",
            "replace-config": bundle_root / "pause/config.json", "replace-output": workspace / "output"}[fault]
        replaced = False

        def replace_during_write(fd, wire):
            nonlocal replaced
            if not replaced:
                replaced = True
                replacement.rename(replacement.with_name(replacement.name + "-original"))
                if fault == "replace-config":
                    replacement.write_text("foreign")
                else:
                    replacement.mkdir()
                    (replacement / "keep").write_text("foreign")
            return original_write(fd, wire)

        monkeypatch.setattr(module.os, "write", replace_during_write)
    if fault == "short-write":
        monkeypatch.setattr(module.os, "write", lambda fd, wire: original_write(fd, wire[:7]))
    if fault == "write-error":
        def failed_write(*args):
            raise OSError("fixture bundle write failed")
        monkeypatch.setattr(module.os, "write", failed_write)
    if fault in {None, "short-write"}:
        bundles = module.prepare_native_oci_material(spec.context, policy=policy, bundle_root=bundle_root)
        for role in ("pause", "buildkit", "client"):
            target = bundle_root / role / "config.json"
            assert target.read_bytes() == getattr(bundles, role)
            assert target.stat().st_mode & 0o777 == 0o444
            assert target.parent.stat().st_mode & 0o777 == 0o555
        client = json.loads((bundle_root / "client/config.json").read_bytes())
        assert client["process"]["args"][2] == "loom.personal_dev_sandbox_builder"
        output = workspace / "output"
        assert output.stat().st_mode & 0o777 == 0o700
        assert owners[(output.stat().st_dev, output.stat().st_ino)] == (1000, 1000)
        assert (workspace / "buildkit-run").stat().st_mode & 0o7777 == 0o1777
    else:
        with pytest.raises((ValueError, OSError)):
            module.prepare_native_oci_material(spec.context, policy=policy, bundle_root=bundle_root)
        if fault in {"reused-output", "reused-bundles"}:
            assert (foreign / "keep").read_text() == "foreign"
        if replacement is not None:
            kept = replacement if fault == "replace-config" else replacement / "keep"
            assert kept.read_text() == "foreign"
            if fault == "replace-before-open":
                assert replacement.stat().st_mode & 0o777 == 0o755
                assert (replacement.stat().st_dev, replacement.stat().st_ino) not in owners
        if fault not in {"reused-bundles", "replace-root", "replace-role", "replace-config"}:
            assert not bundle_root.exists()
        if fault not in {"reused-output", "replace-output", "replace-before-open"}:
            assert not (workspace / "output").exists()
        assert not (workspace / "buildkit-run").exists()
    assert (workspace / "input/keep").read_text() == "staged source"
