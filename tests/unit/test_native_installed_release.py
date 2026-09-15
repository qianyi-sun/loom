"""Installed material is authenticated before entering an overflow-UID namespace."""

import hashlib
import json
import os
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def release(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_installed_release")
    # Only ownership/initial namespace are simulated here. Real-root installation
    # coverage belongs to the disposable Docker lane, not a production bypass.
    monkeypatch.setattr(module, "_require_original_identity", lambda: None)
    real_fstat = os.fstat

    def root_metadata(fd):
        value = real_fstat(fd)
        fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
        fields["st_uid"] = fields["st_gid"] = 0
        if value.st_mode & 0o170000 == 0o040000 and Path(os.readlink(f"/proc/self/fd/{fd}")) not in (tmp_path, *tmp_path.rglob("*")):
            fields["st_mode"] &= ~0o7022  # pytest ancestors are not an installed root.
        return SimpleNamespace(**fields)

    monkeypatch.setattr(module.os, "fstat", root_metadata)
    files = {}
    for name in ("gvisor/runsc", "gvisor/containerd-shim-runsc-v1", "gvisor/gvisor-bin/checkpointgofer",
        "gvisor/gvisor-bin/gvisor_sentry", "gvisor/gvisor-bin/runsc-metric-server",
        "python/bin/python3", "python/lib/loom_capacity_executor/__init__.py",
        "rootlesskit", "rootfs.tar", "seccomp.json"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        for directory in (path.parent, *path.parent.parents):
            if directory == tmp_path:
                break
            directory.chmod(0o755)
        content = b"{}" if name == "seccomp.json" else name.encode()
        path.write_bytes(content)
        mode = 0o444 if name.endswith((".tar", ".json", ".py")) else 0o555
        path.chmod(mode)
        files[str(path)] = dict(schema_version=1, path=str(path), sha256=hashlib.sha256(content).hexdigest(), size_bytes=len(content), mode=mode)
    wire = dict(schema_version=1, source_sha="a" * 40, platform="linux/amd64",
        runsc_root=str(tmp_path / "gvisor"), python_root=str(tmp_path / "python"),
        python=str(tmp_path / "python/bin/python3"), rootlesskit=str(tmp_path / "rootlesskit"),
        rootfs=str(tmp_path / "rootfs.tar"), seccomp=str(tmp_path / "seccomp.json"),
        files=sorted(files.values(), key=lambda f: f["path"]))

    def check(*, mutate=None, **kwargs):
        document = json.loads(json.dumps(wire))
        if mutate:
            mutate(document)
        content = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
        manifest = tmp_path / "release.json"
        if manifest.exists():
            manifest.chmod(0o600)
        manifest.write_bytes(content)
        manifest.chmod(0o444)
        return module.verify_native_installed_release(manifest,
            expected_sha256=kwargs.pop("expected_sha256", hashlib.sha256(content).hexdigest()),
            expected_source_sha=kwargs.pop("expected_source_sha", "a" * 40),
            expected_platform=kwargs.pop("expected_platform", "linux/amd64"), **kwargs)

    return module, tmp_path, check


def test_exact_release_returns_bound_material(release):
    _module, root, check = release
    result = check()
    assert result.manifest.rootfs == str(root / "rootfs.tar")
    assert result.client_seccomp == b"{}"
    assert result.manifest.runsc_root == str(root / "gvisor")


@pytest.mark.parametrize("fault", ["extra-helper", "missing-helper", "modified-helper", "writable-file",
    "writable-parent", "symlink-file", "symlink-parent", "hardlink", "wrong-source", "wrong-platform", "wrong-digest"])
def test_untrusted_or_incomplete_release_rejected(release, fault):
    _module, root, check = release
    helper = root / "gvisor/gvisor-bin/gvisor_sentry"
    kwargs = {}
    if fault == "extra-helper":
        (helper.parent / "unexpected").write_bytes(b"extra")
    elif fault == "missing-helper":
        helper.unlink()
    elif fault == "modified-helper":
        helper.chmod(0o755)
        helper.write_bytes(b"x" * helper.stat().st_size)
        helper.chmod(0o555)
    elif fault == "writable-file":
        helper.chmod(0o755)
    elif fault == "writable-parent":
        helper.parent.chmod(0o777)
    elif fault == "symlink-file":
        helper.unlink()
        helper.symlink_to(root / "rootlesskit")
    elif fault == "symlink-parent":
        helper.parent.rename(root / "moved")
        helper.parent.symlink_to(root / "moved", target_is_directory=True)
    elif fault == "hardlink":
        os.link(helper, root / "other")
    elif fault == "wrong-source":
        kwargs["expected_source_sha"] = "b" * 40
    elif fault == "wrong-platform":
        kwargs["expected_platform"] = "linux/arm64"
    elif fault == "wrong-digest":
        kwargs["expected_sha256"] = "f" * 64
    with pytest.raises((ValueError, OSError)):
        check(**kwargs)


@pytest.mark.parametrize("fault", ["duplicate", "traversal", "missing-role", "missing-published-helper", "size-bound", "mutable-mode"])
def test_manifest_contract_rejects_incomplete_inventory(release, fault):
    _module, _root, check = release

    def mutate(wire):
        if fault == "duplicate":
            wire["files"].append(wire["files"][0])
        elif fault == "traversal":
            wire["files"][0]["path"] += "/../file"
        elif fault == "missing-role":
            wire["files"] = [f for f in wire["files"] if f["path"] != wire["python"]]
        elif fault == "missing-published-helper":
            wire["files"] = [f for f in wire["files"] if not f["path"].endswith("checkpointgofer")]
        elif fault == "size-bound":
            wire["files"][0]["size_bytes"] = 100 * 1024**3
        elif fault == "mutable-mode":
            wire["files"][0]["mode"] = 0o755

    with pytest.raises(ValueError):
        check(mutate=mutate)


@pytest.mark.parametrize("fault", ["root", "mapped", "overflow"])
def test_original_identity_required_before_filesystem_reads(monkeypatch, fault):
    module = import_module("loom_capacity_executor.native_installed_release")
    monkeypatch.setattr(module.os, "getuid", lambda: 0 if fault == "root" else 1000)
    monkeypatch.setattr(module.os, "geteuid", lambda: 0 if fault == "root" else 1000)
    monkeypatch.setattr(module.os, "getgid", lambda: 1000)
    monkeypatch.setattr(module.os, "getegid", lambda: 1000)
    monkeypatch.setattr(module, "_read_identity_map", lambda _name: "0 1000 1\n" if fault == "mapped" else "0 0 65535\n")
    with pytest.raises(ValueError, match="original"):
        module.verify_native_installed_release(Path("/nonexistent"), expected_sha256="a" * 64,
            expected_source_sha="a" * 40, expected_platform="linux/amd64")


@pytest.mark.parametrize("fault", ["file-owner", "parent-owner", "replace-file", "replace-parent", "rewrite", "extra-after-scan"])
def test_observation_rejects_ownership_and_inflight_replacements(release, monkeypatch, fault):
    module, root, check = release
    helper = root / "gvisor/gvisor-bin/gvisor_sentry"
    if fault in {"file-owner", "parent-owner"}:
        before = module.os.fstat

        def wrong_owner(fd):
            metadata = before(fd)
            if Path(os.readlink(f"/proc/self/fd/{fd}")) == (helper if fault == "file-owner" else helper.parent):
                metadata.st_uid = 1000
            return metadata

        monkeypatch.setattr(module.os, "fstat", wrong_owner)
    else:
        before = module.os.read
        changed = False

        def racing_read(fd, count):
            nonlocal changed
            content = before(fd, count)
            if not changed and Path(os.readlink(f"/proc/self/fd/{fd}")) == helper:
                changed = True
                if fault == "replace-file":
                    helper.rename(helper.with_suffix(".original"))
                    helper.write_bytes(content)
                    helper.chmod(0o555)
                elif fault == "replace-parent":
                    helper.parent.rename(root / "old-helpers")
                    helper.parent.mkdir(mode=0o755)
                elif fault == "rewrite":
                    helper.chmod(0o755)
                    helper.write_bytes(b"x" * len(content))
                    helper.chmod(0o555)
                else:
                    (helper.parent / "unlisted").write_bytes(b"late")
            return content

        monkeypatch.setattr(module.os, "read", racing_read)
    with pytest.raises((ValueError, OSError)):
        check()


def test_unrelated_sibling_activity_does_not_invalidate_protected_paths(release, monkeypatch):
    module, root, check = release
    original = module.os.read
    changed = False

    def read(fd, count):
        nonlocal changed
        result = original(fd, count)
        if not changed and os.readlink(f"/proc/self/fd/{fd}") == str(root / "gvisor/runsc"):
            changed = True
            # Other installations may change a common protected ancestor's
            # directory timestamps without replacing our tree or any material.
            (root / "unrelated-sibling").mkdir(mode=0o755)
        return result

    monkeypatch.setattr(module.os, "read", read)
    assert check().manifest.rootfs == str(root / "rootfs.tar")
