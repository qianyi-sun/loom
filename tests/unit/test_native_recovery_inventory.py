"""Protected recovery inventory is loaded before constructing management senders."""

import json
import os
from hashlib import sha256
from importlib import import_module

import pytest

from loom_capacity_manager.contracts import canonical_bytes
from tests.unit.test_native_installed_release import release as release
from tests.unit.test_native_recovery_sender import target


def test_inventory_rejects_duplicate_targets_and_empty_scope():
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    item = target(module)
    inventory = module.NativeRecoveryInventoryV1(targets=(item,))
    assert inventory.targets == (item,)
    with pytest.raises(ValueError):
        module.NativeRecoveryInventoryV1(targets=(item, item))
    with pytest.raises(ValueError):
        module.NativeRecoveryInventoryV1(targets=())


@pytest.mark.parametrize("fault", ["exact", "digest", "noncanonical", "extra", "oversized"])
def test_inventory_loader_pins_protected_bytes(monkeypatch, fault):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    configured = module.NativeRecoveryInventoryV1(targets=(target(module),))
    wire = canonical_bytes(configured)
    expected = sha256(wire).hexdigest()
    if fault == "noncanonical":
        wire += b"\n"
    elif fault == "extra":
        wire = json.dumps(json.loads(wire) | {"command": "arbitrary"}).encode()
    elif fault == "oversized":
        wire = b" " * (4 * 1024**2 + 1)
    calls = []

    class Observation:
        def read(self, path, **kwargs):
            calls.append((str(path), kwargs))
            assert kwargs["mode"] == 0o444 and kwargs["bound"] == 4 * 1024**2
            assert kwargs["collect"] is True
            # Real protected reader validates the bytes, digest, file metadata,
            # and all root-owned ancestors; do not bypass that contract here.
            if sha256(wire).hexdigest() != kwargs["digest"] or len(wire) > kwargs["bound"]:
                raise ValueError("protected inventory bytes changed")
            return wire

        def finish(self):
            calls.append("finished")

    monkeypatch.setattr(module, "_Observation", Observation)
    from pathlib import Path

    if fault == "exact":
        observed = module.read_native_recovery_inventory(Path("/etc/loom/recovery.json"), expected_sha256=expected)
        assert observed == configured and calls[-1] == "finished"
    else:
        with pytest.raises(ValueError):
            module.read_native_recovery_inventory(Path("/etc/loom/recovery.json"),
                expected_sha256="f" * 64 if fault == "digest" else sha256(wire).hexdigest())


@pytest.mark.parametrize("fault", ["exact", "mode", "hardlink", "symlink", "parent-mode", "owner", "rotate"])
def test_inventory_uses_real_protected_file_reader(release, monkeypatch, fault):
    reader, root, _ = release  # Only installed ownership and pytest ancestry are simulated.
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    configured = module.NativeRecoveryInventoryV1(targets=(target(module),))
    wire = canonical_bytes(configured)
    path = root / "inventory.json"
    path.write_bytes(wire)
    path.chmod(0o444)
    if fault == "mode":
        path.chmod(0o644)
    elif fault == "hardlink":
        (root / "alias").hardlink_to(path)
    elif fault == "symlink":
        link = root / "link"
        link.symlink_to(path)
        path = link
    elif fault == "parent-mode":
        root.chmod(0o777)
    elif fault == "owner":
        original = reader.os.fstat

        def metadata(fd):
            observed = original(fd)
            if os.readlink(f"/proc/self/fd/{fd}") == str(path):
                observed.st_uid = 1000
            return observed

        monkeypatch.setattr(reader.os, "fstat", metadata)
    elif fault == "rotate":
        original_read = reader._Observation.read

        def read(self, *args, **kwargs):
            result = original_read(self, *args, **kwargs)
            path.rename(root / "old-inventory")
            path.write_bytes(wire)
            path.chmod(0o444)
            return result

        monkeypatch.setattr(reader._Observation, "read", read)
    if fault == "exact":
        assert module.read_native_recovery_inventory(path, expected_sha256=sha256(wire).hexdigest()) == configured
        sender = module.NativeRecoverySender.from_installed_inventory(session_factory=object(), installation=object(),
            inventory_path=path, inventory_sha256=sha256(wire).hexdigest())
        assert sender._targets == configured.targets
    else:
        with pytest.raises((ValueError, OSError)):
            module.read_native_recovery_inventory(path, expected_sha256=sha256(wire).hexdigest())
