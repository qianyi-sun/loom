"""Protected recovery inventory is loaded before constructing management senders."""

import json
from hashlib import sha256
from importlib import import_module

import pytest

from loom_capacity_manager.contracts import canonical_bytes
from tests.unit.test_native_recovery_sender import target


def test_inventory_rejects_duplicate_targets_and_workload_principal():
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
