"""A sealed object snapshot covers the exact selected inventory, without mixing attempts."""

from importlib import import_module
from uuid import UUID

import pytest

from loom.personal_dev_storage_object_capture import CapturedStorageObjectV1, StorageObjectCaptureIntentV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_personal_dev_storage_transfer import _recipe


def _inventory():
    module = import_module("loom.personal_dev_storage_snapshot")
    recipe = _recipe()
    objects = tuple(StorageObjectCaptureIntentV1(
        transfer_binding_sha256=canonical_digest(recipe), capture_id=UUID(int=800),
        purpose=purpose, key="task/data", expected_etag='"source-etag"', size_bytes=1,
    ) for purpose in ("artifacts", "tasks", "trajectories"))
    return module.StorageObjectInventoryV1(
        transfer_binding=recipe, capture_id=UUID(int=800),
        scanned_purposes=("tasks", "trajectories", "artifacts"), objects=objects,
    )


def _captures(inventory):
    return tuple(CapturedStorageObjectV1(**item.model_dump(), payload_sha256="a" * 64,
                                       snapshot_version_id="captured-version") for item in inventory.objects)


def test_snapshot_covers_exact_inventory_and_roundtrips_canonically():
    module = import_module("loom.personal_dev_storage_snapshot")
    inventory = _inventory()
    snapshot = module.StorageObjectSnapshotV1(inventory=inventory, captures=_captures(inventory))
    assert module.parse_storage_object_snapshot(
        canonical_bytes(snapshot), expected_sha256=canonical_digest(snapshot),
    ) == snapshot


@pytest.mark.parametrize("change", ("missing", "duplicate", "order", "capture", "transfer", "key", "size", "etag"))
def test_snapshot_rejects_missing_or_substituted_capture(change):
    module = import_module("loom.personal_dev_storage_snapshot")
    inventory = _inventory()
    captures = _captures(inventory)
    if change == "missing":
        captures = captures[:-1]
    elif change == "duplicate":
        captures = (captures[0], *captures)
    elif change == "order":
        captures = captures[::-1]
    else:
        field, value = {
            "capture": ("capture_id", UUID(int=999)), "transfer": ("transfer_binding_sha256", "b" * 64),
            "key": ("key", "different"), "size": ("size_bytes", 2), "etag": ("expected_etag", '"different"'),
        }[change]
        captures = (captures[0].model_copy(update={field: value}), *captures[1:])
    with pytest.raises(ValueError):
        module.StorageObjectSnapshotV1(inventory=inventory, captures=captures)


@pytest.mark.parametrize("change", ("unscanned", "duplicate", "order", "capture", "transfer", "budget"))
def test_inventory_rejects_incomplete_scan_or_cross_binding_and_budget(change):
    module = import_module("loom.personal_dev_storage_snapshot")
    inventory = _inventory()
    values = inventory.model_dump(mode="python")
    if change == "unscanned":
        values["scanned_purposes"] = ("tasks", "artifacts")
    elif change == "duplicate":
        values["objects"] = (*inventory.objects, inventory.objects[0])
    elif change == "order":
        values["objects"] = inventory.objects[::-1]
    elif change == "budget":
        values["objects"] = tuple(inventory.objects[0].model_copy(update={"key": f"{number:02}", "size_bytes": 64 * 1024**3}) for number in range(17))
    else:
        field, value = ("capture_id", UUID(int=999)) if change == "capture" else ("transfer_binding_sha256", "b" * 64)
        values["objects"] = (inventory.objects[0].model_copy(update={field: value}), *inventory.objects[1:])
    with pytest.raises(ValueError):
        module.StorageObjectInventoryV1(**values)


@pytest.mark.parametrize("change", ("digest", "noncanonical", "oversize"))
def test_snapshot_parser_requires_bounded_canonical_digest(change):
    module = import_module("loom.personal_dev_storage_snapshot")
    inventory = _inventory()
    snapshot = module.StorageObjectSnapshotV1(inventory=inventory, captures=_captures(inventory))
    payload, digest = canonical_bytes(snapshot), canonical_digest(snapshot)
    if change == "digest":
        digest = "b" * 64
    elif change == "noncanonical":
        payload += b"\n"
    else:
        payload = b" " * (8 * 1024 * 1024 + 1)
    with pytest.raises(ValueError):
        module.parse_storage_object_snapshot(payload, expected_sha256=digest)
