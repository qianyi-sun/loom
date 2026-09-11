"""A retained-data recipe is exact owner lineage, never a transfer capability."""

from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


def _recipe(**changes):
    source = _bound_claim().operation.storage_binding
    values = {
        "source": source,
        "destination": source.model_copy(update={"subject_incarnation": UUID(int=500)}),
        "source_destroy_operation_id": UUID(int=501),
        "source_destroy_operation_epoch": 3,
        "source_release_sha256": "a" * 64,
        "destination_operation_id": UUID(int=502),
        "destination_operation_epoch": 4,
    }
    values.update(changes)
    module = import_module("loom.personal_dev_storage_transfer")
    return module.PersonalDevStorageTransferBindingV1(**values)


def test_transfer_roundtrip_maps_only_exact_incarnation_buckets():
    module = import_module("loom.personal_dev_storage_transfer")
    recipe = _recipe()
    assert module.parse_storage_transfer_binding(
        canonical_bytes(recipe), expected_sha256=canonical_digest(recipe),
    ) == recipe
    source, destination = recipe.source.identity, recipe.destination.identity
    assert module.storage_transfer_bucket_pairs(recipe) == (
        ("tasks", source.task_bucket, destination.task_bucket),
        ("trajectories", source.trajectories_bucket, destination.trajectories_bucket),
        ("artifacts", source.artifacts_bucket, destination.artifacts_bucket),
    )


@pytest.mark.parametrize("field,value", (
    ("owner_user_id", UUID(int=700)), ("owner_team_id", UUID(int=700)),
    ("subject_id", UUID(int=700)), ("environment_name", "bob"),
    ("layout", "legacy-name-v1"), ("subject_incarnation", UUID(int=0)),
))
def test_transfer_rejects_foreign_or_legacy_destination(field, value):
    source = _bound_claim().operation.storage_binding
    destination = source.model_copy(update={"subject_incarnation": UUID(int=500), field: value})
    with pytest.raises(ValueError):
        _recipe(destination=destination)


@pytest.mark.parametrize("change", (
    {"destination_operation_epoch": 3}, {"source_destroy_operation_epoch": 0},
    {"source_destroy_operation_epoch": True}, {"source_release_sha256": "0" * 64},
    {"source_release_sha256": "not-a-digest"}, {"source_destroy_operation_id": UUID(int=0)},
    {"destination_operation_id": UUID(int=501)}, {"destination_operation_id": UUID(int=0)},
    {"bucket_override": "shared-bucket"},
))
def test_transfer_rejects_missing_or_reused_operation_coordinates(change):
    with pytest.raises(ValueError):
        _recipe(**change)


def test_transfer_cannot_reuse_source_storage_or_adopt_legacy_source():
    source = _bound_claim().operation.storage_binding
    with pytest.raises(ValueError):
        _recipe(destination=source)
    with pytest.raises(ValueError):
        _recipe(source=source.model_copy(update={"layout": "legacy-name-v1"}))


@pytest.mark.parametrize("change", ("digest", "whitespace", "duplicate", "oversize"))
def test_persisted_transfer_requires_exact_bounded_canonical_recipe(change):
    module = import_module("loom.personal_dev_storage_transfer")
    recipe = _recipe()
    payload, digest = canonical_bytes(recipe), canonical_digest(recipe)
    if change == "digest":
        digest = "b" * 64
    elif change == "whitespace":
        payload += b"\n"
    elif change == "duplicate":
        payload = b'{"schema_version":1,' + payload[1:]
    else:
        payload = b" " * (32 * 1024 + 1)
    with pytest.raises(ValueError):
        module.parse_storage_transfer_binding(payload, expected_sha256=digest)


def test_bucket_resolution_revalidates_unchecked_model_copies():
    module = import_module("loom.personal_dev_storage_transfer")
    recipe = _recipe()
    forged = recipe.model_copy(update={"destination": recipe.source})
    with pytest.raises(ValueError):
        module.storage_transfer_bucket_pairs(forged)
