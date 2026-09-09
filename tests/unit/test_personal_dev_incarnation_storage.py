"""Physical storage identity is explicit, immutable and independent of rollout epochs."""

import json
import re
from dataclasses import fields
from importlib import import_module
from uuid import UUID

import pytest

from loom.dev_instance import derive_identity
from loom.personal_dev_capacity_identity import capacity_role_names
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest


def _binding(**changes):
    module = import_module("loom.personal_dev_incarnation_storage")
    values = dict(layout="incarnation-v1", environment_name="alice", subject_id=str(UUID(int=1)),
                  subject_incarnation=str(UUID(int=2)), owner_user_id=str(UUID(int=3)),
                  owner_team_id=str(UUID(int=4)))
    return module.PersonalDevStorageBindingV1.model_validate_json(json.dumps(values | changes))


def test_incarnation_storage_preserves_routes_and_replaces_all_storage_authority():
    binding = _binding()
    identity = binding.identity
    legacy = derive_identity("alice")
    storage = {"database", "db_role", "task_bucket", "trajectories_bucket", "artifacts_bucket", "storage_incarnation"}
    for field in fields(identity):
        if field.name not in storage:
            assert getattr(identity, field.name) == getattr(legacy, field.name)
    assert identity.database == "ld_alice_" + UUID(int=2).hex
    assert identity.db_role == identity.database
    assert identity.storage_incarnation == UUID(int=2)
    assert binding.object_store_identity == ("ld-alice-" + UUID(int=2).hex,) * 2
    assert set(capacity_role_names(identity)).isdisjoint(capacity_role_names(legacy))


@pytest.mark.parametrize("name", ("a", "my-env", "a" * 20))
def test_names_fit_database_and_object_store_limits(name):
    identity = _binding(environment_name=name).identity
    for identifier in (identity.database, identity.db_role, *capacity_role_names(identity)):
        assert len(identifier.encode("ascii")) <= 63
        assert re.fullmatch(r"[a-z][a-z0-9_]*", identifier)
    for bucket in (identity.task_bucket, identity.trajectories_bucket, identity.artifacts_bucket):
        assert 3 <= len(bucket) <= 63
        assert re.fullmatch(r"[a-z][a-z0-9-]*[a-z0-9]", bucket)


@pytest.mark.parametrize("changes", ({"environment_name": "bob"}, {"subject_incarnation": str(UUID(int=9))}))
def test_names_and_incarnations_have_disjoint_storage(changes):
    left, right = _binding().identity, _binding(**changes).identity
    for field in ("database", "db_role", "task_bucket", "trajectories_bucket", "artifacts_bucket"):
        assert getattr(left, field) != getattr(right, field)
    assert set(capacity_role_names(left)).isdisjoint(capacity_role_names(right))
    assert _binding().object_store_identity != _binding(**changes).object_store_identity


def test_legacy_storage_is_explicit_and_unchanged():
    binding = _binding(layout="legacy-name-v1")
    assert binding.identity == derive_identity("alice")
    assert binding.object_store_identity == ("loomdev-alice", "loom-dev-alice")
    assert capacity_role_names(binding.identity) == tuple(
        "loom_cap_alice_" + suffix for suffix in ("owner", "migrator", "agent", "executor", "observer", "runtime")
    )


@pytest.mark.parametrize("change", (
    {"layout": "latest"}, {"environment_name": "shared"}, {"environment_name": "x" * 21},
    {"subject_id": str(UUID(int=0))}, {"subject_incarnation": str(UUID(int=0))},
    {"owner_user_id": str(UUID(int=0))}, {"owner_team_id": str(UUID(int=0))},
    {"database": "loom_staging"}, {"subject_incarnation": 2},
))
def test_binding_rejects_ambiguous_or_overridden_identity(change):
    with pytest.raises(ValueError):
        _binding(**change)


def test_canonical_binding_is_pinned_and_immutable():
    module = import_module("loom.personal_dev_incarnation_storage")
    binding = _binding()
    payload = canonical_bytes(binding)
    assert module.parse_personal_dev_storage_binding(payload, expected_sha256=canonical_digest(binding)) == binding
    for document, digest in ((payload + b"\n", canonical_digest(binding)), (payload, "a" * 64)):
        with pytest.raises(ValueError):
            module.parse_personal_dev_storage_binding(document, expected_sha256=digest)
    with pytest.raises(ValueError):
        binding.environment_name = "bob"
