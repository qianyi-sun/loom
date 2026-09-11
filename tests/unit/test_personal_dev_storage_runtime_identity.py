"""Every external storage consumer must resolve the same current owner claim."""

from dataclasses import replace
from importlib import import_module
from uuid import uuid4

import pytest

from loom.dev_instance import derive_identity
from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1
from tests.unit.test_personal_dev_reconciler import _claim


def _bound_claim():
    claim = _claim()
    operation = claim.operation
    binding = PersonalDevStorageBindingV1(
        layout="incarnation-v1", environment_name=operation.environment_name,
        subject_id=operation.subject_id, subject_incarnation=operation.subject_incarnation,
        owner_user_id=operation.owner_user_id, owner_team_id=operation.owner_team_id,
    )
    return replace(claim, operation=replace(operation, storage_binding=binding),
                   environment=replace(claim.environment, storage_binding=binding))


def test_resolved_storage_identity_carries_full_owner_binding():
    module = import_module("loom.personal_dev_incarnation_storage")
    claim = _bound_claim()
    identity = module.resolve_personal_dev_storage_identity(claim)
    assert identity == claim.operation.storage_binding.identity
    assert identity.storage_binding == claim.operation.storage_binding
    assert identity.storage_incarnation == claim.operation.subject_incarnation
    assert module.validate_personal_dev_storage_identity(identity) == identity
    legacy = module.resolve_personal_dev_storage_identity(_claim())
    assert legacy == derive_identity("alice")
    assert legacy.storage_binding is None


@pytest.mark.parametrize("part,field,value", (
    ("environment", "storage_binding", None),
    ("operation", "storage_binding", None),
    ("environment", "operation_id", uuid4()),
    ("environment", "operation_epoch", 999),
    ("environment", "subject_incarnation", uuid4()),
    ("environment", "subject_id", uuid4()),
    ("environment", "owner_user_id", uuid4()),
    ("environment", "owner_team_id", uuid4()),
    ("environment", "name", "bob"),
    ("attempt", "operation_id", uuid4()),
    ("attempt", "id", uuid4()),
    ("attempt", "subject_id", uuid4()),
    ("attempt", "subject_incarnation", uuid4()),
    ("attempt", "operation_epoch", 999),
    ("attempt", "attempt_sequence", 999),
    ("candidate", "id", uuid4()),
    ("candidate", "owner_user_id", uuid4()),
    ("candidate", "owner_team_id", uuid4()),
))
def test_storage_resolution_rejects_stale_or_mixed_claim(part, field, value):
    module = import_module("loom.personal_dev_incarnation_storage")
    claim = _bound_claim()
    claim = replace(claim, **{part: replace(getattr(claim, part), **{field: value})})
    with pytest.raises(ValueError, match="storage"):
        module.resolve_personal_dev_storage_identity(claim)


@pytest.mark.parametrize("field,value", (
    ("database", "loom_staging"), ("db_role", "postgres"), ("namespace", "loom-staging"),
    ("task_bucket", "shared-tasks"), ("storage_incarnation", uuid4()), ("storage_binding", None),
))
def test_storage_identity_cannot_override_any_derived_resource(field, value):
    module = import_module("loom.personal_dev_incarnation_storage")
    identity = module.resolve_personal_dev_storage_identity(_bound_claim())
    with pytest.raises(ValueError, match="storage"):
        module.validate_personal_dev_storage_identity(replace(identity, **{field: value}))


def test_identity_taking_sql_plan_keeps_incarnation_targets():
    module = import_module("loom.dev_instance_provision")
    identity = _bound_claim().operation.storage_binding.identity
    plan = module.provisioning_plan_for_identity(identity, "a" * 20)
    assert plan["identity"] == identity
    assert f'CREATE DATABASE "{identity.database}" OWNER "{identity.db_role}"' in plan["create_database_sql"]
    assert identity.task_bucket in plan["buckets"]
    assert "loom_dev_alice" not in plan["role_sql"]
