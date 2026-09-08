"""Executable allocation seals retain the exact delegated membership generation."""

import json
from importlib import import_module

import pytest
from pydantic import ValidationError

from loom_capacity_manager.allocator import allocate_shadow, promote_shadow_epoch
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_manager_executable_allocator import execution_authority_fixture
from tests.unit.test_capacity_membership import delegated_input_with_new_owner


def test_delegated_execution_seals_membership_without_rewriting_base() -> None:
    module = import_module("loom_capacity_manager.membership_execution")
    value = delegated_input_with_new_owner()
    authority = execution_authority_fixture().model_copy(
        update={
            "execution_manifest_sha256": canonical_executable_digest(value.preparation),
        }
    )
    base = promote_shadow_epoch(allocate_shadow(value), authority, allocation_epoch=1)
    delegated = module.bind_executable_membership(base, value)
    assert delegated.schema_version == 3
    assert delegated.membership == value.membership
    assert delegated.configuration == base.configuration
    assert delegated.execution == base.execution
    assert delegated.input_digest == base.input_digest
    assert delegated.allocations == base.allocations
    assert module.parse_executable_epoch(delegated.model_dump_json()) == delegated
    assert canonical_executable_bytes(
        module.parse_executable_epoch(base.model_dump_json())
    ) == canonical_executable_bytes(base)


@pytest.mark.parametrize("tamper", ("input", "manifest", "configuration"))
def test_membership_seal_rejects_a_different_promoted_input(tamper: str) -> None:
    module = import_module("loom_capacity_manager.membership_execution")
    value = delegated_input_with_new_owner()
    # The execution manifest is the actual prepared V3 contract, not arbitrary evidence.
    authority = execution_authority_fixture().model_copy(
        update={
            "execution_manifest_sha256": canonical_executable_digest(value.preparation),
        }
    )
    base = promote_shadow_epoch(allocate_shadow(value), authority, allocation_epoch=1)
    assert module.bind_executable_membership(base, value).membership == value.membership
    changes = {
        "input": {"input_digest": "f" * 64},
        "manifest": {
            "execution": base.execution.model_copy(update={"execution_manifest_sha256": "f" * 64})
        },
        "configuration": {
            "configuration": base.configuration.model_copy(update={"configuration_epoch": 2})
        },
    }
    with pytest.raises(ValueError):
        module.bind_executable_membership(base.model_copy(update=changes[tamper]), value)


def test_allocation_parser_requires_exact_integer_v2_discriminator() -> None:
    module = import_module("loom_capacity_manager.membership_execution")
    value = delegated_input_with_new_owner()
    authority = execution_authority_fixture().model_copy(
        update={
            "execution_manifest_sha256": canonical_executable_digest(value.preparation),
        }
    )
    base = promote_shadow_epoch(allocate_shadow(value), authority, allocation_epoch=1)
    assert module.parse_executable_epoch(base.model_dump_json()) == base
    payload = base.model_dump(mode="json") | {"schema_version": 2.0}
    with pytest.raises(ValueError):
        module.parse_executable_epoch(json.dumps(payload))


@pytest.mark.parametrize("tag", (3.0, "3", 4, None))
def test_delegated_allocation_parser_requires_supported_exact_schema(tag: object) -> None:
    module = import_module("loom_capacity_manager.membership_execution")
    value = delegated_input_with_new_owner()
    authority = execution_authority_fixture().model_copy(
        update={
            "execution_manifest_sha256": canonical_executable_digest(value.preparation),
        }
    )
    base = promote_shadow_epoch(allocate_shadow(value), authority, allocation_epoch=1)
    sealed = module.bind_executable_membership(base, value)
    payload = sealed.model_dump(mode="json")
    payload["schema_version"] = tag
    with pytest.raises((ValidationError, ValueError)):
        module.parse_executable_epoch(json.dumps(payload))
