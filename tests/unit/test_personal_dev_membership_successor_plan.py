"""A bounded protected plan supplies multiple exact predecessor bindings."""

import hashlib
import json
from importlib import import_module
from uuid import uuid4

import pytest

from loom.personal_dev_membership_successor import PersonalDevMembershipSuccessorBindingV1
from tests.unit.test_personal_dev_membership_successor import successor_case


def _plan():
    _, _, values = successor_case()
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    second = binding.model_copy(update={"predecessor_operation_id": uuid4()})
    return binding, {"schema_version": 1, "bindings": [
        binding.model_dump(mode="json"), second.model_dump(mode="json"),
    ]}


def _wire(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def test_successor_plan_loads_multiple_unique_bindings_as_immutable_mapping():
    module = import_module("loom.personal_dev_membership_successor")
    binding, document = _plan()
    result = module.parse_membership_successor_plan(
        _wire(document), expected_plan_sha256=hashlib.sha256(_wire(document)).hexdigest(),
        current_authority=binding.authority,
    )
    assert len(result) == 2 and result[binding.predecessor_operation_id] == binding
    with pytest.raises(TypeError):
        result[uuid4()] = binding


@pytest.mark.parametrize("tamper", ("digest", "duplicate", "empty", "authority", "unknown_field", "too_many", "oversize"))
def test_successor_plan_rejects_unreviewed_or_ambiguous_bundle(tamper):
    module = import_module("loom.personal_dev_membership_successor")
    binding, document = _plan()
    authority = binding.authority
    if tamper == "duplicate":
        document["bindings"][1] = document["bindings"][0]
    elif tamper == "empty":
        document["bindings"] = []
    elif tamper == "authority":
        authority = authority.model_copy(update={"plan_sha256": "e" * 64})
    elif tamper == "unknown_field":
        document["permit_implicit_import"] = True
    elif tamper == "too_many":
        document["bindings"] *= 65
    payload = _wire(document)
    digest = "f" * 64 if tamper == "digest" else hashlib.sha256(payload).hexdigest()
    if tamper == "oversize":
        payload += b" " * module.MAX_CONTRACT_BYTES
    with pytest.raises(ValueError):
        module.parse_membership_successor_plan(payload, expected_plan_sha256=digest, current_authority=authority)
