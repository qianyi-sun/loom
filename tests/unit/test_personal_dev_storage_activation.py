"""Version storage-aware activation without reinterpreting historical intent bytes."""

import json
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest

from loom.personal_dev_activation import PersonalDevActivationIntentRequest
from loom.personal_dev_activation_agent import HttpPersonalDevActivationAuthority
from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1
from loom_capacity_manager.contracts import canonical_digest
from loom_service.routes.dev_instances import _personal_activation_intent_response
from tests.unit.test_personal_dev_activation_agent import _intent


def _binding():
    intent = _intent()
    return PersonalDevStorageBindingV1(
        layout="incarnation-v1", environment_name=intent.environment_name,
        subject_id=intent.subject_id, subject_incarnation=intent.subject_incarnation,
        owner_user_id=uuid4(), owner_team_id=uuid4(),
    )


def _v2():
    binding = _binding()
    return replace(_intent(), schema_version=2, storage_binding=binding,
                   storage_binding_sha256=canonical_digest(binding))


def test_legacy_activation_canonical_and_http_forms_remain_exact():
    intent = _intent()
    wire = _personal_activation_intent_response(intent).model_dump(mode="json")
    assert "schema_version" not in wire
    assert "storage_binding" not in wire
    assert "storage_binding_sha256" not in wire
    wire.pop("intent_sha256")
    wire["schema_version"] = 1
    wire["intent_created_at"] = wire["intent_created_at"].replace("+00:00", "Z")
    assert intent.canonical_bytes() == json.dumps(wire, sort_keys=True, separators=(",", ":")).encode()


def test_bound_activation_canonical_bytes_and_response_pin_full_binding():
    intent = _v2()
    canonical = json.loads(intent.canonical_bytes())
    assert canonical["schema_version"] == 2
    assert canonical["storage_binding"] == intent.storage_binding.model_dump(mode="json")
    assert canonical["storage_binding_sha256"] == canonical_digest(intent.storage_binding)
    wire = _personal_activation_intent_response(intent).model_dump(mode="json")
    assert wire["schema_version"] == 2
    assert wire["storage_binding"] == canonical["storage_binding"]
    assert wire["storage_binding_sha256"] == canonical["storage_binding_sha256"]
    assert wire["intent_sha256"] == intent.intent_sha256


@pytest.mark.parametrize("changes", (
    {"schema_version": 1}, {"schema_version": True}, {"schema_version": 2.0},
    {"storage_binding": None}, {"storage_binding_sha256": None},
    {"storage_binding_sha256": "0" * 64}, {"environment_name": "bob"},
    {"subject_id": uuid4()}, {"subject_incarnation": uuid4()},
))
def test_activation_rejects_mixed_version_and_storage_coordinates(changes):
    with pytest.raises(ValueError, match="storage"):
        replace(_v2(), **changes)


@pytest.mark.parametrize("changes", (
    {}, {"schema_version": 1}, {"schema_version": None}, {"storage_binding": None},
    {"storage_binding_sha256": "0" * 64}, {"extra": "forbidden"},
))
async def test_activation_http_v2_roundtrip_and_fail_closed(changes):
    intent = _v2()
    wire = _personal_activation_intent_response(intent).model_dump(mode="json")
    wire.update(changes)
    async with httpx.AsyncClient(base_url="https://management.example", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=wire),
    )) as client:
        request = PersonalDevActivationIntentRequest(
            agent_key_id="personal-dev-agent-v1", request_nonce=uuid4(), requested_at=intent.intent_created_at,
        )
        authority = HttpPersonalDevActivationAuthority(client)
        if changes:
            with pytest.raises(RuntimeError, match="response is invalid"):
                await authority.next_intent(request, signature="1" * 128)
        else:
            assert await authority.next_intent(request, signature="1" * 128) == intent
