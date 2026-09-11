"""Saved typed membership requests retain exact authority and event identity."""

import json
from importlib import import_module
from uuid import UUID

import httpx
import pytest

from loom.personal_dev_membership_client import PersonalDevMembershipError, PersonalDevMembershipRevisionConflictError
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.typed_membership_commands import PersonalMembershipResultV2, derive_build_member
from tests.unit.test_capacity_typed_membership_commands import typed_build_mutation


def inputs():
    module = import_module("loom.personal_dev_typed_membership_client")
    _contracts, value, request = typed_build_mutation()
    checkpoint = PersonalMembershipCheckpointV1(execution=request.execution, namespace_id=request.namespace_id,
        revision=request.expected_revision, head_sha256="a" * 64)
    envelope = module.PersonalDevTypedMembershipEnvelopeV1(request=request,
        expected_checkpoint=checkpoint, idempotency_key=UUID(int=987))
    member = derive_build_member(request, value.preparation, value.fleet)
    head = canonical_membership_event_head(actor=value.preparation.personal_membership.management_principal_id,
        execution_epoch=request.execution.execution_epoch, idempotency_key=envelope.idempotency_key,
        operation_id=request.command.projection.operation_id, previous_sha256=checkpoint.head_sha256,
        request_digest=canonical_digest(request), request_payload=request.model_dump(mode="json"),
        member=member, revision=member.revision)
    response = PersonalMembershipResultV2(revision=member.revision, head_sha256=head, member=member, replayed=False)
    return module, envelope, value, response


async def test_typed_client_retries_lost_reply_with_identical_saved_bytes():
    module, envelope, value, response = inputs()
    seen = []

    async def handler(request):
        assert request.headers["Authorization"] == "Bearer test-membership"
        if request.method == "GET":
            assert request.url.path == "/v2/personal-memberships/checkpoint"
            return httpx.Response(200, content=canonical_bytes(envelope.expected_checkpoint), headers={"Content-Type": "application/json"})
        seen.append(request)
        assert request.url.path == f"/v2/personal-memberships/{envelope.request.command.acknowledgement.subject_id}"
        if len(seen) == 1:
            raise httpx.ReadTimeout("lost after commit")
        return httpx.Response(200, json=response.model_copy(update={"replayed": True}).model_dump(mode="json"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = module.CapacityManagerPersonalDevTypedMembershipClient(manager_origin="https://capacity.test",
            bearer_token="test-membership", http_client=http)
        assert await client.membership_checkpoint() == envelope.expected_checkpoint
        with pytest.raises(PersonalDevMembershipError, match="unconfirmed"):
            await client.mutate_membership(envelope, preparation=value.preparation, fleet=value.fleet)
        result = await client.mutate_membership(envelope, preparation=value.preparation, fleet=value.fleet)
        assert result.replayed
    assert len(seen) == 2
    assert seen[0].content == seen[1].content == canonical_bytes(envelope.request)
    assert seen[0].headers["Idempotency-Key"] == seen[1].headers["Idempotency-Key"] == str(envelope.idempotency_key)


@pytest.mark.parametrize("boundary", ["head", "owner", "version", "duplicate", "compressed", "oversize", "revision"])
async def test_typed_client_rejects_unconfirmed_or_wrong_result(boundary):
    module, envelope, value, result = inputs()
    payload = result.model_dump(mode="json")
    if boundary == "head":
        payload["head_sha256"] = "e" * 64
    elif boundary == "owner":
        payload["member"]["owner_id"] = str(UUID(int=333))
    elif boundary == "version":
        payload["schema_version"] = 2.0
    wire = json.dumps(payload).encode("ascii")
    if boundary == "duplicate":
        wire = b'{"schema_version":2,' + wire[1:]
    elif boundary == "oversize":
        wire += b" " * (4 * 1024 * 1024)
    headers = {"Content-Type": "application/json"}
    if boundary == "compressed":
        headers["Content-Encoding"] = "unexpected"
    response = (httpx.Response(409, json={"detail": {"code": "membership_revision_conflict"}}) if boundary == "revision"
        else httpx.Response(200, content=wire, headers=headers))
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as http:
        client = module.CapacityManagerPersonalDevTypedMembershipClient(manager_origin="https://capacity.test",
            bearer_token="test-membership", http_client=http)
        with pytest.raises(PersonalDevMembershipError) as caught:
            await client.mutate_membership(envelope, preparation=value.preparation, fleet=value.fleet)
        assert isinstance(caught.value, PersonalDevMembershipRevisionConflictError) is (boundary == "revision")


@pytest.mark.parametrize("boundary", ["checkpoint", "key", "preparation", "request"])
async def test_typed_client_rejects_changed_saved_authority_before_io(boundary):
    module, envelope, value, _response = inputs()
    preparation = value.preparation
    if boundary == "checkpoint":
        envelope = envelope.model_copy(update={"expected_checkpoint": envelope.expected_checkpoint.model_copy(update={"revision": 999})})
    elif boundary == "key":
        envelope = envelope.model_copy(update={"idempotency_key": UUID(int=0)})
    elif boundary == "preparation":
        preparation = preparation.model_copy(update={"trusted_fleet_release_sha256": "9" * 64})
    else:
        request = envelope.request
        envelope = envelope.model_copy(update={"request": request.model_copy(update={"command": request.command.model_copy(update={
            "acknowledgement": request.command.acknowledgement.model_copy(update={"subject_id": UUID(int=333)})})})})

    def forbidden(_request):
        pytest.fail("invalid retained request reached transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        client = module.CapacityManagerPersonalDevTypedMembershipClient(manager_origin="https://capacity.test",
            bearer_token="test-membership", http_client=http)
        with pytest.raises(PersonalDevMembershipError):
            await client.mutate_membership(envelope, preparation=preparation, fleet=value.fleet)
