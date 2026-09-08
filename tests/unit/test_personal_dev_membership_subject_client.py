"""Historical subject clients retain exact receipt identity and release blockers."""

from importlib import import_module

import httpx
import pytest

from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.membership_subject_status import PersonalMembershipSubjectQueryV1
from tests.unit.test_capacity_membership_subject_status import _observation
from tests.unit.test_personal_dev_membership_checkpoint import (
    membership_envelope_values,
    membership_response,
)


def _accepted():
    values = membership_envelope_values()
    request = values["request"]
    request = request.model_copy(
        update={
            "projection": request.projection.model_copy(
                update={"operation_kind": "destroy", "min_slots": 0, "max_slots": 0}
            )
        }
    )
    return PersonalDevMembershipEnvelopeV1.model_validate(
        values
        | {
            "request": request,
            "request_sha256": canonical_digest(request),
            "result": membership_response(request),
        }
    )


def _response(envelope, kind):
    query = PersonalMembershipSubjectQueryV1(membership_receipt=envelope.result)
    value = _observation()
    checkpoint = envelope.result.checkpoint
    value["query_sha256"] = canonical_digest(query)
    value["membership_receipt"] = envelope.result.model_dump(mode="json")
    value["current"].update(
        {
            "authority_incarnation": str(checkpoint.execution.authority_incarnation),
            "writer_epoch": checkpoint.execution.writer_epoch,
            "execution_epoch": checkpoint.execution.execution_epoch,
            "execution_manifest_sha256": checkpoint.execution.execution_manifest_sha256,
            "configuration_epoch": checkpoint.execution.configuration_epoch,
            "membership_execution_epoch": checkpoint.execution.execution_epoch,
            "membership_revision": checkpoint.revision,
            "membership_head_sha256": checkpoint.head_sha256,
            "subject": envelope.result.result.member.configuration.model_dump(mode="json"),
        }
    )
    if kind == "status":
        value["deployment_work"] = dict(value["incarnation_work"])
    elif kind == "pending":
        value.update(outcome="pending", blockers=["observed-commitments"])
        value["incarnation_work"]["observed_commitments"] = 1
    else:
        value.update(outcome="verified", release_set_sha256="c" * 64)
    return query, value


@pytest.mark.parametrize("kind", ("status", "pending", "verified"))
async def test_historical_subject_client_uses_exact_receipt_and_preserves_outcome(kind):
    module = import_module("loom.personal_dev_membership_client")
    envelope = _accepted()
    query, payload = _response(envelope, kind)

    def handler(request):
        path_kind = "status" if kind == "status" else "release"
        assert request.method == "POST"
        assert request.url.path == f"/v1/personal-memberships/subjects/{path_kind}/query"
        assert request.headers["Authorization"] == "Bearer current-observer"
        assert request.content == canonical_bytes(query)
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="current-observer",
            http_client=http,
        )
        method = (
            client.membership_subject_status
            if kind == "status"
            else client.membership_subject_release
        )
        result = await method(envelope)
        assert result.model_dump(mode="json") == payload
        assert not result.worker_available


@pytest.mark.parametrize("tamper", ("query", "receipt", "missing"))
async def test_release_client_rejects_mismatched_or_missing_receipt(tamper):
    module = import_module("loom.personal_dev_membership_client")
    envelope = _accepted()
    _, payload = _response(envelope, "verified")
    if tamper == "query":
        payload["query_sha256"] = "f" * 64
    elif tamper == "receipt":
        payload["membership_receipt"] = membership_response(
            envelope.request, actor="different-delegate"
        ).model_dump(mode="json")
    else:
        envelope = envelope.model_copy(update={"result": None})
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="current-observer",
            http_client=http,
        )
        with pytest.raises(module.PersonalDevMembershipError):
            await client.membership_subject_release(envelope)
    if tamper == "missing":
        assert not requests
