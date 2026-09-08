"""Membership transport never turns an ambiguous send into a different request."""

import json
from importlib import import_module

import httpx
import pytest

from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.membership_outcomes import PersonalMembershipOperationOutcomeQueryV1
from tests.unit.test_personal_dev_membership_checkpoint import (
    membership_envelope_values,
    membership_response,
)


def _envelope():
    return PersonalDevMembershipEnvelopeV1.model_validate(membership_envelope_values())


async def test_membership_client_reads_authenticated_checkpoint_and_sends_exact_saved_request():
    module = import_module("loom.personal_dev_membership_client")
    envelope = _envelope()
    response = membership_response(envelope.request)
    seen = []

    async def handler(request):
        assert request.headers["Authorization"] == "Bearer membership-token"
        seen.append(request)
        if request.method == "GET":
            assert request.url.path == "/v1/personal-memberships/checkpoint"
            return httpx.Response(200, json=envelope.expected_checkpoint.model_dump(mode="json"))
        assert (
            request.url.path == f"/v1/personal-memberships/{envelope.request.projection.subject_id}"
        )
        assert request.headers["Idempotency-Key"] == str(envelope.idempotency_key)
        assert request.content == canonical_bytes(envelope.request)
        return httpx.Response(200, json=response.model_dump(mode="json"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="membership-token",
            http_client=http,
        )
        assert await client.membership_checkpoint() == envelope.expected_checkpoint
        assert await client.mutate_membership(envelope) == response
    assert len(seen) == 2


@pytest.mark.parametrize(
    "detail,typed",
    (
        ({"code": "membership_revision_conflict"}, True),
        ("membership revision is stale", False),
        ({"code": "execution_fenced"}, False),
        ({"code": "membership_revision_conflict", "untrusted": True}, False),
    ),
)
async def test_only_exact_typed_revision_conflict_permits_refresh(detail, typed):
    module = import_module("loom.personal_dev_membership_client")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(409, json={"detail": detail}),
        )
    ) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="membership-token",
            http_client=http,
        )
        with pytest.raises(module.PersonalDevMembershipError) as error:
            await client.mutate_membership(_envelope())
        assert isinstance(error.value, module.PersonalDevMembershipRevisionConflictError) is typed


async def test_response_loss_retries_same_bytes_without_refresh_or_reattest():
    module = import_module("loom.personal_dev_membership_client")
    envelope = _envelope()
    seen = []

    async def handler(request):
        seen.append(request)
        if len(seen) == 1:
            raise httpx.ReadTimeout("response lost after commit")
        response = membership_response(envelope.request)
        response = response.model_copy(
            update={"result": response.result.model_copy(update={"replayed": True})}
        )
        return httpx.Response(200, json=response.model_dump(mode="json"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="membership-token",
            http_client=http,
        )
        with pytest.raises(module.PersonalDevMembershipError) as error:
            await client.mutate_membership(envelope)
        assert not isinstance(error.value, module.PersonalDevMembershipRevisionConflictError)
        assert (await client.mutate_membership(envelope)).result.replayed
    assert len(seen) == 2
    assert seen[0].content == seen[1].content == canonical_bytes(envelope.request)
    assert seen[0].headers["Idempotency-Key"] == seen[1].headers["Idempotency-Key"]


@pytest.mark.parametrize(
    "tamper",
    (
        "namespace",
        "generation",
        "owner",
        "duplicate",
        "version",
        "nested-version",
        "content-type",
        "compressed",
        "oversize",
    ),
)
async def test_membership_client_rejects_wrong_or_unbounded_receipt(tamper):
    module = import_module("loom.personal_dev_membership_client")
    envelope = _envelope()
    payload = membership_response(envelope.request).model_dump(mode="json")
    headers = {"Content-Type": "application/json"}
    if tamper == "namespace":
        payload["checkpoint"]["namespace_id"] = "00000000-0000-0000-0000-000000000999"
    elif tamper == "generation":
        payload["result"]["member"]["configuration"]["configuration_generation"] = 99
    elif tamper == "owner":
        payload["result"]["member"]["configuration"]["account_id"] = "other-owner"
    elif tamper == "version":
        payload["schema_version"] = 1.0
    elif tamper == "nested-version":
        payload["checkpoint"]["schema_version"] = 1.0
    elif tamper == "content-type":
        headers["Content-Type"] = "text/html"
    elif tamper == "compressed":
        headers["Content-Encoding"] = "unknown"
    content = json.dumps(payload).encode()
    if tamper == "duplicate":
        content = b'{"schema_version":1,' + content[1:]
    elif tamper == "oversize":
        content = b" " * (module.MAX_MEMBERSHIP_RESPONSE_BYTES + 1)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=content, headers=headers),
        )
    ) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="membership-token",
            http_client=http,
        )
        with pytest.raises(module.PersonalDevMembershipError):
            await client.mutate_membership(envelope)


def _outcome_payload(envelope, outcome):
    query = PersonalMembershipOperationOutcomeQueryV1(
        original_actor=envelope.management_principal_id,
        idempotency_key=envelope.idempotency_key,
        request=envelope.request,
    )
    payload = {
        "schema_version": 1,
        "outcome": outcome,
        "query_sha256": canonical_digest(query),
        "request_sha256": envelope.request_sha256,
        "original_actor": query.original_actor,
        "idempotency_key": str(query.idempotency_key),
        "operation_id": str(query.request.projection.operation_id),
        "execution_epoch": query.request.execution.execution_epoch,
        "execution_manifest_sha256": query.request.execution.execution_manifest_sha256,
        "namespace_id": str(query.request.namespace_id),
    }
    if outcome == "committed":
        payload["receipt"] = membership_response(envelope.request).model_dump(mode="json")
    elif outcome == "unresolved":
        payload["epoch_state"] = "active"
    else:
        payload["retired_at"] = "2026-09-08T12:00:00Z"
        payload["retirement_sha256"] = "a" * 64
    return query, payload


@pytest.mark.parametrize("outcome", ("committed", "unresolved", "terminal-not-committed"))
async def test_historical_outcome_uses_current_observer_and_exact_original_query(outcome):
    module = import_module("loom.personal_dev_membership_client")
    envelope = _envelope()
    query, payload = _outcome_payload(envelope, outcome)
    seen = []

    async def handler(request):
        seen.append(request)
        assert request.method == "POST"
        assert request.url.path == "/v1/personal-memberships/operation-outcomes/query"
        assert request.headers["Authorization"] == "Bearer current-observer"
        assert "Idempotency-Key" not in request.headers
        assert request.content == canonical_bytes(query)
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="current-observer",
            http_client=http,
        )
        result = await client.membership_operation_outcome(envelope)
        assert result.outcome == outcome
        assert result.model_dump(mode="json") == payload
    assert len(seen) == 1
    assert envelope.result is None


@pytest.mark.parametrize(
    "field",
    (
        "query_sha256",
        "request_sha256",
        "original_actor",
        "idempotency_key",
        "operation_id",
        "execution_epoch",
        "execution_manifest_sha256",
        "namespace_id",
        "receipt",
        "retirement",
    ),
)
async def test_historical_outcome_rejects_substituted_identity_or_receipt(field):
    module = import_module("loom.personal_dev_membership_client")
    envelope = _envelope()
    _, payload = _outcome_payload(
        envelope, "terminal-not-committed" if field == "retirement" else "committed"
    )
    if field == "receipt":
        payload[field] = membership_response(
            envelope.request, actor="different-delegate"
        ).model_dump(mode="json")
    elif field == "retirement":
        del payload["retirement_sha256"]
    elif field in {"idempotency_key", "operation_id", "namespace_id"}:
        payload[field] = "00000000-0000-0000-0000-000000000999"
    elif field == "execution_epoch":
        payload[field] += 1
    elif field == "original_actor":
        payload[field] = "different-delegate"
    else:
        payload[field] = "f" * 64
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as http:
        client = module.CapacityManagerPersonalDevMembershipClient(
            manager_origin="https://capacity.example",
            bearer_token="current-observer",
            http_client=http,
        )
        with pytest.raises(module.PersonalDevMembershipError):
            await client.membership_operation_outcome(envelope)
