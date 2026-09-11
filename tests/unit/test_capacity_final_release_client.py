"""Reporter transport preserves exact manager authority without redirects."""

import json
from uuid import UUID

import httpx
import pytest

from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.capacity_final_release_fixtures import final_release_witness
from tests.unit.test_capacity_agent_client import _configuration, _terminal_inventory_evidence


@pytest.mark.parametrize("change", ("none", "absent", "intent", "subject", "incarnation", "deployment",
    "candidate", "reporter", "proof", "malformed", "oversize", "redirect", "transport"))
async def test_final_release_client_fails_closed(change):
    configuration = _configuration()
    binding = _terminal_inventory_evidence(configuration).binding
    witness = final_release_witness(binding, configuration.reporter_incarnation)
    payload = witness.model_dump(mode="json")
    fields = {"intent": "intent_id", "subject": "subject_id", "incarnation": "subject_incarnation"}
    if change in fields:
        for node in (payload["release"], payload["protected_release"]):
            node["binding"][fields[change]] = str(UUID(int=9999))
    elif change == "deployment":
        for node in (payload["release"], payload["protected_release"]):
            node["binding"]["deployment_generation"] += 1
    elif change == "candidate":
        for node in (payload["release"], payload["protected_release"]):
            node["binding"]["candidate"]["publication_sha256"] = "f" * 64
    elif change == "reporter":
        payload["protected_release"]["reporter_incarnation"] = str(UUID(int=9999))
    elif change == "proof":
        payload["protected_acknowledgement_sha256"] = "f" * 64
    # Keep the publication digest valid for binding tamper so those cases test
    # client pins, independently of the model's protected-release join.
    if change in (*fields, "deployment", "candidate", "reporter"):
        from loom_capacity_manager.executable_contracts import ExecutableProtectedReleaseV2, canonical_executable_digest
        payload["protected_acknowledgement_sha256"] = canonical_executable_digest(
            ExecutableProtectedReleaseV2.model_validate_json(json.dumps(payload["protected_release"])))
    seen = []

    async def handler(request):
        seen.append(request)
        assert request.headers["Authorization"] == "Bearer reporter-secret"
        if change == "transport":
            raise httpx.ConnectError("unavailable")
        if change == "redirect":
            return httpx.Response(302, headers={"Location": "https://other.invalid"})
        content = json.dumps(payload).encode()
        if change == "none":
            content = canonical_executable_bytes(witness)
        elif change == "absent":
            content = b"null"
        elif change == "malformed":
            content = b"{}"
        elif change == "oversize":
            content = b" " * (MAX_CONTRACT_BYTES + 1)
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = DemandReporterClient(configuration, manager_origin="https://capacity.internal",
            bearer_token="reporter-secret", http_client=http)
        if change in ("none", "absent"):
            assert await client.get_final_release_witness(binding.intent_id) == (witness if change == "none" else None)
        else:
            with pytest.raises(DemandPublishError):
                await client.get_final_release_witness(binding.intent_id)
    assert len(seen) == 1
    assert seen[0].url.path == f"/v2/subjects/{binding.subject_id}/intents/{binding.intent_id}/final-release-witness"
