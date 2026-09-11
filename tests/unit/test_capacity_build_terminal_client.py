"""Build terminal transport is explicit and cannot admit application evidence."""

from uuid import uuid4

import httpx
import pytest

from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.unit.test_capacity_agent_typed_terminal import typed_terminal


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("purpose", ["application-worker", "personal-build-worker"])
async def test_native_terminal_client_preserves_only_build_purpose(pool, purpose):
    configuration, evidence = typed_terminal(pool=pool, purpose=purpose)

    async def handler(request):
        assert request.url.path == f"/v3/subjects/{configuration.subject_id}/intents/{evidence.binding.intent_id}/terminal-inventory-evidence"
        assert request.headers["Authorization"] == "Bearer reporter-secret"
        return httpx.Response(200, content=canonical_executable_bytes(evidence))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = DemandReporterClient(configuration, manager_origin="https://capacity.internal",
            bearer_token="reporter-secret", http_client=http)
        if purpose == "personal-build-worker":
            assert await client.get_build_terminal_inventory_evidence(evidence.binding.intent_id) == evidence
        else:
            with pytest.raises(DemandPublishError, match="build"):
                await client.get_build_terminal_inventory_evidence(evidence.binding.intent_id)


@pytest.mark.parametrize("boundary", ["none", "intent", "subject", "incarnation", "deployment", "candidate", "noncanonical", "redirect"])
async def test_native_terminal_client_retains_response_fences(boundary):
    configuration, evidence = typed_terminal(purpose="personal-build-worker")
    intent_id = evidence.binding.intent_id
    if boundary == "intent":
        intent_id = uuid4()
    updates = {"subject":{"subject_id":uuid4()}, "incarnation":{"subject_incarnation":uuid4()},
        "deployment":{"deployment_generation":99}, "candidate":{"candidate_publication_sha256":"f"*64}}
    if boundary in updates:
        configuration = configuration.model_copy(update=updates[boundary])

    async def handler(request):
        if boundary == "none":
            return httpx.Response(200, content=b"null")
        if boundary == "redirect":
            return httpx.Response(302, headers={"Location":"https://foreign.test"})
        return httpx.Response(200, content=canonical_executable_bytes(evidence) + (b" " if boundary == "noncanonical" else b""))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = DemandReporterClient(configuration, manager_origin="https://capacity.internal",
            bearer_token="reporter-secret", http_client=http)
        if boundary == "none":
            assert await client.get_build_terminal_inventory_evidence(intent_id) is None
        else:
            with pytest.raises(DemandPublishError):
                await client.get_build_terminal_inventory_evidence(intent_id)
