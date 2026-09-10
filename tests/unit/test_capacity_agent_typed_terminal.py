"""Application recovery preserves typed evidence but never imports build work."""

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest

from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.store import (
    CapacityAgentStoreError,
    import_executable_terminal_inventory_evidence,
)
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.typed_inventory_contracts import ExecutableTerminalInventoryEvidenceV3
from tests.unit.test_capacity_agent_client import _configuration
from tests.unit.test_capacity_typed_inventory_contracts import typed_inventory


def typed_terminal(*, purpose="application-worker", pool="oldlab"):
    inventory = typed_inventory(purpose=purpose, pool=pool)
    record = inventory.records[0].model_copy(update={
        "state": "terminal", "terminal_evidence_sha256": "b" * 64,
    })
    binding = record.ownership_proof.metadata.binding
    evidence = ExecutableTerminalInventoryEvidenceV3(
        binding=binding, inventory_execution=inventory.execution,
        inventory_sequence=1, inventory_digest=canonical_executable_digest(inventory),
        journal_sequence=0, journal_digest="0" * 64, record=record,
        observed_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    configuration = _configuration().model_copy(update={
        "subject_id": binding.subject_id, "subject_incarnation": binding.subject_incarnation,
        "deployment_generation": binding.deployment_generation,
        "candidate_identity_algorithm": binding.candidate.algorithm,
        "candidate_identity": binding.candidate.identity,
        "candidate_publication_sha256": binding.candidate.publication_sha256,
    })
    return configuration, evidence


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
async def test_agent_typed_terminal_transport_preserves_application_only(pool, purpose):
    configuration, evidence = typed_terminal(purpose=purpose, pool=pool)
    seen = []

    async def handler(request):
        seen.append(request)
        return httpx.Response(200, content=canonical_executable_bytes(evidence))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = DemandReporterClient(configuration, manager_origin="https://capacity.internal",
            bearer_token="reporter-secret", http_client=http)
        if purpose == "personal-build-worker":
            with pytest.raises(DemandPublishError, match="application"):
                await client.get_executable_terminal_inventory_evidence(evidence.binding.intent_id)
        else:
            assert await client.get_executable_terminal_inventory_evidence(evidence.binding.intent_id) == evidence
    assert seen[0].url.path == f"/v3/subjects/{configuration.subject_id}/intents/{evidence.binding.intent_id}/terminal-inventory-evidence"
    assert seen[0].headers["Authorization"] == "Bearer reporter-secret"


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
async def test_agent_typed_terminal_store_preserves_bytes_and_rejects_build_before_sql(pool, purpose):
    configuration, evidence = typed_terminal(purpose=purpose, pool=pool)
    registration = AgentRegistrationV1.model_validate({
        field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields
    })
    calls = []
    attempt = UUID(int=876001)

    class Session:
        @asynccontextmanager
        async def begin_nested(self):
            yield

        async def execute(self, statement, parameters):
            calls.append(parameters)
            payload = json.loads(parameters["payload"])
            assert payload == json.loads(canonical_executable_bytes(evidence))
            assert parameters["canonical_payload"] == canonical_executable_bytes(evidence)
            assert parameters["evidence_digest"] == canonical_executable_digest(evidence)
            receipt = dict(schema_version=2, executable=False, import_state="imported",
                intent_id=str(evidence.binding.intent_id), protected_attempt_id=str(attempt),
                worker_id=str(UUID(int=876002)), worker_incarnation=str(UUID(int=876003)),
                physical_job_id=evidence.record.physical_identity, inventory_sequence=1,
                terminal_evidence_sha256=evidence.record.terminal_evidence_sha256,
                evidence_digest=canonical_executable_digest(evidence))
            return SimpleNamespace(scalar_one=lambda: receipt)

    if purpose == "personal-build-worker":
        with pytest.raises(CapacityAgentStoreError, match="application"):
            await import_executable_terminal_inventory_evidence(Session(), registration=registration,
                protected_attempt_id=attempt, evidence=evidence)
        assert not calls
    else:
        result = await import_executable_terminal_inventory_evidence(Session(), registration=registration,
            protected_attempt_id=attempt, evidence=evidence)
        assert result.evidence_digest == canonical_executable_digest(evidence)
        assert len(calls) == 1
