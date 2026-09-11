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
    record = inventory.records[0].model_copy(
        update={
            "state": "terminal",
            "terminal_evidence_sha256": "b" * 64,
        }
    )
    binding = record.ownership_proof.metadata.binding
    evidence = ExecutableTerminalInventoryEvidenceV3(
        binding=binding,
        inventory_execution=inventory.execution,
        inventory_sequence=1,
        inventory_digest=canonical_executable_digest(inventory),
        journal_sequence=0,
        journal_digest="0" * 64,
        record=record,
        observed_at=datetime(2026, 9, 10, tzinfo=UTC),
    )
    configuration = _configuration().model_copy(
        update={
            "subject_id": binding.subject_id,
            "subject_incarnation": binding.subject_incarnation,
            "deployment_generation": binding.deployment_generation,
            "candidate_identity_algorithm": binding.candidate.algorithm,
            "candidate_identity": binding.candidate.identity,
            "candidate_publication_sha256": binding.candidate.publication_sha256,
        }
    )
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
        client = DemandReporterClient(
            configuration,
            manager_origin="https://capacity.internal",
            bearer_token="reporter-secret",
            http_client=http,
        )
        if purpose == "personal-build-worker":
            with pytest.raises(DemandPublishError, match="application"):
                await client.get_executable_terminal_inventory_evidence(evidence.binding.intent_id)
        else:
            assert (
                await client.get_executable_terminal_inventory_evidence(evidence.binding.intent_id)
                == evidence
            )
    assert (
        seen[0].url.path
        == f"/v3/subjects/{configuration.subject_id}/intents/{evidence.binding.intent_id}/terminal-inventory-evidence"
    )
    assert seen[0].headers["Authorization"] == "Bearer reporter-secret"


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
async def test_agent_typed_terminal_store_preserves_bytes_and_rejects_build_before_sql(
    pool, purpose
):
    configuration, evidence = typed_terminal(purpose=purpose, pool=pool)
    registration = AgentRegistrationV1.model_validate(
        {field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields}
    )
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
            receipt = dict(
                schema_version=2,
                executable=False,
                import_state="imported",
                intent_id=str(evidence.binding.intent_id),
                protected_attempt_id=str(attempt),
                worker_id=str(UUID(int=876002)),
                worker_incarnation=str(UUID(int=876003)),
                physical_job_id=evidence.record.physical_identity,
                inventory_sequence=1,
                terminal_evidence_sha256=evidence.record.terminal_evidence_sha256,
                evidence_digest=canonical_executable_digest(evidence),
            )
            return SimpleNamespace(scalar_one=lambda: receipt)

    if purpose == "personal-build-worker":
        with pytest.raises(CapacityAgentStoreError, match="application"):
            await import_executable_terminal_inventory_evidence(
                Session(),
                registration=registration,
                protected_attempt_id=attempt,
                evidence=evidence,
            )
        assert not calls
    else:
        result = await import_executable_terminal_inventory_evidence(
            Session(), registration=registration, protected_attempt_id=attempt, evidence=evidence
        )
        assert result.evidence_digest == canonical_executable_digest(evidence)
        assert len(calls) == 1


@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
async def test_runtime_checks_purpose_before_importing_from_any_publisher(purpose):
    from loom_capacity_agent.runtime import ExecutableTerminalInventoryEvidenceRecoveryRuntime
    from tests.unit.test_capacity_agent_runtime import _assigned_observation, _Factory

    configuration, evidence = typed_terminal(purpose=purpose)
    attempt = UUID(int=876004)
    observation = _assigned_observation(
        configuration, protected_attempt_id=attempt, submission_intent_id=evidence.binding.intent_id
    )
    imported = []

    class Publisher:
        async def get_executable_terminal_inventory_evidence(self, intent_id):
            assert intent_id == evidence.binding.intent_id
            return evidence

    async def importer(*args, **kwargs):
        imported.append(kwargs["evidence"])

    runtime = ExecutableTerminalInventoryEvidenceRecoveryRuntime(
        configuration=configuration,
        session_factory=_Factory(),
        publisher=Publisher(),
        observation_source=lambda: observation,
        import_evidence=importer,
    )
    if purpose == "personal-build-worker":
        with pytest.raises(ValueError, match="application"):
            await runtime.run_once()
        assert not imported
        assert not runtime.ready
    else:
        await runtime.run_once()
        assert imported == [evidence]
        assert runtime.ready


async def test_large_valid_application_terminal_evidence_is_streamed():
    configuration, evidence = typed_terminal()
    value = evidence.model_dump(mode="json")
    nodes = [f"node-{index:03}-" + "a" * 100 for index in range(60)]
    value["binding"]["node_ids"] = nodes
    value["record"]["node_ids"] = nodes
    value["record"]["ownership_proof"]["metadata"]["binding"]["node_ids"] = nodes
    evidence = ExecutableTerminalInventoryEvidenceV3.model_validate_json(json.dumps(value))
    payload = canonical_executable_bytes(evidence)
    assert len(payload) > 16 * 1024

    from tests.unit.test_capacity_agent_client import _GuardedAdmissionWorkStream

    stream = _GuardedAdmissionWorkStream(payload[:8192], payload[8192:])
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, stream=stream)
    )) as http:
        client = DemandReporterClient(configuration, manager_origin="https://capacity.internal",
            bearer_token="reporter-secret", http_client=http)
        assert await client.get_executable_terminal_inventory_evidence(evidence.binding.intent_id) == evidence
    assert stream.closed


@pytest.mark.parametrize("status", (200, 403))
async def test_terminal_stream_stops_before_unbounded_or_error_body(status):
    from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES
    from tests.unit.test_capacity_agent_client import _GuardedAdmissionWorkStream

    configuration, evidence = typed_terminal()
    chunks = (b"x" * (MAX_CONTRACT_BYTES + 1),) if status == 200 else ()
    stream = _GuardedAdmissionWorkStream(*chunks, fail_if_read_past_chunks=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(status, stream=stream)
    )) as http:
        client = DemandReporterClient(configuration, manager_origin="https://capacity.internal",
            bearer_token="reporter-secret", http_client=http)
        with pytest.raises(DemandPublishError, match="byte bound" if status == 200 else "403"):
            await client.get_executable_terminal_inventory_evidence(evidence.binding.intent_id)
    assert stream.closed
    assert not stream.read_past_limit
