"""Deliver committed protected demand through the actual reporter HTTP client."""

from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
from loom_capacity_build_guard.demand_store import BuildDemandCoordinator
from loom_capacity_manager.contracts import DemandSnapshotV1, canonical_bytes, canonical_digest
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_agent_client import _configuration


def reporter_configuration(retained):
    document = retained.document
    return _configuration().model_copy(update={
        "subject_id": document.subject_id, "subject_incarnation": document.subject_incarnation,
        "reporter_incarnation": document.reporter_incarnation,
        "deployment_generation": document.deployment_generation, "configuration_generation": 1,
    })


@pytest.mark.parametrize("failure", ["lost", "wrong-digest", "wrong-sequence"])
async def test_publish_latest_replays_exact_committed_report_without_database_locks(prepared_input, failure):
    sessions, engine, retained, _proposal, registration, request = prepared_input
    coordinator = BuildDemandCoordinator(sessions, installation=retained)
    snapshot = await coordinator.capture(configuration_generation=1, sources={request.id: registration})
    delivered = []
    snapshot_id = uuid4()

    async def handle(outgoing):
        assert outgoing.url.path == f"/v1/reports/demand/{snapshot.subject_id}"
        assert outgoing.headers["Authorization"] == "Bearer test-only-token"
        delivered.append(outgoing.content)
        assert outgoing.content == canonical_bytes(snapshot)
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL lock_timeout='100ms'"))
            assert connection.scalar(text("SELECT sequence FROM loom_capacity_build_guard.reporter_state WHERE installation_id=:id FOR UPDATE"),
                {"id": retained.id}) == snapshot.sequence
            connection.execute(text("SELECT id FROM loom_capacity_build_guard.installations WHERE id=:id FOR UPDATE"), {"id": retained.id})
        receipt = {"snapshot_id": str(snapshot_id), "digest": canonical_digest(snapshot),
            "sequence": snapshot.sequence, "replayed": len(delivered)>1}
        if len(delivered) == 1:
            if failure == "lost":
                raise httpx.ReadTimeout("reply lost", request=outgoing)
            receipt["digest" if failure == "wrong-digest" else "sequence"] = "0"*64 if failure == "wrong-digest" else 99
        return httpx.Response(200, json=receipt)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        publisher = DemandReporterClient(reporter_configuration(retained), manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        with pytest.raises(DemandPublishError):
            await coordinator.publish_latest(publisher)
        restarted = BuildDemandCoordinator(sessions, installation=retained)
        receipt = await restarted.publish_latest(publisher)
    assert receipt.snapshot_id == snapshot_id and receipt.replayed
    assert delivered == [canonical_bytes(snapshot)]*2


async def test_publish_latest_does_not_invent_empty_report_or_advance_sequence(prepared_input):
    sessions, _engine, retained, _proposal, registration, request = prepared_input
    coordinator = BuildDemandCoordinator(sessions, installation=retained)
    delivered = []

    async def handle(outgoing):
        snapshot = DemandSnapshotV1.model_validate_json(outgoing.content)
        delivered.append(snapshot)
        return httpx.Response(200, json={"snapshot_id": str(uuid4()), "digest": canonical_digest(snapshot),
            "sequence": snapshot.sequence, "replayed": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        publisher = DemandReporterClient(reporter_configuration(retained), manager_origin="https://manager.example",
            bearer_token="test-only-token", http_client=http)
        assert await coordinator.publish_latest(publisher) is None
        assert delivered == []
        first = await coordinator.capture(configuration_generation=1, sources={request.id: registration})
        await coordinator.publish_latest(publisher)
        second = await coordinator.capture(configuration_generation=1, sources={request.id: registration})
        await coordinator.publish_latest(publisher)
    assert delivered == [first, second]
    assert second.sequence == first.sequence+1
