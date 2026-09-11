"""Protected SQL and storage use the actual demand wire contract bounds."""

import json
from dataclasses import replace
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom.personal_dev_build_platform_requests import canonical_build_source
from loom_capacity_build_guard.demand_store import BuildDemandCoordinator, BuildGuardDemandStore
from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from loom_capacity_manager.contracts import DemandSnapshotV1, canonical_bytes, canonical_digest
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def test_direct_sql_rejects_pending_overflow_without_replacing_retained_report(prepared_input):
    sessions, engine, retained, _proposal, registration, request = prepared_input
    original = await BuildDemandCoordinator(sessions, installation=retained).capture(configuration_generation=1)
    sources = {str(request.id): canonical_build_source(registration).decode("ascii")}
    rows = []
    for sequence in range(1, 2049):
        attempt_id, request_id = uuid4(), uuid4()
        source = canonical_build_source(replace(registration, build_attempt=replace(registration.build_attempt, id=attempt_id)))
        sources[str(request_id)] = source.decode("ascii")
        rows.append({"parent": registration.build_attempt.id, "request": request.id,
            "attempt": attempt_id, "id": request_id, "sequence": sequence+registration.build_attempt.attempt_sequence,
            "digest": sha256(source).hexdigest(), "bucket": "build-"+sha256(request_id.bytes).hexdigest()})
    # Valid bulk fixture: unique attempts of the same candidate, each with its
    # own immutable request and canonical source hash. No guards are disabled.
    with engine.begin() as connection:
        connection.execute(text("""INSERT INTO personal_dev_candidate_build_attempts
            SELECT (jsonb_populate_record(NULL::personal_dev_candidate_build_attempts,
                to_jsonb(a) || jsonb_build_object('id',CAST(:attempt AS uuid),'attempt_sequence',CAST(:sequence AS integer)))).*
            FROM personal_dev_candidate_build_attempts a WHERE id=:parent"""), rows)
        connection.execute(text("""INSERT INTO personal_dev_build_platform_requests
            SELECT (jsonb_populate_record(NULL::personal_dev_build_platform_requests,
                to_jsonb(r) || jsonb_build_object('id',CAST(:id AS uuid),'attempt_id',CAST(:attempt AS uuid),
                    'bucket_id',CAST(:bucket AS text),'source_binding_sha256',CAST(:digest AS text)))).*
            FROM personal_dev_build_platform_requests r WHERE id=:request"""), rows)
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="pending requests exceed bound"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.capture_demand(:id,1,CAST(:sources AS jsonb))"),
                    {"id": retained.id, "sources": json.dumps(sources)})
        assert await BuildGuardDemandStore(session, installation=retained).read_latest() == original


async def test_valid_large_assignment_report_fits_demand_storage_and_reads_back(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    original = await BuildDemandCoordinator(sessions, installation=retained).capture(configuration_generation=1)
    assignment = original.current_assignments[0]
    large = original.model_copy(update={"current_assignments": tuple(
        assignment.model_copy(update={"attempt_id": str(uuid4())}) for _ in range(10000))})
    large = DemandSnapshotV1.model_validate_json(large.model_dump_json())
    wire = canonical_bytes(large)
    assert 1048576 < len(wire) < 8388608
    # Exercise storage independently of capture's plan derivation. The disposable
    # installer fixture inserts a valid maximum-count contract, never the agent.
    with engine.begin() as connection:
        connection.execute(text("""UPDATE loom_capacity_build_guard.reporter_state
            SET payload=CAST(:payload AS jsonb),wire_payload=:wire,payload_sha256=:digest WHERE installation_id=:id"""),
            {"payload": wire.decode("ascii"), "wire": wire, "digest": canonical_digest(large), "id": retained.id})
    async with sessions.begin() as session:
        assert await BuildGuardDemandStore(session, installation=retained).read_latest() == large
