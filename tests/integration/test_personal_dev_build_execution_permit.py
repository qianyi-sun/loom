"""Execution freshness reuses the private live allocation/source fence."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutableDrainRequestV2
from loom_capacity_agent.build_admission import BuildExecutionRequestV1, BuildOutcomeRequestV1
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "shortened", "missing", "uncommitted", "credential",
    "claim", "source-binding", "source-change", "cancelled", "expired", "drain", "outcome"])
async def test_native_permission_requires_fresh_exact_allocation_and_source(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    if boundary not in {"missing", "uncommitted"}:
        async with factory.begin() as session:
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    if boundary in {"cancelled", "expired", "source-change", "shortened"}:
        with engine.begin() as connection:
            if boundary == "cancelled":
                connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"),
                    {"id": platform.id})
            elif boundary in {"expired", "shortened"}:
                connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=:deadline WHERE id=:id"),
                    {"id": source.build_attempt.id, "deadline": datetime.now(UTC) + timedelta(seconds=5 if boundary == "shortened" else -1)})
            else:
                connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"),
                    {"id": source.candidate.id})
    if boundary == "drain":
        async with factory.begin() as session:
            await store(session, installation).begin_drain(ExecutableDrainRequestV2(operation_id=uuid4(),
                binding=claim.binding, worker_id=claim.worker_id, worker_incarnation=claim.worker_incarnation,
                expected_claim_high_water=1, drain_epoch=3))
    if boundary == "outcome":
        async with factory.begin() as session:
            await store(session, installation).record_outcome(BuildOutcomeRequestV1(claim=claim,
                operation_id=uuid4(), result="failed"), worker_credential=CREDENTIAL)
    if boundary == "claim":
        claim = claim.model_copy(update={"operation_id": uuid4()})
    request = BuildExecutionRequestV1(claim=claim, challenge=uuid4(), source_binding_sha256=(
        "f" * 64 if boundary == "source-binding" else platform.source_binding_sha256))
    async with factory.begin() as session:
        execution = store(session, installation)
        if boundary == "uncommitted":
            await execution.claim_platform(claim, worker_credential=CREDENTIAL)
        if boundary not in {"exact", "shortened"}:
            with pytest.raises((ValueError, DBAPIError)):
                await execution.authorize_execution(request,
                    worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
            return
        permit = await execution.authorize_execution(request, worker_credential=CREDENTIAL)
        assert permit.request == request and permit.request_digest == canonical_digest(request)
        assert timedelta(0) < permit.not_after - permit.issued_at <= timedelta(seconds=10)
    with engine.connect() as connection:
        deadline = connection.scalar(text("SELECT lease_expires_at FROM personal_dev_candidate_build_attempts WHERE id=:id"),
            {"id": source.build_attempt.id})
        assert permit.not_after <= deadline
        if boundary == "shortened":
            assert permit.not_after == deadline
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0


@pytest.mark.parametrize("boundary", ["execute", "public", "search-path"])
def test_native_permission_acl_is_exact_and_verified(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.authorize_execution(uuid,jsonb,bytea,text,text)"
    statements = {
        "execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
    }
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
