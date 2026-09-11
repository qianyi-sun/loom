"""Historical native claims cannot authorize fresh source access."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "missing", "uncommitted", "credential", "claim", "binding",
    "cancelled", "expired", "source", "drain", "outcome"])
async def test_source_access_requires_exact_committed_live_claim(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, platform = prepared_input
    claim = await claim_input(prepared_input, monkeypatch)
    receipt = None
    if boundary not in {"missing", "uncommitted"}:
        async with factory.begin() as session:
            receipt = await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    if boundary in {"cancelled", "expired", "source"}:
        with engine.begin() as connection:
            if boundary == "cancelled":
                connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": platform.id})
            elif boundary == "expired":
                connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
            else:
                connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": source.candidate.id})
    if boundary == "drain":
        from loom_capacity_agent.admission import ExecutableDrainRequestV2

        async with factory.begin() as session:
            await store(session, installation).begin_drain(ExecutableDrainRequestV2(
                operation_id=uuid4(), binding=claim.binding, worker_id=claim.worker_id,
                worker_incarnation=claim.worker_incarnation, expected_claim_high_water=1, drain_epoch=3))
    if boundary == "outcome":
        from loom_capacity_agent.build_admission import BuildOutcomeRequestV1

        async with factory.begin() as session:
            await store(session, installation).record_outcome(BuildOutcomeRequestV1(
                claim=claim, operation_id=uuid4(), result="failed"), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        if boundary == "uncommitted":
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
        original = claim
        if boundary == "claim":
            claim = claim.model_copy(update={"operation_id": uuid4()})
        elif boundary == "binding":
            claim = claim.model_copy(update={"binding": claim.binding.model_copy(update={"account_id": "foreign"})})
        execution = store(session, installation)
        if boundary == "exact":
            access = await execution.authorize_source(claim, worker_credential=CREDENTIAL)
            assert access.claim == claim
            assert access.object_bucket == source.candidate.object_bucket
            assert access.object_key == source.candidate.object_key
            assert access.archive_sha256 == source.candidate.archive_sha256
            assert access.archive_size_bytes == source.candidate.archive_size_bytes
            assert access.lease_not_after == source.build_attempt.lease_expires_at
        else:
            with pytest.raises((ValueError, DBAPIError)):
                await execution.authorize_source(claim, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
        if receipt is not None:
            # Fresh access rejection must not break historical recovery.
            assert await execution.claim_platform(original, worker_credential=CREDENTIAL) == receipt


@pytest.mark.parametrize("boundary", ["execute", "public", "search-path"])
def test_source_access_private_surface_is_verified(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.authorize_source(uuid,jsonb,bytea,text,text)"
    statements = {"execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
