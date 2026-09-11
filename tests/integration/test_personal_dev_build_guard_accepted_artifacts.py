"""Only a current source's committed accepted claim selects native output."""

from dataclasses import replace
from importlib import import_module

import pytest
from sqlalchemy import text

from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registered_release import (
    registered_release_input,
)
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "failed", "cancelled", "missing", "source", "owner", "platform", "expired"])
async def test_native_artifact_resolver_joins_accepted_claim_to_current_source(prepared_input, monkeypatch, boundary):
    factory, engine, installation, _plan, source, _request = prepared_input
    _release, claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch,
        result="failed" if boundary == "failed" else "live" if boundary == "missing" else "artifact-ready")
    platform = "linux/arm64" if claim.binding.pool_id == "gb10" else "linux/amd64"
    if boundary == "source":
        source = replace(source, candidate=replace(source.candidate, archive_sha256="f" * 64))
    elif boundary == "owner":
        from uuid import uuid4

        source = replace(source, candidate=replace(source.candidate, owner_user_id=uuid4()))
    elif boundary == "platform":
        platform = "linux/amd64" if platform == "linux/arm64" else "linux/arm64"
    elif boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": claim.request_id})
    elif boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"),
                {"id": source.build_attempt.id})
    resolver = import_module("loom_capacity_build_guard.artifact_resolver").BuildAcceptedArtifactResolver(
        session_factory=factory, installation=installation)
    if boundary == "exact":
        result = await resolver.resolve(source, platform=platform)
        assert result.request.result == "artifact-ready" and result.request.claim == claim
        assert result.request.artifact is not None
    else:
        from sqlalchemy.exc import DBAPIError

        with pytest.raises((ValueError, RuntimeError, DBAPIError)):
            await resolver.resolve(source, platform=platform)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_accepted_artifact_requires_committed_outcome_without_reopening_admission(prepared_input, monkeypatch):
    from hashlib import sha256

    from sqlalchemy.exc import DBAPIError

    from loom.personal_dev_build_platform_requests import canonical_build_source
    from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
    from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
    from loom_capacity_build_guard.terminal_store import BuildGuardTerminalStore
    from tests.integration.test_personal_dev_build_guard_hold_retirement import (
        release_witness,
        retirement,
    )
    from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
    from tests.integration.test_personal_dev_build_guard_release_outbox import outbox

    factory, engine, installation, plan, source, _request = prepared_input
    release, claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="live")
    wire = canonical_build_source(source)
    async with factory.begin() as session:
        await BuildGuardExecutionStore(session, installation=installation).record_outcome(
            outcome_request(claim, result="artifact-ready"), worker_credential=CREDENTIAL)
        with pytest.raises(DBAPIError, match="committed outcome"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.read_accepted_artifact(:installation,:request,CAST(:source AS jsonb),:wire,:digest)"),
                    {"installation": installation.id, "request": claim.request_id, "source": wire.decode("ascii"),
                        "wire": wire, "digest": sha256(wire).hexdigest()})
    # Artifact-ready is exportable, but still closes the new-admission path.
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="live lease changed"):
            await BuildGuardPlanStore(session, installation=installation).authorize_publication(plan.plan_id)
    async with factory.begin() as session:
        await BuildGuardExecutionStore(session, installation=installation).acknowledge_release(release, current_worker_credential=CREDENTIAL)
    resolver = import_module("loom_capacity_build_guard.artifact_resolver").BuildAcceptedArtifactResolver(
        session_factory=factory, installation=installation)
    platform = "linux/arm64" if claim.binding.pool_id == "gb10" else "linux/amd64"
    accepted = await resolver.resolve(source, platform=platform)
    assert accepted.request.claim == claim
    async with factory.begin() as session:
        await BuildGuardTerminalStore(session, installation=installation).import_evidence(terminal)
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
    async with factory.begin() as session:
        await retirement(session, installation).retire(release_witness(publication, terminal))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
    assert await resolver.resolve(source, platform=platform) == accepted


@pytest.mark.parametrize("boundary", ["execute", "public", "helper-public", "search-path"])
def test_accepted_artifact_private_authority_drift_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.read_accepted_artifact(uuid,uuid,jsonb,bytea,text)"
    statements = {"execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "helper-public": "GRANT EXECUTE ON FUNCTION loom_capacity_build_guard.assert_live_source(uuid,uuid,jsonb,bytea,text) TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


def test_artifact_helper_downgrade_restores_exact_admission_fence(build_guard_database):
    from alembic import command

    config, engine, *_ = build_guard_database
    command.upgrade(config, "build_guard_0024")
    definition = text("SELECT pg_get_functiondef('loom_capacity_build_guard.assert_current_source(uuid,uuid,jsonb,bytea,text)'::regprocedure)")
    with engine.connect() as connection:
        before = connection.scalar(definition)
    command.upgrade(config, "head")
    command.downgrade(config, "build_guard_0024")
    with engine.connect() as connection:
        assert connection.scalar(definition) == before
        assert connection.scalar(text("SELECT to_regprocedure('loom_capacity_build_guard.assert_live_source(uuid,uuid,jsonb,bytea,text)')")) is None
    command.upgrade(config, "head")
