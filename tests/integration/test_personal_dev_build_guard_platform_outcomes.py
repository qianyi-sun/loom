"""A native waiter distinguishes pending work from exact committed failure."""

from hashlib import sha256
from importlib import import_module

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom.personal_dev_build_platform_requests import canonical_build_source
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


@pytest.mark.parametrize("result", ["unstaged-claim", "unclaimed", "live", "artifact-ready", "failed", "cancelled", "interrupted"])
async def test_observe_latest_platform_outcome_preserves_pending_and_failure(prepared_input, monkeypatch, result):
    factory, engine, installation, _plan, source, request = prepared_input
    resolver = import_module("loom_capacity_build_guard.artifact_resolver").BuildAcceptedArtifactResolver(
        session_factory=factory, installation=installation)
    # Check the public consumer exists before spending time creating claim history.
    observe = resolver.observe
    if result != "unstaged-claim":
        _release, claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result=result)
    receipt = await observe(source, platform=request.platform)
    if result in {"unstaged-claim", "unclaimed", "live"}:
        assert receipt is None
    else:
        assert receipt.request.result == result and receipt.request.claim == claim
        if result == "artifact-ready":
            assert await resolver.resolve(source, platform=request.platform) == receipt
        else:
            with pytest.raises(DBAPIError, match="absent or ambiguous"):
                await resolver.resolve(source, platform=request.platform)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == int(result != "unstaged-claim")


@pytest.mark.parametrize("boundary", ["cancelled", "expired", "source", "permission"])
async def test_platform_observation_never_converts_fence_or_sql_errors_to_pending(prepared_input, boundary):
    factory, engine, installation, _plan, source, request = prepared_input
    resolver = import_module("loom_capacity_build_guard.artifact_resolver").BuildAcceptedArtifactResolver(
        session_factory=factory, installation=installation)
    observe = resolver.observe
    with engine.begin() as connection:
        if boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        elif boundary == "expired":
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now()-interval '1 second' WHERE id=:id"), {"id": source.build_attempt.id})
        elif boundary == "source":
            connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": source.candidate.id})
        else:
            connection.execute(text("REVOKE EXECUTE ON FUNCTION loom_capacity_build_guard.read_platform_outcome(uuid,uuid,jsonb,bytea,text) FROM PUBLIC"))
            agent = factory.kw["bind"].url.username
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION loom_capacity_build_guard.read_platform_outcome(uuid,uuid,jsonb,bytea,text) FROM {engine.dialect.identifier_preparer.quote(agent)}")
    with pytest.raises(DBAPIError):
        await observe(source, platform=request.platform)


async def test_platform_observation_requires_committed_outcome(prepared_input, monkeypatch):
    from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
    from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

    factory, _engine, installation, _plan, source, _request = prepared_input
    _release, claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result="live")
    wire = canonical_build_source(source)
    async with factory.begin() as session:
        await BuildGuardExecutionStore(session, installation=installation).record_outcome(
            outcome_request(claim, result="failed"), worker_credential=CREDENTIAL)
        with pytest.raises(DBAPIError, match="committed"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.read_platform_outcome(:installation,:request,CAST(:source AS jsonb),:wire,:digest)"),
                    {"installation": installation.id, "request": claim.request_id, "source": wire.decode("ascii"), "wire": wire, "digest": sha256(wire).hexdigest()})


@pytest.mark.parametrize("boundary", ["execute", "public", "search-path"])
def test_platform_outcome_private_authority_and_upgrade_boundary(build_guard_database, boundary):
    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "build_guard_0029")
    signature = "loom_capacity_build_guard.read_platform_outcome(uuid,uuid,jsonb,bytea,text)"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT to_regprocedure(:signature)"), {"signature": signature}) is None
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT to_regprocedure(:signature)"), {"signature": signature}) is not None
    command.downgrade(config, "build_guard_0029")
    command.upgrade(config, "head")
    statements = {"execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC", "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.exec_driver_sql(statements[boundary])
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
