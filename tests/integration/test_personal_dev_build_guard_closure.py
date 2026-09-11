"""Manager closure is replayable after source expiry, not physical release."""

from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureV2,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("prepared", [False, True])
async def test_closure_replays_after_source_cancellation_without_releasing_holds(prepared_input, prepared):
    sessions, engine, retained, proposal, registration, request = prepared_input
    if prepared:
        async with sessions.begin() as session:
            await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    async with sessions.begin() as session:
        closed = await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
    async with sessions.begin() as session:
        work = await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id)
    assert work.acknowledgement.closure_id == closure.closure_id
    assert work.acknowledgement.disposition_digest == closed.digest
    assert work.idempotency_key == uuid5(NAMESPACE_URL,
        f"loom:protected-executable-admission-cleanup:{canonical_executable_digest(work.acknowledgement)}")
    assert work.acknowledgement.disposition_kind == ("abandoned" if prepared else "never-converged")
    async with sessions.begin() as session:
        assert await BuildGuardPlanStore(session, installation=retained).close_plan(closure) == closed
        assert await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id) == work
        with pytest.raises(DBAPIError, match="terminal disposition"):
            await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='closure'")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == int(prepared)
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='release'")) == 0


async def test_closure_rejects_rebinding_and_rolls_back_with_caller(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    async with sessions() as session:
        await session.begin()
        await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
        await session.rollback()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match="replay"):
            await BuildGuardPlanStore(session, installation=retained).close_plan(closure.model_copy(update={"closure_id": uuid4()}))


async def test_closure_remains_available_after_lost_publication_reply(prepared_input):
    sessions, engine, retained, proposal, registration, request = prepared_input
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).prepare(proposal, sources={request.id: registration})
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).authorize_publication(proposal.plan_id)
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="allocation-superseded")
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).close_plan(closure)
    async with sessions.begin() as session:
        work = await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id)
    assert work.acknowledgement.disposition_kind == "abandoned"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 2
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_closure_acknowledgement_requires_committed_closure(prepared_input):
    sessions, engine, retained, proposal, _, _ = prepared_input
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    async with sessions.begin() as session:
        store = BuildGuardPlanStore(session, installation=retained)
        async with session.begin_nested():
            await store.close_plan(closure)
        with pytest.raises(DBAPIError, match="committed closure"):
            await store.authorize_closure_publication(proposal.plan_id)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
    async with sessions.begin() as session:
        await BuildGuardPlanStore(session, installation=retained).authorize_closure_publication(proposal.plan_id)


@pytest.mark.parametrize("boundary", ["wire", "unknown", "reporter", "subject", "closure-id", "missing",
    "executable", "slot", "mapping", "duplicate-shape", "shape-resources", "candidate", "execution-state", "node"])
async def test_sql_closure_rejects_contract_or_owner_drift_without_writes(prepared_input, boundary):
    import json
    from hashlib import sha256

    from loom_capacity_manager.executable_contracts import canonical_executable_bytes

    sessions, engine, retained, proposal, _, _ = prepared_input
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    payload = json.loads(canonical_executable_bytes(closure))
    if boundary == "unknown":
        payload["unknown"] = "unexpected"
    elif boundary == "reporter":
        payload["proposal"]["reporter_incarnation"] = str(uuid4())
    elif boundary == "subject":
        for shape in payload["proposal"]["shapes"]:
            shape["binding"]["subject_id"] = str(uuid4())
    elif boundary == "closure-id":
        payload["closure_id"] = payload["proposal"]["plan_id"]
    elif boundary == "missing":
        del payload["close_reason"]
    elif boundary == "executable":
        payload["proposal"]["executable"] = False
    elif boundary == "slot":
        payload["proposal"]["allowances"][0]["shape_slot_index"] = True
    elif boundary == "mapping":
        payload["proposal"]["allowances"][0]["submission_intent_id"] = str(uuid4())
    elif boundary == "duplicate-shape":
        payload["proposal"]["shapes"] *= 2
    elif boundary == "shape-resources":
        payload["proposal"]["shapes"][0]["binding"]["resources"]["slots"] = 2
    elif boundary == "candidate":
        payload["proposal"]["shapes"][0]["binding"]["candidate"]["algorithm"] = "unknown"
    elif boundary == "execution-state":
        payload["proposal"]["shapes"][0]["binding"]["execution"]["execution_state"] = "unknown"
    elif boundary == "node":
        payload["proposal"]["shapes"][0]["binding"]["node_ids"] = [123]
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    if boundary == "wire":
        wire = b" " + wire
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError, match=r"contract|canonical|binding|identity"):
            await session.scalar(text("SELECT loom_capacity_build_guard.close_plan(:installation, CAST(:payload AS jsonb), :wire, :digest)"),
                {"installation": retained.id, "payload": wire.decode("ascii"), "wire": wire, "digest": sha256(wire).hexdigest()})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.plans")) == 0


@pytest.mark.parametrize("signature", ["close_plan(uuid,jsonb,bytea,text)", "authorize_closure_publication(uuid,uuid)"])
@pytest.mark.parametrize("boundary", ["missing", "execute", "search-path", "grant-option"])
def test_migration_requires_exact_closure_entrypoints(build_guard_database, signature, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    function = f"loom_capacity_build_guard.{signature}"
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as connection:
        if boundary == "missing":
            connection.exec_driver_sql(f"DROP FUNCTION {function}")
        elif boundary == "execute":
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {function} FROM {quote(agent)}")
        elif boundary == "search-path":
            connection.exec_driver_sql(f"ALTER FUNCTION {function} SET search_path=public")
        else:
            connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {function} TO {quote(agent)} WITH GRANT OPTION")
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
