"""Native preparation binds admitted source; physical retention survives cancellation."""

from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_build_guard.bootstrap_store import BuildGuardBootstrapStore
from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureV2,
    ExecutableBootstrapRegistrationV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_bootstrap import bootstrap
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def admitted(values, *, publish=True, expires_at=None):
    factory, _engine, installation, plan, source, request = values
    proposal = bootstrap(plan)
    if expires_at is not None:
        proposal = proposal.model_copy(update={"expires_at":expires_at})
    async with factory.begin() as session:
        retained = await BuildGuardBootstrapStore(session, installation=installation).register(proposal)
        await BuildGuardPlanStore(session, installation=installation).prepare(plan, sources={request.id:source})
    if publish:
        async with factory.begin() as session:
            await BuildGuardPlanStore(session, installation=installation).authorize_publication(plan.plan_id)
    return ExecutableBootstrapRegistrationV2(binding=proposal.binding,
        command_sequence=proposal.command_sequence, bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256=retained.digest), proposal.bootstrap_sha256


def store(session, installation):
    return import_module("loom_capacity_build_guard.execution_store").BuildGuardExecutionStore(session, installation=installation)


def physical(registration):
    return PhysicalJobBindingV2(operation_id=uuid4(), binding=registration.binding,
        bootstrap_registration_epoch=1, slurm_job_id="1234", ownership_evidence_sha256="b"*64)


async def test_native_prepare_and_physical_binding_replay_exactly(prepared_input):
    factory, engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        prepared = await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    assert prepared.admission_digest == canonical_executable_digest(registration)
    assert prepared.bootstrap_sha256 == digest
    request = physical(registration)
    async with factory.begin() as session:
        assert await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest) == prepared
        bound = await store(session, installation).bind_slurm_job(request)
    assert bound.binding_digest == canonical_executable_digest(request)
    assert bound.slurm_job_id == request.slurm_job_id
    assert bound.protected_high_water > prepared.protected_high_water
    async with factory.begin() as session:
        assert await store(session, installation).bind_slurm_job(request) == bound
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 2
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


@pytest.mark.parametrize("boundary", ["hash", "evidence", "sequence", "binding", "unpublished", "cancelled"])
async def test_native_preparation_rejects_unadmitted_work(prepared_input, boundary):
    factory, engine, installation, _plan, _source, request = prepared_input
    registration, digest = await admitted(prepared_input, publish=boundary != "unpublished")
    if boundary == "hash":
        digest = "c"*64
    elif boundary == "evidence":
        registration = registration.model_copy(update={"bootstrap_evidence_sha256":"c"*64})
    elif boundary == "sequence":
        registration = registration.model_copy(update={"command_sequence":registration.command_sequence+1})
    elif boundary == "binding":
        registration = registration.model_copy(update={"binding":registration.binding.model_copy(update={"account_id":"foreign"})})
    elif boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":request.id})
    async with factory.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_physical_binding_needs_committed_preparation(prepared_input):
    factory, engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="preparation"):
            await store(session, installation).bind_slurm_job(physical(registration))
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
        with pytest.raises(DBAPIError, match="committed"):
            await store(session, installation).bind_slurm_job(physical(registration))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 1


@pytest.mark.parametrize("terminal", ["cancelled", "closed"])
async def test_physical_retention_after_cancellation_does_not_release_hold(prepared_input, terminal):
    factory, engine, installation, plan, _source, request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        prepared = await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":request.id})
    if terminal == "closed":
        async with factory.begin() as session:
            await BuildGuardPlanStore(session, installation=installation).close_plan(
                ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=plan, close_reason="manager-closed"))
    binding = physical(registration)
    async with factory.begin() as session:
        # Recover a lost committed preparation reply before pending-journal cleanup.
        assert await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest) == prepared
        bound = await store(session, installation).bind_slurm_job(binding)
        assert bound.slurm_job_id == binding.slurm_job_id
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("boundary", ["job", "operation", "ownership", "epoch", "binding"])
async def test_physical_binding_rejects_replacement(prepared_input, boundary):
    factory, _engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    request = physical(registration)
    async with factory.begin() as session:
        original = await store(session, installation).bind_slurm_job(request)
    changes = {"job":{"slurm_job_id":"5678"}, "operation":{"operation_id":uuid4()},
        "ownership":{"ownership_evidence_sha256":"c"*64}, "epoch":{"bootstrap_registration_epoch":2},
        "binding":{"binding":request.binding.model_copy(update={"node_ids":("gb10-2",)})}}[boundary]
    async with factory.begin() as session:
        with pytest.raises((DBAPIError, ValueError)):
            await store(session, installation).bind_slurm_job(request.model_copy(update=changes))
        assert await store(session, installation).bind_slurm_job(request) == original


async def test_preparation_requires_publication_commit(prepared_input):
    factory, engine, installation, plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input, publish=False)
    async with factory.begin() as session:
        await BuildGuardPlanStore(session, installation=installation).authorize_publication(plan.plan_id)
        with pytest.raises(DBAPIError, match="committed publication"):
            await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 0


@pytest.mark.parametrize("operation", ["prepare", "bind"])
async def test_execution_response_validation_rolls_back_event(prepared_input, monkeypatch, operation):
    import json

    factory, engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    if operation == "bind":
        async with factory.begin() as session:
            await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    async with factory.begin() as session:
        scalar = session.scalar

        async def corrupt(*args, **kwargs):
            receipt = json.loads(await scalar(*args, **kwargs))
            receipt["intent_id"] = str(uuid4())
            return json.dumps(receipt, sort_keys=True, separators=(",",":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            if operation == "prepare":
                await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
            else:
                await store(session, installation).bind_slurm_job(physical(registration))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == int(operation=="bind")


@pytest.mark.parametrize("operation", ["prepare", "bind"])
@pytest.mark.parametrize("boundary", ["schema", "extra", "executable", "noncanonical"])
async def test_execution_direct_sql_rejects_non_contract_requests(prepared_input, operation, boundary):
    import json
    from hashlib import sha256

    factory, engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    if operation == "bind":
        async with factory.begin() as session:
            await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    payload = json.loads(canonical_executable_bytes(registration if operation=="prepare" else physical(registration)))
    if boundary=="schema":
        payload["schema_version"] = 1
    elif boundary=="extra":
        payload["untrusted"] = True
    elif boundary=="executable":
        payload["executable"] = False
    wire = json.dumps(payload, sort_keys=True, separators=(",",":"), ensure_ascii=True).encode("ascii")
    if boundary=="noncanonical":
        wire = b" " + wire
    function = "prepare_worker" if operation=="prepare" else "bind_slurm_job"
    extra = ",:bootstrap" if operation=="prepare" else ""
    async with factory.begin() as session, session.begin_nested():
        with pytest.raises(DBAPIError):
            await session.scalar(text(f"SELECT loom_capacity_build_guard.{function}(:installation,CAST(:payload AS jsonb),:wire,:digest{extra})"),
                {"installation":installation.id,"payload":wire.decode("ascii"),"wire":wire,
                    "digest":sha256(wire).hexdigest(),"bootstrap":digest})
        await session.rollback()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == int(operation=="bind")


@pytest.mark.parametrize("signature", ["prepare_worker(uuid,jsonb,bytea,text,text)", "bind_slurm_job(uuid,jsonb,bytea,text)", "observe_intent(uuid,jsonb,bytea,text)"])
@pytest.mark.parametrize("boundary", ["execute", "search-path", "grant-option"])
def test_execution_required_privileges_are_pinned(build_guard_database, signature, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = f"loom_capacity_build_guard.{signature}"
    with engine.begin() as connection:
        quoted = engine.dialect.identifier_preparer.quote(agent)
        if boundary=="execute":
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {signature} FROM {quoted}")
        elif boundary=="search-path":
            connection.exec_driver_sql(f"ALTER FUNCTION {signature} SET search_path=public")
        else:
            connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {signature} TO {quoted} WITH GRANT OPTION")
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_committed_preparation_recovery_survives_actual_bootstrap_expiry(prepared_input):
    import asyncio
    from datetime import UTC, datetime, timedelta

    factory, _engine, installation, _plan, _source, _request = prepared_input
    expiry = datetime.now(UTC)+timedelta(seconds=2)
    registration, digest = await admitted(prepared_input, expires_at=expiry)
    async with factory.begin() as session:
        prepared = await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    async with asyncio.timeout(5):
        while datetime.now(UTC) < expiry:
            await asyncio.sleep(0.05)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="expiry"):
            await BuildGuardBootstrapStore(session, installation=installation).authorize_publication(registration.binding.intent_id)
        assert await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest) == prepared
        assert (await store(session, installation).bind_slurm_job(physical(registration))).intent_id == registration.binding.intent_id
        observed = await store(session, installation).observe_intent(registration.binding)
        assert observed.binding == registration.binding
        assert observed.bootstrap_registration_epoch == 1
        assert observed.prepared_revocation is None
        assert observed.release is None


@pytest.mark.parametrize("collision", ["job", "operation"])
async def test_two_owners_cannot_share_physical_job_or_operation(prepared_input, sessions, owner_sessions, tmp_path, collision):
    from datetime import UTC, datetime

    from loom.personal_dev_build_platform_requests import stage_platform_requests
    from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
    from tests.integration.test_personal_dev_build_platform_requests import build_service
    from tests.integration.test_personal_dev_native_builder_store import _seed_running_attempt

    factory, engine, installation, plan, _source, first_request = prepared_input
    first, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(first, bootstrap_sha256=digest)
    first_physical = physical(first)
    async with factory.begin() as session:
        await store(session, installation).bind_slurm_job(first_physical)

    now = datetime.now(UTC)
    source = await _seed_running_attempt(sessions, now=now)
    member, runtime = build_service(tmp_path, source)
    subject, incarnation, reporter = uuid4(), uuid4(), uuid4()
    member = member.model_copy(update={"configuration":member.configuration.model_copy(update={
        "subject_id":subject,"subject_incarnation":incarnation,"demand_reporter_incarnation":reporter}),
        "acknowledgement":member.acknowledgement.model_copy(update={
            "subject_id":subject,"subject_incarnation":incarnation,"reporter_incarnation":reporter})})
    async with sessions.begin() as session:
        request, = await stage_platform_requests(session, source, member=member, runtime=runtime,
            platforms=(first_request.platform,), now=now)
    owner_factory, owner = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        second_installation = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=runtime)
    intent = uuid4()
    shape = plan.shapes[0].model_copy(update={"binding":plan.shapes[0].binding.model_copy(update={
        "intent_id":intent,"subject_id":subject,"subject_incarnation":incarnation,
        "account_id":member.configuration.account_id})})
    second_plan = plan.model_copy(update={"plan_id":uuid4(),"proposal_id":uuid4(),"admission_incarnation":uuid4(),
        "reporter_incarnation":reporter,"shapes":(shape,),"allowances":(plan.allowances[0].model_copy(update={
            "allowance_id":uuid4(),"protected_attempt_id":request.id,"submission_intent_id":intent}),)})
    second, digest = await admitted((factory,engine,second_installation,second_plan,source,request))
    async with factory.begin() as session:
        await store(session, second_installation).prepare_worker(second, bootstrap_sha256=digest)
    second_physical = physical(second)
    if collision=="operation":
        second_physical = second_physical.model_copy(update={"operation_id":first_physical.operation_id,"slurm_job_id":"5678"})
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="unique"):
            await store(session, second_installation).bind_slurm_job(second_physical)
        # A fresh operation for a distinct job remains possible for the second owner.
        bound = await store(session, second_installation).bind_slurm_job(
            physical(second).model_copy(update={"slurm_job_id":"9012"}))
        assert bound.intent_id == intent
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 2
