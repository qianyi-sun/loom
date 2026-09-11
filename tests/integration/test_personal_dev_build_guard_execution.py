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
    ExecutableBootstrapRegistrationV2,
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


async def admitted(values, *, publish=True):
    factory, _engine, installation, plan, source, request = values
    proposal = bootstrap(plan)
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


async def test_physical_retention_after_cancellation_does_not_release_hold(prepared_input):
    factory, engine, installation, _plan, _source, request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        prepared = await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":request.id})
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
