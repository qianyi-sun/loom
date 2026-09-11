"""Unbound cleanup must survive cancellation before first preparation."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutablePreparedBootstrapRevocationV2
from loom_capacity_build_guard.bootstrap_store import BuildGuardBootstrapStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.integration.test_personal_dev_build_guard_bootstrap import bootstrap
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical, store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("stage", ["bootstrap-only", "admitted", "prepared"])
async def test_unbound_revocation_survives_cancellation_without_releasing_hold(prepared_input, stage):
    factory, engine, installation, plan, _source, request = prepared_input
    if stage == "bootstrap-only":
        proposal = bootstrap(plan)
        async with factory.begin() as session:
            await BuildGuardBootstrapStore(session, installation=installation).register(proposal)
        binding = proposal.binding
    else:
        registration, digest = await admitted(prepared_input)
        binding = registration.binding
        if stage == "prepared":
            async with factory.begin() as session:
                await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":request.id})
    revocation = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        receipt = await store(session, installation).revoke_prepared_bootstrap(revocation)
    assert receipt.binding == binding
    assert receipt.reporter_incarnation == installation.document.reporter_incarnation
    assert receipt.request_digest == canonical_executable_digest(revocation)
    assert receipt.protected_release_sha256 == receipt.request_digest
    async with factory.begin() as session:
        assert await store(session, installation).revoke_prepared_bootstrap(revocation) == receipt
        observation = await store(session, installation).observe_intent(binding)
        assert observation.prepared_revocation == receipt
        assert observation.worker_id is None
        assert observation.release is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == int(stage != "bootstrap-only")
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


async def test_revocation_cannot_replace_bound_physical_cleanup(prepared_input):
    factory, _engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    async with factory.begin() as session:
        await store(session, installation).bind_slurm_job(physical(registration))
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="unbound"):
            await store(session, installation).revoke_prepared_bootstrap(request)


@pytest.mark.parametrize("prepared", [False, True])
async def test_revocation_fences_later_admission_but_preserves_preparation_replay(prepared_input, prepared):
    factory, _engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    original = None
    if prepared:
        async with factory.begin() as session:
            original = await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        revoked = await store(session, installation).revoke_prepared_bootstrap(request)
        if original is not None:
            assert revoked.protected_high_water > original.protected_high_water
        with pytest.raises(DBAPIError, match="committed"):
            await store(session, installation).observe_intent(request.binding)
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="revoked"):
            await BuildGuardBootstrapStore(session, installation=installation).authorize_publication(request.binding.intent_id)
        with pytest.raises(DBAPIError, match="revoked"):
            await store(session, installation).bind_slurm_job(physical(registration))
        if prepared:
            assert await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest) == original
        else:
            with pytest.raises(DBAPIError, match="revoked"):
                await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)


@pytest.mark.parametrize("boundary", ["binding", "epoch", "operation", "protected-epoch"])
async def test_revocation_rejects_changed_replay(prepared_input, boundary):
    factory, _engine, installation, _plan, _source, _request = prepared_input
    registration, _digest = await admitted(prepared_input)
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        original = await store(session, installation).revoke_prepared_bootstrap(request)
    updates = {"binding":{"binding":request.binding.model_copy(update={"account_id":"foreign"})},
        "epoch":{"bootstrap_registration_epoch":2,"protected_registration_epoch":3},
        "operation":{"operation_id":uuid4()},"protected-epoch":{"protected_registration_epoch":3}}
    async with factory.begin() as session:
        with pytest.raises((ValueError,DBAPIError)):
            await store(session, installation).revoke_prepared_bootstrap(request.model_copy(update=updates[boundary]))
        assert await store(session, installation).revoke_prepared_bootstrap(request) == original


async def test_revocation_and_physical_binding_race_has_one_winner(prepared_input):
    import asyncio

    factory, engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    revocation = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    binding = physical(registration)

    async def compete(revoke):
        try:
            async with factory.begin() as session:
                guard = store(session, installation)
                if revoke:
                    await guard.revoke_prepared_bootstrap(revocation)
                else:
                    await guard.bind_slurm_job(binding)
            return True
        except DBAPIError:
            return False

    assert sum(await asyncio.gather(compete(True),compete(False))) == 1
    with engine.connect() as connection:
        terminal = connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.bootstrap_revocations"))
        bound = connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events WHERE kind='bound'"))
        assert terminal + bound == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_unprepared_expired_bootstrap_can_be_revoked_but_not_published(prepared_input):
    import asyncio
    from datetime import UTC, datetime, timedelta

    factory, _engine, installation, plan, _source, _request = prepared_input
    expiry = datetime.now(UTC) + timedelta(seconds=2)
    proposal = bootstrap(plan).model_copy(update={"expires_at":expiry})
    async with factory.begin() as session:
        await BuildGuardBootstrapStore(session, installation=installation).register(proposal)
    async with asyncio.timeout(5):
        while datetime.now(UTC) < expiry:
            await asyncio.sleep(0.05)
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=proposal.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        revoked = await store(session, installation).revoke_prepared_bootstrap(request)
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(request.binding)).prepared_revocation == revoked
        with pytest.raises(DBAPIError, match="revoked"):
            await BuildGuardBootstrapStore(session, installation=installation).authorize_publication(request.binding.intent_id)


async def test_revocation_requires_prior_bootstrap_commit_and_prevents_own_downgrade(prepared_input, build_guard_database):
    from alembic import command

    factory, engine, installation, plan, _source, _request = prepared_input
    proposal = bootstrap(plan)
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=proposal.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        await BuildGuardBootstrapStore(session, installation=installation).register(proposal)
        with pytest.raises(DBAPIError, match="committed"):
            await store(session, installation).revoke_prepared_bootstrap(request)
    async with factory.begin() as session:
        receipt = await store(session, installation).revoke_prepared_bootstrap(request)
    config = build_guard_database[0]
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(config, "build_guard_0010")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0027"
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(request.binding)).prepared_revocation == receipt
