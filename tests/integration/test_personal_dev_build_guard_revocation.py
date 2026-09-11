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
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
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
