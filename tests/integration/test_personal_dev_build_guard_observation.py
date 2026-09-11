"""Recovery reads exact committed build evidence without granting release."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ProtectedIntentObservationV2
from loom_capacity_manager.executable_contracts import ExecutableAdmissionPlanClosureV2
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical, store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("state", ["prepared", "bound", "cancelled", "closed"])
async def test_observation_recovers_committed_preparation_without_release(prepared_input, state):
    factory, engine, installation, plan, _source, request = prepared_input
    registration, digest = await admitted(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    if state == "bound":
        async with factory.begin() as session:
            await store(session, installation).bind_slurm_job(physical(registration))
    elif state in {"cancelled", "closed"}:
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":request.id})
        if state == "closed":
            from loom_capacity_build_guard.plan_store import BuildGuardPlanStore

            async with factory.begin() as session:
                await BuildGuardPlanStore(session, installation=installation).close_plan(
                    ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=plan, close_reason="manager-closed"))
    expected = ProtectedIntentObservationV2(binding=registration.binding, bootstrap_registration_epoch=1)
    async with factory.begin() as session:
        observed = await store(session, installation).observe_intent(registration.binding)
        assert observed == expected
    async with factory.begin() as session:
        assert await store(session, installation).observe_intent(registration.binding) == expected
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == (2 if state == "bound" else 1)
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


@pytest.mark.parametrize("boundary", ["missing", "uncommitted", "intent", "subject", "account", "pool", "executor", "generation"])
async def test_observation_rejects_absent_or_changed_preparation(prepared_input, boundary):
    factory, _engine, installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    if boundary not in {"missing", "uncommitted"}:
        async with factory.begin() as session:
            await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
    binding = registration.binding
    changes = {
        "intent": {"intent_id":uuid4()}, "subject": {"subject_id":uuid4()},
        "account": {"account_id":"foreign"}, "pool": {"pool_id":"foreign"},
        "executor": {"executor_incarnation":uuid4()},
        "generation": {"deployment_generation":binding.deployment_generation+1},
    }
    binding = binding.model_copy(update=changes.get(boundary, {}))
    async with factory.begin() as session:
        if boundary == "uncommitted":
            await store(session, installation).prepare_worker(registration, bootstrap_sha256=digest)
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).observe_intent(binding)
