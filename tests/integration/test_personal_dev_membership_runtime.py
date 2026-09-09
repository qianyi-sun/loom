"""Real guard primitives used by the trusted membership installer."""

from dataclasses import replace
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import loom.personal_dev_membership_runtime as membership_runtime
from loom.dev_instance import derive_identity
from loom.personal_dev_membership_runtime import (
    _registration,
    observe_database,
    read_protected_membership,
)
from loom_capacity_agent.contracts import AgentPoolCapabilityV1, ReporterConfigurationV1
from loom_capacity_agent.store import CapacityAgentStore
from loom_capacity_guard.contracts import GuardFenceV1
from loom_capacity_guard.store import CapacityGuardStore
from tests.integration.test_capacity_guard_store import _attempt, _requirements, _seed_trial


def _configuration():
    return ReporterConfigurationV1(
        environment_id="dev-alice",
        subject_id=UUID(int=1),
        subject_incarnation=UUID(int=2),
        authority_incarnation=UUID(int=3),
        agent_incarnation=UUID(int=4),
        reporter_incarnation=UUID(int=5),
        candidate_digest="a" * 64,
        candidate_identity="a" * 64,
        candidate_publication_sha256="b" * 64,
        deployment_generation=1,
        configuration_generation=1,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


@pytest.mark.parametrize("starting_generation", (1, 2, 3, 4))
@pytest.mark.asyncio
async def test_retirement_advances_only_reviewed_retained_generations(capacity_guard_database, starting_generation):
    database = capacity_guard_database
    engine = create_async_engine(database["admin_url"], isolation_level="SERIALIZABLE")
    owner, agent = database["owner_role"], database["agent_role"]
    old = _configuration().model_copy(update={"configuration_generation": starting_generation})
    target = _configuration().model_copy(update={"configuration_generation": 3})
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            quoted = engine.sync_engine.dialect.identifier_preparer.quote(owner)
            await session.execute(text(f"SET LOCAL ROLE {quoted}"))
            guard = CapacityGuardStore(session, expected_owner_role=owner)
            fence = GuardFenceV1.model_validate({name: getattr(old, name) for name in GuardFenceV1.model_fields})
            await guard.initialize_disabled_authority(fence)
            store = CapacityAgentStore(session, expected_owner_role=owner, expected_agent_role=agent)
            await store.register_agent(_registration(old))
            await session.execute(text("UPDATE loom_capacity_guard.agent_reporter_state SET high_water = high_water + 1"))
            if starting_generation == 4:
                with pytest.raises(ValueError, match="retirement"):
                    await read_protected_membership(session, owner=owner, agent=agent, configuration=target,
                                                    retirement_from_generation=(1, 2))
                assert await guard.read_guard_fence() == fence
            else:
                observed = await read_protected_membership(session, owner=owner, agent=agent, configuration=target,
                                                           retirement_from_generation=(1, 2))
                assert observed["reporter_high_water"] == 1
                assert (await guard.read_guard_fence()).configuration_generation == 3
                assert (await guard.read_guard_fence()).authority_mode == "disabled"
                audit_count = (await session.execute(text("SELECT count(*) FROM loom_capacity_guard.audit_events"))).scalar_one()
                assert await read_protected_membership(session, owner=owner, agent=agent, configuration=target,
                                                       retirement_from_generation=(1, 2)) == observed
                assert (await session.execute(text("SELECT count(*) FROM loom_capacity_guard.audit_events"))).scalar_one() == audit_count
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_protected_observation_measures_zero_and_retirement_preserves_history(
    capacity_guard_database,
    monkeypatch,
):
    database = capacity_guard_database
    trial_id = _seed_trial(database)
    requirements = _requirements()
    prior_attempt = _attempt(trial_id, requirements)
    engine = create_async_engine(database["admin_url"], isolation_level="SERIALIZABLE")
    owner, agent = database["owner_role"], database["agent_role"]
    configuration = _configuration()
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            quoted = engine.sync_engine.dialect.identifier_preparer.quote(owner)
            await session.execute(text(f"SET LOCAL ROLE {quoted}"))
            fence = GuardFenceV1.model_validate(
                {name: getattr(configuration, name) for name in GuardFenceV1.model_fields}
            )
            await CapacityGuardStore(
                session, expected_owner_role=owner
            ).initialize_disabled_authority(fence)
            await CapacityGuardStore(session, expected_owner_role=owner).register_trial_attempt(
                prior_attempt, requirements
            )
            await CapacityAgentStore(
                session, expected_owner_role=owner, expected_agent_role=agent
            ).register_agent(_registration(configuration))
            observed = await read_protected_membership(
                session, owner=owner, agent=agent, configuration=configuration
            )
            assert observed["legacy_writer_high_water"] == 0
            assert (
                await session.execute(
                    text("SELECT count(*) FROM loom_capacity_guard.legacy_compatibility_freezes")
                )
            ).scalar_one() == 0
            # A legitimate prior reporter sequence is not a legacy writer cursor.
            await session.execute(
                text(
                    "UPDATE loom_capacity_guard.agent_reporter_state SET high_water = high_water + 1"
                )
            )
            retiring = configuration.model_copy(update={"configuration_generation": 2})
            after = await read_protected_membership(
                session,
                owner=owner,
                agent=agent,
                configuration=retiring,
                retirement_from_generation=1,
            )
            assert after["reporter_high_water"] == 1
            assert after["legacy_writer_high_water"] == 0
            assert (
                await CapacityGuardStore(session, expected_owner_role=owner).read_protected_attempt(
                    prior_attempt.protected_attempt_id
                )
                == prior_attempt
            )
            audit_count = (
                await session.execute(text("SELECT count(*) FROM loom_capacity_guard.audit_events"))
            ).scalar_one()
            assert (
                await read_protected_membership(
                    session,
                    owner=owner,
                    agent=agent,
                    configuration=retiring,
                    retirement_from_generation=1,
                )
                == after
            )
            assert (
                await session.execute(text("SELECT count(*) FROM loom_capacity_guard.audit_events"))
            ).scalar_one() == audit_count
            for replacement in (
                retiring.model_copy(update={"candidate_publication_sha256": "c" * 64}),
                retiring.model_copy(update={"reporter_incarnation": UUID(int=99)}),
                retiring.model_copy(update={"configuration_generation": 3}),
            ):
                with pytest.raises(ValueError, match="registration"):
                    await read_protected_membership(
                        session, owner=owner, agent=agent, configuration=replacement
                    )
        monkeypatch.setattr(
            membership_runtime,
            "capacity_role_names",
            lambda _identity: (
                owner,
                database["migrator_role"],
                agent,
                database["executor_role"],
                database["observer_role"],
                database["runtime_role"],
            ),
        )
        identity = replace(
            derive_identity("alice"),
            database=database["database_name"],
            db_role=database["runtime_role"],
        )
        measured = await observe_database(
            database["admin_url"],
            identity=identity,
            configuration=retiring,
            agent_database_url=database["agent_url"],
        )
        assert measured["reporter_high_water"] == 1
        assert measured["legacy_writer_high_water"] == 0
        quoted_runtime = engine.sync_engine.dialect.identifier_preparer.quote(
            database["runtime_role"]
        )
        async with factory() as session, session.begin():
            await session.execute(
                text(f"GRANT INSERT ON loom_capacity_guard.audit_events TO {quoted_runtime}")
            )
        try:
            with pytest.raises(ValueError, match="mutation authority"):
                await observe_database(
                    database["admin_url"],
                    identity=identity,
                    configuration=retiring,
                    agent_database_url=database["agent_url"],
                )
        finally:
            async with factory() as session, session.begin():
                await session.execute(
                    text(f"REVOKE INSERT ON loom_capacity_guard.audit_events FROM {quoted_runtime}")
                )
        with pytest.raises(ValueError, match="endpoint"):
            await observe_database(
                database["admin_url"],
                identity=identity,
                configuration=retiring,
                agent_database_url=database["observer_url"],
            )
        # Ordinary replacement uses the existing shadow convergence primitives;
        # observation must retain legitimate predecessor attempts and sequence.
        updated = retiring.model_copy(
            update={
                "configuration_generation": 3,
                "deployment_generation": 2,
                "reporter_incarnation": UUID(int=6),
                "candidate_digest": "c" * 64,
                "candidate_identity": "c" * 64,
                "candidate_publication_sha256": "d" * 64,
            }
        )
        async with factory() as session, session.begin():
            await session.execute(text(f"SET LOCAL ROLE {quoted}"))
            guard = CapacityGuardStore(session, expected_owner_role=owner)
            await guard.reconfigure_disabled_authority(
                GuardFenceV1.model_validate(
                    {name: getattr(updated, name) for name in GuardFenceV1.model_fields}
                ),
                expected_configuration_generation=2,
            )
            await CapacityAgentStore(
                session, expected_owner_role=owner, expected_agent_role=agent
            ).reconfigure_agent(_registration(updated), expected_configuration_generation=2)
            observed_update = await read_protected_membership(
                session, owner=owner, agent=agent, configuration=updated
            )
            assert observed_update["reporter_high_water"] == 1
            assert (
                await guard.read_protected_attempt(prior_attempt.protected_attempt_id)
                == prior_attempt
            )
            with pytest.raises(ValueError, match="registration"):
                await read_protected_membership(
                    session, owner=owner, agent=agent, configuration=retiring
                )
    finally:
        await engine.dispose()
