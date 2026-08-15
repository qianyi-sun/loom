"""Protected subject-side persistence for executable bootstrap proposals."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_bootstrap import (
    ProtectedExecutableBootstrapCoordinator,
    ProtectedExecutableBootstrapError,
)
from loom_capacity_agent.store import CapacityAgentStore
from loom_capacity_guard.contracts import GuardFenceV1
from loom_capacity_guard.store import CapacityGuardStore
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapProposalV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
    canonical_executable_digest,
)


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


@asynccontextmanager
async def _session(
    database: dict[str, object],
    *,
    role: str,
) -> AsyncIterator[AsyncSession]:
    url_key = "agent_url" if role == "agent" else "migrator_url"
    engine = create_async_engine(
        make_url(_value(database, url_key)), isolation_level="SERIALIZABLE"
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            if role == "owner":
                quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(
                    _value(database, "owner_role")
                )
                await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
            yield session
    finally:
        await engine.dispose()


async def _configuration(
    database: dict[str, object],
) -> ReporterConfigurationV1:
    fence = GuardFenceV1(
        environment_id="dev-alice",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        deployment_generation=7,
        configuration_generation=11,
        candidate_digest="a" * 64,
    )
    registration = AgentRegistrationV1(
        **fence.model_dump(mode="python"),
        agent_incarnation=uuid4(),
    )
    async with _session(database, role="owner") as session:
        await CapacityGuardStore(
            session,
            expected_owner_role=_value(database, "owner_role"),
        ).initialize_disabled_authority(fence)
        await CapacityAgentStore(
            session,
            expected_owner_role=_value(database, "owner_role"),
            expected_agent_role=_value(database, "agent_role"),
        ).register_agent(registration)
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        protected_admission_sha256="3" * 64,
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


def _proposal(configuration: ReporterConfigurationV1) -> ExecutableBootstrapProposalV2:
    execution = ExecutionFenceV2(
        authority_incarnation=uuid4(),
        writer_epoch=2,
        configuration_epoch=3,
        execution_epoch=4,
        execution_manifest_sha256="c" * 64,
        execution_state="active",
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        trusted_fleet_release_sha256="d" * 64,
        allocation_epoch=5,
    )
    return ExecutableBootstrapProposalV2(
        binding=ExecutableIntentBindingV2(
            execution=execution,
            tranche_id=uuid4(),
            intent_id=uuid4(),
            shape_instance_id="shape-1",
            subject_id=configuration.subject_id,
            subject_incarnation=configuration.subject_incarnation,
            account_id="owner-1",
            tier_id="development",
            candidate=CandidateBindingV2(
                algorithm="source-sha256",
                identity="a" * 64,
                publication_sha256="b" * 64,
            ),
            candidate_generation=6,
            deployment_generation=configuration.deployment_generation,
            pool_id="oldlab",
            pool_generation=8,
            executor_id="oldlab-executor",
            executor_incarnation=uuid4(),
            shape_id="one-slot",
            profile_id="profile-1",
            profile_generation=1,
            profile_digest="e" * 64,
            concurrency_slots=1,
            resources=ResourceVectorV1(
                slots=1,
                cpu_millicores=1_000,
                memory_bytes=1_073_741_824,
            ),
            node_ids=("node-1",),
        ),
        command_sequence=1,
        proposal_epoch=1,
        bootstrap_sha256="f" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )


@pytest.mark.asyncio
async def test_protected_bootstrap_is_durable_and_replays_exact_acknowledgement(
    capacity_guard_database: dict[str, object],
) -> None:
    configuration = await _configuration(capacity_guard_database)
    proposal = _proposal(configuration)

    async def protect_once():  # type: ignore[no-untyped-def]
        async with _session(capacity_guard_database, role="agent") as session:
            return await ProtectedExecutableBootstrapCoordinator(
                session,
                configuration=configuration,
            ).protect(proposal)

    first = await protect_once()
    replay = await protect_once()

    assert replay == first
    assert first.registration.intent_id == proposal.binding.intent_id
    assert first.registration.proposal_digest == canonical_executable_digest(proposal)
    assert first.acknowledgement.binding == proposal.binding
    assert first.acknowledgement.bootstrap_evidence_sha256 == canonical_executable_digest(
        first.registration
    )
    assert first.acknowledgement.protected_admission_sha256 == "3" * 64
    async with _session(capacity_guard_database, role="owner") as session:
        assert (
            await session.execute(
                text(
                    "SELECT count(*) FROM "
                    "loom_capacity_guard.protected_executable_bootstrap_registrations"
                )
            )
        ).scalar_one() == 1


@pytest.mark.asyncio
async def test_protected_bootstrap_rejects_changed_subject_before_persistence(
    capacity_guard_database: dict[str, object],
) -> None:
    configuration = await _configuration(capacity_guard_database)
    proposal = _proposal(configuration)
    changed = proposal.model_copy(
        update={"binding": proposal.binding.model_copy(update={"subject_id": uuid4()})}
    )

    with pytest.raises(ProtectedExecutableBootstrapError, match="subject_id"):
        async with _session(capacity_guard_database, role="agent") as session:
            await ProtectedExecutableBootstrapCoordinator(
                session,
                configuration=configuration,
            ).protect(changed)

    async with _session(capacity_guard_database, role="owner") as session:
        assert (
            await session.execute(
                text(
                    "SELECT count(*) FROM "
                    "loom_capacity_guard.protected_executable_bootstrap_registrations"
                )
            )
        ).scalar_one() == 0


@pytest.mark.asyncio
async def test_protected_bootstrap_rejects_unconfigured_physical_pool(
    capacity_guard_database: dict[str, object],
) -> None:
    configuration = await _configuration(capacity_guard_database)
    proposal = _proposal(configuration)
    changed = proposal.model_copy(
        update={
            "binding": proposal.binding.model_copy(
                update={
                    "pool_id": "gb10",
                    "executor_id": "gb10-executor",
                }
            )
        }
    )

    with pytest.raises(ProtectedExecutableBootstrapError, match="pool_id"):
        async with _session(capacity_guard_database, role="agent") as session:
            await ProtectedExecutableBootstrapCoordinator(
                session,
                configuration=configuration,
            ).protect(changed)


@pytest.mark.asyncio
async def test_protected_bootstrap_rejects_conflicting_epoch_and_is_append_only(
    capacity_guard_database: dict[str, object],
) -> None:
    configuration = await _configuration(capacity_guard_database)
    proposal = _proposal(configuration)
    async with _session(capacity_guard_database, role="agent") as session:
        await ProtectedExecutableBootstrapCoordinator(
            session,
            configuration=configuration,
        ).protect(proposal)

    conflicting = proposal.model_copy(update={"bootstrap_sha256": "0" * 64})
    with pytest.raises(DBAPIError, match="conflicting protected bootstrap replay"):
        async with _session(capacity_guard_database, role="agent") as session:
            await ProtectedExecutableBootstrapCoordinator(
                session,
                configuration=configuration,
            ).protect(conflicting)

    statements = (
        "UPDATE loom_capacity_guard.protected_executable_bootstrap_registrations "
        "SET bootstrap_sha256 = repeat('1', 64)",
        "DELETE FROM loom_capacity_guard.protected_executable_bootstrap_registrations",
        "TRUNCATE loom_capacity_guard.protected_executable_bootstrap_registrations",
    )
    for statement in statements:
        with pytest.raises(DBAPIError, match="append-only"):
            async with _session(capacity_guard_database, role="owner") as session:
                await session.execute(text(statement))


@pytest.mark.asyncio
async def test_protected_bootstrap_evidence_blocks_guard_downgrade(
    capacity_guard_database: dict[str, object],
) -> None:
    configuration = await _configuration(capacity_guard_database)
    async with _session(capacity_guard_database, role="agent") as session:
        await ProtectedExecutableBootstrapCoordinator(
            session,
            configuration=configuration,
        ).protect(_proposal(configuration))

    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    previous = {
        name: os.environ.get(name)
        for name in (
            "LOOM_CAPACITY_GUARD_DB_URL",
            "LOOM_CAPACITY_GUARD_OWNER_ROLE",
            "LOOM_CAPACITY_GUARD_AGENT_ROLE",
            "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE",
            "LOOM_CAPACITY_GUARD_OBSERVER_ROLE",
        )
    }
    os.environ["LOOM_CAPACITY_GUARD_DB_URL"] = _value(capacity_guard_database, "migrator_url")
    os.environ["LOOM_CAPACITY_GUARD_OWNER_ROLE"] = _value(capacity_guard_database, "owner_role")
    os.environ["LOOM_CAPACITY_GUARD_AGENT_ROLE"] = _value(capacity_guard_database, "agent_role")
    os.environ["LOOM_CAPACITY_GUARD_EXECUTOR_ROLE"] = _value(
        capacity_guard_database, "executor_role"
    )
    os.environ["LOOM_CAPACITY_GUARD_OBSERVER_ROLE"] = _value(
        capacity_guard_database, "observer_role"
    )
    try:
        with pytest.raises(RuntimeError, match="protected bootstrap evidence exists"):
            command.downgrade(config, "guard_0011")
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    async with _session(capacity_guard_database, role="owner") as session:
        assert (
            await session.execute(
                text("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version")
            )
        ).scalar_one() == "guard_0019"
