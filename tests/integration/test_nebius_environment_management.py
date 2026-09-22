"""Management transactions use real PostgreSQL, not an in-memory owner registry."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.db.schema import Team, User
from loom.nebius_environment_contract import EnvironmentRegistrationV1, new_environment_registration
from loom.nebius_environment_render import render_environment
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import ROOT, platform_inputs as platform_inputs


@pytest.fixture
async def environment_registry(isolated_migration_postgres_url, platform_inputs):
    from loom.db.nebius_environment_schema import NebiusPlatformBudget
    from loom_service.environment_management.registry import EnvironmentRegistry

    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    foundation = foundation_from(platform_inputs[0])
    team, alice, bob = uuid4(), uuid4(), uuid4()
    async with factory.begin() as session:
        session.add(Team(id=team, name="environment-owners"))
        for identity, name in ((alice, "alice"), (bob, "bob")):
            session.add(User(id=identity, username=name, username_normalized=name, status="active"))
        session.add(NebiusPlatformBudget(
            cluster_id=foundation.platform_config["cluster_id"], cpu_millis=100000,
            memory_mib=1000000, storage_mib=1000000, ephemeral_storage_mib=1000000,
        ))
    principals = [AuthContext(
        token_hash=b"", type="team", scopes=["read:own", "submit"], team_id=team,
        expires_at=None, user_id=user, auth_kind="session",
    ) for user in (alice, bob)]

    def prepare(slug="alice", principal=principals[0], *, candidate_id=None):
        row = new_environment_registration(
            foundation, environment_id=uuid4(), incarnation=uuid4(),
            owner_user_id=principal.user_id, owner_team_id=principal.team_id, slug=slug,
        )
        row = EnvironmentRegistrationV1.model_validate({
            **row.model_dump(), "candidate_id": candidate_id or candidate_identity,
        })
        return render_environment(row, platform_inputs[1], foundation,
                                  profile=platform_inputs[2], keyring={}, repo_root=ROOT)

    candidate_identity = uuid4()
    try:
        yield EnvironmentRegistry(factory), factory, principals, prepare
    finally:
        await engine.dispose()


async def test_create_replay_commits_one_environment_complete_namespaces_and_one_budget_hold(environment_registry):
    from loom.db.nebius_environment_schema import (
        NebiusEnvironment, NebiusEnvironmentNamespace, NebiusPlatformReservation,
    )

    registry, factory, (alice, _), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="create-1", prepared=prepare())
    replay = await registry.create(principal=alice, idempotency_key="create-1", prepared=prepare())
    assert replay == first
    assert first.phase == "pending"
    assert first.execution_enabled is False
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironmentNamespace)) == 3
        reservation = (await session.execute(select(NebiusPlatformReservation))).scalar_one()
        assert reservation.cpu_millis == 1050
        assert reservation.memory_mib == 2688


async def test_concurrent_retry_has_one_identity_and_different_payload_conflicts(environment_registry):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, _), prepare = environment_registry
    results = await asyncio.gather(*[
        registry.create(principal=alice, idempotency_key="same-key", prepared=prepare())
        for _ in range(3)
    ])
    assert len({item.operation_id for item in results}) == 1
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, idempotency_key="same-key", prepared=prepare("another"))


async def test_owner_conflict_and_cross_owner_status_do_not_change_registration(environment_registry):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, bob), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="create-1", prepared=prepare())
    with pytest.raises(ManagementError, match="environment_name_conflict"):
        await registry.create(principal=bob, idempotency_key="create-1", prepared=prepare("alice", bob))
    with pytest.raises(ManagementError, match="environment_forbidden"):
        await registry.get_operation(first.operation_id, principal=bob)
    assert await registry.get_operation(first.operation_id, principal=alice) == first
    assert await registry.list_environments(principal=bob) == []
    assert len(await registry.list_environments(principal=alice)) == 1


async def test_supplied_owner_or_generic_team_credentials_cannot_create(environment_registry):
    from loom_service.environment_management.registry import ManagementError

    registry, _, (alice, bob), prepare = environment_registry
    with pytest.raises(ManagementError, match="environment_owner_mismatch"):
        await registry.create(principal=bob, idempotency_key="create-1", prepared=prepare())
    with pytest.raises(ManagementError, match="user_identity_required"):
        await registry.create(principal=replace(alice, user_id=None), idempotency_key="create-1", prepared=prepare())


async def test_two_owners_cannot_overbook_last_platform_capacity(environment_registry):
    from sqlalchemy import update

    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget
    from loom_service.environment_management.registry import ManagementError

    registry, factory, (alice, bob), prepare = environment_registry
    async with factory.begin() as session:
        await session.execute(update(NebiusPlatformBudget).values(cpu_millis=1050))
    results = await asyncio.gather(
        registry.create(principal=alice, idempotency_key="a", prepared=prepare()),
        registry.create(principal=bob, idempotency_key="b", prepared=prepare("bob", bob)),
        return_exceptions=True,
    )
    failures = [value for value in results if isinstance(value, ManagementError)]
    assert len(failures) == 1
    assert failures[0].code == "platform_capacity_exhausted"
    assert failures[0].details["needed"]["cpu_millis"] == 1050
    assert failures[0].details["available"]["cpu_millis"] == 0
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 1


async def test_resource_namespace_collision_rolls_back_every_claim(environment_registry):
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusEnvironmentNamespace
    from loom_service.environment_management.registry import ManagementError

    registry, factory, (alice, bob), prepare = environment_registry
    first = await registry.create(principal=alice, idempotency_key="a", prepared=prepare())
    second = prepare("bob", bob)
    async with factory.begin() as session:
        claim = (await session.execute(select(NebiusEnvironmentNamespace).where(
            NebiusEnvironmentNamespace.environment_id == first.environment_id,
            NebiusEnvironmentNamespace.role == "execution",
        ))).scalar_one()
        claim.namespace_name = second.registration.application_namespace
    with pytest.raises(ManagementError, match="environment_name_conflict"):
        await registry.create(principal=bob, idempotency_key="b", prepared=second)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironment)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusEnvironmentNamespace)) == 3
