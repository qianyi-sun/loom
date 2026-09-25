"""Real PostgreSQL application intent, replay, mixed admission and lease fencing."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_render import render_application
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_environment_management import environment_registry as environment_registry
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def applications(environment_registry, platform_inputs):
    from loom_service.application_management.registry import ApplicationRegistry

    legacy, factory, principals, prepare_legacy = environment_registry
    release_id = uuid4()

    def prepare(slug="alice", principal=principals[0], **changes):
        row, release, shared, foundation = inputs(platform_inputs, slug)
        release = release.model_copy(update={"release_id": release_id})
        row = ApplicationRegistrationV1.model_validate(row.model_dump() | {
            "owner_user_id": principal.user_id, "owner_team_id": principal.team_id,
            "release_id": release_id,
        } | changes)
        return dict(prepared=render_application(row, release, shared, foundation), release=release, shared=shared)

    return ApplicationRegistry(factory), factory, principals, prepare, legacy, prepare_legacy


async def test_concurrent_create_replay_freezes_one_plan_and_one_reservation(applications):
    from loom.db.nebius_application_schema import NebiusApplication
    from loom.db.nebius_application_operation_schema import NebiusApplicationOperation, NebiusApplicationReservation

    registry, factory, (alice, _), prepare, _, _ = applications
    results = await asyncio.gather(*[
        registry.create(principal=alice, idempotency_key="same-request", **prepare()) for _ in range(3)
    ])
    assert all(result == results[0] for result in results)
    assert results[0].phase == "pending"
    assert results[0].deployment_generation == results[0].access_generation == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplication)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusApplicationOperation)) == 1
        reservation = (await session.scalars(select(NebiusApplicationReservation))).one()
        assert reservation.storage_mib == 0
        assert reservation.cpu_millis > 0
        operation = await session.get(NebiusApplicationOperation, results[0].operation_id)
        original = operation.plan_json
    changed = prepare()
    changed["prepared"].files["10-network.yaml"][0]["metadata"]["annotations"] = {"changed-default": "later"}
    assert await registry.create(principal=alice, idempotency_key="same-request", **changed) == results[0]
    async with factory() as session:
        operation = await session.get(NebiusApplicationOperation, results[0].operation_id)
        assert operation.plan_json == original


async def test_replay_needs_no_publication_or_render_inputs_and_conflicting_intent_fails(applications):
    registry, _, (alice, _), prepare, _, _ = applications
    plan = prepare()
    first = await registry.create(principal=alice, idempotency_key="replay", **plan)
    assert await registry.replay_create(principal=alice, idempotency_key="replay", slug="alice",
                                       release_id=plan["release"].release_id) == first
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.replay_create(principal=alice, idempotency_key="replay", slug="alice", release_id=uuid4())
    with pytest.raises(ManagementError, match="idempotency_conflict"):
        await registry.create(principal=alice, idempotency_key="replay", **prepare("another"))


async def test_owner_and_team_isolation_and_scopes_apply_to_reads_and_writes(applications):
    registry, _, (alice, bob), prepare, _, _ = applications
    first = await registry.create(principal=alice, idempotency_key="owned", **prepare())
    assert await registry.get_operation(first.operation_id, principal=alice) == first
    for foreign in (bob, replace(alice, team_id=uuid4())):
        with pytest.raises(ManagementError, match="application_forbidden"):
            await registry.get_operation(first.operation_id, principal=foreign)
        assert await registry.list_applications(principal=foreign) == []
    with pytest.raises(ManagementError, match="application_owner_mismatch"):
        await registry.create(principal=bob, idempotency_key="stolen", **prepare())
    with pytest.raises(ManagementError) as denied:
        await registry.create(principal=replace(alice, scopes=["read:own"]), idempotency_key="weak", **prepare())
    assert denied.value.status_code == 403


@pytest.mark.parametrize("change", ["data", "release", "storage", "envelope", "generation"])
async def test_invalid_prepared_binding_never_commits(applications, change):
    from loom.db.nebius_application_schema import NebiusApplication

    registry, factory, (alice, _), prepare, _, _ = applications
    plan = prepare()
    if change == "data":
        plan["shared"] = plan["shared"].model_copy(update={"data_environment_id": uuid4()})
    elif change == "release":
        plan["release"] = plan["release"].model_copy(update={"release_id": uuid4()})
    elif change in {"storage", "envelope"}:
        costs = replace(plan["prepared"].platform_envelope, **({"storage_mib": 1} if change == "storage" else {"cpu_millis": 0}))
        plan["prepared"] = replace(plan["prepared"], platform_envelope=costs)
    else:
        row = plan["prepared"].registration.model_copy(update={"access_generation": 2})
        plan["prepared"] = replace(plan["prepared"], registration=row)
    with pytest.raises(ManagementError, match="invalid_application_plan"):
        await registry.create(principal=alice, idempotency_key="invalid", **plan)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusApplication)) == 0


@pytest.mark.parametrize("first", ["application", "legacy", "concurrent"])
async def test_application_and_legacy_share_one_capacity_allowance(applications, first):
    from loom.db.nebius_application_schema import NebiusApplication
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget

    registry, factory, (alice, _), prepare, legacy, prepare_legacy = applications
    app, old = prepare(), prepare_legacy("old")
    async with factory.begin() as session:
        budget = await session.get(NebiusPlatformBudget, old.registration.cluster_id)
        budget.cpu_millis = max(old.platform_envelope.cpu_millis, app["prepared"].platform_envelope.cpu_millis)
    async def create_app():
        return await registry.create(principal=alice, idempotency_key="app", **app)
    async def create_old():
        return await legacy.create(principal=alice, idempotency_key="old", prepared=old)
    if first == "concurrent":
        results = await asyncio.gather(create_app(), create_old(), return_exceptions=True)
        assert sum(not isinstance(result, Exception) for result in results) == 1
        errors = [result for result in results if isinstance(result, Exception)]
        assert isinstance(errors[0], ManagementError) and errors[0].code == "platform_capacity_exhausted"
    else:
        winner, loser = (create_app, create_old) if first == "application" else (create_old, create_app)
        await winner()
        with pytest.raises(ManagementError, match="platform_capacity_exhausted"):
            await loser()
    async with factory() as session:
        assert (await session.scalar(select(func.count()).select_from(NebiusApplication))
                + await session.scalar(select(func.count()).select_from(NebiusEnvironment))) == 1


async def test_name_conflict_rolls_back_operation_and_reservation(applications):
    from loom.db.nebius_application_operation_schema import NebiusApplicationOperation, NebiusApplicationReservation

    registry, factory, (alice, _), prepare, legacy, prepare_legacy = applications
    await legacy.create(principal=alice, idempotency_key="legacy-name", prepared=prepare_legacy())
    with pytest.raises(ManagementError, match="application_name_conflict"):
        await registry.create(principal=alice, idempotency_key="colliding-name", **prepare())
    async with factory() as session:
        for model in (NebiusApplicationOperation, NebiusApplicationReservation):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
