"""DRF fairness across teams — strict alternation."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, TeamQuota, Trial, Worker
from loom_control_plane.scheduler.claim import claim_one


async def _setup_two_teams_two_trials(session_factory) -> tuple[UUID, UUID, UUID]:  # type: ignore[no-untyped-def]
    async with session_factory() as s:
        team_a = uuid4()
        team_b = uuid4()
        await s.execute(insert(Team).values(id=team_a, name=f"a-{team_a}"))
        await s.execute(insert(Team).values(id=team_b, name=f"b-{team_b}"))
        await s.execute(insert(TeamQuota).values(team_id=team_a, fair_share_weight=1.0))
        await s.execute(insert(TeamQuota).values(team_id=team_b, fair_share_weight=1.0))
        await s.execute(insert(Task).values(
            id="t", checksum="0" * 64, config={"schema_version": "1"},
        ))
        for team_id in (team_a, team_b):
            for _ in range(2):
                await s.execute(insert(Trial).values(
                    id=uuid4(), team_id=team_id, task_id="t",
                    config={},
                    requires_caps={
                        "os": "linux", "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                ))
        worker_id = uuid4()
        await s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v",
            capabilities=[{
                "os": "linux", "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": True, "mounted_fs": True,
                "resource_modes": ["auto"],
            }],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        await s.commit()
        return team_a, team_b, worker_id


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str):  # type: ignore[no-untyped-def]
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Trial))
        await s.execute(delete(Worker))
        await s.execute(delete(TeamQuota))
        await s.execute(delete(Team))
        await s.execute(delete(Task))
        await s.commit()
    await engine.dispose()


async def test_drf_alternates_between_teams(postgres_url: str):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_a, team_b, worker_id = await _setup_two_teams_two_trials(session_factory)

    claimed_team_order: list[UUID] = []
    for _ in range(4):
        async with session_factory() as session:
            row = await claim_one(
                session,
                worker_id=worker_id,
                worker_os=["linux"],
                worker_cpu_arches=["x86_64"],
                worker_gpu_vendors=["none"],
                worker_network_policies=["public"],
            )
            await session.commit()
            if row is None:
                break
            claimed_team_order.append(row["team_id"])

    assert len(claimed_team_order) == 4
    # DRF strict alternation: first two claims must come from different teams
    # (after the first claim, the chosen team's in_flight_count climbs to 1
    # which makes the other team's share/weight strictly lower → it wins next).
    assert claimed_team_order[0] != claimed_team_order[1]
    assert {claimed_team_order[0], claimed_team_order[1]} == {team_a, team_b}

    await engine.dispose()


async def test_claim_returns_none_when_no_eligible_trials(postgres_url: str):
    """Empty queue → claim_one returns None (Response 204 in the endpoint)."""
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        row = await claim_one(
            session, worker_id=uuid4(),
            worker_os=["linux"], worker_gpu_vendors=["none"],
            worker_network_policies=["public"], worker_cpu_arches=["x86_64"],
        )
        assert row is None
    await engine.dispose()


async def test_claim_filters_by_cpu_architecture(postgres_url: str):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as s:
        team_id = uuid4()
        await s.execute(insert(Team).values(id=team_id, name=f"arch-{team_id}"))
        await s.execute(insert(TeamQuota).values(team_id=team_id, fair_share_weight=1.0))
        await s.execute(insert(Task).values(
            id="arch-task", checksum="0" * 64, config={"schema_version": "1"},
        ))
        legacy_x86_trial = uuid4()
        explicit_x86_trial = uuid4()
        arm64_trial = uuid4()
        any_trial = uuid4()
        arm64_worker = uuid4()
        await s.execute(insert(Worker).values(
            id=arm64_worker,
            hostname="gb10",
            version="v",
            capabilities=[{
                "os": "linux",
                "cpu_arch": "arm64",
                "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": True,
                "mounted_fs": True,
                "resource_modes": ["auto"],
            }],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            status="active",
        ))
        for trial_id, requires_caps in (
            (
                legacy_x86_trial,
                {
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                },
            ),
            (
                explicit_x86_trial,
                {
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "cpu_arch": "x86_64",
                },
            ),
            (
                arm64_trial,
                {
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "cpu_arch": "arm64",
                },
            ),
            (
                any_trial,
                {
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "cpu_arch": "any",
                },
            ),
        ):
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="arch-task",
                config={},
                requires_caps=requires_caps,
                state="queued",
            ))
        await s.commit()

    claimed: list[UUID] = []
    for _ in range(2):
        async with session_factory() as session:
            row = await claim_one(
                session,
                worker_id=arm64_worker,
                worker_os=["linux"],
                worker_gpu_vendors=["none"],
                worker_network_policies=["public"],
                worker_cpu_arches=["arm64"],
            )
            await session.commit()
            assert row is not None
            claimed.append(row["id"])

    assert set(claimed) == {arm64_trial, any_trial}

    async with session_factory() as session:
        row = await claim_one(
            session,
            worker_id=arm64_worker,
            worker_os=["linux"],
            worker_gpu_vendors=["none"],
            worker_network_policies=["public"],
            worker_cpu_arches=["arm64"],
        )
        assert row is None

    await engine.dispose()


async def test_claim_filters_by_required_worker_pool(postgres_url: str):
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as s:
        team_id = uuid4()
        await s.execute(insert(Team).values(id=team_id, name=f"pool-{team_id}"))
        await s.execute(insert(TeamQuota).values(team_id=team_id, fair_share_weight=1.0))
        await s.execute(insert(Task).values(
            id="pool-task", checksum="0" * 64, config={"schema_version": "1"},
        ))
        gb10_trial = uuid4()
        oldlab_trial = uuid4()
        oldlab_worker = uuid4()
        await s.execute(insert(Worker).values(
            id=oldlab_worker,
            hostname="oldlab-1",
            version="v",
            pool_name="oldlab",
            capabilities=[{
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": True,
                "mounted_fs": True,
                "resource_modes": ["auto"],
            }],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            status="active",
        ))
        for trial_id, required_pool in (
            (gb10_trial, "gb10"),
            (oldlab_trial, "oldlab"),
        ):
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="pool-task",
                config={},
                requires_caps={
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "cpu_arch": "any",
                    "worker_pool": required_pool,
                },
                state="queued",
            ))
        await s.commit()

    async with session_factory() as session:
        row = await claim_one(
            session,
            worker_id=oldlab_worker,
            worker_os=["linux"],
            worker_gpu_vendors=["none"],
            worker_network_policies=["public"],
            worker_cpu_arches=["x86_64"],
        )
        await session.commit()

    assert row is not None
    assert row["id"] == oldlab_trial

    async with session_factory() as session:
        row = await claim_one(
            session,
            worker_id=oldlab_worker,
            worker_os=["linux"],
            worker_gpu_vendors=["none"],
            worker_network_policies=["public"],
            worker_cpu_arches=["x86_64"],
        )
        assert row is None

    await engine.dispose()
