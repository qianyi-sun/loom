from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Task,
    Team,
    TeamQuota,
    Trial,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom.execution_contract import ExecutionRoutingDecisionV1
from loom_control_plane.autoscaler_demand_router import assign_neutral_queued_trials
from loom_control_plane.scheduler.claim import claim_one


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str) -> Iterator[None]:
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        await session.execute(delete(Trial))
        await session.execute(delete(WorkerPoolAutoscalerPolicy))
        await session.execute(delete(Worker))
        await session.execute(delete(Task))
        await session.execute(delete(TeamQuota))
        await session.execute(delete(Team))
        await session.commit()
    await engine.dispose()


async def test_router_assigns_each_neutral_trial_to_one_enabled_pool(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with factory() as session:
            await session.execute(insert(Team).values(id=team_id, name="routing-team"))
            await session.execute(
                insert(Task).values(id="routing-task", checksum="0" * 64, config={})
            )
            for pool_name, max_slots in (("gb10", 150), ("oldlab", 5)):
                await session.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment="staging",
                        pool_name=pool_name,
                        actuator="slurm",
                        enabled=True,
                        min_slots=0,
                        max_slots=max_slots,
                        scale_up_threshold_slots=1,
                        scale_down_idle_seconds=60,
                        scale_up_cooldown_seconds=0,
                        scale_down_cooldown_seconds=0,
                        drain_timeout_seconds=60,
                        actuator_config={
                            "backend": "docker",
                            "cpu_arch": "arm64" if pool_name == "gb10" else "x86_64",
                            "external_runner": True,
                        },
                    ),
                )
            for index, (key, requires_caps) in enumerate(
                (
                    ("neutral-1", {"backend": "docker", "cpu_arch": "any"}),
                    ("neutral-2", {"backend": "docker", "cpu_arch": "any"}),
                    (
                        "explicit-oldlab",
                        {
                            "backend": "docker",
                            "cpu_arch": "any",
                            "worker_pool": "oldlab",
                        },
                    ),
                    ("concrete-arm", {"backend": "docker", "cpu_arch": "arm64"}),
                )
            ):
                await session.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="routing-task",
                        config={},
                        requires_caps=requires_caps,
                        state="queued",
                        idempotency_key=key,
                        submitted_at=now + timedelta(seconds=index),
                    ),
                )
            await session.commit()

        async with factory() as session:
            summary = await assign_neutral_queued_trials(
                session,
                environment="staging",
                now=now,
            )
            await session.commit()

        async with factory() as session:
            rows = (
                await session.execute(
                    select(
                        Trial.idempotency_key,
                        Trial.autoscaler_pool_name,
                        Trial.autoscaler_pool_assigned_at,
                        Trial.execution_route_generation,
                        Trial.execution_route_json,
                        Trial.execution_route_sha256,
                    ).order_by(Trial.idempotency_key),
                )
            ).all()

        assert summary.assigned_count == 2
        assert summary.unroutable_count == 0
        assert [(row[0], row[1], row[2]) for row in rows] == [
            ("concrete-arm", None, None),
            ("explicit-oldlab", None, None),
            ("neutral-1", "gb10", now),
            ("neutral-2", "oldlab", now),
        ]
        for row in rows[2:]:
            assert row.execution_route_generation == 1
            assert row.execution_route_sha256.startswith("sha256:")
            decision = ExecutionRoutingDecisionV1.model_validate(row.execution_route_json)
            assert decision.selected_pool_id == row.autoscaler_pool_name
            assert decision.reason == "configured_scale_headroom"
            assert {item.logical_pool_id for item in decision.candidates} == {
                "gb10",
                "oldlab",
            }
    finally:
        await engine.dispose()


async def test_pool_scoped_router_preserves_global_balancing_for_neutral_batch(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 21, 18, 0, tzinfo=UTC)
    team_id = uuid4()
    try:
        async with factory() as session:
            await session.execute(insert(Team).values(id=team_id, name="scoped-routing-team"))
            await session.execute(
                insert(Task).values(id="scoped-routing-task", checksum="0" * 64, config={})
            )
            for pool_name, cpu_arch in (("gb10", "arm64"), ("oldlab", "x86_64")):
                await session.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment="staging",
                        pool_name=pool_name,
                        actuator="slurm",
                        enabled=True,
                        min_slots=0,
                        max_slots=10,
                        scale_up_threshold_slots=1,
                        scale_down_idle_seconds=60,
                        scale_up_cooldown_seconds=0,
                        scale_down_cooldown_seconds=0,
                        drain_timeout_seconds=60,
                        actuator_config={
                            "backend": "docker",
                            "cpu_arch": cpu_arch,
                            "external_runner": True,
                        },
                    ),
                )
            for index in range(4):
                await session.execute(
                    insert(Trial).values(
                        id=uuid4(),
                        team_id=team_id,
                        task_id="scoped-routing-task",
                        config={},
                        requires_caps={"backend": "docker", "cpu_arch": "any"},
                        state="queued",
                        idempotency_key=f"neutral-{index + 1}",
                        submitted_at=now + timedelta(seconds=index),
                    ),
                )
            await session.commit()

        async with factory() as session:
            summary = await assign_neutral_queued_trials(
                session,
                environment="staging",
                now=now,
                assignment_pool_names=frozenset({"gb10"}),
            )
            await session.commit()

        async with factory() as session:
            rows = (
                await session.execute(
                    select(Trial.idempotency_key, Trial.autoscaler_pool_name).order_by(
                        Trial.submitted_at,
                    )
                )
            ).all()

        assert summary.assigned_count == 2
        assert rows == [
            ("neutral-1", "gb10"),
            ("neutral-2", None),
            ("neutral-3", "gb10"),
            ("neutral-4", None),
        ]
    finally:
        await engine.dispose()


async def test_route_reassignment_increments_generation_and_records_fresh_capacity(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 26, 20, 0, tzinfo=UTC)
    team_id = uuid4()
    trial_id = uuid4()
    try:
        async with factory() as session:
            await session.execute(insert(Team).values(id=team_id, name="route-generation-team"))
            await session.execute(
                insert(Task).values(id="route-generation-task", checksum="0" * 64, config={})
            )
            for pool_name, actual_slots in (("gb10", 10), ("oldlab", 2)):
                await session.execute(
                    insert(WorkerPoolAutoscalerPolicy).values(
                        environment="staging",
                        pool_name=pool_name,
                        actuator="slurm",
                        enabled=True,
                        min_slots=0,
                        max_slots=20,
                        scale_up_threshold_slots=1,
                        scale_down_idle_seconds=60,
                        scale_up_cooldown_seconds=0,
                        scale_down_cooldown_seconds=0,
                        drain_timeout_seconds=60,
                        actuator_config={"backend": "docker", "cpu_arch": "any"},
                        last_actual_slots=actual_slots,
                        last_occupied_slots=0,
                        last_pending_slots=0,
                        last_decision_at=now,
                    )
                )
            await session.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id="route-generation-task",
                    config={},
                    requires_caps={"backend": "docker", "cpu_arch": "any"},
                    state="queued",
                    idempotency_key="route-generation-trial",
                    submitted_at=now,
                )
            )
            await session.commit()

        async with factory() as session:
            await assign_neutral_queued_trials(session, environment="staging", now=now)
            await session.commit()
        async with factory() as session:
            first = await session.get(Trial, trial_id)
            assert first is not None
            assert first.execution_route_pool_name == "gb10"
            assert first.execution_route_generation == 1
            assert first.execution_route_json["reason"] == "fresh_executable_capacity"
            await session.execute(
                update(WorkerPoolAutoscalerPolicy)
                .where(WorkerPoolAutoscalerPolicy.pool_name == "gb10")
                .values(last_blocked_reason="maintenance")
            )
            await session.commit()

        async with factory() as session:
            await assign_neutral_queued_trials(
                session,
                environment="staging",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
        async with factory() as session:
            second = await session.get(Trial, trial_id)
            assert second is not None
            assert second.execution_route_pool_name == "oldlab"
            assert second.execution_route_generation == 2
            assert second.execution_route_json["reason"] == "fresh_executable_capacity"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("_iteration", range(5))
async def test_router_lock_does_not_hide_concrete_trial_from_claim(
    postgres_url: str,
    _iteration: int,
) -> None:
    """A held routing transaction must not cause a false claim miss.

    Keeping the router transaction open makes the historical race
    deterministic: before the query was scoped, it locked both rows and
    ``claim_one`` returned ``None`` through its ``SKIP LOCKED`` query.
    """
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    worker_id = uuid4()
    concrete_trial_id = uuid4()
    try:
        async with factory() as session:
            await session.execute(insert(Team).values(id=team_id, name="claim-race-team"))
            await session.execute(insert(TeamQuota).values(team_id=team_id))
            await session.execute(
                insert(Task).values(id="claim-race-task", checksum="0" * 64, config={})
            )
            await session.execute(
                insert(Trial).values(
                    id=concrete_trial_id,
                    team_id=team_id,
                    task_id="claim-race-task",
                    config={},
                    requires_caps={
                        "os": "linux",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                    submitted_at=now,
                ),
            )
            await session.execute(
                insert(Trial).values(
                    id=uuid4(),
                    team_id=team_id,
                    task_id="claim-race-task",
                    config={},
                    requires_caps={
                        "os": "linux",
                        "cpu_arch": "any",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                    },
                    state="queued",
                    submitted_at=now + timedelta(seconds=1),
                ),
            )
            await session.execute(
                insert(Worker).values(
                    id=worker_id,
                    hostname="claim-race-worker",
                    version="v",
                    pool_name="default",
                    capabilities=[
                        {
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                        },
                    ],
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                ),
            )
            await session.commit()

        async with factory() as router_session:
            summary = await assign_neutral_queued_trials(
                router_session,
                environment="development",
                now=now,
            )
            assert summary.unroutable_count == 1

            async with factory() as claim_session:
                claimed = await claim_one(
                    claim_session,
                    worker_id=worker_id,
                    worker_os=["linux"],
                    worker_cpu_arches=["x86_64"],
                    worker_gpu_vendors=["none"],
                    worker_network_policies=["public"],
                )
                await claim_session.commit()

            assert claimed is not None
            assert claimed["id"] == concrete_trial_id
            await router_session.rollback()
    finally:
        await engine.dispose()
