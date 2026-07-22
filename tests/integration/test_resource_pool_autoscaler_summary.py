from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Task,
    Team,
    TeamQuota,
    Trial,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom.resource_pools import get_resource_pool_summary


@pytest.fixture(autouse=True)
async def _cleanup_db(postgres_url: str) -> Iterator[None]:
    yield
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Trial))
        await s.execute(delete(WorkerPoolAutoscalerPolicy))
        await s.execute(delete(Worker))
        await s.execute(delete(Task))
        await s.execute(delete(TeamQuota))
        await s.execute(delete(Team))
        await s.commit()
    await engine.dispose()


async def test_resource_summary_exposes_policy_and_draining_capacity(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    active_worker_id = uuid4()
    draining_worker_id = uuid4()
    trial_id = uuid4()
    now = datetime.now(UTC)
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-a"))
            await s.execute(insert(Task).values(id="task-a", checksum="0" * 64, config={}))
            await s.execute(insert(Worker).values(
                id=active_worker_id,
                hostname="oldlab-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=6,
                pool_name="oldlab",
                drain_state="active",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(Worker).values(
                id=draining_worker_id,
                hostname="oldlab-2",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=6,
                pool_name="oldlab",
                drain_state="draining",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="task-a",
                config={},
                requires_caps={"cpu_arch": "x86_64", "backend": "docker"},
                state="running",
                worker_id=draining_worker_id,
                idempotency_key="draining-running",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=6,
                max_slots=30,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={"backend": "docker", "cpu_arch": "x86_64"},
                idle_since_at=now - timedelta(seconds=123),
                last_decision="request_drain",
                last_decision_reason="idle_excess_capacity",
                last_desired_slots=6,
                last_pending_slots=0,
                last_draining_slots=6,
            ))
            await s.commit()

        async with session_factory() as s:
            summary = await get_resource_pool_summary(s, freshness_sec=120)

        pool = summary["pools"][0]
        assert pool["active_workers"] == 1
        assert pool["total_slots"] == 6
        assert pool["draining_workers"] == 1
        assert pool["draining_slots"] == 6
        assert pool["occupied_slots"] == 1
        assert pool["free_slots"] == 6
        assert pool["desired_slots"] == 6
        assert pool["pending_slots"] == 0
        assert pool["current_active_slots"] == 6
        assert pool["max_slots"] == 30
        assert pool["ceiling_slots"] == 30
        assert pool["autoscaler_enabled"] is True
        assert pool["autoscaler_idle_since_at"] is not None
        assert pool["autoscaler_idle_seconds"] >= 120
        assert pool["last_autoscaler_decision"] == "request_drain"
        assert pool["last_autoscaler_reason"] == "idle_excess_capacity"
        assert pool["decision_reason"] == "idle_excess_capacity"
        assert pool["blocked_reason"] is None
        assert summary["aggregate"]["draining_slots"] == 6
        assert summary["aggregate"]["current_active_slots"] == 6
        assert summary["aggregate"]["max_slots"] == 30
        assert summary["aggregate"]["ceiling_slots"] == 30
    finally:
        await engine.dispose()


async def test_resource_summary_excludes_released_drained_workers(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    worker_id = uuid4()
    now = datetime.now(UTC)
    try:
        async with session_factory() as s:
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="oldlab-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["none"],
                }],
                max_concurrent=6,
                pool_name="oldlab",
                drain_state="drained",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(WorkerPoolAutoscalerPolicy).values(
                environment="production",
                pool_name="oldlab",
                actuator="slurm",
                enabled=True,
                min_slots=0,
                max_slots=6,
                scale_up_threshold_slots=1,
                scale_down_idle_seconds=600,
                scale_up_cooldown_seconds=60,
                scale_down_cooldown_seconds=300,
                drain_timeout_seconds=600,
                actuator_config={"backend": "docker", "cpu_arch": "x86_64"},
                last_decision="release_drained",
                last_decision_reason="drain_complete",
                last_desired_slots=0,
                last_pending_slots=0,
                last_draining_slots=0,
            ))
            await s.commit()

        async with session_factory() as s:
            summary = await get_resource_pool_summary(s, freshness_sec=120)

        pool = summary["pools"][0]
        assert pool["active_workers"] == 0
        assert pool["total_slots"] == 0
        assert pool["draining_workers"] == 0
        assert pool["draining_slots"] == 0
        assert pool["current_active_slots"] == 0
        assert pool["max_slots"] == 6
        assert pool["ceiling_slots"] == 6
        assert summary["aggregate"]["draining_slots"] == 0
        assert summary["aggregate"]["current_active_slots"] == 0
        assert summary["aggregate"]["max_slots"] == 6
        assert summary["aggregate"]["ceiling_slots"] == 6
    finally:
        await engine.dispose()


async def test_resource_summary_exposes_pre_start_queue_diagnostics(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    now = datetime.now(UTC)
    try:
        async with session_factory() as s:
            await s.execute(insert(Team).values(id=team_id, name="team-prestart"))
            await s.execute(insert(Task).values(id="task-prestart", checksum="0" * 64, config={}))
            await s.execute(insert(Worker).values(
                id=worker_id,
                hostname="trt-gb10-1",
                version="test",
                capabilities=[{
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "arm64",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                }],
                max_concurrent=10,
                pool_name="gb10",
                drain_state="active",
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            await s.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="task-prestart",
                config={},
                requires_caps={"cpu_arch": "arm64", "backend": "docker"},
                state="claimed",
                worker_id=worker_id,
                claimed_at=now - timedelta(seconds=1800),
                started_at=None,
                pre_start_heartbeat_at=now - timedelta(seconds=30),
            ))
            await s.commit()

        async with session_factory() as s:
            summary = await get_resource_pool_summary(s, freshness_sec=120)

        pool = summary["pools"][0]
        assert pool["starting_tasks"] == 1
        assert pool["pre_start_heartbeat_fresh_tasks"] == 1
        assert pool["oldest_starting_task_age_sec"] >= 1700
        assert summary["aggregate"]["pre_start_heartbeat_fresh_tasks"] == 1
        assert summary["aggregate"]["oldest_starting_task_age_sec"] >= 1700
    finally:
        await engine.dispose()
