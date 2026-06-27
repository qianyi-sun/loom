from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, Team, Trial, Worker, WorkerPoolAutoscalerPolicy
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
        assert pool["autoscaler_enabled"] is True
        assert pool["autoscaler_idle_since_at"] is not None
        assert pool["autoscaler_idle_seconds"] >= 120
        assert pool["last_autoscaler_decision"] == "request_drain"
        assert summary["aggregate"]["draining_slots"] == 6
    finally:
        await engine.dispose()
