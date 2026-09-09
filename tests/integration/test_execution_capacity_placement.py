"""Real PostgreSQL quota admission, shared-resource races and recovery."""

import asyncio
import inspect
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ExecutionCapacityObservation, ServiceExecutionLease, Trial
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    create_execution_capacity_observation,
)
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- shared fixture
    _reserve,
    _seed_ready_trial,
)


async def _record(session, target_id, now, placement):
    old = (
        await session.execute(
            select(ExecutionCapacityObservation)
            .where(ExecutionCapacityObservation.target_id == target_id)
            .order_by(ExecutionCapacityObservation.observed_at.desc())
            .limit(1)
        )
    ).scalar_one()
    names = inspect.signature(create_execution_capacity_observation).parameters
    payload = {key: deepcopy(value) for key, value in old.observation_json.items() if key in names}
    payload.update(observed_at=now, source_version=str(uuid4()), placement=placement)
    for key, suffix in (
        ("nodes", "nodes"),
        ("vcpu", "vcpu_millis"),
        ("memory", "memory_mib"),
        ("storage", "storage_mib"),
    ):
        payload["provider_quota_" + suffix] = placement["quota_resources"][key]["limit"]
        payload["provider_used_" + suffix] = placement["quota_resources"][key]["used"]
    return await create_execution_capacity_observation(session, **payload)


@pytest.mark.parametrize("shared", [True, False])
async def test_two_targets_race_for_one_native_quota_and_quota_growth_recovers(
    postgres_url, shared
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            a = await _seed_ready_trial(session, now=now)
            b = await _seed_ready_trial(session, now=now)
            for index, (_, target) in enumerate((a, b)):
                placement = placement_fixture(
                    target_id=target.target_id,
                    nodes=0,
                    used_nodes=0,
                    quota_nodes=1,
                    parent_id="tenant" if shared else f"tenant-{index}",
                )
                await _record(session, target.target_id, now + timedelta(seconds=1), placement)

        async def claim(pair):
            try:
                async with sessions() as session, session.begin():
                    return await _reserve(
                        session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=2)
                    )
            except ExecutionProvisioningBlockedError as exc:
                return exc.reason

        results = await asyncio.gather(claim(a), claim(b))
        assert sum(isinstance(row, ServiceExecutionLease) for row in results) == (
            1 if shared else 2
        )
        if shared:
            assert "execution_capacity_provider_quota_nodes_exceeded" in results
            blocked = (a, b)[next(i for i, row in enumerate(results) if isinstance(row, str))]
            async with sessions() as session, session.begin():
                trial = await session.get(Trial, blocked[0])
                assert trial.state == "queued" and trial.attempt_count == 0
                for _, target in (a, b):
                    placement = placement_fixture(
                        target_id=target.target_id,
                        nodes=0,
                        used_nodes=0,
                        quota_nodes=2,
                        parent_id="tenant",
                    )
                    await _record(session, target.target_id, now + timedelta(seconds=3), placement)
            async with sessions() as session, session.begin():
                lease = await _reserve(
                    session, trial_id=blocked[0], target=blocked[1], now=now + timedelta(seconds=4)
                )
                assert lease.attempt == 1
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "case,reason",
    [
        ("zero", "execution_capacity_provider_quota_nodes_exceeded"),
        ("disk", "execution_capacity_provider_quota_storage_exceeded"),
        ("external", "execution_capacity_provider_quota_nodes_exceeded"),
        ("no_sample", "execution_capacity_node_allocatable_unknown"),
    ],
)
async def test_native_quota_and_cold_shape_boundaries_preserve_unclaimed_trial(
    postgres_url,
    case,
    reason,
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            placement = placement_fixture(
                target_id=target.target_id,
                nodes=0,
                used_nodes=1 if case == "external" else 0,
                quota_nodes=0 if case == "zero" else 1,
                node_storage=65_536,
                raw_storage=81_920,
            )
            if case == "disk":
                placement["quota_resources"]["storage"]["limit"] = 65_536
            if case == "no_sample":
                placement["template_samples"] = []
                placement["node_group"]["template"]["preset"] = "never-observed"
            await _record(session, target.target_id, now + timedelta(seconds=1), placement)
        with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
            async with sessions() as session, session.begin():
                await _reserve(
                    session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2)
                )
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.state == "queued" and trial.attempt_count == 0
            assert trial.claimed_at is None
    finally:
        await engine.dispose()


async def test_independent_cpu_quotas_still_share_the_same_ssd_allowance(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            first = await _seed_ready_trial(session, now=now)
            second = await _seed_ready_trial(session, now=now)
            for index, (_, target) in enumerate((first, second)):
                placement = placement_fixture(
                    target_id=target.target_id,
                    nodes=0,
                    used_nodes=0,
                    quota_nodes=1,
                    parent_id=f"cpu-domain-{index}",
                )
                placement["quota_resources"]["storage"]["parent_id"] = "shared-ssd-tenant"
                await _record(session, target.target_id, now + timedelta(seconds=1), placement)
        async with sessions() as session, session.begin():
            await _reserve(
                session, trial_id=first[0], target=first[1], now=now + timedelta(seconds=2)
            )
        with pytest.raises(ExecutionProvisioningBlockedError, match="quota_storage_exceeded"):
            async with sessions() as session, session.begin():
                await _reserve(
                    session, trial_id=second[0], target=second[1], now=now + timedelta(seconds=2)
                )
    finally:
        await engine.dispose()


async def test_native_usage_floor_sums_distinct_pools_when_account_usage_lags(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session, session.begin():
            first = await _seed_ready_trial(session, now=now)
            second = await _seed_ready_trial(session, now=now)
            for _, target in (first, second):
                # Each collector sees its native VM, while the account API
                # still reports only one VM for the two-pool account.
                placement = placement_fixture(
                    target_id=target.target_id,
                    nodes=1,
                    used_nodes=1,
                    quota_nodes=2,
                    parent_id="tenant",
                    requested_cpu=64_000,
                )
                await _record(session, target.target_id, now + timedelta(seconds=1), placement)
        with pytest.raises(ExecutionProvisioningBlockedError, match="quota_nodes_exceeded"):
            async with sessions() as session, session.begin():
                await _reserve(
                    session, trial_id=first[0], target=first[1], now=now + timedelta(seconds=2)
                )
    finally:
        await engine.dispose()
