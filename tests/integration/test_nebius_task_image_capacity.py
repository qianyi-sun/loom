"""Native builds and execution Pods share one PostgreSQL capacity authority."""

from __future__ import annotations

import asyncio
import inspect
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ServiceExecutionLease,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
)
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    create_execution_capacity_observation,
)
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


@pytest.fixture
async def native_setup(postgres_url):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owned = []
    try:
        yield sessions, owned
    finally:
        async with sessions() as session, session.begin():
            await session.execute(delete(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.materialization_id.in_(owned),
            ))
            await session.execute(delete(TaskImageMaterialization).where(TaskImageMaterialization.id.in_(owned)))
        await engine.dispose()


async def _native(session, owned, pair, now, *, cpu=64000):
    trial = await session.get(Trial, pair[0])
    row = TaskImageMaterialization(
        id=uuid4(), materialization_key=uuid4().hex * 2, task_id=trial.task_id, task_checksum="2" * 64,
        cpu_arch="x86_64", task_config={}, state="claimed", claimed_by="native", lease_epoch=1,
        attempt_count=1, lease_expires_at=now + timedelta(minutes=5),
    )
    owned.append(row.id)
    session.add(row)
    await session.flush()
    attempt = TaskImageMaterializationAttempt(
        id=uuid4(), materialization_id=row.id, attempt_number=1, lease_epoch=1,
        builder_id="native", claimed_at=now,
        native_build={"target_id": pair[1].target_id, "namespace": "builds", "job_name": "test-build",
                      "state": "reserved", "reserved_at": now.isoformat(),
                      "resources": {"vcpu_millis": cpu, "memory_mib": 1024, "storage_mib": 2048}},
    )
    session.add(attempt)
    await session.flush()
    return attempt.id, row.id


@pytest.mark.parametrize("competitor", ["execution", "native"])
async def test_execution_and_native_builds_race_for_one_shared_node(native_setup, competitor):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        a, b = await _seed_ready_trial(session, now=now), await _seed_ready_trial(session, now=now)
        for pair in (a, b):
            await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement_fixture(
                target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=1, parent_id="shared-tenant",
            ))
        first, _ = await _native(session, owned, a, now)
        second, _ = await _native(session, owned, b, now)

    async def reserve(attempt_id=None):
        try:
            async with sessions() as session, session.begin():
                if attempt_id is not None:
                    return await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
                return await _reserve(session, trial_id=b[0], target=b[1], now=now + timedelta(seconds=2))
        except ExecutionProvisioningBlockedError as error:
            return error.reason

    results = await asyncio.gather(reserve(first), reserve(second if competitor == "native" else None))
    assert sum(isinstance(result, (dict, ServiceExecutionLease)) for result in results) == 1
    assert "execution_capacity_provider_quota_nodes_exceeded" in results
    async with sessions() as session:
        saved = (await session.scalars(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.id.in_([first, second]),
        ))).all()
        assert sum("capacity_reserved_at" in row.native_build for row in saved) <= 1


async def test_observed_native_pod_is_not_charged_twice(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        placement = placement_fixture(target_id=pair[1].target_id, nodes=1, used_nodes=1, quota_nodes=1)
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        attempt_id, materialization_id = await _native(session, owned, pair, now, cpu=63000)
        original = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
    async with sessions() as session, session.begin():
        placement["nodes"][0]["requested"] = {"cpu_millis": 63000, "memory_mib": 1024, "storage_mib": 2048}
        placement["nodes"][0]["used_pod_slots"] = 1
        placement["nodes"][0]["managed_pods"] = [{
            "uid": "native-pod", "lease_id": f"task-image:{materialization_id}", "generation": 1,
            "requests": placement["nodes"][0]["requested"],
        }]
        await _record(session, pair[1].target_id, now + timedelta(seconds=3), placement)
        assert await reserve_native_task_image_capacity(
            session, attempt_id=attempt_id, now=now + timedelta(seconds=4), revalidate_existing=True,
        ) == original
        lease = await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=4))
        assert isinstance(lease, ServiceExecutionLease)


async def test_revalidating_observed_pending_build_does_not_count_another_pending_job(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        policy = await session.get(ExecutionCapacityPolicy, pair[1].target_id)
        policy.max_pending_jobs = 1
        placement = placement_fixture(target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=1)
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        attempt_id, materialization_id = await _native(session, owned, pair, now)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
    async with sessions() as session, session.begin():
        placement["pending_pods"] = [{
            "uid": "pending-build-pod", "lease_id": f"task-image:{materialization_id}", "generation": 1,
            "requests": {"cpu_millis": 64000, "memory_mib": 1024, "storage_mib": 2048},
        }]
        observation, _ = await _record(session, pair[1].target_id, now + timedelta(seconds=3), placement)
        names = inspect.signature(create_execution_capacity_observation).parameters
        payload = {key: value for key, value in observation.observation_json.items() if key in names}
        payload.update(pending_jobs=1, observed_at=now + timedelta(seconds=4), source_version=str(uuid4()))
        await create_execution_capacity_observation(session, **payload)
        assert await reserve_native_task_image_capacity(
            session, attempt_id=attempt_id, now=now + timedelta(seconds=5), revalidate_existing=True,
        )


async def test_terminal_or_expired_native_attempt_holds_capacity_until_cleanup(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement_fixture(
            target_id=pair[1].target_id, nodes=0, used_nodes=0, quota_nodes=1,
        ))
        attempt_id, materialization_id = await _native(session, owned, pair, now)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=2))
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "state": "released"}
        materialization = await session.get(TaskImageMaterialization, materialization_id)
        materialization.state = "failed"
        materialization.lease_expires_at = now - timedelta(seconds=1)
    with pytest.raises(ExecutionProvisioningBlockedError, match="quota_nodes_exceeded"):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=3))
    async with sessions() as session, session.begin():
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "capacity_released_at": (now + timedelta(seconds=4)).isoformat()}
    async with sessions() as session, session.begin():
        assert isinstance(await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=5)), ServiceExecutionLease)


@pytest.mark.parametrize("limit,reason", [
    ("max_pending_jobs", "pending_limit"), ("max_create_per_minute", "create_rate"),
])
async def test_execution_limits_include_native_reservations(native_setup, limit, reason):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        attempt_id, _ = await _native(session, owned, pair, now, cpu=1000)
        policy = await session.get(ExecutionCapacityPolicy, pair[1].target_id)
        setattr(policy, limit, 1)
        await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now)
    with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=pair[0], target=pair[1], now=now + timedelta(seconds=1))


async def test_native_admission_accounts_execution_in_an_independent_cpu_but_shared_disk_domain(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        a, b = await _seed_ready_trial(session, now=now), await _seed_ready_trial(session, now=now)
        for index, pair in enumerate((a, b)):
            placement = placement_fixture(target_id=pair[1].target_id, nodes=0, used_nodes=0,
                                          quota_nodes=1, parent_id=f"cpu-{index}")
            placement["quota_resources"]["storage"]["parent_id"] = "shared-ssd"
            await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        attempt_id, _ = await _native(session, owned, b, now)
        await _reserve(session, trial_id=a[0], target=a[1], now=now + timedelta(seconds=2))
    with pytest.raises(ExecutionProvisioningBlockedError, match="quota_storage_exceeded"):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=3))


@pytest.mark.parametrize("invalid", ["oversize", "stale_observation", "stale_lease"])
async def test_native_admission_preserves_existing_resource_and_freshness_boundaries(native_setup, invalid):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        attempt_id, materialization_id = await _native(session, owned, pair, now, cpu=64001 if invalid == "oversize" else 1000)
        row = await session.get(TaskImageMaterialization, materialization_id)
        if invalid == "stale_lease":
            row.lease_expires_at = now - timedelta(seconds=1)
        elif invalid == "stale_observation":
            row.lease_expires_at = now + timedelta(days=2)
    reason = {"oversize": "node_shape", "stale_observation": "observation_stale", "stale_lease": "native_lease_stale"}[invalid]
    with pytest.raises(ExecutionProvisioningBlockedError, match=reason):
        async with sessions() as session, session.begin():
            await reserve_native_task_image_capacity(session, attempt_id=attempt_id,
                                                    now=now + timedelta(days=1) if invalid == "stale_observation" else now)


@pytest.mark.parametrize("uses_added_label", [False, True])
async def test_native_cold_admission_reuses_history_only_for_unused_added_labels(native_setup, uses_added_label):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        historical = placement_fixture(target_id=pair[1].target_id, nodes=1)
        historical["node_group"]["template"]["labels"] = {}
        historical["daemonsets"] = [{
            "uid": "resident", "generation": 1,
            "requests": {"cpu_millis": 100, "memory_mib": 100, "storage_mib": 0},
            "scheduling": {"node_selector": {
                "loom.nebius/node-os" if uses_added_label else "kubernetes.io/os": "linux",
            }},
        }]
        historical["template_samples"][0].update(
            daemonsets={"resident": 1}, daemonset_slots=1,
            daemonset_requests=historical["daemonsets"][0]["requests"],
        )
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), historical)
        current = deepcopy(historical)
        current["nodes"] = []
        current["template_samples"] = []
        current["node_group"]["node_count"] = 0
        current["node_group"]["template"]["labels"] = {
            "loom.nebius/node-os": "linux", "loom.nebius/node-arch": "amd64",
        }
        for quota in current["quota_resources"].values():
            quota["used"] = 0
        await _record(session, pair[1].target_id, now + timedelta(seconds=2), current)
        attempt_id, _ = await _native(session, owned, pair, now, cpu=1000)
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "resources": {
            "vcpu_millis": 1000, "memory_mib": 2048, "storage_mib": 16384,
        }}
    if uses_added_label:
        with pytest.raises(ExecutionProvisioningBlockedError, match="node_allocatable_unknown"):
            async with sessions() as session, session.begin():
                await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=3))
        async with sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            assert not attempt.native_build.get("capacity_reserved_at")
    else:
        async with sessions() as session, session.begin():
            decision = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, now=now + timedelta(seconds=3))
            assert decision["capacity_reserved_at"]
            assert decision["resources"] == {"vcpu_millis": 1000, "memory_mib": 2048, "storage_mib": 16384}
        async with sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            assert attempt.native_build["capacity_reserved_at"] == decision["capacity_reserved_at"]
