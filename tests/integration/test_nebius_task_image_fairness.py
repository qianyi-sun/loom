"""Waiting native builds keep their place across real capacity transactions."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    Trial,
)
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    reserve_execution_provisioning,
)
from loom_control_plane.service_execution import set_execution_target_health
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_controller import (
    NativeTaskImageController,
    NativeTaskImageSettings,
)
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_nebius_task_image_claims import claim_setup  # noqa: F401
from tests.integration.test_nebius_task_image_controller import (
    FakeKube,
    rows,
    seed_image,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


@pytest.fixture
async def waiting_build(claim_setup):  # noqa: F811
    sessions, team_id = claim_setup
    kube = FakeKube()
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        trial_id, target = await _seed_ready_trial(session, now=now)
        occupied = placement_fixture(
            target_id=target.target_id, nodes=1, used_nodes=1,
            quota_nodes=1, requested_cpu=64_000,
        )
        await _record(session, target.target_id, now + timedelta(milliseconds=1), occupied)
    controller = NativeTaskImageController(
        sessions=sessions, kubernetes=kube,
        target=ExecutionTargetRuntime(target_id=target.target_id, namespace="executions"),
        settings=NativeTaskImageSettings(
            namespace="test-builds", service_image="registry.example/service@sha256:" + "b" * 64,
            storage_endpoint="https://storage.example", storage_region="eu-north1", source_bucket="tasks",
            registry_repository="registry.example/tasks", registry_auth_kind="docker-config",
            cpu_millis=64_000,
        ),
    )
    image_id, builder_trial_id = await seed_image(sessions, team_id)

    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert (row.state, row.lease_epoch, row.attempt_count) == ("queued", 0, 0)
    assert not attempts and kube.ensure_calls == 0
    async with sessions() as session:
        wait = await session.get(TaskImageCapacityWait, target.target_id)
        assert wait is not None and wait.materialization_id == image_id
        assert wait.expires_at - wait.renewed_at == timedelta(seconds=120)
    return controller, sessions, kube, image_id, builder_trial_id, trial_id, target, now


async def _free(sessions, target, now, *, quota_nodes=1):
    async with sessions() as session, session.begin():
        free = placement_fixture(
            target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=quota_nodes,
        )
        await _record(session, target.target_id, now + timedelta(seconds=1), free)


async def test_new_trial_cannot_take_last_capacity_a_waiting_builder_needs(waiting_build):
    controller, sessions, kube, image_id, _, trial_id, target, now = waiting_build
    # The occupied allocation drains. A trial arrives before the next builder
    # reconcile: serialization alone lets it jump ahead on every iteration.
    await _free(sessions, target, now)
    for index in range(3):
        with pytest.raises(ExecutionProvisioningBlockedError):
            async with sessions() as session, session.begin():
                await _reserve(session, trial_id=trial_id, target=target,
                               now=now + timedelta(seconds=2 + index))

    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state in {"claimed", "running"}
    assert row.attempt_count == len(attempts) == kube.ensure_calls == 1
    assert attempts[0].native_build["capacity_reserved_at"]
    async with sessions() as session:
        assert await session.get(TaskImageCapacityWait, target.target_id) is None


@pytest.mark.parametrize("invalidated", ["expired", "cancelled", "epoch", "shape"])
async def test_abandoned_or_invalid_wait_cannot_hold_trial_capacity(waiting_build, invalidated):
    _, sessions, _, image_id, builder_trial_id, trial_id, target, now = waiting_build
    await _free(sessions, target, now)
    async with sessions() as session, session.begin():
        if invalidated == "cancelled":
            (await session.get(Trial, builder_trial_id)).cancellation_requested_at = now
        elif invalidated == "epoch":
            (await session.get(TaskImageMaterialization, image_id)).lease_epoch += 1
        elif invalidated == "shape":
            (await session.get(ExecutionCapacityPolicy, target.target_id)).node_cpu_millis = 63_000
    claim_time = now + timedelta(seconds=130 if invalidated == "expired" else 2)
    async with sessions() as session, session.begin():
        if invalidated == "expired":
            await set_execution_target_health(
                session, target_id=target.target_id, desired_state="active", observed_state="ready",
                health_status="healthy", observed_at=claim_time,
            )
        assert await _reserve(session, trial_id=trial_id, target=target, now=claim_time)


async def test_wait_renewal_preserves_order_without_attempts_or_creates(waiting_build):
    controller, sessions, kube, image_id, _, _, target, _ = waiting_build
    async with sessions() as session:
        original = await session.get(TaskImageCapacityWait, target.target_id)
        first, expiry = original.first_waited_at, original.expires_at
    await controller.run_once()
    async with sessions() as session:
        renewed = await session.get(TaskImageCapacityWait, target.target_id)
        assert renewed.first_waited_at == first and renewed.expires_at >= expiry
        assert not (await session.scalars(select(ExecutionProvisioningAuthorization)
                    .where(ExecutionProvisioningAuthorization.target_id == target.target_id))).all()
    row, attempts = await rows(sessions, image_id)
    assert row.attempt_count == row.lease_epoch == kube.ensure_calls == 0 and not attempts


async def test_real_spare_headroom_can_admit_trial_without_displacing_wait(waiting_build):
    controller, sessions, _, _, _, trial_id, target, now = waiting_build
    await _free(sessions, target, now, quota_nodes=2)
    async with sessions() as session, session.begin():
        lease = await _reserve(session, trial_id=trial_id, target=target,
                               now=now + timedelta(seconds=2))
        authorization = await session.scalar(select(ExecutionProvisioningAuthorization)
                                             .where(ExecutionProvisioningAuthorization.lease_id == lease.id))
        assert authorization.incremental_nodes == 0
        assert authorization.decision_reason == "existing_allocatable"
    # An already reserved operation can still start if a later fence exists.
    # Revalidation must not treat waiting state as prior acquired authority.
    async with sessions() as session, session.begin():
        assert await reserve_execution_provisioning(
            session, lease_id=lease.id, now=now + timedelta(seconds=3), revalidate_existing=True,
        )
    await controller.run_once()


async def test_wait_reuses_compatible_native_allocatable_history(waiting_build):
    _, sessions, _, _, _, trial_id, target, now = waiting_build
    async with sessions() as session, session.begin():
        current = placement_fixture(
            target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1,
        )
        current["template_samples"] = []
        await _record(session, target.target_id, now + timedelta(seconds=1), current)
    with pytest.raises(ExecutionProvisioningBlockedError):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=trial_id, target=target,
                           now=now + timedelta(seconds=2))


@pytest.mark.parametrize("domain", ["shared", "independent", "shared-disk", "impossible-policy"])
async def test_wait_protects_shared_native_quota_but_not_an_independent_domain(waiting_build, domain):
    _, sessions, _, _, _, _, first, now = waiting_build
    async with sessions() as session, session.begin():
        trial_id, second = await _seed_ready_trial(session, now=now)
        for target in (first, second):
            placement = placement_fixture(
                target_id=target.target_id, nodes=0, used_nodes=0, quota_nodes=1,
                parent_id="wait-shared" if domain in {"shared", "impossible-policy"} else target.target_id,
            )
            if domain == "shared-disk":
                placement["quota_resources"]["storage"]["parent_id"] = "wait-shared-ssd"
            if domain == "impossible-policy" and target == first:
                placement["node_group"]["raw_node"]["cpu_millis"] = 128_000
            await _record(session, target.target_id, now + timedelta(seconds=1), placement)
        if domain == "impossible-policy":
            (await session.get(ExecutionCapacityPolicy, first.target_id)).max_vcpu_millis = 64_000
    if domain in {"shared", "shared-disk"}:
        with pytest.raises(ExecutionProvisioningBlockedError, match="provider_quota"):
            async with sessions() as session, session.begin():
                await _reserve(session, trial_id=trial_id, target=second,
                               now=now + timedelta(seconds=2))
    else:
        async with sessions() as session, session.begin():
            assert await _reserve(session, trial_id=trial_id, target=second,
                                  now=now + timedelta(seconds=2))
