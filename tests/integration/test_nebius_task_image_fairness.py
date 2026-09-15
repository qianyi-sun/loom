"""Waiting native builds keep their place across real capacity transactions."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    ServiceExecutionTarget,
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
from tests.integration.test_nebius_task_image_claims import _seed, claim_setup  # noqa: F401
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
async def native_build_setup(claim_setup):  # noqa: F811
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
    return controller, sessions, kube, image_id, builder_trial_id, trial_id, target, now


@pytest.fixture
async def waiting_build(native_build_setup):
    controller, sessions, kube, image_id, _, _, target, _ = native_build_setup
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert (row.state, row.lease_epoch, row.attempt_count) == ("queued", 0, 0)
    assert not attempts and kube.ensure_calls == 0
    async with sessions() as session:
        wait = await session.get(TaskImageCapacityWait, target.target_id)
        assert wait is not None and wait.materialization_id == image_id
        assert wait.expires_at - wait.renewed_at == timedelta(seconds=120)
    return native_build_setup


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


@pytest.mark.parametrize("invalidated", ["expired", "cancelled", "epoch", "shape", "pool"])
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
        elif invalidated == "pool":
            (await session.get(ServiceExecutionTarget, target.target_id)).logical_pool_id = "unrelated-pool"
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


@pytest.mark.parametrize("limit", ["max_pending_jobs", "max_create_per_minute"])
async def test_wait_protects_a_scarce_admission_slot_without_consuming_it(waiting_build, limit):
    controller, sessions, kube, _, _, trial_id, target, now = waiting_build
    await _free(sessions, target, now, quota_nodes=2)
    async with sessions() as session, session.begin():
        setattr(await session.get(ExecutionCapacityPolicy, target.target_id), limit, 1)
    with pytest.raises(ExecutionProvisioningBlockedError, match="pending_limit|create_rate"):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=trial_id, target=target, now=now + timedelta(seconds=2))
    await controller.run_once()
    assert kube.ensure_calls == 1


async def test_existing_trial_authority_precedes_a_later_wait(native_build_setup):
    controller, sessions, _, _, _, trial_id, target, now = native_build_setup
    await _free(sessions, target, now)
    async with sessions() as session, session.begin():
        lease = await _reserve(session, trial_id=trial_id, target=target,
                               now=now + timedelta(seconds=2))
    await controller.run_once()
    async with sessions() as session, session.begin():
        assert await session.get(TaskImageCapacityWait, target.target_id) is not None
        assert await reserve_execution_provisioning(
            session, lease_id=lease.id, now=now + timedelta(seconds=3), revalidate_existing=True,
        )


@pytest.mark.parametrize("admitted", [False, True])
async def test_failed_outer_commit_keeps_claim_and_fence_atomic(waiting_build, admitted):
    controller, sessions, kube, image_id, _, _, target, now = waiting_build
    if admitted:
        await _free(sessions, target, now)
    async with sessions() as session:
        before = await session.get(TaskImageCapacityWait, target.target_id)
        original = (before.first_waited_at, before.renewed_at, before.expires_at)

    def refuse_outer_commit(session):
        if not session.in_nested_transaction():
            raise RuntimeError("simulated outer commit failure")

    class RefuseCommitSession(AsyncSession):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            event.listen(self.sync_session, "before_commit", refuse_outer_commit)

    controller.sessions = async_sessionmaker(
        sessions.kw["bind"], class_=RefuseCommitSession, expire_on_commit=False,
    )
    with pytest.raises(RuntimeError, match="simulated outer commit failure"):
        await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "queued" and row.lease_epoch == row.attempt_count == 0
    assert not attempts and kube.ensure_calls == 0
    async with sessions() as session:
        after = await session.get(TaskImageCapacityWait, target.target_id)
        assert (after.first_waited_at, after.renewed_at, after.expires_at) == original


async def test_concurrent_controller_replicas_consume_one_wait_once(waiting_build):
    controller, sessions, kube, image_id, _, _, target, now = waiting_build
    await _free(sessions, target, now)
    await asyncio.gather(controller.run_once(), controller.run_once())
    row, attempts = await rows(sessions, image_id)
    assert row.attempt_count == len(attempts) == kube.ensure_calls == 1


@pytest.fixture
async def two_waiting_builds(waiting_build):
    first, sessions, first_kube, image_id, builder_trial_id, _, target, now = waiting_build
    async with sessions() as session, session.begin():
        _, second_target = await _seed_ready_trial(session, now=now)
        image = await session.get(TaskImageMaterialization, image_id)
        first_trial = await session.scalar(select(Trial).where(Trial.task_id == image.task_id))
        second_image, _ = await _seed(
            session, first_trial.team_id,
            snapshot_values={
                "task_config": image.task_config,
                "task_source": image.task_source,
            },
        )
        for current in (target, second_target):
            occupied = placement_fixture(
                target_id=current.target_id, nodes=0, used_nodes=1, quota_nodes=1,
                parent_id="ordered-waits",
            )
            await _record(session, current.target_id, now + timedelta(milliseconds=2), occupied)
    second_kube = FakeKube()
    second = NativeTaskImageController(
        sessions=sessions, kubernetes=second_kube,
        target=ExecutionTargetRuntime(target_id=second_target.target_id, namespace="executions"),
        settings=first.settings,
    )
    # Both targets serve the supported automatic pool. Model another claimant
    # holding the oldest row: SKIP LOCKED lets the second controller consider
    # the younger build without inventing an unsupported Dockerfile binding.
    async with sessions() as locked, locked.begin():
        await locked.get(TaskImageMaterialization, image_id, with_for_update=True)
        await second.run_once()
    async with sessions() as session, session.begin():
        assert await session.get(TaskImageCapacityWait, second_target.target_id) is not None
        for current in (target, second_target):
            free = placement_fixture(target_id=current.target_id, nodes=0, used_nodes=0,
                                     quota_nodes=1, parent_id="ordered-waits")
            await _record(session, current.target_id, now + timedelta(seconds=1), free)
    return waiting_build, second, second_kube, second_image, second_target


async def test_older_waiting_builder_wins_without_mutual_wait(two_waiting_builds):
    waiting_build, second, second_kube, second_image, _ = two_waiting_builds
    first, sessions, first_kube, image_id, builder_trial_id, _, _, now = waiting_build
    async with sessions() as locked, locked.begin():
        await locked.get(TaskImageMaterialization, image_id, with_for_update=True)
        await second.run_once()
    assert second_kube.ensure_calls == 0
    await first.run_once()
    assert first_kube.ensure_calls == 1
    assert (await rows(sessions, second_image))[0].attempt_count == 0
    # The younger builder progresses after actual UID-fenced cleanup releases
    # the older reservation, not merely after its materialization is cancelled.
    async with sessions() as session, session.begin():
        (await session.get(Trial, builder_trial_id)).cancellation_requested_at = now
    _, attempts = await rows(sessions, image_id)
    await first._reconcile(attempts[0].id)
    assert first_kube.delete_calls and not first_kube.jobs
    await second.run_once()
    assert second_kube.ensure_calls == 1
    assert (await rows(sessions, second_image))[0].attempt_count == 1


@pytest.mark.parametrize("changed", ["epoch", "resources"])
async def test_changed_claim_cannot_inherit_an_old_waiting_priority(two_waiting_builds, changed):
    waiting_build, second, second_kube, _, second_target = two_waiting_builds
    first, sessions, first_kube, image_id, _, _, target, _ = waiting_build
    if changed == "epoch":
        async with sessions() as session, session.begin():
            (await session.get(TaskImageMaterialization, image_id)).lease_epoch += 1
    else:
        first.settings = first.settings.model_copy(update={"cpu_millis": 63_000})
    await first.run_once()
    assert first_kube.ensure_calls == 0
    async with sessions() as session:
        renewed = await session.get(TaskImageCapacityWait, target.target_id)
        other = await session.get(TaskImageCapacityWait, second_target.target_id)
        assert renewed.first_waited_at > other.first_waited_at
    async with sessions() as locked, locked.begin():
        await locked.get(TaskImageMaterialization, image_id, with_for_update=True)
        await second.run_once()
    assert second_kube.ensure_calls == 1
