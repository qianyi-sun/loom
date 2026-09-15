"""Waiting native builds keep their place across real capacity transactions."""

from datetime import UTC, datetime, timedelta

import pytest

from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
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


async def test_new_trial_cannot_take_last_capacity_a_waiting_builder_needs(
    claim_setup,  # noqa: F811
):
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
    image_id, _ = await seed_image(sessions, team_id)

    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert (row.state, row.lease_epoch, row.attempt_count) == ("queued", 0, 0)
    assert not attempts and kube.ensure_calls == 0

    # The occupied allocation drains. A trial arrives before the next builder
    # reconcile: serialization alone lets it jump ahead on every iteration.
    async with sessions() as session, session.begin():
        free = placement_fixture(
            target_id=target.target_id, nodes=1, used_nodes=1, quota_nodes=1,
        )
        await _record(session, target.target_id, now + timedelta(seconds=1), free)
    with pytest.raises(ExecutionProvisioningBlockedError):
        async with sessions() as session, session.begin():
            await _reserve(session, trial_id=trial_id, target=target,
                           now=now + timedelta(seconds=2))

    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state in {"claimed", "running"}
    assert row.attempt_count == len(attempts) == kube.ensure_calls == 1
    assert attempts[0].native_build["capacity_reserved_at"]
