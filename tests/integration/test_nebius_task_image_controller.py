from __future__ import annotations

import asyncio
import copy
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from loom.db.schema import (
    ServiceExecutionTarget,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEvidence,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.service_execution import (
    persist_execution_catalog,
    set_execution_target_health,
)
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_controller import (
    NativeTaskImageController,
    NativeTaskImageSettings,
)
from tests.integration.test_nebius_task_image_claims import _seed, claim_setup  # noqa: F401
from tests.integration.test_service_execution_leases import NEBIUS_CPU_EXECUTION_CLASS_V1, _target
from tests.unit.test_nebius_task_image_controller import canonical_api_job
from tests.unit.test_service_execution_materialization import _task

MODULE = "loom_execution_actuator.task_image_controller"


class FakeKube:
    def __init__(self):
        self.jobs = {}
        self.ensure_calls = 0
        self.delete_calls = []
        self.allow_delete = True
        self.lose_create_response = False

    async def inventory(self, namespace):
        return [copy.deepcopy(job) for job in self.jobs.values() if not job.get("job_missing")]

    async def ensure(self, configmap, job):
        self.ensure_calls += 1
        name = job["metadata"]["name"]
        self.jobs.setdefault(name, {**copy.deepcopy(job), "metadata": {**job["metadata"], "uid": str(uuid4())}})
        if self.lose_create_response:
            self.lose_create_response = False
            raise TimeoutError("create response lost")
        return copy.deepcopy(self.jobs[name])

    async def observe(self, namespace, name):
        return copy.deepcopy(self.jobs.get(name))

    async def delete(self, namespace, name, uid, *, configmap):
        self.delete_calls.append((name, uid))
        if not self.allow_delete:
            return False
        self.jobs.pop(name, None)
        return True

    def finish(self, *, valid=True, failed_phase=None):
        job = next(iter(self.jobs.values()))
        labels = job["metadata"]["labels"]
        message = json.dumps({"materialization_id": labels["loom.materialization-id"],
                              "lease_epoch": int(labels["loom.lease-epoch"]),
                              "registry_images": {"task": "registry.example/tasks@sha256:" + "a" * 64}}) if valid else "invalid receipt password=should-not-persist"
        job["status"] = {"failed" if failed_phase else "succeeded": 1}
        job["pods"] = [{"metadata": {"uid": "pod-uid", "name": "pod", "ownerReferences": [{"kind": "Job", "uid": job["metadata"]["uid"]}]},
                        "status": {"containerStatuses": [{"name": "publish", "state": {"terminated": {"exitCode": 0, "message": message}}}],
                                   "initContainerStatuses": [{"name": failed_phase or "build", "state": {"terminated": {"exitCode": 1 if failed_phase else 0,
                                                                                                               "message": "secret=must-not-persist", "reason": "Error" if failed_phase else "Completed"}}}]}}]
        job["builder_log"] = "useful compilation error token=do-not-persist"


@pytest.fixture
async def controller_setup(claim_setup, monkeypatch):  # noqa: F811
    sessions, team_id = claim_setup
    target = _target(uuid4().hex[:12])
    async with sessions() as session, session.begin():
        await persist_execution_catalog(session, execution_class=NEBIUS_CPU_EXECUTION_CLASS_V1, targets=(target,))
        await set_execution_target_health(session, target_id=target.target_id, desired_state="active", observed_state="ready",
                                          health_status="healthy", observed_at=datetime.now(UTC))
    settings = NativeTaskImageSettings(namespace="test-builds", service_image="registry.example/service@sha256:" + "b" * 64,
                                       storage_endpoint="https://storage.example", storage_region="eu-north1", source_bucket="tasks",
                                       registry_repository="registry.example/tasks", registry_auth_kind="docker-config")
    kube = FakeKube()
    controller = NativeTaskImageController(sessions=sessions, kubernetes=kube,
                                          target=ExecutionTargetRuntime(target_id=target.target_id, namespace="executions"), settings=settings)

    async def reserve(session, *, attempt_id, revalidate_existing=False, **kwargs):
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        assert attempt is not None and attempt.native_build["resources"]["vcpu_millis"] == 1000
        row = await session.get(TaskImageMaterialization, attempt.materialization_id)
        assert row.state == "claimed" and row.claimed_by == controller.builder_id
        native = {**attempt.native_build}
        native.setdefault("capacity_reserved_at", datetime.now(UTC).isoformat())
        attempt.native_build = native
        return native

    monkeypatch.setattr(MODULE + ".reserve_native_task_image_capacity", reserve)
    try:
        yield controller, sessions, team_id, kube
    finally:
        async with sessions() as session, session.begin():
            image_ids = select(TaskImageMaterialization.id).where(TaskImageMaterialization.task_id.startswith(f"nebius-image-claims/{team_id}/"))
            await session.execute(delete(TaskImagePublicationEvidence).where(TaskImagePublicationEvidence.materialization_id.in_(image_ids)))
            await session.execute(delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target.target_id))


async def seed_image(sessions, team_id, *, unsupported=False):
    config = {} if unsupported else _task(environment={"os": "linux", "dockerfile": "Dockerfile", "docker_build_context": "."}).model_dump(mode="json")
    async with sessions() as session, session.begin():
        image_id, trial_id = await _seed(session, team_id, snapshot_values={"task_config": config, "task_source": "s3://tasks/example"})
    return image_id, trial_id


async def rows(sessions, image_id):
    async with sessions() as session:
        row = await session.get(TaskImageMaterialization, image_id)
        attempts = (await session.scalars(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.materialization_id == image_id,
        ).order_by(TaskImageMaterializationAttempt.lease_epoch))).all()
        return row, attempts


async def test_claim_reserve_transaction_rolls_back_without_job_or_attempt(controller_setup, monkeypatch):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)

    async def blocked(*args, **kwargs):
        raise ExecutionProvisioningBlockedError("execution_capacity_provider_quota_nodes_exceeded")

    monkeypatch.setattr(MODULE + ".reserve_native_task_image_capacity", blocked)
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert (row.state, row.lease_epoch, row.attempt_count) == ("queued", 0, 0)
    assert not attempts and not kube.ensure_calls


@pytest.mark.parametrize("normalized_api", [False, True])
async def test_restart_recovers_ambiguous_create_once_and_persists_success_before_release(controller_setup, normalized_api):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    kube.lose_create_response = True
    with pytest.raises(TimeoutError):
        await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "claimed" and attempts[0].native_build["job_uid"] is None
    if normalized_api:
        kube.jobs = {name: canonical_api_job(job) for name, job in kube.jobs.items()}
    expected_uid = next(iter(kube.jobs.values()))["metadata"]["uid"]
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "running" and attempts[0].native_build["job_uid"] == expected_uid
    assert kube.ensure_calls == 1
    kube.finish()
    kube.allow_delete = False
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "ready" and "task" in row.registry_images
    assert not attempts[0].native_build.get("capacity_released_at")
    encoded = json.dumps(attempts[0].native_build)
    assert "useful compilation error" in encoded and "do-not-persist" not in encoded and "must-not-persist" not in encoded
    await controller.run_once()
    assert not (await rows(sessions, image_id))[1][0].native_build.get("capacity_released_at")
    kube.allow_delete = True
    await controller.run_once()
    assert (await rows(sessions, image_id))[1][0].native_build["capacity_released_at"]
    assert kube.delete_calls[-1][1] == expected_uid


async def test_cancel_recovers_normalized_unacknowledged_job_uid_and_releases_only_after_delete(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, trial_id = await seed_image(sessions, team_id)
    kube.lose_create_response = True
    with pytest.raises(TimeoutError):
        await controller.run_once()
    kube.jobs = {name: canonical_api_job(job) for name, job in kube.jobs.items()}
    expected_uid = next(iter(kube.jobs.values()))["metadata"]["uid"]
    assert (await rows(sessions, image_id))[1][0].native_build["job_uid"] is None
    async with sessions() as session, session.begin():
        (await session.get(Trial, trial_id)).state = "cancelled"
    kube.allow_delete = False
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.failure_reason == "build_cancelled"
    assert attempts[0].native_build["job_uid"] == expected_uid
    assert not attempts[0].native_build.get("capacity_released_at")
    assert kube.delete_calls[-1][1] == expected_uid
    kube.allow_delete = True
    await controller.run_once()
    assert (await rows(sessions, image_id))[1][0].native_build["capacity_released_at"]
    assert not kube.jobs and kube.ensure_calls == 1


async def test_db_scan_recovers_missing_acknowledged_job_without_recreating_same_epoch(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    kube.jobs.clear()
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "queued" and row.failure_reason == "build_job_missing"
    assert attempts[0].native_build["capacity_released_at"]
    assert kube.ensure_calls == 1


@pytest.mark.parametrize("invalid_success", [True, False])
async def test_bad_receipt_and_deterministic_builder_failure_reach_terminal_state(controller_setup, invalid_success):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    kube.finish(valid=not invalid_success, failed_phase=None if invalid_success else "build")
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "failed"
    if not invalid_success:
        assert "useful compilation error" in row.failure_message and "do-not-persist" not in row.failure_message
    assert row.failure_reason == ("build_publication_receipt_invalid" if invalid_success else "build_build_failed")
    assert "should-not-persist" not in json.dumps(attempts[0].native_build)
    await controller.run_once()
    assert (await rows(sessions, image_id))[1][0].native_build["capacity_released_at"]


async def test_unsupported_oldest_input_fails_once_then_next_work_can_claim(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    bad, _ = await seed_image(sessions, team_id, unsupported=True)
    good, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    assert (await rows(sessions, bad))[0].failure_reason == "build_input_unsupported"
    await controller.run_once()
    assert (await rows(sessions, good))[0].state == "running" and kube.ensure_calls == 1


async def test_cancel_only_last_consumer_and_hold_capacity_until_cleanup(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, first_id = await seed_image(sessions, team_id)
    async with sessions() as session, session.begin():
        first = await session.get(Trial, first_id)
        second = Trial(id=uuid4(), team_id=team_id, task_id=first.task_id, batch_id=first.batch_id,
                       config={}, requires_caps={"worker_pool": "nebius-cpu"}, state="queued")
        session.add(second)
        await session.flush()
        session.add(TrialTaskImageMaterialization(trial_id=second.id, materialization_id=image_id))
        second_id = second.id
    await controller.run_once()
    async with sessions() as session, session.begin():
        (await session.get(Trial, first_id)).state = "cancelled"
    await controller.run_once()
    assert (await rows(sessions, image_id))[0].state == "running" and not kube.delete_calls
    async with sessions() as session, session.begin():
        (await session.get(Trial, second_id)).state = "cancelled"
    kube.allow_delete = False
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.failure_reason == "build_cancelled" and not attempts[0].native_build.get("capacity_released_at")
    kube.allow_delete = True
    await controller.run_once()
    assert (await rows(sessions, image_id))[1][0].native_build["capacity_released_at"]


async def test_stale_epoch_cleanup_does_not_mutate_successor(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    async with sessions() as session, session.begin():
        row = await session.get(TaskImageMaterialization, image_id)
        row.lease_epoch = 2
        row.attempt_count = 2
        row.claimed_by = "another-builder"
        row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
    kube.allow_delete = False
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.lease_epoch == 2 and row.claimed_by == "another-builder"
    assert not attempts[0].native_build.get("capacity_released_at")
    kube.allow_delete = True
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.claimed_by == "another-builder" and attempts[0].native_build["capacity_released_at"]


async def test_two_replicas_do_not_start_two_jobs(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await asyncio.gather(controller.run_once(), controller.run_once())
    row, attempts = await rows(sessions, image_id)
    assert row.state == "running" and len(attempts) == 1 and kube.ensure_calls == 1


async def test_unstarted_expired_attempt_is_cleaned_by_db_scan(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    attempt_id = await controller._claim()
    async with sessions() as session, session.begin():
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
        attempt.native_build = {**attempt.native_build, "deadline_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()}
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.failure_reason == "build_deadline_exceeded"
    assert attempts[0].native_build["capacity_released_at"] and not kube.ensure_calls
    assert kube.delete_calls[0][1] is None


async def test_unexpected_kube_error_is_visible_and_does_not_release_capacity(controller_setup, monkeypatch):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()

    async def broken(*args):
        raise PermissionError("sensitive provider detail")

    monkeypatch.setattr(kube, "observe", broken)
    with pytest.raises(RuntimeError, match="reconciliation failed") as failure:
        await controller.run_once()
    assert "sensitive" not in str(failure.value)
    row, attempts = await rows(sessions, image_id)
    assert row.state == "running" and not attempts[0].native_build.get("capacity_released_at")


async def test_multiple_owned_pods_fail_instead_of_renewing_forever(controller_setup):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    kube.finish()
    job = next(iter(kube.jobs.values()))
    second = copy.deepcopy(job["pods"][0])
    second["metadata"]["uid"] = "unexpected-second-pod"
    job["pods"].append(second)
    await controller.run_once()
    assert (await rows(sessions, image_id))[0].failure_reason == "build_pod_identity_changed"
    await controller.run_once()
    assert (await rows(sessions, image_id))[1][0].native_build["capacity_released_at"]


async def test_expired_lease_is_not_renewed_and_cleanup_retains_capacity_until_gone(controller_setup, monkeypatch):
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with sessions() as session, session.begin():
        (await session.get(TaskImageMaterialization, image_id)).lease_expires_at = expired

    async def no_claim():
        return None

    monkeypatch.setattr(controller, "_claim", no_claim)
    kube.allow_delete = False
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.lease_expires_at == expired and not attempts[0].native_build.get("capacity_released_at")
    kube.allow_delete = True
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.lease_expires_at == expired and attempts[0].native_build["capacity_released_at"]
