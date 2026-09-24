"""Restart diagnostics survive the existing bounded native-failure cleanup."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ServiceExecutionEvent, ServiceExecutionLease, Trial
from loom_execution_actuator.contracts import (
    ContainerDiagnostic,
    ContainerTerminationDiagnostic,
    NormalizedJobState,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _FakeKubernetesJobApi,
    _reserve,
    _seed_ready_trial,
)


@pytest.mark.parametrize("initial_oom", [True, False])
async def test_sandbox_restart_diagnostics_persist_before_bounded_cleanup(
    postgres_url, initial_oom
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    kubernetes = _FakeKubernetesJobApi()
    try:
        async with sessions() as session, session.begin():
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
        actuator = ExecutionActuator(
            sessions=sessions,
            kubernetes=kubernetes,
            target=ExecutionTargetRuntime(
                target_id=target.target_id, namespace=target.namespace_name
            ),
            controller_id="sandbox-loss-test",
            command_lease_seconds=5,
        )
        assert await actuator.run_commands_once(now=now) == 1
        diagnostic = ContainerDiagnostic(
            name="task-sandbox",
            restart_count=1,
            previous_termination=ContainerTerminationDiagnostic(
                reason="OOMKilled" if initial_oom else "Error",
                exit_code=137,
                signal=9,
                finished_at=now,
            ),
        )
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.OOM_KILLED
                if initial_oom
                else NormalizedJobState.FAILED,
                "reason": "SandboxRestarted",
                "message": "task-sandbox lost its attempt process state",
                "resource_version": "same-job-version",
                "pod_uid": "sandbox-loss-pod",
                "pod_resource_version": "pod-1",
                "container_diagnostics": (diagnostic,),
            }
        )
        await actuator.reconcile_full_once(now=now)
        # A second Pod status is distinct even though the Job has not changed.
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "pod_resource_version": "pod-2",
                "normalized_state": NormalizedJobState.FAILED,
                "message": "later restart observed",
                "container_diagnostics": (
                    diagnostic.model_copy(
                        update={
                            "restart_count": 2,
                            "previous_termination": ContainerTerminationDiagnostic(
                                reason="Error", exit_code=1
                            ),
                        }
                    ),
                ),
            }
        )
        await actuator.reconcile_full_once(now=now + timedelta(seconds=1))
        await actuator.reconcile_full_once(now=now + timedelta(seconds=2))
        async with sessions() as session:
            events = list(
                (
                    await session.scalars(
                        select(ServiceExecutionEvent)
                        .where(
                            ServiceExecutionEvent.lease_id == lease.id,
                            ServiceExecutionEvent.event_kind == "kubernetes_observed",
                        )
                        .order_by(ServiceExecutionEvent.ordinal)
                    )
                ).all()
            )
            losses = [
                event for event in events if event.payload_json.get("reason") == "SandboxRestarted"
            ]
            assert len(losses) == 2
            assert [
                event.payload_json["container_diagnostics"][0]["restart_count"] for event in losses
            ] == [1, 2]
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current.observed_state == "failed"
            assert current.error_code == ("oom_killed" if initial_oom else "failed")
            if initial_oom:
                assert "OOMKilled" in current.error_message
            else:
                assert current.error_message == "task-sandbox lost its attempt process state"
            assert current.revoked_at is None
            assert current.output_commit_state == "not_started"
            assert kubernetes.delete_count == 0
        # A later normal teardown observation cannot erase an already pinned
        # real loss of the same Pod incarnation.
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.SUCCEEDED,
                "reason": None,
                "message": None,
                "pod_resource_version": "pod-completed",
            }
        )
        await actuator.reconcile_full_once(now=now + timedelta(seconds=3))
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current.error_code == ("oom_killed" if initial_oom else "failed")
            assert current.observed_state == "failed"
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.TERMINATING,
                "pod_resource_version": "pod-terminating",
            }
        )
        await actuator.reconcile_full_once(now=now + timedelta(seconds=3))
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "normalized_state": NormalizedJobState.FAILED,
                "reason": "BackoffLimitExceeded",
                "message": "generic job failure",
                "pod_uid": None,
                "pod_resource_version": None,
                "container_diagnostics": (),
            }
        )
        await actuator.reconcile_full_once(now=now + timedelta(minutes=5))
        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            assert trial.state == "failed"
            assert ("OOMKilled" if initial_oom else "SandboxRestarted") in trial.failure_message
            assert trial.failure_reason == ("oom_killed" if initial_oom else "native_execution_failed")
        # A final status update can first arrive on the deletion path.
        kubernetes.jobs[lease.job_name] = kubernetes.jobs[lease.job_name].model_copy(
            update={
                "pod_resource_version": "pod-3",
                "pod_uid": "sandbox-loss-pod",
                "container_diagnostics": (diagnostic.model_copy(update={"restart_count": 3}),),
            }
        )
        await actuator.run_commands_once(now=now + timedelta(minutes=5, seconds=1))
        assert kubernetes.delete_count == 1
        async with sessions() as session:
            events = list(
                (
                    await session.scalars(
                        select(ServiceExecutionEvent).where(
                            ServiceExecutionEvent.lease_id == lease.id,
                            ServiceExecutionEvent.event_kind == "kubernetes_observed",
                        )
                    )
                ).all()
            )
            if initial_oom:
                from loom.execution_diagnosis_store import read_execution_failure
                current = await session.get(ServiceExecutionLease, lease.id)
                retained = await read_execution_failure(session, current)
                assert retained["reason"] == "oom_killed"
                assert retained["container_incarnation"] == 0
                assert retained["supporting_events"]
            assert any(
                event.payload_json["container_diagnostics"][0]["previous_termination"]["reason"]
                == ("OOMKilled" if initial_oom else "Error")
                and event.payload_json["container_diagnostics"][0]["restart_count"] == 3
                for event in events
                if event.payload_json.get("container_diagnostics")
            )
    finally:
        await engine.dispose()
