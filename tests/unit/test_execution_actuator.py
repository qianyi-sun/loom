from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom.db.schema import ServiceExecutionLease
from loom_execution_actuator.contracts import ActuatorContractError
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job


def _lease() -> ServiceExecutionLease:
    now = datetime.now(UTC)
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="nebius-eu-north1-staging",
        workload_requirements_json={
            "image_ref": "registry.example/task@sha256:" + "a" * 64,
            "cpu_millis": 1500,
            "memory_mib": 2048,
            "ephemeral_storage_mib": 4096,
        },
        workload_requirements_sha256="sha256:" + "b" * 64,
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name="loom-nebius-staging",
        job_name="loom-123456789abc-a1-g1",
        execution_unit_key=uuid4(),
        deadline_at=now + timedelta(minutes=10),
    )


def test_job_renderer_is_deterministic_restricted_and_lease_scoped() -> None:
    lease = _lease()
    now = datetime.now(UTC)
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
        runtime_class_name="loom-sandbox",
    )

    first = render_execution_job(lease, target=target, now=now)
    second = render_execution_job(lease, target=target, now=now)

    assert first == second
    assert first["metadata"]["name"] == lease.job_name
    assert first["spec"]["backoffLimit"] == 0
    pod = first["spec"]["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    assert pod["automountServiceAccountToken"] is False
    assert pod["runtimeClassName"] == "loom-sandbox"
    assert pod["serviceAccountName"] == "loom-execution-attempt"
    assert "serviceAccountToken" not in str(pod)
    container = pod["containers"][0]
    assert container["image"].endswith("@sha256:" + "a" * 64)
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    assert container["resources"]["limits"] == {
        "cpu": "1500m",
        "memory": "2048Mi",
        "ephemeral-storage": "4096Mi",
    }
    assert not ({"privileged", "hostNetwork", "hostPID", "hostIPC"} & pod.keys())
    assert "hostPath" not in str(pod)


def test_job_renderer_rejects_mutable_image_missing_limits_and_wrong_scope() -> None:
    lease = _lease()
    target = ExecutionTargetRuntime(
        target_id="nebius-eu-north1-staging",
        namespace="loom-nebius-staging",
        runtime_class_name="loom-sandbox",
    )
    lease.workload_requirements_json = {
        **lease.workload_requirements_json,
        "image_ref": "registry.example/task:latest",
    }
    with pytest.raises(ActuatorContractError, match="immutable"):
        render_execution_job(lease, target=target)

    lease.workload_requirements_json = {
        "image_ref": "registry.example/task@sha256:" + "a" * 64,
        "cpu_millis": 1000,
        "memory_mib": None,
        "ephemeral_storage_mib": 1000,
    }
    with pytest.raises(ActuatorContractError, match="memory_mib"):
        render_execution_job(lease, target=target)

    with pytest.raises(ActuatorContractError, match="namespace"):
        render_execution_job(
            lease,
            target=ExecutionTargetRuntime(
                target_id="nebius-eu-north1-staging",
                namespace="foreign",
                runtime_class_name="loom-sandbox",
            ),
        )
