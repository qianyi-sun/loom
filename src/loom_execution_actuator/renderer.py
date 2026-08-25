from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from loom.db.schema import ServiceExecutionLease
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.contracts import ActuatorContractError

_DIGEST_IMAGE = re.compile(r"^.+@sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class ExecutionTargetRuntime:
    target_id: str
    namespace: str
    runtime_class_name: str
    service_account_name: str = "loom-execution-attempt"

    def __post_init__(self) -> None:
        if not self.target_id or not self.namespace or not self.runtime_class_name:
            raise ValueError("target, namespace, and sandbox runtime class are required")


def _required_positive(requirements: dict[str, Any], key: str) -> int:
    value = requirements.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ActuatorContractError(f"execution requirement {key} must be positive")
    return value


def render_execution_job(
    lease: ServiceExecutionLease,
    *,
    target: ExecutionTargetRuntime,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Render the exact v1 Job envelope; task process mapping remains #1550."""

    if lease.target_id != target.target_id:
        raise ActuatorContractError("lease target does not match actuator target")
    if lease.namespace_name != target.namespace:
        raise ActuatorContractError("lease namespace does not match actuator namespace")
    requirements = lease.workload_requirements_json
    image_ref = requirements.get("image_ref")
    if not isinstance(image_ref, str) or not _DIGEST_IMAGE.fullmatch(image_ref):
        raise ActuatorContractError("execution image must be immutable by sha256 digest")
    cpu_millis = _required_positive(requirements, "cpu_millis")
    memory_mib = _required_positive(requirements, "memory_mib")
    storage_mib = _required_positive(requirements, "ephemeral_storage_mib")
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    active_deadline = int((lease.deadline_at - current_time).total_seconds())
    if active_deadline <= 0:
        raise ActuatorContractError("execution deadline has expired")
    target_label = canonical_digest(
        {"schema_version": "loom.target-label.v1", "target_id": target.target_id},
        persisted=False,
    ).removeprefix("sha256:")[:32]
    labels = {
        "app.kubernetes.io/managed-by": "loom-execution-actuator",
        "app.kubernetes.io/component": "execution-unit",
        "loom.openai.com/lease-id": str(lease.id),
        "loom.openai.com/generation": str(lease.resource_generation),
        "loom.openai.com/target": target_label,
    }
    annotations = {
        "loom.openai.com/schema-version": "loom.execution-job.v1",
        "loom.openai.com/target-id": target.target_id,
        "loom.openai.com/execution-unit-key": str(lease.execution_unit_key),
        "loom.openai.com/workload-requirements-sha256": lease.workload_requirements_sha256,
    }
    resources = {
        "requests": {
            "cpu": f"{cpu_millis}m",
            "memory": f"{memory_mib}Mi",
            "ephemeral-storage": f"{storage_mib}Mi",
        },
        "limits": {
            "cpu": f"{cpu_millis}m",
            "memory": f"{memory_mib}Mi",
            "ephemeral-storage": f"{storage_mib}Mi",
        },
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": lease.job_name,
            "namespace": target.namespace,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "activeDeadlineSeconds": active_deadline,
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "serviceAccountName": target.service_account_name,
                    "runtimeClassName": target.runtime_class_name,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "execution",
                            "image": image_ref,
                            "imagePullPolicy": "IfNotPresent",
                            "resources": resources,
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "env": [
                                {"name": "LOOM_EXECUTION_LEASE_ID", "value": str(lease.id)},
                                {
                                    "name": "LOOM_EXECUTION_GENERATION",
                                    "value": str(lease.generation),
                                },
                            ],
                        }
                    ],
                },
            },
        },
    }


__all__ = ["ExecutionTargetRuntime", "render_execution_job"]
