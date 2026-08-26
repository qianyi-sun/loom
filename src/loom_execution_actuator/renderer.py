from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from loom.db.schema import ServiceExecutionLease
from loom.execution_contract import WorkloadRequirementsV1
from loom.execution_image_admission import (
    ImageAdmissionError,
    validate_execution_image_admission_bundle,
)
from loom.execution_runtime_contract import (
    ExecutionRuntimePlanV1,
    ProbeV1,
    RuntimeComposition,
    SidecarContainerV1,
    validate_runtime_plan_requirements,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_execution_actuator.contracts import ActuatorContractError

_DIGEST_IMAGE = re.compile(r"^.+@sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class ExecutionTargetRuntime:
    target_id: str
    namespace: str
    runtime_class_name: str
    service_account_name: str = "loom-execution-attempt"
    credential_broker_url: str = (
        "http://loom-llm-gateway.loom.svc.cluster.local:9100/internal/service-execution"
    )

    def __post_init__(self) -> None:
        if not self.target_id or not self.namespace or not self.runtime_class_name:
            raise ValueError("target, namespace, and sandbox runtime class are required")
        if not self.credential_broker_url.startswith(("http://", "https://")):
            raise ValueError("credential broker URL must be HTTP(S)")


def _required_positive(requirements: dict[str, Any], key: str) -> int:
    value = requirements.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ActuatorContractError(f"execution requirement {key} must be positive")
    return value


def _resources(*, cpu_millis: int, memory_mib: int, storage_mib: int) -> dict[str, Any]:
    values = {
        "cpu": f"{cpu_millis}m",
        "memory": f"{memory_mib}Mi",
        "ephemeral-storage": f"{storage_mib}Mi",
    }
    return {"requests": values.copy(), "limits": values.copy()}


def _security_context(*, read_only_root: bool = True) -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": read_only_root,
        "runAsNonRoot": True,
    }


def _probe(value: ProbeV1) -> dict[str, Any]:
    result: dict[str, Any] = {
        "timeoutSeconds": value.timeout_seconds,
        "periodSeconds": value.period_seconds,
        "failureThreshold": value.failure_threshold,
    }
    if value.kind == "http":
        result["httpGet"] = {"port": value.port, "path": value.path}
    elif value.kind == "tcp":
        result["tcpSocket"] = {"port": value.port}
    else:
        result["exec"] = {"command": list(value.argv)}
    return result


def _sidecar(value: SidecarContainerV1) -> dict[str, Any]:
    return {
        "name": value.role_name,
        "image": value.image_ref,
        "imagePullPolicy": "IfNotPresent",
        "command": list(value.argv),
        "env": [{"name": name, "value": item} for name, item in sorted(value.environment.items())],
        "resources": _resources(
            cpu_millis=value.resources.cpu_millis,
            memory_mib=value.resources.memory_mib,
            storage_mib=value.resources.ephemeral_storage_mib,
        ),
        "restartPolicy": "Always",
        "startupProbe": _probe(value.startup_probe),
        "readinessProbe": _probe(value.readiness_probe),
        "securityContext": _security_context(),
        "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}],
    }


def _runtime_plan(lease: ServiceExecutionLease) -> ExecutionRuntimePlanV1:
    if lease.runtime_contract_json is None or lease.runtime_contract_sha256 is None:
        raise ActuatorContractError("execution lease has no immutable runtime contract")
    if canonical_digest(lease.runtime_contract_json) != lease.runtime_contract_sha256:
        raise ActuatorContractError("execution runtime contract digest does not match lease")
    try:
        plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        requirements = WorkloadRequirementsV1.model_validate(lease.workload_requirements_json)
        validate_runtime_plan_requirements(plan, requirements)
    except (ValidationError, ValueError) as exc:
        raise ActuatorContractError(f"invalid execution runtime contract: {exc}") from exc
    if plan.execution_class_id != lease.execution_class_id:
        raise ActuatorContractError("runtime contract execution class does not match lease")
    if plan.execution_role != lease.execution_role:
        raise ActuatorContractError("runtime contract execution role does not match lease")
    if plan.composition != RuntimeComposition.INIT_PAYLOAD:
        raise ActuatorContractError("runtime composition is not supported by this actuator")
    return plan


def render_execution_job(
    lease: ServiceExecutionLease,
    *,
    target: ExecutionTargetRuntime,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Render one immutable Pod-native execution unit from its durable lease."""

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
    plan = _runtime_plan(lease)
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        validate_execution_image_admission_bundle(
            plan.image_admission,
            required_image_refs=(
                plan.task_image_ref,
                plan.runtime_image_ref,
                *(sidecar.image_ref for sidecar in plan.sidecars),
            ),
            now=current_time,
        )
    except ImageAdmissionError as exc:
        raise ActuatorContractError(str(exc)) from exc
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
        "loom.openai.com/runtime-contract-sha256": lease.runtime_contract_sha256,
        "loom.openai.com/candidate-sha": plan.candidate_sha,
        "loom.openai.com/task-revision-sha256": plan.task_revision_sha256,
        "loom.openai.com/command-identity-sha256": plan.command_identity_sha256,
        "loom.openai.com/execution-role": plan.execution_role,
        "loom.openai.com/image-admission-sha256": canonical_digest(
            plan.image_admission.model_dump(mode="json")
        ),
    }
    if lease.parent_lease_id is not None:
        annotations["loom.openai.com/parent-lease-id"] = str(lease.parent_lease_id)
    annotations["loom.openai.com/container-roles"] = ",".join(
        [
            "execution",
            plan.main.role,
            *[sidecar.role_name for sidecar in plan.sidecars],
            *(["verifier"] if plan.verifier is not None else []),
        ]
    )
    resources = _resources(
        cpu_millis=cpu_millis,
        memory_mib=memory_mib,
        storage_mib=storage_mib,
    )
    plan_payload = canonical_document(plan.canonical_payload())
    encoded_plan = base64.urlsafe_b64encode(plan_payload).rstrip(b"=").decode("ascii")
    runtime_mount = {"name": "runtime", "mountPath": "/loom/runtime"}
    execution_runtime_mount = {**runtime_mount, "readOnly": True}
    workspace_mount = {"name": "workspace", "mountPath": "/workspace"}
    output_mount = {"name": "output", "mountPath": "/loom/output"}
    init_containers = [
        {
            "name": "runtime-materializer",
            "image": plan.runtime_image_ref,
            "imagePullPolicy": "IfNotPresent",
            "command": [
                "/loom-execution-runtime",
                "materialize",
                "--encoded-plan",
                encoded_plan,
            ],
            "resources": _resources(cpu_millis=50, memory_mib=64, storage_mib=32),
            "securityContext": _security_context(),
            "volumeMounts": [runtime_mount],
        },
        *[_sidecar(sidecar) for sidecar in plan.sidecars],
    ]
    output_mib = max(
        1,
        (plan.max_artifact_bytes + 2 * plan.max_log_bytes_per_stream + 1_048_575) // 1_048_576,
    )
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
                        "runAsUser": plan.run_as_user,
                        "runAsGroup": plan.run_as_group,
                        "fsGroup": plan.fs_group,
                        "fsGroupChangePolicy": "OnRootMismatch",
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "terminationGracePeriodSeconds": plan.termination_grace_seconds,
                    "volumes": [
                        {
                            "name": "runtime",
                            "emptyDir": {"sizeLimit": f"{plan.runtime_volume_mib}Mi"},
                        },
                        {
                            "name": "workspace",
                            "emptyDir": {"sizeLimit": f"{plan.workspace_mib}Mi"},
                        },
                        {"name": "output", "emptyDir": {"sizeLimit": f"{output_mib}Mi"}},
                    ],
                    "initContainers": init_containers,
                    "containers": [
                        {
                            "name": "execution",
                            "image": plan.task_image_ref,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/loom/runtime/loom-execution-runtime"],
                            "args": [
                                "--plan",
                                "/loom/runtime/execution-plan.json",
                                "--workspace",
                                "/workspace",
                                "--output-root",
                                "/loom/output",
                            ],
                            "resources": resources,
                            "securityContext": _security_context(read_only_root=False),
                            "volumeMounts": [
                                execution_runtime_mount,
                                workspace_mount,
                                output_mount,
                            ],
                            "terminationMessagePath": "/loom/output/termination-message",
                            "terminationMessagePolicy": "File",
                            "env": [
                                {"name": "LOOM_EXECUTION_LEASE_ID", "value": str(lease.id)},
                                {
                                    "name": "LOOM_EXECUTION_GENERATION",
                                    "value": str(lease.generation),
                                },
                                {
                                    "name": "LOOM_EXECUTION_ROLE",
                                    "value": lease.execution_role,
                                },
                                {
                                    "name": "LOOM_EXECUTION_BROKER_URL",
                                    "value": target.credential_broker_url,
                                },
                            ],
                        }
                    ],
                },
            },
        },
    }


__all__ = ["ExecutionTargetRuntime", "render_execution_job"]
