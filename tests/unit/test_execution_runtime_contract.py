from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    ProcessPhaseV1,
    SidecarContainerV1,
    VerifierExecution,
)
from tests.support.execution_image_admission import signed_image_admission_bundle

_IMAGE = "registry.example/runtime@sha256:" + "a" * 64
_RUNTIME_IMAGE = "registry.example/loom-runtime@sha256:" + "b" * 64
_RESOURCES = ContainerResourcesV1(
    cpu_millis=1000,
    memory_mib=1024,
    ephemeral_storage_mib=2048,
)


def _phase(role: str) -> ProcessPhaseV1:
    return ProcessPhaseV1(
        role=role,
        argv=("/bin/loom-task", role),
        working_directory="/workspace",
        timeout_seconds=60,
        environment={"LOOM_PHASE": role},
    )


def _plan(**updates: object) -> ExecutionRuntimePlanV1:
    values: dict[str, object] = {
        "candidate_sha": "1" * 40,
        "task_revision_sha256": "sha256:" + "2" * 64,
        "command_identity_sha256": "sha256:" + "3" * 64,
        "execution_role": "attempt",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "composition": "init_payload",
        "task_image_ref": _IMAGE,
        "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "c" * 64,
        "image_admission": signed_image_admission_bundle((_IMAGE, _RUNTIME_IMAGE)),
        "task_resources": _RESOURCES,
        "workspace_mib": 4096,
        "runtime_volume_mib": 128,
        "setup": (_phase("setup"),),
        "main": _phase("agent"),
        "verifier_execution": "in_attempt",
        "verifier": _phase("verifier"),
    }
    values.update(updates)
    return ExecutionRuntimePlanV1.model_validate(values)


def test_runtime_plan_is_immutable_bounded_and_digest_pinned() -> None:
    plan = _plan()
    assert plan.schema_version == "loom.execution-runtime-plan.v1"
    assert plan.canonical_payload()["task_image_ref"] == _IMAGE
    with pytest.raises(ValidationError, match="digest-pinned"):
        _plan(task_image_ref="registry.example/task:latest")
    with pytest.raises(ValidationError, match="forbidden name"):
        ProcessPhaseV1.model_validate(
            {
                **_phase("agent").model_dump(mode="json"),
                "environment": {"API_TOKEN": "x"},
            }
        )


def test_sidecar_order_and_probes_are_explicit() -> None:
    first = SidecarContainerV1(
        role_name="database",
        image_ref=_IMAGE,
        argv=("/bin/database",),
        resources=_RESOURCES,
        startup_probe=ProbeV1(kind="tcp", port=5432),
        readiness_probe=ProbeV1(kind="tcp", port=5432),
    )
    second = SidecarContainerV1(
        role_name="api",
        image_ref=_IMAGE,
        argv=("/bin/api",),
        resources=_RESOURCES,
        startup_probe=ProbeV1(kind="http", port=8080, path="/healthz"),
        readiness_probe=ProbeV1(kind="http", port=8080, path="/readyz"),
        depends_on=("database",),
    )
    assert [item.role_name for item in _plan(sidecars=(first, second)).sidecars] == [
        "database",
        "api",
    ]
    with pytest.raises(ValidationError, match="earlier sidecars"):
        _plan(sidecars=(second, first))
    with pytest.raises(ValidationError, match="reserved container role"):
        _plan(
            sidecars=(
                SidecarContainerV1(
                    **{
                        **first.model_dump(mode="python"),
                        "role_name": "verifier",
                    }
                ),
            )
        )


def test_verifier_topology_is_fail_closed() -> None:
    separate = _plan(
        verifier_execution=VerifierExecution.SEPARATE_EXECUTION,
        verifier=None,
    )
    assert separate.verifier is None
    with pytest.raises(ValidationError, match="requires a verifier"):
        _plan(verifier=None)
    with pytest.raises(ValidationError, match="cannot run"):
        _plan(verifier_execution="skipped")

    verifier_unit = _plan(
        execution_role="verifier",
        main=_phase("verifier"),
        verifier_execution="skipped",
        verifier=None,
    )
    assert verifier_unit.main.role == "verifier"
    with pytest.raises(ValidationError, match="verifier main phase"):
        _plan(
            execution_role="verifier",
            verifier_execution="skipped",
            verifier=None,
        )
