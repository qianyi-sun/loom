"""User fixtures must remain separate from trusted runtime sidecars."""
from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

import pytest

from loom.execution_runtime_contract import ExecutionRuntimePlanV1, runtime_pod_resources
from loom.models.task import TaskConfig, TaskSidecarConfig
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
    runtime_profile_rejections,
)
from loom.task_image_materialization import TaskImageExecutionGrantV1
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

FIXTURE_IMAGE = "registry.example/fixture@sha256:" + "b" * 64
TASK_IMAGE = "registry.example/task@sha256:" + "c" * 64


def _fixture() -> dict[str, object]:
    return {
        "name": "server", "fixture": True,
        "dockerfile": "fixtures/server/Dockerfile", "docker_build_context": "fixtures/server",
        "command": ["python3", "/server.py"], "hostname": "fixture.example",
        "ports": [23], "cpus": 0.1, "memory_mb": 128, "storage_mb": 128,
        "healthcheck": {"command": "python3 /healthcheck.py", "start_period_sec": 5,
                        "interval_sec": 2, "timeout_sec": 5, "retries": 15},
    }


def _source() -> dict:
    task, _, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"].update(docker_image=None, dockerfile="environment/Dockerfile",
                              docker_build_context="environment", sidecars=[_fixture()])
    return raw


def _prepared():
    _, trial, profile = _inputs()
    profile = profile.model_copy(update={"service_lifecycle_ready": True})
    task = TaskConfig.model_validate(_source())
    grant = TaskImageExecutionGrantV1(
        schema_version="loom.task-image-execution-grant.v1", materialization_id=uuid4(),
        materialization_key="1" * 64, cpu_arch="x86_64", task_checksum="2" * 64,
        task_config=task.model_dump(mode="json"), task_source=None, task_source_provenance=_provenance(),
        registry_images={"task": TASK_IMAGE, "sidecar:server": FIXTURE_IMAGE},
    )
    return task, trial, profile, grant


def _plan():
    task, trial, profile, grant = _prepared()
    return compile_service_execution_plan(task=task, trial=trial, profile=profile,
        task_image_grant=grant, source_provenance=_provenance(), task_revision_sha256="sha256:" + "2" * 64)


def test_fixture_declaration_is_explicit_and_legacy_payload_stays_identical() -> None:
    legacy = TaskSidecarConfig(name="cache", docker_image="redis:7").model_dump(mode="json")
    assert not {"fixture", "ports", "cpus", "memory_mb", "storage_mb"} & legacy.keys()
    fixture = TaskSidecarConfig.model_validate(_fixture())
    assert fixture.fixture and fixture.ports == (23,)
    assert TaskSidecarConfig.model_validate_json(fixture.model_dump_json()) == fixture


@pytest.mark.parametrize("changed", [
    {"name": "task-sandbox"}, {"name": "bad_name"}, {"hostname": "localhost"},
    {"hostname": "fixture.example\n127.0.0.1 controller"}, {"hostname": None},
    {"command": "python3 /server.py"}, {"command": []}, {"depends_on": ["database"]},
    {"ports": []}, {"ports": [23, 23]}, {"ports": [0]}, {"cpus": None},
    {"memory_mb": None}, {"storage_mb": None}, {"healthcheck": None},
    {"healthcheck": {"command": "true", "retries": 0}},
    {"healthcheck": {"command": "true", "start_period_sec": 301}},
    {"docker_build_context": "."}, {"docker_build_context": "../fixture"},
])
def test_invalid_fixture_declarations_fail_closed(changed: dict) -> None:
    raw = _fixture() | changed
    with pytest.raises(ValueError, match="fixture"):
        TaskSidecarConfig.model_validate(raw)


@pytest.mark.parametrize("context", [".", "fixtures", "fixtures/server", "fixtures/server/main"])
def test_fixture_context_cannot_enter_main_image(context: str) -> None:
    raw = _source()
    raw["environment"]["docker_build_context"] = context
    with pytest.raises(ValueError, match="fixture"):
        TaskConfig.model_validate(raw)


def test_only_one_self_contained_fixture_is_admitted() -> None:
    raw = _source()
    second = deepcopy(raw["environment"]["sidecars"][0])
    second.update(name="another", hostname="another.example")
    raw["environment"]["sidecars"].append(second)
    with pytest.raises(ValueError, match=r"one.*fixture"):
        TaskConfig.model_validate(raw)


def test_compiled_fixture_preserves_probe_resources_and_separate_image_authority() -> None:
    plan = _plan()
    fixture, agent, verifier = plan.sidecars
    assert fixture.role_name == "fixture-server" and fixture.task_fixture
    assert fixture.task_image_component == "sidecar:server"
    assert fixture.hostname == "fixture.example" and fixture.image_ref == FIXTURE_IMAGE
    assert fixture.argv == ("python3", "/server.py")
    assert not fixture.private_sandbox and fixture.identity is None
    assert fixture.startup_probe.argv == ("/bin/sh", "-c", "python3 /healthcheck.py")
    assert fixture.startup_probe.initial_delay_seconds == 5
    assert fixture.startup_probe.timeout_seconds == 5 and fixture.startup_probe.period_seconds == 2
    assert fixture.startup_probe.failure_threshold == 15
    assert fixture.resources.cpu_millis == 100 and fixture.resources.memory_mib == 128
    assert agent.private_sandbox and verifier.private_sandbox
    assert FIXTURE_IMAGE not in plan.published_image_refs()
    assert TASK_IMAGE not in plan.published_image_refs()
    assert ExecutionRuntimePlanV1.model_validate(plan.canonical_payload()) == plan
    without_fixture = plan.model_copy(update={"sidecars": (agent, verifier)})
    assert runtime_pod_resources(plan).cpu_millis == runtime_pod_resources(without_fixture).cpu_millis + 100


def test_automatic_intake_admits_only_explicit_built_fixture_class() -> None:
    task, trial, profile, _ = _prepared()
    assert not automatic_service_execution_rejections(task, trial,
        source_provenance=_provenance(), allow_task_image_preparation=True)
    assert "service_lifecycle_runtime_unavailable" in runtime_profile_rejections(
        task, trial, profile.model_copy(update={"service_lifecycle_ready": False}),
        allow_task_image_preparation=True,
    )


def test_fixture_cannot_use_manual_template_without_prepared_grant() -> None:
    plan = _plan()
    payload = plan.canonical_payload()
    payload.pop("task_image_materialization_id")
    with pytest.raises(ValueError, match="fixture"):
        ExecutionRuntimePlanV1.model_validate(payload)


@pytest.mark.parametrize("changed", [
    {"private_sandbox": True}, {"task_image_component": "task"},
    {"role_name": "execution"}, {"hostname": "localhost"},
    {"hostname": "fixture\nforged"}, {"task_image_component": None},
])
def test_runtime_fixture_shape_cannot_be_reinterpreted_as_trusted(changed: dict) -> None:
    payload = _plan().canonical_payload()
    payload["sidecars"][0].update(changed)
    with pytest.raises(ValueError):
        ExecutionRuntimePlanV1.model_validate(payload)
