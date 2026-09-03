from __future__ import annotations

import hashlib
import json
from pathlib import Path

from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    automatic_service_execution_rejections,
    build_service_execution_input_manifest,
    compile_service_execution_plan,
)
from tests.support.execution_image_admission import signed_image_admission_bundle

_TASK_IMAGE = "registry.example/task@sha256:" + "a" * 64
_RUNTIME_IMAGE = "registry.example/runtime@sha256:" + "b" * 64
_REVISION = "sha256:" + "c" * 64


def _task(**updates: object) -> TaskConfig:
    raw: dict[str, object] = {
        "schema_version": "1",
        "task": {"id": "task-1", "name": "Automatic Nebius task"},
        "environment": {
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "docker_image": _TASK_IMAGE,
            "cpus": 1,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "network_policies_supported": ["gateway-only"],
            "baseline_network_policy": {"kind": "gateway-only"},
        },
        "agent": {"name": "direct-completion"},
        "verifier": {
            "name": "script",
            "args": {"script_path": "verifier/check.sh"},
        },
        "steps": [
            {
                "name": "main",
                "instruction_file": "instruction.md",
                "artifacts": ["answer.txt"],
            }
        ],
    }
    raw.update(updates)
    return TaskConfig.model_validate(raw)


def _trial() -> TrialConfig:
    return TrialConfig(
        agent_name="direct-completion",
        agent_model=ModelSpec(provider="openai", name="gpt-5"),
        request_params={"temperature": 0.2},
    )


def _provenance() -> dict[str, object]:
    return {
        "service_execution_input": {
            "schema_version": "loom.service-execution-input.v1",
            "manifest_uri": "s3://artifacts/task-inputs/task-1.json",
            "manifest_sha256": "sha256:" + "d" * 64,
            "file_count": 3,
            "total_bytes": 4096,
        }
    }


def _profile() -> ServiceExecutionRuntimeProfileV1:
    return ServiceExecutionRuntimeProfileV1(
        candidate_sha="1" * 40,
        execution_class_id="linux-amd64-cpu-pod-v1",
        task_image_ref=_TASK_IMAGE,
        runtime_image_ref=_RUNTIME_IMAGE,
        runtime_binary_sha256="sha256:" + "e" * 64,
        image_admission=signed_image_admission_bundle((_TASK_IMAGE, _RUNTIME_IMAGE)),
    )


def test_input_manifest_is_canonical_and_preserves_executable_mode(tmp_path: Path) -> None:
    (tmp_path / "instruction.md").write_text("hello\n", encoding="utf-8")
    script = tmp_path / "verifier" / "check.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)

    manifest = build_service_execution_input_manifest(
        tmp_path,
        task_checksum=_REVISION,
    )

    assert [item.relative_path for item in manifest.files] == [
        "instruction.md",
        "verifier/check.sh",
    ]
    assert [item.mode for item in manifest.files] == ["0644", "0755"]
    assert json.loads(manifest.canonical_bytes()) == manifest.model_dump(mode="json")


def test_ordinary_task_compiles_to_profile_owned_nebius_plan() -> None:
    task = _task()
    trial = _trial()

    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )

    assert plan.execution_class_id == "linux-amd64-cpu-pod-v1"
    assert plan.task_input is not None
    assert plan.task_input.file_count == 3
    assert plan.main.argv == (
        "python",
        "-m",
        "loom.service_execution_task",
        "direct-completion",
    )
    assert plan.main.environment["LOOM_TASK_MODEL"] == "openai/gpt-5"
    assert plan.verifier is not None
    assert plan.verifier.argv == ("/bin/sh", "verifier/check.sh")
    assert plan.task_resources.cpu_millis == 1000
    assert plan.workspace_mib == 2048
    assert [item.model_dump(mode="json") for item in plan.output_declarations] == [
        {
            "source_path": "answer.txt",
            "relative_path": "artifacts/answer.txt",
            "kind": "task_artifact",
            "required": True,
        },
        {
            "source_path": ".loom/agent/trajectory.jsonl",
            "relative_path": "trajectory/events.jsonl",
            "kind": "trajectory",
            "required": True,
        },
        {
            "source_path": ".loom/agent/usage.json",
            "relative_path": "accounting/usage.json",
            "kind": "usage",
            "required": True,
        },
        {
            "source_path": ".loom/verifier/output.json",
            "relative_path": "verifier/output.json",
            "kind": "verifier",
            "required": True,
        },
    ]


def test_automatic_compiler_rejects_unimplemented_task_semantics() -> None:
    task = _task(
        environment={
            **_task().environment.model_dump(mode="json"),
            "environment": {"UNHANDLED": "value"},
        }
    )

    assert automatic_service_execution_rejections(
        task,
        _trial(),
        source_provenance={},
    ) == (
        "immutable_task_input_unavailable",
        "extended_environment_unsupported",
    )


def test_manifest_digest_matches_persisted_binding_bytes(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("schema_version = '1'\n", encoding="utf-8")
    manifest = build_service_execution_input_manifest(tmp_path, task_checksum=_REVISION)
    body = manifest.canonical_bytes()
    assert "sha256:" + hashlib.sha256(body).hexdigest() == (
        "sha256:" + hashlib.sha256(manifest.canonical_bytes()).hexdigest()
    )
