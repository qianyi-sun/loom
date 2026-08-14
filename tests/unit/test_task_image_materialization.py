from __future__ import annotations

import pytest

from loom.models.task import TaskConfig
from loom.task_image_materialization import (
    TaskImageExecutionGrantV1,
    canonical_task_checksum,
    required_task_image_architectures,
    task_image_materialization_key,
)


def _task_config(
    *,
    cpu_arch: str = "x86_64",
    docker_image: str | None = None,
    dockerfile: str | None = None,
    sidecars: list[dict[str, object]] | None = None,
) -> TaskConfig:
    environment: dict[str, object] = {
        "os": "linux",
        "cpu_arch": cpu_arch,
        "sidecars": sidecars or [],
    }
    if docker_image is not None:
        environment["docker_image"] = docker_image
    if dockerfile is not None:
        environment["dockerfile"] = dockerfile
    return TaskConfig.model_validate(
        {
            "schema_version": "1",
            "task": {"id": "benchmark/task-1", "name": "task-1"},
            "environment": environment,
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        }
    )


def test_prebuilt_only_task_needs_no_materialization() -> None:
    task = _task_config(docker_image="registry.example/task@sha256:1234")

    assert required_task_image_architectures(task) == ()


def test_main_dockerfile_needs_declared_native_architecture() -> None:
    task = _task_config(cpu_arch="arm64", dockerfile="environment/Dockerfile")

    assert required_task_image_architectures(task) == ("arm64",)


def test_any_architecture_expands_for_dockerfile_sidecar() -> None:
    task = _task_config(
        cpu_arch="any",
        docker_image="registry.example/prebuilt:latest",
        sidecars=[
            {
                "name": "database",
                "dockerfile": "environment/database.Dockerfile",
            }
        ],
    )

    assert required_task_image_architectures(task) == ("x86_64", "arm64")


def test_prebuilt_sidecars_do_not_create_build_work() -> None:
    task = _task_config(
        docker_image="registry.example/prebuilt:latest",
        sidecars=[
            {
                "name": "database",
                "docker_image": "postgres:17",
            }
        ],
    )

    assert required_task_image_architectures(task) == ()


def test_materialization_key_is_stable_and_architecture_qualified() -> None:
    checksum = "a" * 64

    assert (
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum=checksum,
            cpu_arch="arm64",
        )
        == "df469479b973f7365515571e28627719d98a7740c56070708022c51832810095"
    )
    assert task_image_materialization_key(
        task_id="benchmark/task-1",
        task_checksum=checksum,
        cpu_arch="x86_64",
    ) != task_image_materialization_key(
        task_id="benchmark/task-1",
        task_checksum=checksum,
        cpu_arch="arm64",
    )


def test_canonical_task_checksum_accepts_prefixed_benchmark_digest() -> None:
    checksum = "a" * 64

    assert canonical_task_checksum(checksum) == checksum
    assert canonical_task_checksum(f"sha256:{checksum}") == checksum


@pytest.mark.parametrize("cpu_arch", ["any", "amd64", "", "ARM64"])
def test_materialization_key_rejects_non_native_architecture(cpu_arch: str) -> None:
    with pytest.raises(ValueError, match="cpu_arch"):
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum="a" * 64,
            cpu_arch=cpu_arch,
        )


def test_materialization_key_rejects_malformed_checksum() -> None:
    with pytest.raises(ValueError, match="task_checksum"):
        task_image_materialization_key(
            task_id="benchmark/task-1",
            task_checksum="not-a-checksum",
            cpu_arch="arm64",
        )


def test_execution_grant_requires_exact_snapshot_component_set() -> None:
    with pytest.raises(ValueError, match="registry_images do not match"):
        TaskImageExecutionGrantV1.model_validate(
            {
                "schema_version": "loom.task-image-execution-grant.v1",
                "materialization_id": "00000000-0000-0000-0000-000000000123",
                "materialization_key": "1" * 64,
                "cpu_arch": "x86_64",
                "task_checksum": "2" * 64,
                "task_config": _task_config(
                    cpu_arch="x86_64",
                    dockerfile="environment/Dockerfile",
                ).model_dump(mode="json"),
                "task_source": None,
                "task_source_provenance": {},
                "registry_images": {
                    "sidecar:unexpected": ("registry.example/loom-task@sha256:" + "3" * 64),
                },
            }
        )
