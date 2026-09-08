from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.task_image_build_plan import (
    MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
    MAX_TASK_IMAGE_BUILD_PLAN_BYTES,
    MAX_TASK_IMAGE_BUILD_TIMEOUT_SECONDS,
    TaskImageBuildPlanV1,
    derive_task_image_build_plan,
)
from loom_task_image_authority.store import TaskImageBuildSessionAuthorization

NOW = datetime(2026, 9, 3, 14, 0, tzinfo=UTC)
GRANT_ID = UUID("11111111-1111-1111-1111-111111111111")
SESSION_ID = UUID("22222222-2222-2222-2222-222222222222")
MATERIALIZATION_ID = UUID("33333333-3333-3333-3333-333333333333")


def _authorization(**changes: object) -> TaskImageBuildSessionAuthorization:
    values: dict[str, object] = {
        "grant_id": GRANT_ID,
        "session_id": SESSION_ID,
        "session_generation": 3,
        "authority_version": 2,
        "builder_release_sha256": "1" * 64,
        "supervisor_executable_sha256": "2" * 64,
        "purpose": "production",
        "shadow_campaign_id": None,
        "environment": "staging",
        "pool_id": "staging-gb10-task-image",
        "cpu_arch": "arm64",
        "attestation_generation": 4,
        "attestation_sha256": "3" * 64,
        "attestation_expires_at": NOW + timedelta(seconds=50),
        "session_expires_at": NOW + timedelta(minutes=10),
        "grant_expires_at": NOW + timedelta(hours=1),
    }
    values.update(changes)
    return TaskImageBuildSessionAuthorization(**values)  # type: ignore[arg-type]


def _task_config() -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": "bench/task-1", "name": "Task 1"},
        "environment": {
            "os": "linux",
            "cpu_arch": "any",
            "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment",
            "build_timeout_sec": 900.0,
            "sidecars": [
                {
                    "name": "database",
                    "dockerfile": "services/database.Dockerfile",
                    "docker_build_context": "services",
                },
                {
                    "name": "ignored-image",
                    "docker_image": "postgres:17",
                },
                {
                    "name": "cache",
                    "dockerfile": "cache/Dockerfile",
                },
            ],
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
    }


def _row(**changes: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": MATERIALIZATION_ID,
        "task_id": "bench/task-1",
        "task_checksum": "4" * 64,
        "cpu_arch": "arm64",
        "task_config": _task_config(),
        "task_source": "s3://loom-bundles/bench/revision/task-1/",
        "task_source_provenance": {
            "bundle_file_metadata_sha256": "sha256:" + "5" * 64,
        },
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_derives_exact_frozen_native_plan_without_url_or_credentials() -> None:
    plan = derive_task_image_build_plan(_row(), _authorization())

    assert plan.model_dump(mode="json") == {
        "schema_version": "loom.task-image-build-plan.v1",
        "grant_id": "11111111-1111-1111-1111-111111111111",
        "session_id": "22222222-2222-2222-2222-222222222222",
        "session_generation": 3,
        "materialization_id": "33333333-3333-3333-3333-333333333333",
        "builder_id": "rootless:22222222222222222222222222222222",
        "task_id": "bench/task-1",
        "task_checksum": "4" * 64,
        "cpu_arch": "arm64",
        "platform": "linux/arm64",
        "bundle_bucket": "loom-bundles",
        "bundle_prefix": "bench/revision/task-1/",
        "bundle_file_metadata_sha256": "5" * 64,
        "bundle_file_limit": 2_000,
        "bundle_byte_limit": 512 * 1024 * 1024,
        "build_timeout_seconds": 900.0,
        "authorization_expires_at": "2026-09-03T14:00:50Z",
        "components": [
            {
                "name": "task",
                "dockerfile_path": "environment/Dockerfile",
                "context_path": "environment",
                "oci_output_path": "oci/0000.tar",
            },
            {
                "name": "sidecar:cache",
                "dockerfile_path": "cache/Dockerfile",
                "context_path": ".",
                "oci_output_path": "oci/0001.tar",
            },
            {
                "name": "sidecar:database",
                "dockerfile_path": "services/database.Dockerfile",
                "context_path": "services",
                "oci_output_path": "oci/0002.tar",
            },
        ],
    }
    serialized = plan.model_dump_json()
    assert "s3://" not in serialized
    assert "http://" not in serialized
    assert "https://" not in serialized
    assert "credential" not in serialized
    assert "token" not in serialized


def test_maps_x86_64_to_native_oci_platform() -> None:
    row = _row(cpu_arch="x86_64")
    plan = derive_task_image_build_plan(
        row,
        _authorization(cpu_arch="x86_64", pool_id="staging-oldlab-task-image"),
    )

    assert plan.cpu_arch == "x86_64"
    assert plan.platform == "linux/amd64"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dockerfile", "/etc/passwd"),
        ("dockerfile", "../Dockerfile"),
        ("dockerfile", "environment//Dockerfile"),
        ("dockerfile", "environment/./Dockerfile"),
        ("dockerfile", ""),
        ("docker_build_context", "/environment"),
        ("docker_build_context", "../environment"),
        ("docker_build_context", "environment//nested"),
        ("docker_build_context", "environment/./nested"),
        ("docker_build_context", ""),
    ],
)
def test_rejects_noncanonical_primary_paths(field: str, value: str) -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    environment[field] = value

    with pytest.raises(ValueError, match="path"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


def test_rejects_dockerfile_outside_declared_context() -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    environment["docker_build_context"] = "other"

    with pytest.raises(ValueError, match="context"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


def test_rejects_duplicate_sidecar_names_before_component_deduplication() -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    sidecars = environment["sidecars"]
    assert isinstance(sidecars, list)
    sidecars.append({"name": "cache", "docker_image": "redis:8"})

    with pytest.raises(ValueError, match="duplicate sidecar"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


def test_rejects_materialization_and_session_architecture_mismatch() -> None:
    with pytest.raises(ValueError, match="architecture"):
        derive_task_image_build_plan(_row(cpu_arch="x86_64"), _authorization())


def test_rejects_architecture_not_required_by_frozen_task() -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    environment["cpu_arch"] = "x86_64"

    with pytest.raises(ValueError, match="architecture"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


def test_rejects_snapshot_without_dockerfile_backed_components() -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    environment.pop("dockerfile")
    environment.pop("docker_build_context")
    environment["sidecars"] = [{"name": "database", "docker_image": "postgres:17"}]

    with pytest.raises(ValueError, match="Dockerfile-backed"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        "",
        "5" * 64,
        "sha256:" + "0" * 64,
        "sha256:" + "A" * 64,
        "sha256:" + "5" * 63,
    ],
)
def test_rejects_missing_or_noncanonical_metadata_digest(metadata: object) -> None:
    provenance = {} if metadata is None else {"bundle_file_metadata_sha256": metadata}

    with pytest.raises(ValueError, match="metadata"):
        derive_task_image_build_plan(
            _row(task_source_provenance=provenance),
            _authorization(),
        )


@pytest.mark.parametrize(
    "source",
    [
        None,
        "s3://loom-bundles",
        "s3://loom-bundles/bench/task",
        "s3://LOOM-BUNDLES/bench/task/",
        "s3://loom-bundles/bench//task/",
        "s3://loom-bundles/bench/./task/",
        "s3://loom-bundles/bench/../task/",
        "s3://loom-bundles/bench/task/?version=1",
        "https://loom-bundles.example/bench/task/",
    ],
)
def test_rejects_noncanonical_bundle_source(source: object) -> None:
    with pytest.raises(ValueError, match="bundle source"):
        derive_task_image_build_plan(_row(task_source=source), _authorization())


@pytest.mark.parametrize("timeout", [0.0, -1.0, 7_200.1, float("inf"), float("nan")])
def test_rejects_invalid_or_oversized_build_timeout(timeout: float) -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    environment["build_timeout_sec"] = timeout

    with pytest.raises(
        (ValueError, ValidationError),
        match=r"build_timeout|finite|less than",
    ):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("bundle_file_limit", 0),
        ("bundle_file_limit", MAX_TASK_IMAGE_BUILD_BUNDLE_FILES + 1),
        ("bundle_byte_limit", 0),
        ("bundle_byte_limit", MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES + 1),
        ("build_timeout_seconds", MAX_TASK_IMAGE_BUILD_TIMEOUT_SECONDS + 0.1),
    ],
)
def test_contract_rejects_invalid_or_oversized_ceilings(field: str, value: object) -> None:
    plan = derive_task_image_build_plan(_row(), _authorization())
    payload = plan.model_dump(mode="python")
    payload[field] = value

    with pytest.raises(ValidationError):
        TaskImageBuildPlanV1.model_validate(payload)


def test_rejects_frozen_snapshot_identity_drift() -> None:
    config = _task_config()
    task = config["task"]
    assert isinstance(task, dict)
    task["id"] = "bench/other"

    with pytest.raises(ValueError, match="task identity"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())


def test_rejects_a_plan_that_cannot_fit_the_bounded_authority_response() -> None:
    config = _task_config()
    environment = config["environment"]
    assert isinstance(environment, dict)
    environment["sidecars"] = [
        {
            "name": f"component-{index:03d}",
            "dockerfile": f"component-{index:03d}/" + "x" * 1024,
        }
        for index in range(127)
    ]

    with pytest.raises(ValueError, match="plan exceeds"):
        derive_task_image_build_plan(_row(task_config=config), _authorization())

    assert MAX_TASK_IMAGE_BUILD_PLAN_BYTES == 64 * 1024
