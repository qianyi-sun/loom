"""Unit tests for transform gating during materialization (#242 sub-plan 4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from loom.models.taskset import UserTaskSetManifest
from loom.taskset.materialize import materialize_task_set
from loom.taskset.transform_sandbox import TransformSandboxConfig
from loom.workload_trust import WorkloadTrustContract
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

_MANIFEST = {
    "apiVersion": "loom.taskset/v1",
    "kind": "UserTaskSet",
    "metadata": {"name": "tasks", "display_name": "Tasks"},
    "source": {"type": "jsonl-inline", "locator": '{"id":"1","question":"raw"}\n'},
    "instance_mapping": {"prompt": "row.question", "task_id": "row.id"},
    "task_template": {
        "task": {"id": "{{ instance.task_id }}", "name": "t"},
        "environment": {"os": "linux"},
        "agent": {"name": "default"},
        "steps": [{"name": "main", "artifacts": ["out.txt"]}],
    },
    "transform": {"file": "transform.py"},
}

_V1_INTERNAL_TRUSTED = WorkloadTrustContract(
    workload_trust_mode="internal_trusted",
    taskset_transforms_enabled=False,
    taskset_transform_network_isolated=False,
    untrusted_workload_isolation=False,
)


def _legacy_gates_on() -> TransformSandboxConfig:
    return TransformSandboxConfig(
        enabled=True,
        network_isolated=True,
        workload_contract=_V1_INTERNAL_TRUSTED,
    )


def test_v1_contract_rejects_transform_before_blob_fetch_or_runner() -> None:
    manifest = UserTaskSetManifest.model_validate(_MANIFEST)
    minio = MagicMock()
    with (
        patch(
            "loom.taskset.materialize._fetch_blob_bytes",
            side_effect=AssertionError("v1 rejection must not fetch blobs"),
        ) as fetch_blob,
        patch(
            "loom.taskset.transform_sandbox.run_transform",
            side_effect=AssertionError("v1 rejection must not invoke a runner"),
        ) as run,
    ):
        output = materialize_task_set(
            manifest=manifest,
            task_set_id="ts/team/tasks",
            owning_team_id="team",
            output_generation="unit-test/1",
            intents=["trajectory_generation"],
            verifier_blob_uri=None,
            transform_blob_uri="s3://artifacts/x/transform.py",
            transform_config=_legacy_gates_on(),
            minio_client=minio,
            artifacts_bucket="artifacts",
            upstream_cache_root=__import__("pathlib").Path("/tmp/loom-test"),
        )

    assert output.status == "failed"
    assert output.status_reason == "transform_unavailable_in_internal_trusted"
    assert output.job_failure_reason == "transform_unavailable_in_internal_trusted"
    assert output.task_rows == []
    assert output.task_count == 0
    assert minio.mock_calls == []
    fetch_blob.assert_not_called()
    run.assert_not_called()


def test_service_startup_rejects_invalid_v1_workload_trust_tuple() -> None:
    settings = LoomServiceSettings(
        _env_file=None,
        db_url="postgresql+asyncpg://user:password@db.example/loom",
        minio_access_key="access-key",
        minio_secret_key="secret-key",
        taskset_materializer_transforms_enabled=True,
    )

    with pytest.raises(RuntimeError) as exc_info:
        create_app(settings)

    assert str(exc_info.value) == (
        "invalid v1 workload trust contract: "
        "taskset_transforms_enabled must be false"
    )
