"""Unit tests for transform gating during materialization (#242 sub-plan 4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from loom.models.taskset import UserTaskSetManifest
from loom.taskset.materialize import materialize_task_set
from loom.taskset.transform_sandbox import TransformSandboxConfig

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

_GATES_OFF = TransformSandboxConfig(enabled=False, network_isolated=False)
_GATES_ON = TransformSandboxConfig(enabled=True, network_isolated=True)


def test_transform_gates_disabled_fails_job() -> None:
    manifest = UserTaskSetManifest.model_validate(_MANIFEST)
    output = materialize_task_set(
        manifest=manifest,
        task_set_id="ts/team/tasks",
        owning_team_id="team",
        intents=["trajectory_generation"],
        verifier_blob_uri=None,
        transform_blob_uri="s3://artifacts/x/transform.py",
        transform_config=_GATES_OFF,
        minio_client=MagicMock(),
        artifacts_bucket="artifacts",
        upstream_cache_root=__import__("pathlib").Path("/tmp/loom-test"),
    )
    assert output.status == "failed"
    assert output.status_reason == "transform_unsupported_on_host"
    assert output.job_failure_reason == "transform_unsupported_on_host"


def test_transform_applied_before_mapping() -> None:
    manifest = UserTaskSetManifest.model_validate(_MANIFEST)
    minio = MagicMock()
    minio.get_object.return_value = {"Body": MagicMock(read=lambda: b"def transform(row): return row")}

    with patch(
        "loom.taskset.materialize.run_transform",
        return_value={"id": "1", "question": "mapped"},
    ) as mock_transform:
        output = materialize_task_set(
            manifest=manifest,
            task_set_id="ts/team/tasks",
            owning_team_id="team",
            intents=["trajectory_generation"],
            verifier_blob_uri=None,
            transform_blob_uri="s3://artifacts/tasksets/user/team/tasks/transform.py",
            transform_config=_GATES_ON,
            minio_client=minio,
            artifacts_bucket="artifacts",
            upstream_cache_root=__import__("pathlib").Path("/tmp/loom-test"),
        )

    mock_transform.assert_called_once()
    assert output.status == "ready"
    assert output.task_count == 1
