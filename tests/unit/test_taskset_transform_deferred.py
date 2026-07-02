"""Unit tests for transform deferral during materialization (#242 sub-plan 3)."""

from __future__ import annotations

from unittest.mock import MagicMock

from loom.models.taskset import UserTaskSetManifest
from loom.taskset.materialize import materialize_task_set


def test_transform_manifest_fails_fast() -> None:
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1",
        "kind": "UserTaskSet",
        "metadata": {"name": "tasks", "display_name": "Tasks"},
        "source": {"type": "jsonl-inline", "locator": '{"id":"1"}\n'},
        "instance_mapping": {"task_id": "row.id"},
        "task_template": {
            "task": {"id": "{{ instance.task_id }}", "name": "t"},
            "environment": {"os": "linux"},
            "agent": {"name": "default"},
            "steps": [{"name": "main", "artifacts": ["out.txt"]}],
        },
        "transform": {"file": "transform.py"},
    })
    output = materialize_task_set(
        manifest=manifest,
        task_set_id="ts/team/tasks",
        owning_team_id="team",
        intents=["trajectory_generation"],
        verifier_blob_uri=None,
        minio_client=MagicMock(),
        artifacts_bucket="artifacts",
        upstream_cache_root=__import__("pathlib").Path("/tmp/loom-test"),
    )
    assert output.status == "failed"
    assert output.status_reason == "transform_not_supported_yet"
    assert output.job_failure_reason == "transform_not_supported_yet"
