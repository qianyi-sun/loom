"""Anonymous Harbor tasks retain source-relative identity through ordinary TaskSet intake."""

import io
import tarfile
from uuid import uuid4

import pytest

from loom.models.taskset import UserTaskSetManifest
from loom.taskset.materialize import materialize_task_set


class ObjectStore:
    """Replace only network storage; run the actual materializer and publisher."""

    def __init__(self, archive):
        self.objects = {"tasksets/user/team/slice/bundle.tar.gz": archive}

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


@pytest.mark.parametrize("bundle_root,expected_id", [("tasks/alpha", "alpha"), ("", "slice")])
def test_taskset_intake_uses_stable_source_identity_and_preserves_authored_bytes(tmp_path, bundle_root, expected_id):
    authored = b'version = "1.0"\n[metadata]\ntags = ["shell"]\n[environment]\ncpus = 2\nmemory = "2G"\nstorage = "5G"\n'
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as stream:
        for name, content in {
            "task.toml": authored,
            "instruction.md": b"Do the task.\n",
            "environment/Dockerfile": b"FROM ubuntu:24.04\nWORKDIR /app\n",
        }.items():
            entry = tarfile.TarInfo(f"{bundle_root}/{name}" if bundle_root else name)
            entry.size = len(content)
            stream.addfile(entry, io.BytesIO(content))
    store = ObjectStore(archive.getvalue())
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "slice", "display_name": "Slice"},
        "intents": ["trajectory_generation"],
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz"},
    })

    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team/slice", owning_team_id="team",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None,
        minio_client=store, artifacts_bucket="artifacts", upstream_cache_root=tmp_path,
    )

    assert result.status == "ready", result.error_summary
    assert result.task_count == 1
    task = result.task_rows[0]
    assert task.id == f"ts/team/slice/tasks/{expected_id}"
    assert task.config["task"]["id"] == expected_id
    assert task.config["environment"]["memory_mb"] == 2048
    assert task.config["environment"]["cpus"] == 2
    stored_key = task.source.removeprefix("s3://artifacts/") + "task.toml"
    assert store.objects[stored_key] == authored
