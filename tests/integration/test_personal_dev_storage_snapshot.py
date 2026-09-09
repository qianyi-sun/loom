"""Snapshot orchestration scans every exact bucket and rejects changed inventories."""

from importlib import import_module
from uuid import UUID

import pytest

from loom.personal_dev_storage_object_capture import StorageObjectCaptureError
from tests.integration.test_personal_dev_storage_minio import pinned_minio  # noqa: F401
from tests.integration.test_personal_dev_storage_object_capture import object_store  # noqa: F401


def _populate(client, recipe):
    identity = recipe.source.identity
    client.create_bucket(Bucket=identity.trajectories_bucket)
    client.create_bucket(Bucket=identity.artifacts_bucket)
    for number in range(3):
        client.put_object(Bucket=identity.task_bucket, Key=f"task/{number}", Body=b"task")
    client.put_object(Bucket=identity.trajectories_bucket, Key="trace", Body=b"trace")
    client.put_object(Bucket=identity.artifacts_bucket, Key="artifact", Body=b"artifact")


def test_snapshot_scans_all_three_buckets_with_pagination_before_capture(object_store):  # noqa: F811
    module = import_module("loom.personal_dev_storage_snapshot")
    client, recipe, bucket = object_store
    _populate(client, recipe)
    calls = []

    class SmallPages:
        def __getattr__(self, name):
            return getattr(client, name)

        def list_objects_v2(self, **kwargs):
            calls.append(kwargs["Bucket"])
            return client.list_objects_v2(**{**kwargs, "MaxKeys": 1})

    snapshots = module.S3RetainedObjectSnapshot(SmallPages(), snapshot_bucket=bucket)
    inventory = snapshots.inventory(recipe, capture_id=UUID(int=800))
    identity = recipe.source.identity
    assert set(calls) == {identity.task_bucket, identity.trajectories_bucket, identity.artifacts_bucket}
    assert calls.count(identity.task_bucket) == 3
    assert len(inventory.objects) == 5
    snapshot = snapshots.capture_inventory(inventory)
    assert len(snapshot.captures) == 5
    assert {item.size_bytes for item in snapshot.captures} == {4, 5, 8}
    assert all(item.snapshot_version_id not in {"null", ""} for item in snapshot.captures)


def test_snapshot_cannot_complete_when_a_selected_source_object_changes(object_store):  # noqa: F811
    module = import_module("loom.personal_dev_storage_snapshot")
    client, recipe, bucket = object_store
    _populate(client, recipe)
    snapshots = module.S3RetainedObjectSnapshot(client, snapshot_bucket=bucket)
    inventory = snapshots.inventory(recipe, capture_id=UUID(int=800))
    client.put_object(Bucket=recipe.source.identity.task_bucket, Key="task/0", Body=b"changed")
    with pytest.raises(StorageObjectCaptureError):
        snapshots.capture_inventory(inventory)
    assert client.head_object(Bucket=recipe.source.identity.artifacts_bucket, Key="artifact")["ContentLength"] == 8
