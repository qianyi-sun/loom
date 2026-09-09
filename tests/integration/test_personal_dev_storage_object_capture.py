"""Real version-pinned capture, including multipart and ambiguous completion."""

import hashlib
from importlib import import_module
from uuid import UUID

import boto3
import pytest
from botocore.config import Config

from tests.integration.test_personal_dev_storage_minio import pinned_minio  # noqa: F401
from tests.unit.test_personal_dev_storage_transfer import _recipe


@pytest.fixture
def object_store(pinned_minio):  # noqa: F811
    server, _ = pinned_minio
    client = boto3.client(
        "s3", endpoint_url="http://" + server.get_config()["endpoint"],
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
        region_name="us-east-1", config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
    recipe = _recipe()
    bucket = "loom-transfer-snapshot-fixture"
    client.create_bucket(Bucket=recipe.source.identity.task_bucket)
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    try:
        yield client, recipe, bucket
    finally:
        client.close()


@pytest.mark.parametrize("size", (0, 7, 9 * 1024 * 1024))
def test_capture_pins_verified_bytes_and_not_the_mutable_latest_key(object_store, size):
    module = import_module("loom.personal_dev_storage_object_capture")
    client, recipe, bucket = object_store
    payload = b"x" * size
    source = recipe.source.identity.task_bucket
    uploaded = client.put_object(Bucket=source, Key="task/data", Body=payload,
                                 ContentType="application/x-loom-test", Metadata={"sample": "preserved"})
    capture = module.S3RetainedObjectCapture(client, snapshot_bucket=bucket)
    receipt = capture.capture(recipe, capture_id=UUID(int=800), purpose="tasks", key="task/data",
                              expected_etag=uploaded["ETag"], size_bytes=size)
    assert receipt.payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert receipt.size_bytes == size
    assert receipt.snapshot_version_id not in {"", "null"}
    client.put_object(Bucket=source, Key="task/data", Body=b"later-source")
    client.put_object(Bucket=bucket, Key=receipt.snapshot_key, Body=b"later-capture")
    captured = client.get_object(Bucket=bucket, Key=receipt.snapshot_key, VersionId=receipt.snapshot_version_id)
    try:
        assert captured["Body"].read() == payload
        assert captured["Metadata"] == {"sample": "preserved"}
        assert captured["ContentType"] == "application/x-loom-test"
    finally:
        captured["Body"].close()


@pytest.mark.parametrize("boundary", ("versioning", "etag", "size"))
def test_capture_rejects_unversioned_or_changed_source_without_receipt(object_store, boundary):
    module = import_module("loom.personal_dev_storage_object_capture")
    client, recipe, bucket = object_store
    source = recipe.source.identity.task_bucket
    uploaded = client.put_object(Bucket=source, Key="task/data", Body=b"original")
    if boundary == "versioning":
        client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    elif boundary == "etag":
        client.put_object(Bucket=source, Key="task/data", Body=b"replaced")
    with pytest.raises(module.StorageObjectCaptureError):
        module.S3RetainedObjectCapture(client, snapshot_bucket=bucket).capture(
            recipe, capture_id=UUID(int=800), purpose="tasks", key="task/data",
            expected_etag=uploaded["ETag"], size_bytes=7 if boundary == "size" else 8,
        )
    assert not client.list_objects_v2(Bucket=bucket).get("Contents")


def test_lost_multipart_completion_can_retry_without_reusing_the_unproven_receipt(object_store):
    module = import_module("loom.personal_dev_storage_object_capture")
    client, recipe, bucket = object_store
    payload = b"x" * (9 * 1024 * 1024)
    uploaded = client.put_object(Bucket=recipe.source.identity.task_bucket, Key="task/data", Body=payload)

    class LostReply:
        lost = False

        def __getattr__(self, name):
            return getattr(client, name)

        def complete_multipart_upload(self, **kwargs):
            result = client.complete_multipart_upload(**kwargs)
            if not self.lost:
                self.lost = True
                raise TimeoutError("injected lost completion")
            return result

    transport = LostReply()
    capture = module.S3RetainedObjectCapture(transport, snapshot_bucket=bucket)
    arguments = dict(capture_id=UUID(int=800), purpose="tasks", key="task/data",
                     expected_etag=uploaded["ETag"], size_bytes=len(payload))
    with pytest.raises(module.StorageObjectCaptureError):
        capture.capture(recipe, **arguments)
    assert transport.lost
    receipt = capture.capture(recipe, **arguments)
    assert receipt.payload_sha256 == hashlib.sha256(payload).hexdigest()
    versions = client.list_object_versions(Bucket=bucket, Prefix=receipt.snapshot_key)["Versions"]
    assert len(versions) == 2
    assert receipt.snapshot_version_id in {version["VersionId"] for version in versions}
