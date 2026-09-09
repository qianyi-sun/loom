"""Pinned object-store primitives needed by retained-data snapshots."""

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from tests.integration.test_personal_dev_storage_minio import pinned_minio  # noqa: F401
from tests.unit.test_personal_dev_storage_transfer import _recipe


def test_conditional_capture_rejects_replaced_source_and_preserves_captured_bytes(pinned_minio):  # noqa: F811
    server, _ = pinned_minio
    recipe = _recipe()
    source = recipe.source.identity.task_bucket
    # Test-only control-owned storage, inaccessible to source and target tenants.
    snapshot = "loom-transfer-snapshot-fixture"
    client = boto3.client(
        "s3", endpoint_url="http://" + server.get_config()["endpoint"],
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
        region_name="us-east-1", config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )
    try:
        client.create_bucket(Bucket=source)
        client.create_bucket(Bucket=snapshot)
        original = client.put_object(Bucket=source, Key="task/data", Body=b"original")
        client.copy_object(
            Bucket=snapshot, Key="capture-1", CopySource={"Bucket": source, "Key": "task/data"},
            CopySourceIfMatch=original["ETag"],
        )
        client.put_object(Bucket=source, Key="task/data", Body=b"replacement")
        with pytest.raises(ClientError) as copy_error:
            client.copy_object(
                Bucket=snapshot, Key="capture-2", CopySource={"Bucket": source, "Key": "task/data"},
                CopySourceIfMatch=original["ETag"],
            )
        assert copy_error.value.response["Error"]["Code"] == "PreconditionFailed"
        with pytest.raises(ClientError) as read_error:
            client.get_object(Bucket=source, Key="task/data", IfMatch=original["ETag"])
        assert read_error.value.response["Error"]["Code"] == "PreconditionFailed"
        captured = client.get_object(Bucket=snapshot, Key="capture-1")["Body"]
        try:
            assert captured.read() == b"original"
        finally:
            captured.close()
        with pytest.raises(ClientError) as absent:
            client.head_object(Bucket=snapshot, Key="capture-2")
        assert absent.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        client.close()
