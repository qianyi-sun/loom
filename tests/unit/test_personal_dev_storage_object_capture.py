"""Capture rejects ambiguous paths and incomplete identity before remote I/O."""

import io
from uuid import UUID

import pytest

from loom.personal_dev_storage_object_capture import (
    S3RetainedObjectCapture,
    StorageObjectCaptureError,
)
from tests.unit.test_personal_dev_storage_transfer import _recipe


@pytest.mark.parametrize("key", ("../foreign", "/foreign", "x/../foreign", "x/./file", "x//file", "x\\file", "x\nfile", "x" * 1025))
def test_ambiguous_object_paths_fail_before_administrative_io(key):
    class NoIO:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            self.calls.append(name)
            raise AssertionError("invalid capture must not access storage")

    client = NoIO()
    with pytest.raises(StorageObjectCaptureError):
        S3RetainedObjectCapture(client, snapshot_bucket="loom-transfer-fixture").capture(
            _recipe(), capture_id=UUID(int=800), purpose="tasks", key=key,
            expected_etag='"source-etag"', size_bytes=1,
        )
    assert client.calls == []


@pytest.mark.parametrize("change", (
    {"ContentLength": True}, {"VersionId": "another-version"},
    {"ContentLength": 2}, {"Body": b""}, {"Body": b"longer"},
))
def test_capture_rejects_malformed_or_truncated_version_readback_and_closes_body(change):
    body = io.BytesIO(change.get("Body", b"x"))

    class Client:
        def get_bucket_versioning(self, **kwargs):
            return {"Status": "Enabled"}

        def head_object(self, **kwargs):
            return {"ContentLength": 1, "ETag": '"source-etag"'}

        def copy_object(self, **kwargs):
            return {"VersionId": "captured-version"}

        def get_object(self, **kwargs):
            return {"ContentLength": 1, "VersionId": "captured-version", **change, "Body": body}

    with pytest.raises(StorageObjectCaptureError):
        S3RetainedObjectCapture(Client(), snapshot_bucket="loom-transfer-fixture").capture(
            _recipe(), capture_id=UUID(int=800), purpose="tasks", key="task/data",
            expected_etag='"source-etag"', size_bytes=1,
        )
    assert body.closed
