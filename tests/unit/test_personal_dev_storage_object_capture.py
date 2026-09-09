"""Capture rejects ambiguous paths and incomplete identity before remote I/O."""

from uuid import UUID

import pytest

from loom.personal_dev_storage_object_capture import S3RetainedObjectCapture, StorageObjectCaptureError
from tests.unit.test_personal_dev_storage_transfer import _recipe


@pytest.mark.parametrize("key", ("../foreign", "/foreign", "x/../foreign", "x/./file", "x//file", "x\\file", "x\nfile", "x" * 1025))
def test_ambiguous_object_paths_fail_before_administrative_io(key):
    class NoIO:
        calls = []

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
