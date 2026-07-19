from __future__ import annotations

import hashlib
from io import BytesIO
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from loom.data_lifecycle_gc import LifecycleGcExecutionError, RegisteredObject
from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter


class _Body(BytesIO):
    pass


class _Client:
    def __init__(self, body: bytes, *, version_id: str | None = None) -> None:
        self.body = body
        self.version_id = version_id
        self.deleted = False
        self.calls: list[tuple[str, dict[str, str]]] = []

    def head_object(self, **params: str):
        self.calls.append(("head", params))
        if self.deleted:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "HeadObject",
            )
        result: dict[str, object] = {"ContentLength": len(self.body)}
        if self.version_id is not None:
            result["VersionId"] = self.version_id
        return result

    def get_object(self, **params: str):
        self.calls.append(("get", params))
        return {"Body": _Body(self.body)}

    def delete_object(self, **params: str):
        self.calls.append(("delete", params))
        self.deleted = True
        return {}


def _item(body: bytes, **overrides: object) -> RegisteredObject:
    values: dict[str, object] = {
        "id": uuid4(),
        "authority_id": uuid4(),
        "environment": "staging",
        "namespace": "loom-staging",
        "bucket": "loom-staging-artifacts",
        "object_key": "team/trial/result.json",
        "version_id": None,
        "content_sha256": hashlib.sha256(body).hexdigest(),
        "size_bytes": len(body),
        "state": "active",
    }
    values.update(overrides)
    return RegisteredObject(**values)  # type: ignore[arg-type]


def test_exact_unversioned_delete_hashes_before_deleting() -> None:
    body = b"registered bytes"
    client = _Client(body)
    deleter = S3ExactObjectDeleter(client, read_chunk_bytes=3)

    deleter.delete_exact(_item(body))

    assert [name for name, _params in client.calls] == ["head", "get", "delete"]
    assert deleter.exact_absent(_item(body))


def test_digest_or_size_drift_fails_before_delete() -> None:
    body = b"registered bytes"
    for item in (
        _item(body, content_sha256="0" * 64),
        _item(body, size_bytes=len(body) + 1),
    ):
        client = _Client(body)
        with pytest.raises(LifecycleGcExecutionError, match="drifted"):
            S3ExactObjectDeleter(client).delete_exact(item)
        assert not client.deleted


def test_versioned_delete_binds_exact_version_without_body_read() -> None:
    body = b"versioned"
    client = _Client(body, version_id="v1")
    item = _item(body, version_id="v1", content_sha256=None)

    S3ExactObjectDeleter(client).delete_exact(item)

    assert client.calls == [
        ("head", {"Bucket": item.bucket, "Key": item.object_key, "VersionId": "v1"}),
        ("delete", {"Bucket": item.bucket, "Key": item.object_key, "VersionId": "v1"}),
    ]


def test_version_drift_fails_closed() -> None:
    body = b"versioned"
    client = _Client(body, version_id="v2")
    with pytest.raises(LifecycleGcExecutionError, match="version drifted"):
        S3ExactObjectDeleter(client).delete_exact(_item(body, version_id="v1", content_sha256=None))
    assert not client.deleted
