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


class _BatchClient:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = dict(objects)
        self.calls: list[tuple[str, object]] = []

    def head_object(self, **kwargs: str):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        body = self.objects.get((bucket, key))
        if body is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "HeadObject",
            )
        return {"ContentLength": len(body)}

    def get_object(self, **kwargs: str):
        return {"Body": _Body(self.objects[(kwargs["Bucket"], kwargs["Key"])])}

    def delete_objects(self, **kwargs: object):
        bucket = str(kwargs["Bucket"])
        delete = kwargs["Delete"]
        assert isinstance(delete, dict)
        requested = delete["Objects"]
        assert isinstance(requested, list)
        self.calls.append(("delete_objects", (bucket, requested)))
        deleted: list[dict[str, str]] = []
        for item in requested:
            assert isinstance(item, dict)
            key = str(item["Key"])
            self.objects.pop((bucket, key), None)
            deleted.append({"Key": key})
        return {"Deleted": deleted}


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


def test_null_version_delete_accepts_minio_omitted_version_evidence() -> None:
    body = b"pre-versioning"
    client = _Client(body)
    item = _item(body, version_id="null", content_sha256=None)

    S3ExactObjectDeleter(client).delete_exact(item)

    assert client.calls == [
        ("head", {"Bucket": item.bucket, "Key": item.object_key, "VersionId": "null"}),
        ("delete", {"Bucket": item.bucket, "Key": item.object_key, "VersionId": "null"}),
    ]


def test_null_version_delete_rejects_present_none_version_evidence() -> None:
    body = b"pre-versioning"

    class Client(_Client):
        def head_object(self, **params: str):
            result = super().head_object(**params)
            result["VersionId"] = None
            return result

    client = Client(body)
    item = _item(body, version_id="null", content_sha256=None)

    with pytest.raises(LifecycleGcExecutionError, match="version drifted"):
        S3ExactObjectDeleter(client).delete_exact(item)

    assert not client.deleted


def test_version_drift_fails_closed() -> None:
    body = b"versioned"
    client = _Client(body, version_id="v2")
    with pytest.raises(LifecycleGcExecutionError, match="version drifted"):
        S3ExactObjectDeleter(client).delete_exact(_item(body, version_id="v1", content_sha256=None))
    assert not client.deleted


def test_real_version_delete_rejects_omitted_version_evidence() -> None:
    body = b"versioned"
    client = _Client(body)
    item = _item(body, version_id="v1", content_sha256=None)

    with pytest.raises(LifecycleGcExecutionError, match="version drifted"):
        S3ExactObjectDeleter(client).delete_exact(item)

    assert not client.deleted


def test_batch_delete_verifies_all_bytes_and_binds_exact_returned_identities() -> None:
    first_body = b"first"
    second_body = b"second"
    first = _item(first_body, object_key="first.json")
    second = _item(second_body, object_key="second.json")
    client = _BatchClient(
        {
            (first.bucket, first.object_key): first_body,
            (second.bucket, second.object_key): second_body,
        }
    )
    deleter = S3ExactObjectDeleter(client, workers=2)

    deleter.delete_exact_many((first, second))

    assert len(client.calls) == 1
    assert deleter.exact_absent_many((first, second)) == {
        first.id: True,
        second.id: True,
    }


def test_batch_delete_accepts_minio_omitted_null_version_evidence() -> None:
    body = b"pre-versioning"
    item = _item(body, version_id="null")
    client = _BatchClient({(item.bucket, item.object_key): body})
    deleter = S3ExactObjectDeleter(client)

    deleter.delete_exact_many((item,))

    assert client.calls == [
        (
            "delete_objects",
            (
                item.bucket,
                [{"Key": item.object_key, "VersionId": "null"}],
            ),
        )
    ]
    assert deleter.exact_absent_many((item,)) == {item.id: True}


def test_batch_delete_rejects_present_none_null_version_evidence() -> None:
    body = b"pre-versioning"
    item = _item(body, version_id="null")

    class Client(_BatchClient):
        def delete_objects(self, **kwargs: object):
            response = super().delete_objects(**kwargs)
            response["Deleted"][0]["VersionId"] = None
            return response

    with pytest.raises(LifecycleGcExecutionError, match="identity drifted"):
        S3ExactObjectDeleter(Client({(item.bucket, item.object_key): body})).delete_exact_many(
            (item,)
        )


def test_batch_delete_rejects_ambiguous_unversioned_and_null_version_evidence() -> None:
    body = b"pre-versioning"
    unversioned = _item(body)
    null_version = _item(body, version_id="null")

    with pytest.raises(LifecycleGcExecutionError, match="identity drifted"):
        S3ExactObjectDeleter(
            _BatchClient({(unversioned.bucket, unversioned.object_key): body})
        ).delete_exact_many((unversioned, null_version))


def test_batch_delete_rejects_incomplete_identity_evidence() -> None:
    body = b"registered"
    item = _item(body)

    class Client(_BatchClient):
        def delete_objects(self, **kwargs: object):
            super().delete_objects(**kwargs)
            return {"Deleted": []}

    with pytest.raises(LifecycleGcExecutionError, match="identity drifted"):
        S3ExactObjectDeleter(
            Client({(item.bucket, item.object_key): body}),
        ).delete_exact_many((item,))


def test_batch_delete_verifies_entire_batch_before_first_mutation() -> None:
    body = b"registered"
    first = _item(body, object_key="first.json")
    drifted = _item(body, object_key="drifted.json", content_sha256="0" * 64)
    client = _BatchClient(
        {
            (first.bucket, first.object_key): body,
            (drifted.bucket, drifted.object_key): body,
        }
    )

    with pytest.raises(LifecycleGcExecutionError, match="digest drifted"):
        S3ExactObjectDeleter(client, workers=2).delete_exact_many((first, drifted))

    assert client.calls == []
    assert set(client.objects) == {
        (first.bucket, first.object_key),
        (drifted.bucket, drifted.object_key),
    }


@pytest.mark.parametrize("workers", (0, 65))
def test_batch_delete_rejects_unbounded_workers(workers: int) -> None:
    with pytest.raises(ValueError, match="workers"):
        S3ExactObjectDeleter(_BatchClient({}), workers=workers)
