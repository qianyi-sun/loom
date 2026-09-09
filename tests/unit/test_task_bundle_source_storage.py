"""Recover only an intent's exact versions, never equal-byte foreign objects."""

from __future__ import annotations

import hashlib
import importlib
import io
from uuid import uuid4

import pytest


def _module():
    return importlib.import_module("loom.task_bundle_source_storage")


def _intent(**changes):
    values = dict(
        id=uuid4(),
        bucket="sources",
        object_key="registered/task.toml",
        content_sha256=hashlib.sha256(b"abc").hexdigest(),
        size_bytes=3,
    )
    values.update(changes)
    return _module().TaskBundleObjectIntentV1(**values)


class _Inventory:
    def __init__(self, intent):
        self.intent = intent
        self.versions = {}
        self.heads = []
        self.reads = []
        self.streams = []

    def get_bucket_versioning(self, **kwargs):
        return {"Status": "Enabled"}

    def list_object_versions(self, **kwargs):
        versions = [
                dict(Key=self.intent.object_key, VersionId=version, Size=len(body))
                for version, (body, _) in self.versions.items()
            ]
        marker = kwargs.get("VersionIdMarker")
        start = next((i + 1 for i, item in enumerate(versions) if item["VersionId"] == marker), 0)
        selected = versions[start:start + kwargs["MaxKeys"]]
        truncated = start + len(selected) < len(versions)
        result = {"Versions": selected, "IsTruncated": truncated}
        if truncated:
            result.update(NextKeyMarker=self.intent.object_key, NextVersionIdMarker=selected[-1]["VersionId"])
        return result

    def head_object(self, **kwargs):
        self.heads.append(kwargs)
        body, metadata = self.versions[kwargs["VersionId"]]
        return dict(VersionId=kwargs["VersionId"], ContentLength=len(body), Metadata=metadata)

    def get_object(self, **kwargs):
        self.reads.append(kwargs)
        body, metadata = self.versions[kwargs["VersionId"]]
        stream = io.BytesIO(body)
        self.streams.append(stream)
        return dict(
            VersionId=kwargs["VersionId"], ContentLength=len(body), Metadata=metadata, Body=stream
        )


def test_inventory_verifies_every_own_version_and_excludes_identical_foreign_write():
    intent = _intent()
    client = _Inventory(intent)
    client.versions = {
        "ours-1": (b"abc", intent.metadata),
        "ours-2": (b"abc", intent.metadata),
        "theirs": (b"abc", {**intent.metadata, "loom-source-write-id": str(uuid4())}),
    }
    versions = _module().S3TaskBundleVersionInventory(client).scan(intent)
    assert {item.version_id for item in versions} == {"ours-1", "ours-2"}
    assert all(item.uri == "s3://sources/registered/task.toml" for item in versions)
    assert {call["VersionId"] for call in client.reads} == {"ours-1", "ours-2"}
    assert all(stream.closed for stream in client.streams)


@pytest.mark.parametrize("change", ["bytes", "size", "metadata", "version", "null"])
def test_inventory_rejects_changed_own_identity(change):
    intent = _intent()
    client = _Inventory(intent)
    client.versions = {"ours": (b"abc", intent.metadata)}
    if change == "bytes":
        client.versions["ours"] = (b"xyz", intent.metadata)
    elif change == "size":
        client.versions["ours"] = (b"abcd", intent.metadata)
    elif change == "metadata":
        client.versions["ours"] = (b"abc", {**intent.metadata, "loom-source-sha256": "b" * 64})
    elif change == "version":
        original = client.head_object
        client.head_object = lambda **kwargs: {**original(**kwargs), "VersionId": "foreign"}
    else:
        client.versions = {"null": (b"abc", intent.metadata)}
    with pytest.raises(ValueError):
        _module().S3TaskBundleVersionInventory(client).scan(intent)
    assert all(stream.closed for stream in client.streams)


def test_inventory_rejects_unbounded_or_nonprogressing_pagination():
    intent = _intent()
    client = _Inventory(intent)
    calls = []

    def list_versions(**kwargs):
        calls.append(kwargs)
        return dict(
            IsTruncated=True,
            NextKeyMarker=intent.object_key,
            NextVersionIdMarker="same-version",
            Versions=[],
        )

    client.list_object_versions = list_versions
    with pytest.raises(ValueError, match="pagination"):
        _module().S3TaskBundleVersionInventory(client).scan(intent)
    assert len(calls) == 2


async def test_write_verifies_intended_bytes_before_storage_and_checks_exact_receipt():
    from loom.trajectory.storage import ObjectWriteResult

    intent = _intent()
    calls = []

    class Store:
        async def put_object_with_metadata(self, **kwargs):
            calls.append(kwargs)
            return ObjectWriteResult(uri="s3://wrong/key", version_id="v1")

    for body in (b"xyz", b"abcd"):
        with pytest.raises(ValueError, match="bytes"):
            await _module().write_task_bundle_source_object(Store(), intent, body)
    assert calls == []
    with pytest.raises(ValueError, match="receipt"):
        await _module().write_task_bundle_source_object(Store(), intent, b"abc")
    assert calls == [
        dict(
            bucket="sources",
            key=intent.object_key,
            body=b"abc",
            metadata=intent.metadata,
            require_versioning=True,
        )
    ]


@pytest.mark.parametrize(
    "changes",
    [
        dict(object_key="../x"),
        dict(object_key=""),
        dict(bucket="sources/path"),
        dict(content_sha256="sha256:" + "a" * 64),
        dict(size_bytes=-1),
    ],
)
def test_intent_rejects_invalid_persisted_descriptor(changes):
    with pytest.raises(ValueError):
        _intent(**changes)


@pytest.mark.parametrize("limits", [dict(max_read_bytes=3), dict(max_versions=1)])
def test_inventory_returns_resumable_progress_under_hard_budgets(limits):
    intent = _intent()
    client = _Inventory(intent)
    client.versions = {"ours-1": (b"abc", intent.metadata),
                       "theirs": (b"abc", {**intent.metadata, "loom-source-write-id": str(uuid4())}),
                       "ours-2": (b"abc", intent.metadata)}
    reader = _module().S3TaskBundleVersionInventory(client, **limits)
    first = reader.scan_batch(intent)
    assert [item.version_id for item in first.versions] == ["ours-1"]
    assert not first.observed_end and first.continuation is not None
    # Restart from a serialized progress checkpoint, not an in-memory reader.
    cursor = _module().TaskBundleInventoryCursorV1.model_validate_json(first.continuation.model_dump_json())
    second = _module().S3TaskBundleVersionInventory(client, **limits).scan_batch(intent, cursor=cursor)
    assert second.versions == () and not second.observed_end
    third = reader.scan_batch(intent, cursor=second.continuation)
    assert [item.version_id for item in third.versions] == ["ours-2"]
    assert third.observed_end and third.continuation is None
    assert len(client.reads) == 2
    # A scan endpoint never proves that a delayed writer has terminated.
    client.versions["late"] = (b"abc", intent.metadata)
    fourth = reader.scan_batch(intent, cursor=second.continuation)
    assert not fourth.observed_end


@pytest.mark.parametrize("changes", [dict(id=uuid4()), dict(bucket="another"),
    dict(object_key="another/file"), dict(content_sha256="b" * 64), dict(size_bytes=2)])
def test_inventory_continuation_cannot_cross_intent_boundaries(changes):
    intent = _intent()
    client = _Inventory(intent)
    client.versions = {"v1": (b"abc", intent.metadata), "v2": (b"abc", intent.metadata)}
    reader = _module().S3TaskBundleVersionInventory(client, max_versions=1)
    first = reader.scan_batch(intent)
    another = _module().TaskBundleObjectIntentV1.model_validate({**intent.model_dump(), **changes})
    before = len(client.heads)
    with pytest.raises(ValueError, match="cursor"):
        reader.scan_batch(another, cursor=first.continuation)
    assert len(client.heads) == before
