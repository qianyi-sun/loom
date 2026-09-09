"""Real TLS MinIO evidence for late version recovery, not durable admission."""

from __future__ import annotations

import asyncio
import hashlib
import threading
from uuid import uuid4

import pytest

from loom.data_lifecycle_gc import RegisteredObject
from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter
from loom.task_bundle_source_storage import (
    S3TaskBundleVersionInventory,
    TaskBundleObjectIntentV1,
    write_task_bundle_source_object,
)
from loom.trajectory.storage import MinioObjectStore
from tests.integration.test_task_image_bundle_minio_signing import minio_tls  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def _intent(bucket, key):
    return TaskBundleObjectIntentV1(
        id=uuid4(),
        bucket=bucket,
        object_key=key,
        content_sha256=hashlib.sha256(b"abc").hexdigest(),
        size_bytes=3,
    )


def _store(fixture):
    origin, credentials, _, admin, _ = fixture
    store = MinioObjectStore(
        endpoint_url=origin,
        access_key=credentials.access_key,
        secret_key=credentials.secret_key,
        operation_attempts=2,
        operation_timeout=2,
    )
    store._client.close()
    # Reuse the fixture's owned TLS CA configuration, never disable verification.
    store._client = admin
    return store


def _registered(intent, version):
    return RegisteredObject(
        id=uuid4(),
        authority_id=intent.id,
        environment="test",
        namespace="source-fixture",
        bucket=intent.bucket,
        object_key=intent.object_key,
        version_id=version,
        content_sha256=intent.content_sha256,
        size_bytes=intent.size_bytes,
        state="deleting",
    )


async def test_real_minio_strong_write_refuses_disabled_and_suspended_versioning(minio_tls):  # noqa: F811
    store = _store(minio_tls)
    admin = minio_tls[3]
    bucket = "source-versioning-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    intent = _intent(bucket, "bundle/file")
    with pytest.raises(ValueError, match="versioning"):
        await write_task_bundle_source_object(store, intent, b"abc")
    assert "Contents" not in admin.list_objects_v2(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    result = await write_task_bundle_source_object(store, intent, b"abc")
    assert result.version_id not in {None, "null"}
    versions = await asyncio.to_thread(S3TaskBundleVersionInventory(admin).scan, intent)
    assert versions == (result,)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    with pytest.raises(ValueError, match="versioning"):
        await write_task_bundle_source_object(store, _intent(bucket, "other/file"), b"abc")
    # Ordinary legacy writes remain supported in a suspended bucket.
    assert await store.put_object(bucket=bucket, key="legacy/file", body=b"legacy")
    response = admin.get_object(Bucket=bucket, Key="legacy/file")["Body"]
    try:
        assert response.read() == b"legacy"
    finally:
        response.close()


async def test_late_retry_version_is_recovered_after_empty_scan_without_deleting_new_publish(
    minio_tls,  # noqa: F811
):
    store = _store(minio_tls)
    admin = minio_tls[3]
    bucket = "source-late-write-" + uuid4().hex
    admin.create_bucket(Bucket=bucket)
    admin.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    old, new = _intent(bucket, "bundle/task.toml"), _intent(bucket, "bundle/task.toml")
    started, release, completed = threading.Event(), threading.Event(), threading.Event()

    class DelayedClient:
        def get_bucket_versioning(self, **kwargs):
            return admin.get_bucket_versioning(**kwargs)

        def put_object(self, **kwargs):
            started.set()
            try:
                assert release.wait(30), "fixture did not release delayed PUT"
                return admin.put_object(**kwargs)
            finally:
                completed.set()

        def close(self):
            # A closed client cannot undo a request the server already accepted.
            # Keep this fixture-owned wrapper alive until its delayed write joins.
            pass

    store._client = DelayedClient()
    store._build_client = lambda: admin
    store._operation_timeout = 0.1
    inventory, deleter = S3TaskBundleVersionInventory(admin), S3ExactObjectDeleter(admin)
    try:
        known = await write_task_bundle_source_object(store, old, b"abc")
        assert started.is_set() and not completed.is_set()
        # Only the retry's version is visible. Retire it and observe an empty scan.
        assert await asyncio.to_thread(inventory.scan, old) == (known,)
        await asyncio.to_thread(deleter.delete_exact, _registered(old, known.version_id))
        assert await asyncio.to_thread(inventory.scan, old) == ()
        replacement = await write_task_bundle_source_object(store, new, b"abc")
        assert replacement.version_id != known.version_id
        release.set()
        assert await asyncio.to_thread(completed.wait, 10)
        late = await asyncio.to_thread(inventory.scan, old)
        assert len(late) == 1 and late[0].version_id not in {
            known.version_id,
            replacement.version_id,
        }
        assert (
            admin.head_object(Bucket=bucket, Key=old.object_key)["VersionId"] == late[0].version_id
        )
        await asyncio.to_thread(deleter.delete_exact, _registered(old, late[0].version_id))
        assert await asyncio.to_thread(inventory.scan, old) == ()
        assert await asyncio.to_thread(inventory.scan, new) == (replacement,)
        assert (
            admin.head_object(Bucket=bucket, Key=new.object_key)["VersionId"]
            == replacement.version_id
        )
    finally:
        release.set()
        if started.is_set():
            assert await asyncio.to_thread(completed.wait, 10)
