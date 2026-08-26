import time
from types import SimpleNamespace

import botocore.handlers
import pytest
from botocore.exceptions import ClientError

from loom.trajectory.storage import MinioObjectStore, _remove_expect_header


def test_minio_object_store_uses_import_safe_s3_client_defaults() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )

    config = store._client.meta.config

    assert config.max_pool_connections >= 64
    assert config.tcp_keepalive is True
    assert config.connect_timeout <= 10
    assert config.read_timeout <= 30


def test_remove_expect_header_drops_botocore_continue_handshake() -> None:
    params = {"headers": {"Expect": "100-continue", "Content-Type": "text/plain"}}

    _remove_expect_header(model=object(), params=params)

    assert params == {"headers": {"Content-Type": "text/plain"}}


class _FakeEvents:
    def __init__(self) -> None:
        self.unregistered: list[tuple[str, object]] = []
        self.registered_last: list[tuple[str, object]] = []

    def unregister(self, event_name: str, handler: object) -> None:
        self.unregistered.append((event_name, handler))

    def register_last(self, event_name: str, handler: object) -> None:
        self.registered_last.append((event_name, handler))


def test_minio_client_disables_botocore_expect_continue_handler() -> None:
    events = _FakeEvents()
    client = SimpleNamespace(meta=SimpleNamespace(events=events))

    MinioObjectStore._configure_client_events(client)

    assert (
        "before-call.s3",
        botocore.handlers.add_expect_header,
    ) in events.unregistered
    assert ("before-call.s3", _remove_expect_header) in events.registered_last


class _SlowPutClient:
    def __init__(self) -> None:
        self.closed = False
        self.put_calls = 0

    def put_object(self, **_kwargs: object) -> None:
        self.put_calls += 1
        time.sleep(0.2)

    def close(self) -> None:
        self.closed = True


class _FastPutClient:
    def __init__(self) -> None:
        self.put_calls = 0

    def put_object(self, **_kwargs: object) -> None:
        self.put_calls += 1


class _VersionedWriteClient:
    def put_object(self, **_kwargs: object) -> dict[str, str]:
        return {"VersionId": "put-version-123"}

    def complete_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
        return {"VersionId": "multipart-version-456"}


class _UnversionedWriteClient:
    def put_object(self, **_kwargs: object) -> dict[str, str]:
        return {}

    def complete_multipart_upload(self, **_kwargs: object) -> dict[str, str]:
        return {}


class _MalformedVersionWriteClient:
    def __init__(self, version_id: object) -> None:
        self.version_id = version_id

    def put_object(self, **_kwargs: object) -> dict[str, object]:
        return {"VersionId": self.version_id}


@pytest.mark.asyncio
async def test_minio_write_metadata_preserves_returned_object_versions() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store._client = _VersionedWriteClient()

    put_result = await store.put_object_with_metadata(
        bucket="artifacts",
        key="team/trial/result.txt",
        body=b"result",
    )
    upload = SimpleNamespace(
        bucket="trajectories",
        key="team/trial/events.jsonl",
        upload_id="upload-1",
        parts=[(1, "etag-1")],
    )
    multipart_result = await store.complete_multipart_upload_with_metadata(upload)

    assert put_result.uri == "s3://artifacts/team/trial/result.txt"
    assert put_result.version_id == "put-version-123"
    assert multipart_result.uri == "s3://trajectories/team/trial/events.jsonl"
    assert multipart_result.version_id == "multipart-version-456"


@pytest.mark.asyncio
async def test_minio_write_metadata_preserves_unversioned_response() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store._client = _UnversionedWriteClient()

    put_result = await store.put_object_with_metadata(
        bucket="artifacts",
        key="team/trial/result.txt",
        body=b"result",
    )
    upload = SimpleNamespace(
        bucket="trajectories",
        key="team/trial/events.jsonl",
        upload_id="upload-1",
        parts=[(1, "etag-1")],
    )
    multipart_result = await store.complete_multipart_upload_with_metadata(upload)

    assert put_result.version_id is None
    assert multipart_result.version_id is None


@pytest.mark.parametrize("version_id", [None, "", " surrounding ", 7, False, {}])
@pytest.mark.asyncio
async def test_minio_write_metadata_rejects_malformed_version_response(
    version_id: object,
) -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    store._client = _MalformedVersionWriteClient(version_id)

    with pytest.raises(ValueError, match="malformed VersionId"):
        await store.put_object_with_metadata(
            bucket="artifacts",
            key="team/trial/result.txt",
            body=b"result",
        )


@pytest.mark.asyncio
async def test_put_object_reconnects_and_retries_once_after_client_timeout() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        operation_timeout=0.01,
    )
    slow_client = _SlowPutClient()
    fast_client = _FastPutClient()
    store._client = slow_client
    store._build_client = lambda: fast_client  # type: ignore[method-assign]

    uri = await store.put_object(bucket="benchmarks", key="task/file.txt", body=b"x")

    assert uri == "s3://benchmarks/task/file.txt"
    assert slow_client.put_calls == 1
    assert slow_client.closed is True
    assert fast_client.put_calls == 1


class _BucketClient:
    def __init__(self, *, head_code: str | None = None) -> None:
        self.head_code = head_code
        self.head_calls: list[str] = []
        self.create_calls: list[str] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        self.head_calls.append(Bucket)
        if self.head_code is not None:
            raise ClientError(
                {"Error": {"Code": self.head_code}},
                "HeadBucket",
            )

    def create_bucket(self, *, Bucket: str) -> None:  # noqa: N803
        self.create_calls.append(Bucket)


@pytest.mark.asyncio
async def test_ensure_bucket_noops_when_bucket_exists() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    client = _BucketClient()
    store._client = client

    await store.ensure_bucket("trajectories")

    assert client.head_calls == ["trajectories"]
    assert client.create_calls == []


@pytest.mark.asyncio
async def test_ensure_bucket_creates_missing_bucket() -> None:
    store = MinioObjectStore(
        endpoint_url="http://127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    client = _BucketClient(head_code="NoSuchBucket")
    store._client = client

    await store.ensure_bucket("trajectories")

    assert client.head_calls == ["trajectories"]
    assert client.create_calls == ["trajectories"]
