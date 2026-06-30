from __future__ import annotations

from botocore.exceptions import ClientError, ConnectionClosedError

from loom.trajectory.storage import MinioObjectStore


class _FakeEvents:
    def unregister(self, *_args: object, **_kwargs: object) -> None:
        return None

    def register_last(self, *_args: object, **_kwargs: object) -> None:
        return None


class _FakeMeta:
    events = _FakeEvents()


class _FakeS3Client:
    meta = _FakeMeta()

    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _store_with_clients(monkeypatch, clients: list[_FakeS3Client]) -> MinioObjectStore:  # type: ignore[no-untyped-def]
    created: list[_FakeS3Client] = []

    def fake_boto3_client(**_kwargs: object) -> _FakeS3Client:
        client = clients[len(created)]
        created.append(client)
        return client

    monkeypatch.setattr("loom.trajectory.storage.boto3.client", fake_boto3_client)
    store = MinioObjectStore(
        endpoint_url="http://minio:9000",
        access_key="ak",
        secret_key="sk",
        operation_attempts=2,
        operation_timeout=1,
    )
    assert created == [clients[0]]
    return store


async def test_run_client_call_retries_connection_closed_with_fresh_client(
    monkeypatch,
) -> None:
    clients = [_FakeS3Client("first"), _FakeS3Client("second")]
    store = _store_with_clients(monkeypatch, clients)
    attempts: list[str] = []

    def call(client: _FakeS3Client) -> str:
        attempts.append(client.name)
        if client.name == "first":
            raise ConnectionClosedError(endpoint_url="http://minio:9000")
        return "ok"

    result = await store._run_client_call("put_object", call)

    assert result == "ok"
    assert attempts == ["first", "second"]
    assert clients[0].closed is True


async def test_run_client_call_retries_transient_s3_500(monkeypatch) -> None:
    clients = [_FakeS3Client("first"), _FakeS3Client("second")]
    store = _store_with_clients(monkeypatch, clients)
    attempts: list[str] = []

    def call(client: _FakeS3Client) -> str:
        attempts.append(client.name)
        if client.name == "first":
            raise ClientError(
                {
                    "Error": {"Code": "InternalError", "Message": "try later"},
                    "ResponseMetadata": {"HTTPStatusCode": 500},
                },
                "PutObject",
            )
        return "ok"

    result = await store._run_client_call("put_object", call)

    assert result == "ok"
    assert attempts == ["first", "second"]
    assert clients[0].closed is True


async def test_run_client_call_does_not_retry_permanent_s3_403(monkeypatch) -> None:
    clients = [_FakeS3Client("first"), _FakeS3Client("second")]
    store = _store_with_clients(monkeypatch, clients)

    def call(_client: _FakeS3Client) -> str:
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "denied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "PutObject",
        )

    try:
        await store._run_client_call("put_object", call)
    except ClientError:
        pass
    else:
        raise AssertionError("expected permanent ClientError")

    assert clients[0].closed is False
    assert clients[1].closed is False
