import time
from types import SimpleNamespace

import botocore.handlers
import pytest

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
