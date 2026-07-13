"""Unit tests for `loom_benchmark_tool.sync_cmd.run_sync_mirror`.

The sync tool bridges any two S3-compatible endpoints. We exercise the
logic against a pair of in-memory fake S3 clients rather than moto so
the tests stay fast and don't require the moto dependency in the base
test env.

The fake client methods deliberately use PascalCase kwarg names
(`Bucket=`, `Key=`, `Body=`) because that's boto3's actual API surface
and the sync code invokes it that way. Ruff's N803 lint against
non-snake_case arguments is silenced per-method to reflect this."""

# ruff: noqa: N803

from __future__ import annotations

from typing import Any

import pytest
from botocore.exceptions import ClientError

from loom_benchmark_tool import sync_cmd


class _FakeS3:
    """Minimal S3 client stub: list_objects_v2, head_object, get_object,
    put_object. Enough for `_sync_sync` to exercise every branch."""

    def __init__(self, initial: dict[tuple[str, str], bytes] | None = None) -> None:
        # (bucket, key) -> body bytes
        self.store: dict[tuple[str, str], bytes] = dict(initial or {})
        self.puts: list[tuple[str, str, int]] = []  # (bucket, key, size)

    def get_paginator(self, _name: str) -> _FakeS3._Paginator:
        return _FakeS3._Paginator(self)

    class _Paginator:
        def __init__(self, parent: _FakeS3) -> None:
            self._parent = parent

        def paginate(self, *, Bucket: str, Prefix: str = "") -> list[dict[str, Any]]:
            contents = []
            for (bucket, key), body in sorted(self._parent.store.items()):
                if bucket != Bucket:
                    continue
                if Prefix and not key.startswith(Prefix):
                    continue
                contents.append({"Key": key, "Size": len(body)})
            return [{"Contents": contents}]

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        body = self.store.get((Bucket, Key))
        if body is None:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject",
            )
        return {"ContentLength": len(body)}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        body = self.store[(Bucket, Key)]

        class _Body:
            def __init__(self, buf: bytes) -> None:
                self._buf = buf

            def read(self) -> bytes:
                return self._buf

        return {"Body": _Body(body)}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.store[(Bucket, Key)] = Body
        self.puts.append((Bucket, Key, len(Body)))


@pytest.fixture()
def patched_build_client(monkeypatch: pytest.MonkeyPatch) -> dict[str, _FakeS3]:
    """Replace `_build_client` with a factory that returns pre-seeded
    fake clients keyed by endpoint URL. Tests populate the source fake
    and observe the destination fake."""
    clients: dict[str, _FakeS3] = {}

    def _factory(*, endpoint_url: str, access_key: str, secret_key: str) -> _FakeS3:
        return clients.setdefault(endpoint_url, _FakeS3())

    monkeypatch.setattr(sync_cmd, "_build_client", _factory)
    return clients


@pytest.mark.asyncio
async def test_sync_copies_missing_objects(
    patched_build_client: dict[str, _FakeS3],
) -> None:
    """Objects present in source, absent in dest → all copied."""
    source = _FakeS3({
        ("src-bucket", "aime-25/rev-abc/manifest.json"): b'{"benchmark_id":"aime-25"}',
        ("src-bucket", "aime-25/rev-abc/task-01/task.toml"): b"[task]\nid='aime-25/1'\n",
    })
    patched_build_client["http://source.local:9000"] = source
    patched_build_client["https://dest.r2:443"] = _FakeS3()

    stats = await sync_cmd.run_sync_mirror(
        source_endpoint="http://source.local:9000",
        source_access_key="s-a",
        source_secret_key="s-s",
        source_bucket="src-bucket",
        dest_endpoint="https://dest.r2:443",
        dest_access_key="d-a",
        dest_secret_key="d-s",
        dest_bucket="dst-bucket",
    )

    assert stats.listed == 2
    assert stats.uploaded == 2
    assert stats.skipped_size_match == 0
    dest = patched_build_client["https://dest.r2:443"]
    assert ("dst-bucket", "aime-25/rev-abc/manifest.json") in dest.store
    assert ("dst-bucket", "aime-25/rev-abc/task-01/task.toml") in dest.store


@pytest.mark.asyncio
async def test_sync_skips_size_matched_objects(
    patched_build_client: dict[str, _FakeS3],
) -> None:
    """Objects already at dest with matching size → skipped, no PUT."""
    body = b'{"benchmark_id":"aime-25"}'
    patched_build_client["http://source.local:9000"] = _FakeS3({
        ("src-bucket", "aime-25/rev-abc/manifest.json"): body,
    })
    patched_build_client["https://dest.r2:443"] = _FakeS3({
        ("dst-bucket", "aime-25/rev-abc/manifest.json"): body,
    })

    stats = await sync_cmd.run_sync_mirror(
        source_endpoint="http://source.local:9000",
        source_access_key="s-a",
        source_secret_key="s-s",
        source_bucket="src-bucket",
        dest_endpoint="https://dest.r2:443",
        dest_access_key="d-a",
        dest_secret_key="d-s",
        dest_bucket="dst-bucket",
    )

    assert stats.listed == 1
    assert stats.uploaded == 0
    assert stats.skipped_size_match == 1
    dest = patched_build_client["https://dest.r2:443"]
    assert dest.puts == []


@pytest.mark.asyncio
async def test_sync_uploads_size_mismatched_objects(
    patched_build_client: dict[str, _FakeS3],
) -> None:
    """Dest has an object with a *different* size — treat as stale
    and re-copy."""
    patched_build_client["http://source.local:9000"] = _FakeS3({
        ("src-bucket", "aime-25/rev-abc/manifest.json"): b'{"v":"new"}',
    })
    patched_build_client["https://dest.r2:443"] = _FakeS3({
        ("dst-bucket", "aime-25/rev-abc/manifest.json"): b'{"v":"stale"}' * 5,
    })

    stats = await sync_cmd.run_sync_mirror(
        source_endpoint="http://source.local:9000",
        source_access_key="s-a",
        source_secret_key="s-s",
        source_bucket="src-bucket",
        dest_endpoint="https://dest.r2:443",
        dest_access_key="d-a",
        dest_secret_key="d-s",
        dest_bucket="dst-bucket",
    )

    assert stats.uploaded == 1
    dest = patched_build_client["https://dest.r2:443"]
    assert dest.store[("dst-bucket", "aime-25/rev-abc/manifest.json")] == b'{"v":"new"}'


@pytest.mark.asyncio
async def test_sync_prefix_scopes_the_walk(
    patched_build_client: dict[str, _FakeS3],
) -> None:
    """`--prefix aime-25/` restricts the walk to that benchmark's keys."""
    patched_build_client["http://source.local:9000"] = _FakeS3({
        ("src-bucket", "aime-25/rev-abc/x.txt"): b"a",
        ("src-bucket", "mmlu-pro/rev-def/x.txt"): b"b",
    })
    patched_build_client["https://dest.r2:443"] = _FakeS3()

    stats = await sync_cmd.run_sync_mirror(
        source_endpoint="http://source.local:9000",
        source_access_key="s-a",
        source_secret_key="s-s",
        source_bucket="src-bucket",
        dest_endpoint="https://dest.r2:443",
        dest_access_key="d-a",
        dest_secret_key="d-s",
        dest_bucket="dst-bucket",
        prefix="aime-25/",
    )

    assert stats.listed == 1
    assert stats.uploaded == 1
    dest = patched_build_client["https://dest.r2:443"]
    assert list(dest.store) == [("dst-bucket", "aime-25/rev-abc/x.txt")]


@pytest.mark.asyncio
async def test_sync_dry_run_counts_but_does_not_copy(
    patched_build_client: dict[str, _FakeS3],
) -> None:
    """`dry_run=True` reports what WOULD be uploaded but issues no PUTs."""
    patched_build_client["http://source.local:9000"] = _FakeS3({
        ("src-bucket", "aime-25/rev-abc/x.txt"): b"payload",
    })
    dest = _FakeS3()
    patched_build_client["https://dest.r2:443"] = dest

    stats = await sync_cmd.run_sync_mirror(
        source_endpoint="http://source.local:9000",
        source_access_key="s-a",
        source_secret_key="s-s",
        source_bucket="src-bucket",
        dest_endpoint="https://dest.r2:443",
        dest_access_key="d-a",
        dest_secret_key="d-s",
        dest_bucket="dst-bucket",
        dry_run=True,
    )

    assert stats.uploaded == 1  # would-upload count still tallied
    assert stats.bytes_uploaded == len("payload")
    assert dest.puts == []
    assert dest.store == {}


def test_dest_size_returns_none_on_404() -> None:
    """The 'missing' branch in `_dest_size` is exercised via the sync
    integration tests above, but pin the shape of the ClientError
    handling directly too so future refactors don't silently regress
    it into raising."""

    class _MissingS3:
        def head_object(self, **_kwargs: Any) -> None:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject",
            )

    assert sync_cmd._dest_size(client=_MissingS3(), bucket="b", key="k") is None
