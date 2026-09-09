"""Whole-inventory budgets and explicit native MinIO authority contracts."""

import asyncio
import importlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlsplit
from xml.sax.saxutils import escape

import pytest

from loom_task_image_authority.bundle_s3_signing import S3SigningCredentials

NOW = datetime(2026, 9, 9, 12, tzinfo=UTC)


def _module():
    return importlib.import_module("loom_task_image_authority.bundle_s3_backend")


def _page(query, *, keys=("revision/a space+%2F",), size=7, next_token=None):
    prefix, maximum = query["prefix"][0], query["max-keys"][0]
    fields = ""
    if "continuation-token" in query:
        fields += "<ContinuationToken>" + escape(query["continuation-token"][0]) + "</ContinuationToken>"
    if next_token is not None:
        fields += "<NextContinuationToken>" + escape(next_token) + "</NextContinuationToken>"
    entries = "".join(f"<Contents><Key>{quote_plus(key)}</Key><Size>{size}</Size></Contents>" for key in keys)
    return (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>loom-bundles</Name><Prefix>{quote_plus(prefix)}</Prefix><MaxKeys>{maximum}</MaxKeys><KeyCount>{len(keys)}</KeyCount><EncodingType>url</EncodingType><IsTruncated>{str(next_token is not None).lower()}</IsTruncated>{entries}{fields}</ListBucketResult>').encode()


class _Reader:
    def __init__(self):
        self.requests = []
        self.pages = [lambda q: _page(q)]
        self.on_fetch = lambda: None
        self.closed = False

    async def fetch(self, url, *, deadline):
        self.requests.append((url, deadline))
        self.on_fetch()
        return self.pages[len(self.requests) - 1](parse_qs(urlsplit(url).query))

    async def aclose(self):
        self.closed = True


def _backend(monkeypatch, *, reader=None, clock=lambda: NOW, **changes):
    module = _module()
    reader = reader or _Reader()
    monkeypatch.setattr(module, "HTTPSBundleListingReader", lambda **kwargs: reader)
    options = dict(
        origin="https://objects.example:9443", bucket="loom-bundles", region="us-east-1",
        credentials=S3SigningCredentials(access_key="fixture-access", secret_key="private-secret"),
        ca_file=Path("/fixture/ca.pem"), clock=clock,
    )
    options.update(changes)
    return module.MinioTaskImageBundleBackend(**options), reader


async def _list(backend, **changes):
    options = dict(bucket="loom-bundles", prefix="revision/", maximum_objects=2000, maximum_bytes=1000, expires_at=NOW + timedelta(seconds=60))
    options.update(changes)
    return await backend.list_objects(**options)


async def test_lists_exact_inventory_with_fixed_deadline_and_owned_close(monkeypatch):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="page-2"), lambda q: _page(q, keys=("revision/z",))]
    backend, _ = _backend(monkeypatch, reader=reader)
    async with backend:
        objects = await _list(backend)
        assert [(obj.key, obj.size_bytes) for obj in objects] == [("revision/a space+%2F", 7), ("revision/z", 7)]
        get = backend.presign_get(bucket="loom-bundles", key=objects[0].key, expires_at=NOW + timedelta(seconds=60))
        assert urlsplit(get).path == "/loom-bundles/revision/a%20space%2B%252F"
    assert reader.closed
    assert reader.requests[0][1] == reader.requests[1][1]
    assert parse_qs(urlsplit(reader.requests[1][0]).query)["continuation-token"] == ["page-2"]
    for url, _ in reader.requests:
        query = parse_qs(urlsplit(url).query)
        assert query["X-Amz-Date"] == ["20260909T120000Z"]
        assert query["X-Amz-Expires"] == ["60"]


@pytest.mark.parametrize("second_keys", [("revision/a space+%2F",), ("revision/0",)])
async def test_rejects_duplicate_or_backward_keys_across_pages(monkeypatch, second_keys):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=second_keys)]
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _list(backend)


async def test_rejects_continuation_cycle_across_multiple_pages(monkeypatch):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=("revision/b",), next_token="p3"), lambda q: _page(q, keys=("revision/c",), next_token="p2")]
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _list(backend)
    assert len(reader.requests) == 3


@pytest.mark.parametrize("limits", [{"maximum_pages": 1}, {"maximum_listing_bytes": 500}])
async def test_bounds_whole_listing_pages_and_xml_bytes(monkeypatch, limits):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=("revision/b",))]
    backend, _ = _backend(monkeypatch, reader=reader, limits=_module().S3InventoryLimits(**limits))
    with pytest.raises(RuntimeError):
        await _list(backend)
    assert len(reader.requests) <= 2


@pytest.mark.parametrize("changes", [{"maximum_objects": 1}, {"maximum_bytes": 10}])
async def test_bounds_accumulated_objects_and_object_bytes(monkeypatch, changes):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=("revision/b",))]
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _list(backend, **changes)


@pytest.mark.parametrize("advanced", [NOW - timedelta(seconds=1), NOW + timedelta(seconds=60), NOW.replace(tzinfo=None)])
async def test_rejects_clock_regression_or_expiry_after_network(monkeypatch, advanced):
    observed = [NOW]
    reader = _Reader()
    reader.on_fetch = lambda: observed.__setitem__(0, advanced)
    backend, _ = _backend(monkeypatch, reader=reader, clock=lambda: observed[0])
    with pytest.raises(RuntimeError):
        await _list(backend)


async def test_expired_authority_never_starts_network(monkeypatch):
    backend, reader = _backend(monkeypatch)
    with pytest.raises(RuntimeError):
        await _list(backend, expires_at=NOW)
    assert not reader.requests


@pytest.mark.parametrize("changes", [{"bucket": "foreign"}, {"maximum_objects": True}, {"maximum_objects": 2001}, {"maximum_bytes": -1}, {"maximum_bytes": 536870913}, {"prefix": "../"}])
async def test_invalid_scope_and_limits_fail_before_network(monkeypatch, changes):
    backend, reader = _backend(monkeypatch)
    with pytest.raises(RuntimeError):
        await _list(backend, **changes)
    assert not reader.requests


async def test_total_timeout_and_cancellation_remain_cancellable(monkeypatch):
    entered = asyncio.Event()

    class WaitingReader(_Reader):
        async def fetch(self, url, *, deadline):
            entered.set()
            await asyncio.Future()

    backend, _ = _backend(monkeypatch, reader=WaitingReader(), limits=_module().S3InventoryLimits(total_timeout_seconds=0.05))
    with pytest.raises(RuntimeError):
        await _list(backend)
    assert entered.is_set()
    task = asyncio.create_task(_list(backend))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_production_backend_refuses_temporary_or_ambient_identity(monkeypatch):
    with pytest.raises(RuntimeError):
        _backend(monkeypatch, credentials=None)
    with pytest.raises(RuntimeError):
        _backend(monkeypatch, credentials=S3SigningCredentials(access_key="a", secret_key="b", session_token="token", expires_at=NOW + timedelta(seconds=60)))
