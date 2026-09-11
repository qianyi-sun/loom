"""Bounded source reads cannot bypass a live pre/post IO fence."""

import asyncio
from datetime import UTC, datetime, timedelta
from importlib import import_module
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest


def reader_input(monkeypatch, *, data=b"source archive", offset=0, length=6):
    from loom_capacity_agent.build_admission import BuildClaimRequestV1
    from tests.unit.test_capacity_build_admission_client import native_registration

    module = import_module("loom_capacity_build_guard.source_reader")
    worker = native_registration()
    claim = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
    from loom_capacity_build_guard.source_access import BuildClaimSourceV1
    from loom_capacity_manager.contracts import canonical_digest

    source = BuildClaimSourceV1(claim=claim, claim_digest=canonical_digest(claim), object_bucket="source",
        object_key="owner/candidate/source.tar", archive_sha256="a" * 64, archive_size_bytes=len(data),
        source_binding_sha256="b" * 64, lease_not_after=datetime.now(UTC) + timedelta(minutes=1))
    body = BytesIO(data[offset:offset + length])
    result = {"Body": body, "ContentLength": len(body.getvalue()),
        "ContentRange": f"bytes {offset}-{offset + len(body.getvalue()) - 1}/{len(data)}"}
    calls = []

    def get_object(**kwargs):
        calls.append(kwargs)
        return result

    reader = module.BuildSourceReader(session_factory=object(), object_store=SimpleNamespace(get_object=get_object))
    authorize = AsyncMock(return_value=source)
    monkeypatch.setattr(reader, "_authorize", authorize)
    return reader, claim, source, authorize, result, calls


@pytest.mark.parametrize("boundary", ["exact", "length", "range", "oversize", "short", "before", "after", "changed", "renewed"])
async def test_source_range_checks_metadata_and_fences_before_return(monkeypatch, boundary):
    reader, claim, source, authorize, result, calls = reader_input(monkeypatch)
    if boundary == "length":
        result["ContentLength"] += 1
    elif boundary == "range":
        result["ContentRange"] = "bytes 0-5/999"
    elif boundary == "oversize":
        result["Body"] = BytesIO(b"toolong")
    elif boundary == "short":
        result["Body"] = BytesIO(b"short")
    elif boundary in {"before", "after"}:
        authorize.side_effect = [ValueError("revoked")] if boundary == "before" else [source, ValueError("revoked")]
    elif boundary in {"changed", "renewed"}:
        changes = {"object_key": "foreign/source.tar"} if boundary == "changed" else {"lease_not_after": source.lease_not_after + timedelta(minutes=1)}
        authorize.side_effect = [source, source.model_copy(update=changes)]
    if boundary in {"exact", "renewed"}:
        chunk = await reader.read(claim, worker_credential="x" * 43, offset=0, length=6)
        assert chunk.data == b"source"
        assert chunk.source.object_key == source.object_key
        assert authorize.await_count == 2
    else:
        with pytest.raises(ValueError):
            await reader.read(claim, worker_credential="x" * 43, offset=0, length=6)
    if boundary == "before":
        assert calls == []
    else:
        assert calls == [{"Bucket": "source", "Key": "owner/candidate/source.tar", "Range": "bytes=0-5"}]
        assert result["Body"].closed


@pytest.mark.parametrize("offset,length", [(True, 1), (-1, 1), (0, True), (0, 0), (0, 1048577), (14, 1)])
async def test_invalid_range_never_reads_object(monkeypatch, offset, length):
    reader, claim, _source, _authorize, _result, calls = reader_input(monkeypatch)
    with pytest.raises(ValueError):
        await reader.read(claim, worker_credential="x" * 43, offset=offset, length=length)
    assert calls == []


async def test_final_range_is_clamped_to_exact_archive_size(monkeypatch):
    reader, claim, _source, _authorize, _result, calls = reader_input(monkeypatch, offset=10, length=100)
    chunk = await reader.read(claim, worker_credential="x" * 43, offset=10, length=100)
    assert chunk.data == b"hive"
    assert calls[0]["Range"] == "bytes=10-13"


async def test_cancelled_read_keeps_thread_capacity_until_body_closed(monkeypatch):
    from threading import Event

    reader, claim, _source, _authorize, result, _calls = reader_input(monkeypatch)
    started, finish = Event(), Event()
    old_read = result["Body"].read

    def slow_read(size):
        started.set()
        assert finish.wait(5)
        return old_read(size)

    monkeypatch.setattr(result["Body"], "read", slow_read)
    reader._slots = asyncio.Semaphore(1)
    task = asyncio.create_task(reader.read(claim, worker_credential="x" * 43, offset=0, length=6))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert reader._slots.locked()
    finally:
        finish.set()
    async with asyncio.timeout(5):
        await reader._slots.acquire()
    assert result["Body"].closed


@pytest.mark.parametrize("cancelled", [False, True])
async def test_reader_shutdown_drains_io_and_rejects_new_reads(monkeypatch, cancelled):
    from threading import Event

    reader, claim, _source, _authorize, result, _calls = reader_input(monkeypatch)
    started, finish = Event(), Event()
    old_read = result["Body"].read

    def slow_read(size):
        started.set()
        assert finish.wait(5)
        return old_read(size)

    monkeypatch.setattr(result["Body"], "read", slow_read)
    task = asyncio.create_task(reader.read(claim, worker_credential="x" * 43, offset=0, length=6))
    assert await asyncio.to_thread(started.wait, 5)
    if cancelled:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    close = asyncio.create_task(reader.aclose())
    try:
        # Observe the close transition, not an arbitrary wall-clock delay.
        async with asyncio.timeout(5):
            while not reader._closed:
                await asyncio.sleep(0)
        assert not close.done()
        with pytest.raises(ValueError, match="closed"):
            await reader.read(claim, worker_credential="x" * 43, offset=0, length=6)
    finally:
        finish.set()
    if not cancelled:
        await task
    await close
    assert result["Body"].closed
    await reader.aclose()
