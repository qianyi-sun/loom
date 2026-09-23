"""Bound management input before parsing, without changing response streaming."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest
from fastapi import Request
from starlette.responses import StreamingResponse

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _app(**overrides):
    settings = LoomServiceSettings(**{
        "_env_file": None, "service_mode": "management", "db_url": "postgresql+psycopg://u:p@localhost/db",
        "management_http_max_body_bytes": 16, "management_http_max_inflight": 1,
        "management_http_body_timeout_sec": 0.1, **overrides,
    })
    app = create_app(settings)

    @app.post("/probe")
    async def probe(request: Request):
        return {"body": (await request.body()).decode()}

    return app


def _scope(headers=(), *, path="/probe"):
    return {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "POST", "scheme": "https", "path": path, "raw_path": path.encode(),
            "query_string": b"", "root_path": "", "headers": list(headers),
            "server": ("manage.example.com", 443), "client": ("127.0.0.1", 1234)}


async def _call(app, chunks=(b"ok",), *, headers=()):
    messages = deque({"type": "http.request", "body": chunk, "more_body": i < len(chunks) - 1}
                     for i, chunk in enumerate(chunks))
    sent = []
    reads = []

    async def receive():
        if messages:
            message = messages.popleft()
            reads.append(message)
            return message
        await asyncio.Future()

    async def send(message):
        sent.append(message)

    await app(_scope(headers), receive, send)
    return sent, reads


def _status(messages):
    return next(m["status"] for m in messages if m["type"] == "http.response.start")


@pytest.mark.parametrize("headers,chunks,status,read_count", [
    ([(b"content-length", b"17")], (b"x" * 17,), 413, 0),
    ([], (b"x" * 9, b"y" * 8), 413, 2),
    ([(b"content-length", b"1")], (b"x" * 17,), 413, 1),
    ([(b"content-length", b"no")], (b"x",), 400, 0),
    ([(b"content-length", b"-1")], (b"x",), 400, 0),
    ([(b"content-length", b"1"), (b"content-length", b"2")], (b"x",), 400, 0),
    ([(b"content-length", b"4")], (b"x",), 400, 1),
    ([(b"content-encoding", b"gzip")], (b"x",), 415, 0),
])
async def test_invalid_management_bodies_never_reach_handler(headers, chunks, status, read_count):
    sent, reads = await _call(_app(), chunks, headers=headers)
    assert _status(sent) == status
    assert len(reads) == read_count
    assert (b"cache-control", b"no-store") in sent[0]["headers"]
    assert (b"connection", b"close") in sent[0]["headers"]


async def test_limit_is_inclusive_and_replays_all_bytes_once():
    sent, _ = await _call(_app(), (b"x" * 8, b"y" * 8), headers=[(b"content-length", b"16")])
    assert _status(sent) == 200
    assert b"xxxxxxxxyyyyyyyy" in b"".join(m.get("body", b"") for m in sent)


async def test_slow_body_times_out_and_releases_admission():
    app = _app()
    sent = []

    async def receive():
        await asyncio.Future()

    async def send(message):
        sent.append(message)

    await asyncio.wait_for(app(_scope(), receive, send), timeout=1)
    assert _status(sent) == 408
    assert _status((await _call(app))[0]) == 200


async def test_full_admission_rejects_without_reading_and_cancel_releases_slot():
    app = _app()
    receiving = asyncio.Event()

    async def receive():
        receiving.set()
        await asyncio.Future()

    async def send(message):
        pass

    pending = asyncio.create_task(app(_scope(), receive, send))
    try:
        await receiving.wait()
        sent, reads = await _call(app)
        assert _status(sent) == 503
        assert reads == []
    finally:
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    assert _status((await _call(app))[0]) == 200


async def test_disconnect_does_not_call_handler_or_leak_slot():
    app = _app()
    sent = []

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    await app(_scope(), receive, send)
    assert sent == []
    assert _status((await _call(app))[0]) == 200


async def test_response_streams_while_admission_remains_bounded():
    app = _app()
    first_byte = asyncio.Event()
    finish = asyncio.Event()

    @app.post("/stream")
    async def stream():
        async def chunks():
            yield b"first"
            await finish.wait()
            yield b"second"
        return StreamingResponse(chunks())

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        if message.get("body") == b"first":
            first_byte.set()

    task = asyncio.create_task(app(_scope(path="/stream"), receive, send))
    try:
        await asyncio.wait_for(first_byte.wait(), timeout=1)
        assert _status((await _call(app))[0]) == 503
    finally:
        finish.set()
        await task
    assert _status((await _call(app))[0]) == 200


async def test_handler_failure_releases_admission():
    app = _app()

    @app.post("/broken")
    async def broken():
        raise RuntimeError("handler failed")

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        pass

    with pytest.raises(RuntimeError, match="handler failed"):
        await app(_scope(path="/broken"), receive, send)
    assert _status((await _call(app))[0]) == 200


async def test_application_upload_behavior_is_unchanged():
    app = _app(service_mode="application", minio_access_key="test", minio_secret_key="test")
    sent, _ = await _call(app, (b"x" * 32,))
    assert _status(sent) == 200


@pytest.mark.parametrize("field,value", [
    ("management_http_max_body_bytes", 0), ("management_http_max_inflight", -1),
    ("management_http_body_timeout_sec", 0), ("management_http_body_timeout_sec", float("inf")),
    ("management_http_body_timeout_sec", float("nan")),
])
def test_invalid_limits_fail_configuration(field, value):
    with pytest.raises(ValueError, match="management HTTP"):
        LoomServiceSettings(_env_file=None, service_mode="management",
                            db_url="postgresql+psycopg://u:p@localhost/db", **{field: value})
