"""The one-use start transport must never retry or accept an ambiguous reply."""

import asyncio
import importlib
from types import SimpleNamespace

import httpx
import pytest
import rfc8785

from loom_worker.control_plane_client import HttpControlPlaneClient
from tests.unit.test_worker_task_image_execution import accepting, consumer, evidence


@pytest.mark.parametrize("case", ["valid", "http", "origin", "redirect", "oversized", "duplicate", "wrong-status"])
async def test_refresh_requires_bounded_canonical_delivery_from_fixed_https_origin(tmp_path, case):
    import json

    from loom_task_image_authority.execution_refresh import (
        MAX_EXECUTION_DELIVERY_BYTES,
        ExecutionRefreshRequest,
    )
    from loom_task_image_authority.execution_start import ExecutionStartRequest
    from tests.unit.test_main_loop_trusted_task_images import delivery

    payload, kwargs = evidence(tmp_path)
    seen = []
    subject = consumer(tmp_path, payload, kwargs, accepting())
    await subject.authorize()
    previous = subject.consume.call_args.args[0]
    request = ExecutionRefreshRequest.model_validate(dict(schema="loom.task-image-execution-refresh-request/v1", previous=previous))
    wire = rfc8785.dumps(delivery(kwargs))
    if case == "oversized":
        wire = b"x" * (MAX_EXECUTION_DELIVERY_BYTES + 1)
    elif case == "duplicate":
        wire = wire[:-1] + b',"schema":"loom.task-image-execution-delivery/v2"}'

    def handle(sent):
        seen.append(sent)
        assert ExecutionStartRequest.model_validate(json.loads(sent.content)["previous"]) == previous
        return httpx.Response(307 if case == "redirect" else 201 if case == "wrong-status" else 200, content=wire,
                              headers={"location": "https://different.example"} if case == "redirect" else {})

    origin = "http://cp.example" if case == "http" else "https://cp.example"
    async with httpx.AsyncClient(base_url="https://different.example" if case == "origin" else origin,
                                transport=httpx.MockTransport(handle)) as http:
        client = HttpControlPlaneClient(origin, "worker-token", _client=http)
        if case == "valid":
            response = await client.refresh_task_image_execution(request)
            assert response.grant_envelope.encode() == kwargs["wire"]
        else:
            with pytest.raises((ValueError, httpx.HTTPError)):
                await client.refresh_task_image_execution(request)
    assert len(seen) == (0 if case in {"http", "origin"} else 1)
    if seen:
        assert seen[0].url.path.endswith("/task-image/refresh")
        assert seen[0].headers["authorization"] == "Bearer worker-token"


@pytest.mark.parametrize("status", [200, 201, 204, 307, 409, 503])
async def test_start_response_requires_exact_created_receipt(tmp_path, status):
    payload, kwargs = evidence(tmp_path)
    seen = []
    accept = accepting()

    async def handle(request):
        seen.append(request)
        m = importlib.import_module("loom_task_image_authority.execution_start")
        parsed = m.ExecutionStartRequest.model_validate_json(request.content)
        receipt = await accept(parsed)
        return httpx.Response(status, content=rfc8785.dumps(
            receipt.model_dump(mode="json", by_alias=True, exclude_none=True),
        ))

    async with httpx.AsyncClient(
        base_url="https://cp.example", transport=httpx.MockTransport(handle),
    ) as http:
        client = HttpControlPlaneClient("https://cp.example", "worker-token", _client=http)
        assert hasattr(client, "consume_task_image_execution_start"), "online start transport missing"
        subject = consumer(tmp_path, payload, kwargs, client.consume_task_image_execution_start)
        if status == 201:
            assert await subject.authorize() is True
        else:
            with pytest.raises((httpx.HTTPError, ValueError)):
                await subject.authorize()
        with pytest.raises(RuntimeError, match="already attempted"):
            await subject.authorize()
    assert len(seen) == 1
    assert seen[0].url.path == f"/trials/{payload['claim']['trial_id']}/task-image/start"
    assert seen[0].headers["authorization"] == "Bearer worker-token"


@pytest.mark.parametrize(
    "reply", [b"{}", b"x" * 8193, b'{"schema":null,"schema":null}'],
    ids=["empty", "oversized", "duplicate"],
)
async def test_malformed_or_oversized_receipt_fails_closed(tmp_path, reply):
    payload, kwargs = evidence(tmp_path)
    async with httpx.AsyncClient(
        base_url="https://cp.example",
        transport=httpx.MockTransport(lambda _: httpx.Response(201, content=reply)),
    ) as http:
        client = HttpControlPlaneClient("https://cp.example", "worker-token", _client=http)
        assert hasattr(client, "consume_task_image_execution_start"), "online start transport missing"
        subject = consumer(tmp_path, payload, kwargs, client.consume_task_image_execution_start)
        with pytest.raises(ValueError):
            await subject.authorize()


async def test_online_deadline_cancels_and_joins_transport(tmp_path, monkeypatch):
    from loom_worker import task_image_execution

    payload, kwargs = evidence(tmp_path)
    closed = asyncio.Event()
    started = asyncio.Event()
    deadline = asyncio.timeout(None)

    def online_timeout(seconds):
        assert seconds == 0.05
        return deadline

    # Keep signed verification real, but expire the real asyncio deadline only
    # after HTTP starts. Host speed must not select the pre-HTTP timeout path.
    monkeypatch.setattr(task_image_execution, "asyncio", SimpleNamespace(timeout=online_timeout))
    monkeypatch.setattr(task_image_execution, "time", SimpleNamespace(monotonic=lambda: 0.0))

    async def handle(_):
        try:
            started.set()
            deadline.reschedule(asyncio.get_running_loop().time())
            await asyncio.Event().wait()
        finally:
            closed.set()

    async with httpx.AsyncClient(
        base_url="https://cp.example", transport=httpx.MockTransport(handle),
    ) as http:
        client = HttpControlPlaneClient("https://cp.example", "worker-token", _client=http)
        assert hasattr(client, "consume_task_image_execution_start"), "online start transport missing"
        subject = consumer(tmp_path, payload, kwargs, client.consume_task_image_execution_start)
        subject.timeout_seconds = 0.05
        with pytest.raises(TimeoutError):
            await subject.authorize()
        with pytest.raises(RuntimeError, match="already attempted"):
            await subject.authorize()
    assert started.is_set()
    assert deadline.expired()
    assert closed.is_set()


async def test_verification_deadline_refuses_before_starting_transport(tmp_path, monkeypatch):
    from loom_worker import task_image_execution

    payload, kwargs = evidence(tmp_path)
    sent = []
    readings = iter((0.0, 0.1))
    monkeypatch.setattr(task_image_execution, "time", SimpleNamespace(
        monotonic=lambda: next(readings, 0.1),
    ))

    def handle(request):
        sent.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(
        base_url="https://cp.example", transport=httpx.MockTransport(handle),
    ) as http:
        client = HttpControlPlaneClient("https://cp.example", "worker-token", _client=http)
        subject = consumer(tmp_path, payload, kwargs, client.consume_task_image_execution_start)
        subject.timeout_seconds = 0.05
        with pytest.raises(TimeoutError, match="verification exceeded deadline"):
            await subject.authorize()
        with pytest.raises(RuntimeError, match="already attempted"):
            await subject.authorize()
    assert sent == []


@pytest.mark.parametrize("configured,actual", [
    ("http://cp.example", "http://cp.example"),
    ("https://cp.example", "https://different.example"),
    ("https://cp.example", "http://cp.example"),
    ("https://user:password@cp.example", "https://user:password@cp.example"),
])
async def test_start_requires_authenticated_configured_origin_before_sending_token(
    tmp_path, configured, actual,
):
    payload, kwargs = evidence(tmp_path)
    sent = []

    def handle(request):
        sent.append(request)
        return httpx.Response(503)

    async with httpx.AsyncClient(
        base_url=actual, transport=httpx.MockTransport(handle),
    ) as http:
        client = HttpControlPlaneClient(configured, "worker-token", _client=http)
        subject = consumer(tmp_path, payload, kwargs, client.consume_task_image_execution_start)
        with pytest.raises(ValueError, match=r"HTTPS|origin"):
            await subject.authorize()
    assert sent == []
