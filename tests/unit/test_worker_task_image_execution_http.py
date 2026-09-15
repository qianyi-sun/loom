"""The one-use start transport must never retry or accept an ambiguous reply."""

import asyncio
import importlib

import httpx
import pytest
import rfc8785

from loom_worker.control_plane_client import HttpControlPlaneClient
from tests.unit.test_worker_task_image_execution import accepting, consumer, evidence


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


async def test_online_deadline_cancels_and_joins_transport(tmp_path):
    payload, kwargs = evidence(tmp_path)
    closed = asyncio.Event()

    async def handle(_):
        try:
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
    assert closed.is_set()
