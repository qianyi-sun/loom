from __future__ import annotations

from uuid import uuid4

import httpx

from loom_worker.control_plane_client import AttemptClaimHeaders, HttpControlPlaneClient


async def test_input_read_sends_only_descriptor_identity_and_fence() -> None:
    attempt_id = uuid4()
    claim = AttemptClaimHeaders(uuid4(), 1, "lease-" + "x" * 40)
    digest = "sha256:" + "a" * 64
    captured: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(206, request=request, content=b"bytes")

    async with httpx.AsyncClient(
        base_url="http://control-plane", transport=httpx.MockTransport(handler)
    ) as http:
        client = HttpControlPlaneClient("http://control-plane", "worker", _client=http)
        response = await client.read_execution_attempt_input_file(
            attempt_id=attempt_id,
            binding_name="task set",
            item_key="shard/one",
            file_index=2,
            manifest_sha256=digest,
            claim=claim,
            range_start=4096,
        )
    assert response.content == b"bytes"
    request = captured[0]
    assert "task%20set" in str(request.url) and "shard%2Fone" in str(request.url)
    assert request.headers["if-match"] == f'"{digest}"'
    assert request.headers["range"] == "bytes=4096-"
    assert "bucket" not in str(request.url) and "credential" not in str(request.url)
