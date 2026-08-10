from __future__ import annotations

from uuid import uuid4

import httpx

from loom_worker.control_plane_client import AttemptClaimHeaders, HttpControlPlaneClient


async def test_output_part_stream_never_exposes_backend_identity() -> None:
    attempt_id = uuid4()
    session_id = uuid4()
    claim = AttemptClaimHeaders(uuid4(), 4, "lease-" + "x" * 40)
    body = b"immutable-part"
    digest = "sha256:" + "b" * 64
    captured: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            request=request,
            json={
                "file_index": 0,
                "part_number": 1,
                "size_bytes": len(body),
                "sha256": digest,
            },
        )

    async with httpx.AsyncClient(
        base_url="http://control-plane", transport=httpx.MockTransport(handler)
    ) as http:
        client = HttpControlPlaneClient("http://control-plane", "worker", _client=http)
        receipt = await client.upload_final_output_part(
            attempt_id=attempt_id,
            session_id=session_id,
            file_index=0,
            part_number=1,
            claim=claim,
            request_id=uuid4(),
            upload_token="upload-opaque",
            content_sha256=digest,
            content=body,
        )
    assert receipt["sha256"] == digest
    request = captured[0]
    assert request.content == body
    assert request.headers["x-loom-content-sha256"] == digest
    assert "x-loom-request-id" in request.headers
    assert request.headers["content-length"] == str(len(body))
    assert not any(term in str(request.url) for term in ("bucket", "multipart", "object-key"))
