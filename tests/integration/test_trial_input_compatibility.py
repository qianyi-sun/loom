from __future__ import annotations

from uuid import uuid4

import httpx

from loom_worker.control_plane_client import HttpControlPlaneClient


async def test_legacy_registration_and_trial_claim_bodies_remain_cache_free() -> None:
    bodies: list[dict[str, object]] = []

    def transport(request: httpx.Request) -> httpx.Response:
        bodies.append(dict(__import__("json").loads(request.content)))
        if request.url.path == "/workers/register":
            return httpx.Response(200, json={"worker_id": str(uuid4())})
        return httpx.Response(204)

    async with httpx.AsyncClient(
        base_url="http://cp", transport=httpx.MockTransport(transport)
    ) as client:
        control_plane = HttpControlPlaneClient(
            "http://cp", "worker-token", _client=client
        )
        await control_plane.register(
            hostname="legacy",
            version="1",
            capabilities=[{"os": "linux"}],
        )
        await control_plane.claim(worker_id=uuid4(), caps=[{"os": "linux"}])

    assert not any(key.startswith("input_cache_") for key in bodies[0])
    assert set(bodies[1]) == {"worker_id", "caps"}
