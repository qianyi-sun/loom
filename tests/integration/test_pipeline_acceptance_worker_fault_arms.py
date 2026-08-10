from __future__ import annotations

from uuid import uuid4

import httpx

from loom_worker.control_plane_client import AttemptClaimHeaders, HttpControlPlaneClient


async def test_fault_arm_lookup_is_claim_bound_and_404_is_inert() -> None:
    attempt_id = uuid4()
    claim = AttemptClaimHeaders(uuid4(), 3, "lease-" + "x" * 40)
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(404, request=request)

    async with httpx.AsyncClient(
        base_url="http://control-plane", transport=httpx.MockTransport(handler)
    ) as http:
        client = HttpControlPlaneClient("http://control-plane", "worker", _client=http)
        arm = await client.get_acceptance_fault_arm(
            attempt_id=attempt_id,
            seam="worker.before_stage_result_validation",
            claim=claim,
        )
    assert arm is None
    assert seen[0].headers["x-loom-claim-id"] == str(claim.claim_id)
    assert seen[0].headers["x-loom-lease-epoch"] == "3"
    assert seen[0].headers["x-loom-lease-token"] == claim.lease_token
    assert seen[0].url.params["seam"] == "worker.before_stage_result_validation"
