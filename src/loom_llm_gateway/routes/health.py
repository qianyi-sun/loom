"""Health + readiness probes."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from loom_llm_gateway.drain import ensure_drain_state

router = APIRouter()


@router.get("/healthz")
async def healthz(request: Request) -> Response:
    """Readiness probe used by the k8s Deployment's readinessProbe.

    Returns 200 during normal operation. Returns 503 while the pod is
    draining (post-`POST /drain`) so the Service load balancer removes
    the pod from the routing pool and no NEW requests arrive. In-flight
    requests continue to completion via the drain mechanism (#547).
    """
    drain_state = ensure_drain_state(request.app)
    in_flight, draining = await drain_state.snapshot()
    if draining:
        return Response(
            status_code=503,
            content=(
                b'{"status":"draining",'
                b'"in_flight":' + str(in_flight).encode() + b"}"
            ),
            media_type="application/json",
        )
    return Response(
        status_code=200,
        content=b'{"status":"ok"}',
        media_type="application/json",
    )
