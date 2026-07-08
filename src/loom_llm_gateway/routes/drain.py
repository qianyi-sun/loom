"""`POST /drain` — called by the k8s `preStop` hook (#547).

The k8s Pod spec's `preStop` hook runs BEFORE SIGTERM. It calls this
endpoint to:

1. Mark the pod as draining (so `/healthz` returns 503 and Service
   stops routing new requests to it)
2. Wait for the in-flight request counter to reach zero (so in-flight
   LLM calls finish cleanly, no truncated responses)
3. Return 200 so kubelet can proceed with the normal SIGTERM path

Only the localhost preStop hook should be able to call this — the
Loom Gateway is internal-only per #77, so exposure is limited to
kubelet-injected requests inside the pod's network namespace.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from loom_llm_gateway.drain import drain_and_report, ensure_drain_state

router = APIRouter()


@router.post("/drain")
async def drain(request: Request) -> dict[str, object]:
    """Flip draining=True, wait for in-flight to reach zero.

    Idempotent: calling `/drain` on an already-draining pod returns
    the current in-flight count without re-triggering the wait
    (still bounded by the same timeout).
    """
    drain_state = ensure_drain_state(request.app)
    settings = request.app.state.settings
    timeout_sec = float(settings.gateway_drain_timeout_sec)
    return await drain_and_report(drain_state, timeout_sec=timeout_sec)
