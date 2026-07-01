"""Runtime probe for OpenAI-Responses-API support at a provider connection's
upstream. Used by `routes/responses.py` to decide whether to dispatch a
codex-shaped request through the native `POST /v1/responses` path or
through the existing `responses_chat_compat` translator.

Fail-closed policy: any 5xx / timeout / connect-error is classified as
"unsupported" so the shim runs and codex sees a real translated response
instead of hanging on a 40-min agent-timeout window. False negatives
here just cost one config knob at startup; false positives would let
codex hang.

The probe uses a *minimal but complete* Responses payload —
`{"model": "<sentinel>", "input": "loom-probe", "max_output_tokens": 1}` —
so it exercises the full upstream routing path rather than short-circuiting
on early validation. Empty-body probes were tried first and rejected
because BYO providers that reverse-proxy /v1/responses (yibuapi) will
happily 400 an empty body from their edge without ever attempting to
route — the response looks like a real Responses handler, but the
first live request hits an upstream timeout. A sentinel model name
that isn't a real provider model forces the proxy to try to route,
which is what actually matters.

Spec: docs/architecture/responses-api-support-probe.md
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

_PROBE_TIMEOUT_SEC = 5.0

_SUPPORTED_STATUSES = frozenset({200, 400, 401})
_UNSUPPORTED_STATUSES = frozenset({404, 501})

# Sentinel model id in the probe body. Deliberately chosen to be one that
# no real provider ships. Real Responses handlers return 400
# ("model not found") quickly; reverse-proxied endpoints that only
# forward valid requests will 4xx or 5xx depending on their edge policy,
# which is the routing signal we care about.
_PROBE_MODEL_SENTINEL = "loom-probe-nonexistent-model"


@dataclass(frozen=True)
class ProbeOutcome:
    """Result of one probe attempt.

    - `supported = True`: upstream implements Responses; route native.
    - `supported = False`: upstream does not implement Responses; dispatch
       through the existing Chat-completions translator.
    - `supported = None`: probe was inconclusive (ambiguous 4xx that
      wasn't 400/401/404). Caller should keep the previously-cached
      value if any, or fall back to the existing per-request behaviour.
    """

    supported: bool | None
    error_detail: str | None


def classify_probe_status(status_code: int) -> bool | None:
    """Pure classifier — see ProbeOutcome for the tri-state semantics."""
    if status_code in _SUPPORTED_STATUSES:
        return True
    if status_code in _UNSUPPORTED_STATUSES:
        return False
    if 500 <= status_code < 600:
        return False
    # Other 4xx (403, 429, 422, …): endpoint is reachable but the
    # response neither confirms nor denies Responses-API semantics.
    return None


async def probe_responses_api(
    *,
    upstream_url: str,
    api_key: str,
    client: httpx.AsyncClient | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> ProbeOutcome:
    """Fire one probe request.

    Callers in production pass the same `httpx.AsyncClient` they use for
    real upstream traffic (via the egress client pool) so the probe
    honours the connection's egress proxy, DNS resolution set, and
    connection reuse. Test callers pass `transport=httpx.MockTransport(...)`
    to intercept probe traffic alongside their upstream mocks.
    """
    if client is None:
        async with httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC, transport=transport,
        ) as owned_client:
            return await _run_probe(owned_client, upstream_url, api_key)
    return await _run_probe(client, upstream_url, api_key)


async def _run_probe(
    client: httpx.AsyncClient, upstream_url: str, api_key: str,
) -> ProbeOutcome:
    try:
        response = await client.post(
            upstream_url,
            json={
                "model": _PROBE_MODEL_SENTINEL,
                "input": "loom-probe",
                "max_output_tokens": 1,
            },
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=_PROBE_TIMEOUT_SEC,
        )
    except (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.RemoteProtocolError,
        httpx.TransportError,
    ) as exc:
        reason = type(exc).__name__
        logger.info(
            "responses_probe transport error url=%s err=%s",
            upstream_url, reason,
        )
        return ProbeOutcome(
            supported=False,
            error_detail=f"transport_{reason.lower()}",
        )
    verdict = classify_probe_status(response.status_code)
    if verdict is True:
        return ProbeOutcome(supported=True, error_detail=None)
    if verdict is False:
        return ProbeOutcome(
            supported=False,
            error_detail=f"upstream_{response.status_code}",
        )
    # Ambiguous 4xx.
    return ProbeOutcome(
        supported=None,
        error_detail=f"ambiguous_{response.status_code}",
    )
