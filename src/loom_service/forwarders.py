"""Thin httpx-based forwarder used to proxy Control Plane writes.

The service layer doesn't reimplement trial submit / cancel — it
authenticates the caller, runs the local scope/team checks, then
proxies the request to the Control Plane with the caller's bearer
token intact. The CP applies its own auth as a defense-in-depth layer.

`propagate` returns a `JSONResponse` so we can carry through the
upstream's status_code AND a small allowlist of response headers
(Retry-After, Location, X-RateLimit-*) — Plan 19's rate-limited
batch submits need Retry-After for client backoff to work.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException
from fastapi.responses import JSONResponse

# Headers the service forwards verbatim from the upstream response.
# Allowlist (not blocklist) so we don't accidentally leak internal
# trace headers or set-cookie values from a misconfigured CP.
_PASSTHROUGH_HEADERS: frozenset[str] = frozenset({
    "retry-after",
    "location",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "x-idempotency-key",
})


async def forward(
    client: httpx.AsyncClient,
    *,
    method: str,
    path: str,
    authorization: str | None,
    json_body: Any | None = None,
) -> httpx.Response:
    headers: dict[str, str] = {}
    if authorization:
        headers["Authorization"] = authorization
    try:
        resp = await client.request(
            method, path, headers=headers, json=json_body,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"upstream unreachable: {exc}",
        ) from exc
    return resp


def _allowed_headers(resp: httpx.Response) -> dict[str, str]:
    return {
        k: v
        for k, v in resp.headers.items()
        if k.lower() in _PASSTHROUGH_HEADERS
    }


def propagate(resp: httpx.Response) -> JSONResponse:
    """Mirror upstream status + allowlisted headers + JSON body.

    4xx/5xx responses are returned as JSONResponse with the upstream
    status code (NOT raised as HTTPException) so the allowlisted
    response headers (Retry-After, etc.) carry through. The body is
    the upstream JSON if parseable, else the raw text wrapped in
    {"detail": ...} for shape consistency.
    """
    if resp.content:
        try:
            body: Any = resp.json()
        except ValueError:
            body = {"detail": resp.text}
    else:
        body = {}
    return JSONResponse(
        status_code=resp.status_code,
        content=body,
        headers=_allowed_headers(resp),
    )
