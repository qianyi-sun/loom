"""Thin httpx-based forwarder used to proxy Control Plane writes.

The service layer doesn't reimplement trial submit / cancel — it
authenticates the caller, runs the local scope/team checks, then
proxies the request to the Control Plane with the caller's bearer
token intact. The CP applies its own auth as a defense-in-depth layer.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException


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


def propagate(resp: httpx.Response) -> Any:
    """Mirror upstream status — convert 4xx/5xx to HTTPException and
    return JSON body on success."""
    if resp.status_code >= 400:
        try:
            detail: Any = resp.json()
        except ValueError:
            detail = resp.text
        raise HTTPException(status_code=resp.status_code, detail=detail)
    if not resp.content:
        return {}
    return resp.json()
