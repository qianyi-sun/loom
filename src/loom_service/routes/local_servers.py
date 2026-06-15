"""Local-server catalog — `GET /api/v1/local-servers`.

Returns the operator-configured local OpenAI-compatible LLM servers
the cluster can route to. Drives the SPA's model picker (PR-C) when
`ModelSpec.source="local-server"` so users see real, currently-
deployed server names rather than typing free-form strings.

Config comes from `LOOM_SVC_LOCAL_SERVERS_JSON`; see
`LoomServiceSettings.local_servers_json` for the shape and an example.

This route is read-only: operators provision via env var, not via the
API. That avoids credential leakage through the DB and keeps the
deploy story simple (the same env var pattern every other secret
uses).
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from loom_service.dependencies import SessionAndCtx

router = APIRouter()


@router.get("/local-servers")
async def list_local_servers(
    request: Request,
    sc: SessionAndCtx,
) -> dict[str, Any]:
    # `sc` triggers auth via Depends. We still need `request` for the
    # settings lookup (and to keep the legacy route shape obvious).
    _ = sc

    raw = request.app.state.settings.local_servers_json
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        # Surface a 500 with the precise error so an operator who
        # mis-edited LOOM_SVC_LOCAL_SERVERS_JSON sees the cause
        # instead of an opaque empty list.
        raise HTTPException(
            status_code=500,
            detail=(
                "LOOM_SVC_LOCAL_SERVERS_JSON is not valid JSON: "
                f"{exc}"
            ),
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=500,
            detail=(
                "LOOM_SVC_LOCAL_SERVERS_JSON must be a JSON object "
                "mapping server name → config; got "
                f"{type(parsed).__name__}"
            ),
        )

    items: list[dict[str, Any]] = []
    for name, entry in sorted(parsed.items()):
        if not isinstance(entry, dict):
            continue
        base_url = entry.get("base_url")
        if not isinstance(base_url, str) or not base_url:
            # Skip silently; an operator's typo on one entry shouldn't
            # mask the others. The 500 path above catches JSON-level
            # damage.
            continue
        items.append({
            "name": name,
            "base_url": base_url,
            "kind": entry.get("kind"),
            "description": entry.get("description"),
        })
    return {"items": items}
