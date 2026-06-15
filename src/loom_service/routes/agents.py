"""Agent catalog — GET /api/v1/agents.

Returns the union of built-in agent names + registered launcher
adapters so the SPA can populate a dropdown rather than ask the user
to type a free-form name.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from loom_service.agent_catalog import list_agents
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


@router.get("/agents")
async def list_agents_route(sc: SessionAndCtx) -> dict[str, Any]:
    # Auth happens inside `authed_session`; this route doesn't query
    # the DB, but the dep still opens + closes a session at request
    # teardown for the bearer-token verify.
    _ = sc
    return {"items": [a.to_dict() for a in list_agents()]}
