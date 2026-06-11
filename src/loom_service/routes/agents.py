"""Agent catalog — GET /api/v1/agents.

Returns the union of built-in agent names + registered launcher
adapters so the SPA can populate a dropdown rather than ask the user
to type a free-form name.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, Request

from loom.auth import verify_bearer_token
from loom_service.agent_catalog import list_agents
from loom_service.auth_guards import require_human_or_admin

router = APIRouter()


@router.get("/agents")
async def list_agents_route(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
    return {"items": [a.to_dict() for a in list_agents()]}
