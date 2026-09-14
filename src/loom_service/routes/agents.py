"""Agent catalog — GET /api/v1/agents.

Returns the union of built-in agent names + registered launcher
adapters so the SPA can populate a dropdown rather than ask the user
to type a free-form name.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from loom.agent_runtime_registry import list_agent_runtimes
from loom_service.agent_catalog import list_agents
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


@router.get("/agents")
async def list_agents_route(sc: SessionAndCtx) -> dict[str, Any]:
    session, _ = sc
    versions: dict[str, list[dict[str, str]]] = {}
    for release in await list_agent_runtimes(session):
        versions.setdefault(release.agent_name, []).append(release.public_metadata())
    return {"items": [
        {**agent.to_dict(), "versions": versions.get(agent.name, [])}
        for agent in list_agents()
    ]}
