"""Secret-free management probes, independent of every child environment."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request, Response
from sqlalchemy import text

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request, response: Response) -> dict[str, str]:
    """Report only this process's database availability, never child readiness.

    Unauthenticated for Kubernetes probes. No credentials, environment identities
    or database exception text may be included in the response.
    """
    ready = False
    try:
        async with asyncio.timeout(3):
            async with request.app.state.session_factory() as session:
                ready = (await session.execute(text("SELECT 1"))).scalar_one() == 1
    except Exception:
        pass
    if not ready:
        response.status_code = 503
    return {
        "status": "ready" if ready else "not-ready",
        "mode": "management",
        "postgres": "ready" if ready else "not-ready",
    }
