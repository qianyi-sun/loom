"""GET /api/v1/health — unauthenticated liveness probe.

Used by the docker-compose healthcheck + k8s readinessProbe. Does NOT
hit the DB — Plan 18 will add `/health/ready` for a deeper check; this
endpoint only proves the FastAPI process is alive."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
