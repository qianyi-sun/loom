"""GET /api/v1/health — unauthenticated liveness probe.

Used by the docker-compose healthcheck + k8s readinessProbe. Does NOT
hit the DB — Plan 18 will add `/health/ready` for a deeper check; this
endpoint only proves the FastAPI process is alive."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from loom_service.dependencies import SessionAndCtx
from loom_service.readiness import probe_dependencies

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
async def dependency_readiness(
    request: Request,
    response: Response,
    sc: SessionAndCtx,
) -> dict[str, object]:
    """Authenticated read-only PostgreSQL and object-store readiness.

    The dedicated rollout readonly principal may call this route.  It never
    returns provider exception text or credential material.
    """
    session, _ctx = sc
    settings = request.app.state.settings
    result = await probe_dependencies(
        session,
        minio_client=request.app.state.minio_client,
        buckets=(settings.artifacts_bucket, settings.trajectories_bucket),
    )
    if not result.ready:
        response.status_code = 503
    return result.to_dict()
