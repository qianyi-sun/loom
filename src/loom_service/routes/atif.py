"""ATIF authenticated download (spec §5.2).

`finalize.py` writes the trial's ATIF document to the trajectories
bucket at `<team_id>/<trial_id>/atif.json`. This route proxies the
object through loom_service so browser clients only need API access,
not direct MinIO access.

Postgres-backed trials can have typed `trial_events` without the legacy
`atif.json` object. For those, the route reprojects ATIF from the
durable event rows and the trial's persisted agent metadata.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select

from loom.db.schema import Trial
from loom.models.trajectory import TrajectoryEvent
from loom.trajectory.atif import project_to_atif
from loom.trajectory.object_identity import resolve_trajectory_object_key
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.metrics import ARTIFACT_DOWNLOAD_BYTES
from loom_service.routes.object_downloads import stream_object_response
from loom_service.trajectory_reconstruction import (
    read_all_events_from_postgres,
    read_llm_calls_from_postgres,
    reconstruct_postgres_trajectory_events,
)

router = APIRouter()

_event_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)
_ATIF_REPROJECTION_UNAVAILABLE = (
    "atif projection metadata unavailable; trajectory events are downloadable"
)
_ATIF_EVENTS_UNAVAILABLE = (
    "atif projection unavailable from stored trajectory events; "
    "trajectory events are downloadable"
)


def _atif_key(trial: Trial, *, expected_bucket: str) -> str:
    index = trial.trajectory_index if isinstance(trial.trajectory_index, dict) else {}
    try:
        return resolve_trajectory_object_key(
            uri=index.get("atif_uri"),
            expected_bucket=expected_bucket,
            team_id=trial.team_id,
            trial_id=trial.id,
            filename="atif.json",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "trajectory_object_identity_invalid",
                "message": str(exc),
            },
        ) from exc


def _projection_metadata(trial: Trial) -> tuple[str, str, str]:
    result = trial.result if isinstance(trial.result, dict) else {}
    task_id = result.get("task_id") or trial.task_id
    agent = result.get("agent")
    if not isinstance(task_id, str) or not task_id.strip():
        raise HTTPException(
            status_code=409,
            detail=_ATIF_REPROJECTION_UNAVAILABLE,
        )
    if not isinstance(agent, dict):
        raise HTTPException(
            status_code=409,
            detail=_ATIF_REPROJECTION_UNAVAILABLE,
        )
    agent_name = agent.get("name")
    agent_version = agent.get("version")
    if (
        not isinstance(agent_name, str)
        or not agent_name.strip()
        or not isinstance(agent_version, str)
        or not agent_version.strip()
    ):
        raise HTTPException(
            status_code=409,
            detail=_ATIF_REPROJECTION_UNAVAILABLE,
        )
    return task_id, agent_name, agent_version


def _parse_events(events: list[dict[str, Any]]) -> list[TrajectoryEvent]:
    try:
        return [
            _event_adapter.validate_python(event)
            for event in events
        ]
    except ValidationError as exc:
        raise HTTPException(
            status_code=409,
            detail=_ATIF_EVENTS_UNAVAILABLE,
        ) from exc


def _postgres_events_atif_response(
    events: list[dict[str, Any]], *, trial: Trial,
) -> Response:
    task_id, agent_name, agent_version = _projection_metadata(trial)
    try:
        atif = project_to_atif(
            _parse_events(events),
            task_id=task_id,
            agent_name=agent_name,
            agent_version=agent_version,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail=_ATIF_EVENTS_UNAVAILABLE,
        ) from exc

    content = atif.model_dump_json(indent=2).encode("utf-8")
    headers = {
        "Content-Disposition": (
            f"attachment; filename*=UTF-8''{quote(f'{trial.id}-atif.json', safe='')}"
        ),
        "Content-Length": str(len(content)),
    }
    ARTIFACT_DOWNLOAD_BYTES.labels(artifact_kind="atif").inc(len(content))
    return Response(
        content=content,
        headers=headers,
        media_type="application/json",
    )


@router.get("/trials/{trial_id}/atif")
async def download_atif(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> Response:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (await s.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    try:
        return stream_object_response(
            client=request.app.state.minio_client,
            bucket=settings.trajectories_bucket,
            key=_atif_key(trial, expected_bucket=settings.trajectories_bucket),
            filename=f"{trial.id}-atif.json",
            artifact_kind="atif",
            media_type="application/json",
        )
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    events = await read_all_events_from_postgres(s, trial_id=trial.id)
    if not events:
        raise HTTPException(
            status_code=404,
            detail="download object not found",
        )
    llm_calls = await read_llm_calls_from_postgres(s, trial_id=trial.id)
    events = reconstruct_postgres_trajectory_events(
        events,
        trial=trial,
        llm_calls=llm_calls,
    )
    return _postgres_events_atif_response(events, trial=trial)
