"""Signed-URL minting for direct worker → MinIO uploads."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Trial as TrialRow

router = APIRouter()


@router.post("/artifacts/upload-url")
async def mint_artifact_upload_url(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or (
        "submit" not in ctx.scopes and "worker:index" not in ctx.scopes
    ):
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        trial_id = payload["trial_id"]
        key = payload["key"]
    except KeyError as exc:
        raise HTTPException(
            status_code=400, detail=f"trial_id + key required: {exc}",
        ) from exc
    settings = request.app.state.settings
    bucket = "artifacts"

    # team_id resolution: team tokens carry team_id directly (and only see
    # their team's trials); worker tokens have team_id=None and must look it
    # up from the trial row. Without this, worker uploads would mint keys
    # with a literal "None" prefix — broken URLs and orphaned objects.
    if ctx.team_id is not None:
        team_id = ctx.team_id
    else:
        async with request.app.state.session_factory() as session:
            row = (await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )).scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=404, detail="trial not found")
        team_id = row
    full_key = f"{team_id}/{trial_id}/{key}"

    url = request.app.state.minio_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": full_key},
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return {
        "url": url,
        "bucket": bucket,
        "key": full_key,
        "expires_in_sec": settings.signed_url_expiry_sec,
    }
