"""Signed-URL minting for direct worker → MinIO uploads."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Trial as TrialRow
from loom_control_plane.routes.execution_fence import (
    OptionalExecutionGenerationHeader,
    OptionalExecutionLeaseIdHeader,
    enforce_trial_execution_fence,
)

router = APIRouter()


def _validate_key(key: str) -> None:
    """Bug 2 fix: reject path-traversal / absolute / NUL-byte keys.

    S3/MinIO doesn't dereference `..` as filesystem traversal, but a key
    like `../../other_team/secret.json` is a literal string that bypasses
    the intended `{team}/{trial}/` prefix on the wire — a team A token
    could presign PUTs against team B's namespace.
    """
    if not key:
        raise HTTPException(status_code=400, detail="key must be non-empty")
    if "\x00" in key:
        raise HTTPException(status_code=400, detail="key must not contain NUL")
    if key.startswith("/"):
        raise HTTPException(
            status_code=400,
            detail="key must be relative (no leading /)",
        )
    for segment in key.split("/"):
        if segment in ("", "..", "."):
            raise HTTPException(
                status_code=400,
                detail="key must not contain '..', '.', or empty segments",
            )


@router.post("/artifacts/upload-url")
async def mint_artifact_upload_url(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    execution_lease_id: OptionalExecutionLeaseIdHeader = None,
    execution_generation: OptionalExecutionGenerationHeader = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or ("submit" not in ctx.scopes and "worker:index" not in ctx.scopes):
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        trial_id = UUID(str(payload["trial_id"]))
        key = payload["key"]
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"trial_id + key required: {exc}",
        ) from exc
    _validate_key(key)

    settings = request.app.state.settings
    bucket = settings.artifacts_bucket

    # Bug 6 fix: for team tokens, additionally verify the trial belongs to
    # that team — otherwise a team A token could mint upload URLs that land
    # under team A's own prefix but using team B's trial_id, polluting the
    # cross-team artifact index.
    async with request.app.state.session_factory() as session:
        await enforce_trial_execution_fence(
            session,
            trial_id=trial_id,
            lease_id=execution_lease_id,
            generation=execution_generation,
            surface="artifact",
        )
        trial_team = (
            await session.execute(
                select(TrialRow.team_id).where(TrialRow.id == trial_id),
            )
        ).scalar_one_or_none()

    if ctx.team_id is not None:
        if trial_team is None or trial_team != ctx.team_id:
            raise HTTPException(
                status_code=403,
                detail="trial belongs to another team",
            )
        team_id = ctx.team_id
    else:
        if trial_team is None:
            raise HTTPException(status_code=404, detail="trial not found")
        team_id = trial_team
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
