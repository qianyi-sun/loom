"""Hidden, authenticated, read-only Stage 1 candidate preparation."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from loom.pipeline.stage1_smoke import Stage1SmokeCandidateV1
from loom_service.auth_guards import require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.pipeline_stage1_smoke_authority import (
    RepositoryStage1CandidateAuthority,
    Stage1CandidateAuthorityError,
    Stage1CandidateInventoryV1,
    Stage1CandidateSelectionV1,
)

router = APIRouter(include_in_schema=False)


def _authority(request: Request) -> RepositoryStage1CandidateAuthority:
    value = getattr(request.app.state, "pipeline_stage1_candidate_authority", None)
    if not isinstance(value, RepositoryStage1CandidateAuthority):
        raise HTTPException(status_code=503, detail="stage1_candidate_authority_unavailable")
    return value


def _exact_team_and_user(sc: SessionAndCtx, team_id: UUID) -> UUID:
    _session, ctx = sc
    # Admin is intentionally not a cross-team candidate preparation authority.
    # The exact operator identity and current team are candidate-bound.
    if ctx.team_id != team_id or ctx.user_id is None:
        raise HTTPException(status_code=404, detail="stage1_candidate_selection_not_found")
    return ctx.user_id


@router.get(
    "/pipeline-stage1-smoke-preparation/teams/{team_id}/inventory",
    response_model=Stage1CandidateInventoryV1,
)
async def inventory(
    team_id: UUID,
    request: Request,
    sc: SessionAndCtx,
) -> Stage1CandidateInventoryV1:
    session, ctx = sc
    require_scope(ctx, "read:own")
    _exact_team_and_user(sc, team_id)
    try:
        return await _authority(request).inventory(session, team_id=team_id)
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="stage1_candidate_registry_unavailable") from exc


@router.post(
    "/pipeline-stage1-smoke-preparation/candidate",
    response_model=Stage1SmokeCandidateV1,
)
async def prepare_candidate(
    selection: Stage1CandidateSelectionV1,
    request: Request,
    sc: SessionAndCtx,
) -> Stage1SmokeCandidateV1:
    session, ctx = sc
    require_scope(ctx, "submit")
    operator_user_id = _exact_team_and_user(sc, selection.team_id)
    try:
        return await _authority(request).prepare(
            session,
            operator_user_id=operator_user_id,
            selection=selection,
        )
    except Stage1CandidateAuthorityError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.reason_code) from exc
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="stage1_candidate_registry_unavailable") from exc


__all__ = ["router"]
