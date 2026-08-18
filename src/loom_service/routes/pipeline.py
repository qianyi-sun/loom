"""Team-safe REST surface for official Recipe Pipeline runs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import Field, StringConstraints, model_validator
from sqlalchemy import and_, or_, select

from loom.db.schema import (
    Artifact,
    ArtifactUploadSession,
    ExecutionAttempt,
    PipelineBudgetLedger,
    PipelineEvent,
    PipelineLivePreviewFrame,
    PipelineLivePreviewGeneration,
    PipelineRun,
    PipelineStageRun,
    TeamMembership,
    Worker,
)
from loom.pipeline.artifact_access import artifact_read_allowed
from loom.pipeline.artifact_commit import PartReceiptV1
from loom.pipeline.behavior_input_import import BehaviorInputImportManifestV1
from loom.pipeline.control_bindings import (
    JudgeExecutionProfileApplyV1,
    RecipeProviderBindingApplyV1,
)
from loom.pipeline.keys import canonical_digest
from loom.pipeline.live_preview import LivePreviewMetadataV1, is_stage1_live_preview_eligible
from loom.pipeline.public_api import (
    PipelineArtifactDetailV1,
    PipelineExecutionAttemptListV1,
    PipelineRunCancelRequestV1,
    PipelineRunDetailV1,
    PipelineRunEventsQueryV1,
    PipelineRunEventsResponseV1,
    PipelineRunListQueryV1,
    PipelineRunListResponseV1,
    PipelineRunRetryRequestV1,
    PipelineRunSubmitRequestV1,
    PipelineStageRunDetailV1,
    validate_idempotency_key,
)
from loom.pipeline.recipes import OfficialRecipeRegistry
from loom.pipeline.spec import ArtifactType, Digest, PipelineModel
from loom_service.auth_guards import is_admin, require_scope, require_submitting_user
from loom_service.dependencies import SessionAndCtx
from loom_service.metrics import PIPELINE_LIVE_PREVIEW_READS_TOTAL
from loom_service.pipeline_api_service import (
    PipelineApiError,
    create_public_run,
    create_retry_run,
    decode_pipeline_cursor,
    encode_pipeline_cursor,
    request_user_cancellation,
    run_projection,
)
from loom_service.pipeline_artifact_files import (
    public_artifact_projection,
    resolve_public_artifact,
    stream_public_artifact_file,
    validate_public_artifact,
)
from loom_service.pipeline_control_bindings import (
    apply_judge_profile,
    apply_provider_binding,
    read_current_binding,
    read_current_profile,
)
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter(tags=["pipeline"])

_ARTIFACT_FILE_RESPONSE_HEADERS = {
    "Accept-Ranges": {"schema": {"type": "string"}},
    "Content-Length": {"schema": {"type": "integer"}},
    "Content-Type": {"schema": {"type": "string"}},
    "ETag": {"schema": {"type": "string"}},
    "Content-Disposition": {"schema": {"type": "string"}},
    "Content-Range": {"schema": {"type": "string"}},
}
_ARTIFACT_FILE_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "Complete committed Artifact file",
        "headers": _ARTIFACT_FILE_RESPONSE_HEADERS,
        "content": {"application/octet-stream": {}},
    },
    206: {
        "description": "Single byte range",
        "headers": _ARTIFACT_FILE_RESPONSE_HEADERS,
        "content": {"application/octet-stream": {}},
    },
    304: {"description": "Artifact file ETag has not changed"},
    416: {
        "description": "Invalid, multiple, or unsatisfied byte range",
        "headers": _ARTIFACT_FILE_RESPONSE_HEADERS,
    },
}

_LIVE_PREVIEW_FRAME_HEADERS = {
    "Cache-Control": {"schema": {"type": "string"}},
    "Content-Length": {"schema": {"type": "integer"}},
    "Content-Security-Policy": {"schema": {"type": "string"}},
    "Content-Type": {"schema": {"type": "string"}},
    "ETag": {"schema": {"type": "string"}},
    "X-Content-Type-Options": {"schema": {"type": "string"}},
}
_LIVE_PREVIEW_FRAME_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "Latest bounded ephemeral JPEG preview",
        "headers": _LIVE_PREVIEW_FRAME_HEADERS,
        "content": {"image/jpeg": {}},
    },
    304: {"description": "Latest preview ETag has not changed"},
    404: {"description": "Preview is absent, stale, or outside the current team"},
    416: {"description": "Range reads are not supported"},
}


def _error(exc: PipelineApiError) -> HTTPException:
    return HTTPException(
        status_code=exc.status_code,
        detail={"reason_code": exc.reason_code, "message": exc.message},
    )


def _validate_key(value: str) -> None:
    try:
        validate_idempotency_key(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=422,
            detail={"reason_code": "invalid_idempotency_key", "message": str(exc)},
        ) from exc


def _team_and_user(sc: SessionAndCtx, *, mutation: bool) -> tuple[UUID, UUID | None]:
    _session, ctx = sc
    if ctx.team_id is None:
        raise HTTPException(
            status_code=403,
            detail={"reason_code": "team_required", "message": "Select a team first"},
        )
    require_scope(ctx, "submit" if mutation else "read:own")
    if mutation:
        require_submitting_user(ctx)
    return ctx.team_id, ctx.user_id


def _registry(request: Request) -> OfficialRecipeRegistry:
    value = getattr(request.app.state, "pipeline_recipe_registry", None)
    return value if isinstance(value, OfficialRecipeRegistry) else OfficialRecipeRegistry()


def _cursor_key(request: Request) -> bytes:
    configured = getattr(request.app.state, "pipeline_cursor_signing_key", None)
    if isinstance(configured, bytes) and len(configured) >= 32:
        return configured
    settings = request.app.state.settings
    return cast(bytes, settings.minio_secret_key.get_secret_value().encode())


async def _run_for_team(sc: SessionAndCtx, run_id: UUID) -> PipelineRun:
    session, _ctx = sc
    team_id, _ = _team_and_user(sc, mutation=False)
    run = (
        await session.execute(
            select(PipelineRun).where(PipelineRun.id == run_id, PipelineRun.team_id == team_id)
        )
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Pipeline run was not found"},
        )
    return run


async def _require_team_admin(sc: SessionAndCtx) -> None:
    session, ctx = sc
    if is_admin(ctx):
        return
    if ctx.team_id is None or ctx.user_id is None:
        raise HTTPException(
            status_code=403,
            detail={"reason_code": "team_admin_required", "message": "Team admin is required"},
        )
    role = (
        await session.execute(
            select(TeamMembership.role).where(
                TeamMembership.team_id == ctx.team_id,
                TeamMembership.user_id == ctx.user_id,
            )
        )
    ).scalar_one_or_none()
    if role != "owner":
        raise HTTPException(
            status_code=403,
            detail={"reason_code": "team_admin_required", "message": "Team admin is required"},
        )


def _site_admin_actor(sc: SessionAndCtx) -> UUID:
    if not is_admin(sc[1]) or sc[1].user_id is None:
        raise HTTPException(
            status_code=403,
            detail={"reason_code": "site_admin_required", "message": "Site admin is required"},
        )
    return sc[1].user_id


def _apply_preconditions(
    *, if_none_match: str | None, if_match_version: int | None
) -> tuple[bool, int | None]:
    if if_none_match == "*" and if_match_version is None:
        return True, None
    if if_none_match is None and if_match_version is not None and if_match_version > 0:
        return False, if_match_version
    raise HTTPException(
        status_code=428,
        detail={
            "reason_code": "precondition_required",
            "message": "Use If-None-Match:* for create or positive If-Match-Version for update",
        },
    )


@router.get("/admin/judge-execution-profiles/{recipe_name}/{recipe_version}/{profile_name}")
async def get_admin_judge_profile(
    sc: SessionAndCtx, recipe_name: str, recipe_version: int, profile_name: str
) -> dict[str, Any]:
    _site_admin_actor(sc)
    value = await read_current_profile(
        sc[0], recipe_name=recipe_name, recipe_version=recipe_version, profile_name=profile_name
    )
    if value is None:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Judge profile was not found"},
        )
    return value


@router.put("/admin/judge-execution-profiles/{recipe_name}/{recipe_version}/{profile_name}")
async def put_admin_judge_profile(
    response: Response,
    sc: SessionAndCtx,
    recipe_name: str,
    recipe_version: int,
    profile_name: str,
    payload: JudgeExecutionProfileApplyV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    if_match_version: Annotated[int | None, Header(alias="If-Match-Version")] = None,
) -> dict[str, Any]:
    actor = _site_admin_actor(sc)
    _validate_key(idempotency_key)
    create_only, expected_version = _apply_preconditions(
        if_none_match=if_none_match, if_match_version=if_match_version
    )
    try:
        value, replay = await apply_judge_profile(
            sc[0],
            actor_id=actor,
            recipe_name=recipe_name,
            recipe_version=recipe_version,
            profile_name=profile_name,
            payload=payload,
            idempotency_key=idempotency_key,
            create_only=create_only,
            expected_version=expected_version,
        )
        await sc[0].commit()
    except PipelineApiError as exc:
        await sc[0].rollback()
        raise _error(exc) from exc
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    response.headers["ETag"] = f'"{value["snapshot_sha256"]}"'
    response.headers["X-Config-Version"] = str(value["version"])
    return value


@router.get("/admin/recipe-provider-bindings/{recipe_name}/{recipe_version}/{logical_name}")
async def get_admin_provider_binding(
    sc: SessionAndCtx, recipe_name: str, recipe_version: int, logical_name: str
) -> dict[str, Any]:
    _site_admin_actor(sc)
    value = await read_current_binding(
        sc[0], recipe_name=recipe_name, recipe_version=recipe_version, logical_name=logical_name
    )
    if value is None:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Provider binding was not found"},
        )
    return value


@router.put("/admin/recipe-provider-bindings/{recipe_name}/{recipe_version}/{logical_name}")
async def put_admin_provider_binding(
    response: Response,
    sc: SessionAndCtx,
    recipe_name: str,
    recipe_version: int,
    logical_name: str,
    payload: RecipeProviderBindingApplyV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    if_match_version: Annotated[int | None, Header(alias="If-Match-Version")] = None,
) -> dict[str, Any]:
    actor = _site_admin_actor(sc)
    _validate_key(idempotency_key)
    create_only, expected_version = _apply_preconditions(
        if_none_match=if_none_match, if_match_version=if_match_version
    )
    try:
        value, replay = await apply_provider_binding(
            sc[0],
            actor_id=actor,
            recipe_name=recipe_name,
            recipe_version=recipe_version,
            logical_name=logical_name,
            payload=payload,
            idempotency_key=idempotency_key,
            create_only=create_only,
            expected_version=expected_version,
        )
        await sc[0].commit()
    except PipelineApiError as exc:
        await sc[0].rollback()
        raise _error(exc) from exc
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    response.headers["ETag"] = f'"{value["snapshot_sha256"]}"'
    response.headers["X-Config-Version"] = str(value["version"])
    return value


@router.post("/pipeline-runs")
async def submit_pipeline_run(
    request: Request,
    response: Response,
    sc: SessionAndCtx,
    payload: PipelineRunSubmitRequestV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> dict[str, Any]:
    team_id, user_id = _team_and_user(sc, mutation=True)
    assert user_id is not None
    try:
        _validate_key(idempotency_key)
        body, replay = await create_public_run(
            sc[0],
            team_id=team_id,
            user_id=user_id,
            idempotency_key=idempotency_key,
            request=payload,
            registry=_registry(request),
            binding_resolver=getattr(request.app.state, "pipeline_binding_resolver", None),
        )
        await sc[0].commit()
    except PipelineApiError as exc:
        await sc[0].rollback()
        raise _error(exc) from exc
    response.status_code = 200 if replay else 201
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    return body


@router.get("/pipeline-runs", response_model=PipelineRunListResponseV1)
async def list_pipeline_runs(
    request: Request,
    sc: SessionAndCtx,
    state: Annotated[str | None, Query()] = None,
    result: Annotated[str | None, Query()] = None,
    recipe: Annotated[str | None, Query()] = None,
    created_after: Annotated[datetime | None, Query()] = None,
    created_before: Annotated[datetime | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
) -> dict[str, Any]:
    team_id, _ = _team_and_user(sc, mutation=False)
    try:
        query = PipelineRunListQueryV1.model_validate(
            {
                "state": state,
                "result": result,
                "recipe": recipe,
                "created_after": created_after,
                "created_before": created_before,
                "cursor": cursor,
                "limit": limit,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail={"reason_code": "invalid_query", "message": "Invalid Pipeline list query"},
        ) from exc
    filter_value = query.model_dump(mode="json", exclude={"cursor", "limit"})
    filter_digest = canonical_digest(filter_value)
    statement = select(PipelineRun).where(PipelineRun.team_id == team_id)
    if query.state is not None:
        statement = statement.where(PipelineRun.state == query.state.value)
    if query.result is not None:
        statement = statement.where(PipelineRun.result == query.result.value)
    if query.recipe is not None:
        name, version = query.recipe.rsplit("@", 1)
        statement = statement.where(
            PipelineRun.recipe_name == name, PipelineRun.recipe_version == int(version)
        )
    if query.created_after is not None:
        statement = statement.where(PipelineRun.created_at >= query.created_after)
    if query.created_before is not None:
        statement = statement.where(PipelineRun.created_at < query.created_before)
    if query.cursor is not None:
        timestamp, item_id = decode_pipeline_cursor(
            query.cursor, filter_digest=filter_digest, signing_key=_cursor_key(request)
        )
        statement = statement.where(
            or_(
                PipelineRun.created_at < timestamp,
                and_(PipelineRun.created_at == timestamp, PipelineRun.id < item_id),
            )
        )
    rows = list(
        (
            await sc[0].execute(
                statement.order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc()).limit(
                    query.limit + 1
                )
            )
        ).scalars()
    )
    has_more = len(rows) > query.limit
    rows = rows[: query.limit]
    next_cursor = None
    if has_more and rows:
        next_cursor = encode_pipeline_cursor(
            created_at=rows[-1].created_at,
            item_id=rows[-1].id,
            filter_digest=filter_digest,
            signing_key=_cursor_key(request),
        )
    stages_by_run: dict[UUID, list[PipelineStageRun]] = {item.id: [] for item in rows}
    ledgers_by_run: dict[UUID, PipelineBudgetLedger] = {}
    if rows:
        stage_rows = list(
            (
                await sc[0].execute(
                    select(PipelineStageRun).where(
                        PipelineStageRun.pipeline_run_id.in_([item.id for item in rows])
                    )
                )
            ).scalars()
        )
        for stage in stage_rows:
            stages_by_run.setdefault(stage.pipeline_run_id, []).append(stage)
        ledger_rows = list(
            (
                await sc[0].execute(
                    select(PipelineBudgetLedger).where(
                        PipelineBudgetLedger.pipeline_run_id.in_([item.id for item in rows])
                    )
                )
            ).scalars()
        )
        ledgers_by_run = {item.pipeline_run_id: item for item in ledger_rows}
    return {
        "items": [
            _run_list_projection(
                item,
                stages_by_run.get(item.id, []),
                ledgers_by_run.get(item.id),
            )
            for item in rows
        ],
        "next_cursor": next_cursor,
    }


@router.get("/pipeline-runs/{run_id}", response_model=PipelineRunDetailV1)
async def get_pipeline_run(sc: SessionAndCtx, run_id: UUID) -> dict[str, Any]:
    run = await _run_for_team(sc, run_id)
    stages = list(
        (
            await sc[0].execute(
                select(PipelineStageRun)
                .where(PipelineStageRun.pipeline_run_id == run.id)
                .order_by(PipelineStageRun.node_key, PipelineStageRun.shard_key)
            )
        ).scalars()
    )
    artifacts = list(
        (
            await sc[0].execute(
                select(Artifact)
                .where(Artifact.pipeline_run_id == run.id)
                .order_by(Artifact.created_at, Artifact.id)
            )
        ).scalars()
    )
    ledger = await sc[0].get(PipelineBudgetLedger, run.id)
    body = run_projection(run)
    topology = _graph_topology(run.graph_spec_json)
    stage_projections = [
        _stage_summary_projection(item, topology=topology, run_state=run.state) for item in stages
    ]
    body["stages"] = sorted(
        stage_projections,
        key=lambda item: (
            item["topological_level"],
            item["node_key"].encode("utf-8"),
            item["shard_key"].encode("utf-8"),
        ),
    )
    body["artifacts"] = [
        _artifact_projection(item)
        for item in artifacts
        if _artifact_read_allowed_for_context(item, run=run, sc=sc)
    ]
    body["budget"] = _budget_projection(ledger, run)
    return body


@router.post("/pipeline-runs/{run_id}/cancel")
async def cancel_pipeline_run(
    sc: SessionAndCtx, run_id: UUID, payload: PipelineRunCancelRequestV1
) -> JSONResponse:
    _team_id, user_id = _team_and_user(sc, mutation=True)
    assert user_id is not None
    run = await _run_for_team(sc, run_id)
    try:
        body, replay = await request_user_cancellation(
            sc[0], run=run, actor_id=user_id, reason=payload.reason
        )
        await sc[0].commit()
    except PipelineApiError as exc:
        await sc[0].rollback()
        raise _error(exc) from exc
    return JSONResponse(status_code=200 if replay else 202, content=body)


@router.get("/pipeline-runs/{run_id}/events", response_model=PipelineRunEventsResponseV1)
async def pipeline_run_events(
    sc: SessionAndCtx,
    run_id: UUID,
    after_seq: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
) -> dict[str, Any]:
    run = await _run_for_team(sc, run_id)
    query = PipelineRunEventsQueryV1(after_seq=after_seq, limit=limit)
    rows = list(
        (
            await sc[0].execute(
                select(PipelineEvent)
                .where(PipelineEvent.pipeline_run_id == run.id, PipelineEvent.seq > query.after_seq)
                .order_by(PipelineEvent.seq)
                .limit(query.limit)
            )
        ).scalars()
    )
    return {
        "events": [
            {
                "seq": item.seq,
                "stage_run_id": str(item.stage_run_id) if item.stage_run_id else None,
                "execution_attempt_id": str(item.execution_attempt_id)
                if item.execution_attempt_id
                else None,
                "event_type": item.event_type,
                "payload": item.payload_json,
                "created_at": item.created_at.isoformat(),
            }
            for item in rows
        ],
        "next_after_seq": rows[-1].seq if rows else after_seq,
        "terminal": run.state == "finished",
        "retry_after_ms": None if run.state == "finished" else 1000,
    }


async def _stage_for_team(sc: SessionAndCtx, stage_run_id: UUID) -> PipelineStageRun:
    team_id, _ = _team_and_user(sc, mutation=False)
    stage = (
        await sc[0].execute(
            select(PipelineStageRun)
            .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
            .where(PipelineStageRun.id == stage_run_id, PipelineRun.team_id == team_id)
        )
    ).scalar_one_or_none()
    if stage is None:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Pipeline stage was not found"},
        )
    return stage


@router.get("/pipeline-stage-runs/{stage_run_id}", response_model=PipelineStageRunDetailV1)
async def get_pipeline_stage_run(sc: SessionAndCtx, stage_run_id: UUID) -> dict[str, Any]:
    stage = await _stage_for_team(sc, stage_run_id)
    run = (
        await sc[0].execute(select(PipelineRun).where(PipelineRun.id == stage.pipeline_run_id))
    ).scalar_one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail={"reason_code": "not_found"})
    artifacts = list(
        (
            await sc[0].execute(
                select(Artifact)
                .where(Artifact.pipeline_stage_run_id == stage.id)
                .order_by(Artifact.created_at, Artifact.id)
            )
        ).scalars()
    )
    return _stage_detail_projection(
        stage,
        topology=_graph_topology(run.graph_spec_json),
        run_state=run.state,
        artifacts=[
            artifact
            for artifact in artifacts
            if _artifact_read_allowed_for_context(artifact, run=run, sc=sc)
        ],
    )


@router.get(
    "/pipeline-stage-runs/{stage_run_id}/attempts",
    response_model=PipelineExecutionAttemptListV1,
)
async def list_pipeline_stage_attempts(sc: SessionAndCtx, stage_run_id: UUID) -> dict[str, Any]:
    stage = await _stage_for_team(sc, stage_run_id)
    rows = list(
        (
            await sc[0].execute(
                select(ExecutionAttempt, Worker.pool_name)
                .outerjoin(Worker, Worker.id == ExecutionAttempt.worker_id)
                .where(ExecutionAttempt.stage_run_id == stage.id)
                .order_by(ExecutionAttempt.attempt_number)
            )
        ).all()
    )
    return {
        "items": [
            _attempt_projection(attempt, worker_pool_class=worker_pool_class)
            for attempt, worker_pool_class in rows
        ]
    }


async def _preview_for_team(
    sc: SessionAndCtx,
    *,
    run_id: UUID,
    stage_run_id: UUID,
    attempt_id: UUID,
) -> tuple[PipelineLivePreviewGeneration, ExecutionAttempt, PipelineStageRun]:
    team_id, _ = _team_and_user(sc, mutation=False)
    row = (
        await sc[0].execute(
            select(PipelineLivePreviewGeneration, ExecutionAttempt, PipelineStageRun)
            .join(
                ExecutionAttempt,
                ExecutionAttempt.id == PipelineLivePreviewGeneration.execution_attempt_id,
            )
            .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
            .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
            .where(
                PipelineLivePreviewGeneration.execution_attempt_id == attempt_id,
                PipelineLivePreviewGeneration.pipeline_run_id == run_id,
                PipelineLivePreviewGeneration.pipeline_stage_run_id == stage_run_id,
                PipelineLivePreviewGeneration.team_id == team_id,
                PipelineRun.team_id == team_id,
                PipelineStageRun.id == stage_run_id,
                ExecutionAttempt.stage_run_id == stage_run_id,
            )
        )
    ).one_or_none()
    if row is None:
        PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="rejected", reason="not_found").inc()
        raise HTTPException(status_code=404, detail={"reason_code": "not_found"})
    generation, attempt, stage = row
    now = datetime.now(UTC)
    invalid = (
        generation.purged_at is not None
        or generation.expires_at <= now
        or generation.generation != attempt.id
        or generation.worker_id != attempt.worker_id
        or generation.claim_id != attempt.claim_id
        or generation.lease_epoch != attempt.lease_epoch
        or not is_stage1_live_preview_eligible(stage.resolved_execution_spec_json)
    )
    active_state = attempt.state == "running" and attempt.cancellation_requested_at is None
    handoff_state = attempt.state == "succeeded" and generation.state == "handoff"
    if invalid or not (active_state or handoff_state):
        PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="rejected", reason="stale").inc()
        raise HTTPException(status_code=404, detail={"reason_code": "not_found"})
    return generation, attempt, stage


@router.get(
    "/pipeline-runs/{run_id}/stages/{stage_run_id}/attempts/{attempt_id}/live-preview",
    response_model=LivePreviewMetadataV1,
)
async def get_pipeline_live_preview_metadata(
    sc: SessionAndCtx,
    run_id: UUID,
    stage_run_id: UUID,
    attempt_id: UUID,
) -> dict[str, Any]:
    generation, attempt, _stage = await _preview_for_team(
        sc, run_id=run_id, stage_run_id=stage_run_id, attempt_id=attempt_id
    )
    state = "handoff" if attempt.state == "succeeded" else generation.state
    PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="accepted", reason="metadata").inc()
    return {
        "schema_version": "loom.behavior-stage1-live-preview.v1",
        "state": state,
        "attempt_id": attempt_id,
        "generation": generation.generation,
        "latest_sequence": generation.latest_sequence,
        "latest_step_idx": int(generation.latest_step_idx)
        if generation.latest_step_idx is not None
        else None,
        "received_at": generation.received_at,
        "retry_after_ms": 500,
    }


async def _read_pipeline_live_preview_frame(
    *,
    sc: SessionAndCtx,
    run_id: UUID,
    stage_run_id: UUID,
    attempt_id: UUID,
    sequence: int,
    method: Literal["GET", "HEAD"],
    if_none_match: str | None,
    range_header: str | None,
) -> Response:
    if range_header is not None:
        PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="rejected", reason="range").inc()
        raise HTTPException(status_code=416, detail={"reason_code": "range_not_supported"})
    generation, _attempt, _stage = await _preview_for_team(
        sc, run_id=run_id, stage_run_id=stage_run_id, attempt_id=attempt_id
    )
    if sequence != generation.latest_sequence:
        PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="rejected", reason="sequence").inc()
        raise HTTPException(status_code=404, detail={"reason_code": "not_found"})
    frame = await sc[0].get(PipelineLivePreviewFrame, (attempt_id, sequence))
    if frame is None:
        PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="rejected", reason="missing").inc()
        raise HTTPException(status_code=404, detail={"reason_code": "not_found"})
    etag = f'"{frame.jpeg_sha256}"'
    headers = {
        "Cache-Control": "private, no-store",
        "Content-Security-Policy": "default-src 'none'; sandbox",
        "X-Content-Type-Options": "nosniff",
        "Content-Length": str(frame.jpeg_size_bytes),
        "ETag": etag,
    }
    if if_none_match == etag:
        PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="accepted", reason="not_modified").inc()
        return Response(status_code=304, headers=headers)
    PIPELINE_LIVE_PREVIEW_READS_TOTAL.labels(result="accepted", reason=method.lower()).inc()
    return Response(
        status_code=200,
        content=b"" if method == "HEAD" else frame.jpeg_bytes,
        media_type="image/jpeg",
        headers=headers,
    )


@router.get(
    "/pipeline-runs/{run_id}/stages/{stage_run_id}/attempts/{attempt_id}"
    "/live-preview/frames/{sequence}",
    response_class=Response,
    responses=_LIVE_PREVIEW_FRAME_RESPONSES,
)
async def get_pipeline_live_preview_frame(
    sc: SessionAndCtx,
    run_id: UUID,
    stage_run_id: UUID,
    attempt_id: UUID,
    sequence: Annotated[int, Path(ge=0)],
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
) -> Response:
    return await _read_pipeline_live_preview_frame(
        sc=sc,
        run_id=run_id,
        stage_run_id=stage_run_id,
        attempt_id=attempt_id,
        sequence=sequence,
        method="GET",
        if_none_match=if_none_match,
        range_header=range_header,
    )


@router.head(
    "/pipeline-runs/{run_id}/stages/{stage_run_id}/attempts/{attempt_id}"
    "/live-preview/frames/{sequence}",
    response_class=Response,
    responses=_LIVE_PREVIEW_FRAME_RESPONSES,
)
async def head_pipeline_live_preview_frame(
    sc: SessionAndCtx,
    run_id: UUID,
    stage_run_id: UUID,
    attempt_id: UUID,
    sequence: Annotated[int, Path(ge=0)],
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
) -> Response:
    return await _read_pipeline_live_preview_frame(
        sc=sc,
        run_id=run_id,
        stage_run_id=stage_run_id,
        attempt_id=attempt_id,
        sequence=sequence,
        method="HEAD",
        if_none_match=if_none_match,
        range_header=range_header,
    )


@router.post("/pipeline-stage-runs/{stage_run_id}/retry")
async def retry_pipeline_stage(
    request: Request,
    response: Response,
    sc: SessionAndCtx,
    stage_run_id: UUID,
    payload: PipelineRunRetryRequestV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> dict[str, Any]:
    team_id, user_id = _team_and_user(sc, mutation=True)
    assert user_id is not None
    _validate_key(idempotency_key)
    stage = await _stage_for_team(sc, stage_run_id)
    try:
        body, replay = await create_retry_run(
            sc[0],
            team_id=team_id,
            user_id=user_id,
            stage=stage,
            idempotency_key=idempotency_key,
            request=payload,
            registry=_registry(request),
            binding_resolver=getattr(request.app.state, "pipeline_binding_resolver", None),
        )
        await sc[0].commit()
    except PipelineApiError as exc:
        await sc[0].rollback()
        raise _error(exc) from exc
    response.status_code = 200 if replay else 201
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    return body


@router.get("/pipeline-recipes")
async def list_pipeline_recipes(request: Request, sc: SessionAndCtx) -> dict[str, Any]:
    _team_and_user(sc, mutation=False)
    items = []
    for identity in _registry(request).list_identities():
        registration = _registry(request).get(identity.name, identity.version)
        if registration.submission_policy == "ordinary" or is_admin(sc[1]):
            items.append(_recipe_projection(registration))
    return {"items": items}


@router.get("/pipeline-recipes/{name}/{version}")
async def get_pipeline_recipe(
    request: Request, sc: SessionAndCtx, name: str, version: int
) -> dict[str, Any]:
    _team_and_user(sc, mutation=False)
    try:
        registration = _registry(request).get(name, version)
    except KeyError as exc:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Pipeline Recipe was not found"},
        ) from exc
    if registration.submission_policy != "ordinary" and not is_admin(sc[1]):
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Pipeline Recipe was not found"},
        )
    return _recipe_projection(registration)


@router.get("/pipeline-recipes/{name}/{version}/judge-profiles")
async def list_judge_profiles(
    request: Request, sc: SessionAndCtx, name: str, version: int
) -> dict[str, Any]:
    team_id, _ = _team_and_user(sc, mutation=False)
    resolver = getattr(request.app.state, "pipeline_judge_profile_reader", None)
    if resolver is None:
        return {"items": []}
    return {"items": await resolver.list(team_id=team_id, recipe_name=name, recipe_version=version)}


class PipelineInputImportCreateV1(PipelineModel):
    kind: Literal["dataset", "policy", "mop_bank"]
    manifest: BehaviorInputImportManifestV1
    recipe: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{0,127}@[1-9][0-9]{0,9}$")]

    @model_validator(mode="after")
    def fixed_behavior_import(self) -> PipelineInputImportCreateV1:
        if self.recipe != "behavior-recovery@1":
            raise ValueError("input imports are registered only for behavior-recovery@1")
        if self.kind != self.manifest.kind:
            raise ValueError("request kind differs from the signed import manifest")
        return self


class PipelineInputImportRenewV1(PipelineModel):
    upload_session_id: UUID


class PipelineInputImportCompleteV1(PipelineModel):
    upload_session_id: UUID
    bundle_sha256: Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
    bundle_size_bytes: Annotated[int, Field(strict=True, ge=1)]
    parts: list[PartReceiptV1]


class PipelineInputImportAbortV1(PipelineModel):
    reason: str
    upload_session_id: UUID


class PipelineMaterializeInputsV1(PipelineModel):
    inputs: dict[str, UUID]
    parameters: dict[str, Any]
    task_set_id: UUID

    @model_validator(mode="before")
    @classmethod
    def normalize_behavior_parameters(cls, value: Any) -> Any:
        if not isinstance(value, dict) or not isinstance(value.get("parameters"), dict):
            return value
        result = dict(value)
        parameters = dict(result["parameters"])
        expected = {"episodes_per_instance", "seed_base"}
        if set(parameters) - expected:
            raise ValueError("behavior parameters contain extra fields")
        episodes = parameters.get("episodes_per_instance", 1)
        seed = parameters.get("seed_base", 0)
        if isinstance(episodes, bool) or not isinstance(episodes, int) or not 1 <= episodes <= 10:
            raise ValueError("episodes_per_instance must be an integer from 1 through 10")
        if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= 4_294_967_295:
            raise ValueError("seed_base must be an unsigned 32-bit integer")
        result["parameters"] = {"episodes_per_instance": episodes, "seed_base": seed}
        return result

    @model_validator(mode="after")
    def exact_behavior_inputs(self) -> PipelineMaterializeInputsV1:
        if set(self.inputs) != {"dataset", "policy", "mop_bank"}:
            raise ValueError("behavior-recovery requires exactly dataset, policy, and mop_bank")
        return self


class PipelineMaterializedGraphInputV1(PipelineModel):
    name: Literal["task_set", "task_instances", "dataset", "policy", "mop_bank"]
    artifact_id: UUID
    artifact_type: ArtifactType
    manifest_sha256: Digest


class PipelineMaterializeInputsResponseV1(PipelineModel):
    materialization_id: UUID
    state: Literal["committed"]
    results: Annotated[list[PipelineMaterializedGraphInputV1], Field(min_length=5, max_length=5)]

    @model_validator(mode="after")
    def exact_five_refs(self) -> PipelineMaterializeInputsResponseV1:
        expected = ["task_set", "task_instances", "dataset", "policy", "mop_bank"]
        if [item.name for item in self.results] != expected:
            raise ValueError("materialization response must contain the exact five graph inputs")
        return self


async def _adapter_call(app_request: Request, name: str, **kwargs: Any) -> Any:
    adapter = getattr(app_request.app.state, "pipeline_public_adapter", None)
    method = getattr(adapter, name, None)
    if method is None:
        raise HTTPException(
            status_code=503,
            detail={
                "reason_code": "adapter_unavailable",
                "message": "Pipeline adapter is not configured",
            },
        )
    try:
        return await method(**kwargs)
    except PipelineApiError as exc:
        raise _error(exc) from exc


@router.post("/pipeline-input-imports")
async def create_pipeline_input_import(
    request: Request,
    sc: SessionAndCtx,
    payload: PipelineInputImportCreateV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> Any:
    team_id, user_id = _team_and_user(sc, mutation=True)
    await _require_team_admin(sc)
    _validate_key(idempotency_key)
    return await _adapter_call(
        request,
        "create_import",
        session=sc[0],
        team_id=team_id,
        user_id=user_id,
        payload=payload,
        idempotency_key=idempotency_key,
    )


@router.put("/pipeline-input-imports/{import_id}/parts/{part_number}")
async def put_pipeline_input_part(
    request: Request,
    sc: SessionAndCtx,
    import_id: UUID,
    part_number: Annotated[int, Field(ge=1, le=9990)],
    upload_session_id: Annotated[UUID, Header(alias="X-Loom-Upload-Session-Id")],
    upload_token: Annotated[str, Header(alias="X-Loom-Upload-Token")],
    content_length: Annotated[int, Header(alias="Content-Length", ge=1)],
    content_sha256: Annotated[
        str, Header(alias="X-Loom-Content-SHA256", pattern=r"^sha256:[0-9a-f]{64}$")
    ],
) -> Any:
    team_id, user_id = _team_and_user(sc, mutation=True)
    await _require_team_admin(sc)
    return await _adapter_call(
        request,
        "put_import_part",
        session=sc[0],
        team_id=team_id,
        user_id=user_id,
        import_id=import_id,
        part_number=part_number,
        upload_session_id=upload_session_id,
        upload_token=upload_token,
        content_length=content_length,
        content_sha256=content_sha256,
        body=request.stream(),
    )


@router.post("/pipeline-input-imports/{import_id}/renew-upload-token")
async def renew_pipeline_import_token(
    request: Request, sc: SessionAndCtx, import_id: UUID, payload: PipelineInputImportRenewV1
) -> Any:
    team_id, user_id = _team_and_user(sc, mutation=True)
    await _require_team_admin(sc)
    return await _adapter_call(
        request,
        "renew_import",
        session=sc[0],
        team_id=team_id,
        user_id=user_id,
        import_id=import_id,
        payload=payload,
    )


@router.post("/pipeline-input-imports/{import_id}/complete")
async def complete_pipeline_import(
    request: Request,
    sc: SessionAndCtx,
    import_id: UUID,
    payload: PipelineInputImportCompleteV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> Any:
    team_id, user_id = _team_and_user(sc, mutation=True)
    await _require_team_admin(sc)
    _validate_key(idempotency_key)
    return await _adapter_call(
        request,
        "complete_import",
        session=sc[0],
        team_id=team_id,
        user_id=user_id,
        import_id=import_id,
        payload=payload,
        idempotency_key=idempotency_key,
    )


@router.post("/pipeline-input-imports/{import_id}/abort")
async def abort_pipeline_import(
    request: Request,
    sc: SessionAndCtx,
    import_id: UUID,
    payload: PipelineInputImportAbortV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> Any:
    team_id, user_id = _team_and_user(sc, mutation=True)
    await _require_team_admin(sc)
    _validate_key(idempotency_key)
    return await _adapter_call(
        request,
        "abort_import",
        session=sc[0],
        team_id=team_id,
        user_id=user_id,
        import_id=import_id,
        payload=payload,
        idempotency_key=idempotency_key,
    )


@router.post(
    "/pipeline-recipes/{name}/{version}/materialize-inputs",
    response_model=PipelineMaterializeInputsResponseV1,
)
async def materialize_pipeline_inputs(
    request: Request,
    sc: SessionAndCtx,
    name: str,
    version: int,
    payload: PipelineMaterializeInputsV1,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> Any:
    team_id, user_id = _team_and_user(sc, mutation=True)
    _validate_key(idempotency_key)
    return await _adapter_call(
        request,
        "materialize_inputs",
        session=sc[0],
        team_id=team_id,
        user_id=user_id,
        recipe_name=name,
        recipe_version=version,
        payload=payload,
        idempotency_key=idempotency_key,
    )


@router.get("/pipeline-artifacts/{artifact_id}/download")
async def download_pipeline_artifact(
    request: Request, sc: SessionAndCtx, artifact_id: UUID
) -> StreamingResponse:
    team_id, _ = _team_and_user(sc, mutation=False)
    artifact = (
        await sc[0].execute(
            select(Artifact).where(
                Artifact.id == artifact_id,
                Artifact.team_id == team_id,
                Artifact.pipeline_run_id.is_not(None),
            )
        )
    ).scalar_one_or_none()
    if artifact is None or artifact.artifact_upload_session_id is None:
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Pipeline Artifact was not found"},
        )
    run = await sc[0].get(PipelineRun, artifact.pipeline_run_id)
    if run is None or not _artifact_read_allowed_for_context(artifact, run=run, sc=sc):
        raise HTTPException(
            status_code=404,
            detail={"reason_code": "not_found", "message": "Pipeline Artifact was not found"},
        )
    upload = await sc[0].get(ArtifactUploadSession, artifact.artifact_upload_session_id)
    files = artifact.storage.get("files") if isinstance(artifact.storage, dict) else None
    if upload is None or not isinstance(files, list) or not files:
        raise HTTPException(
            status_code=409,
            detail={
                "reason_code": "artifact_unavailable",
                "message": "Pipeline Artifact is not downloadable",
            },
        )
    selected = next(
        (item for item in files if item.get("relative_path") == "artifact.json"),
        files[0] if len(files) == 1 else None,
    )
    if not isinstance(selected, dict) or not isinstance(selected.get("relative_path"), str):
        raise HTTPException(
            status_code=409,
            detail={
                "reason_code": "artifact_requires_file_selection",
                "message": "Artifact contains multiple files",
            },
        )
    relative_path = cast(str, selected["relative_path"])
    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=request.app.state.settings.artifacts_bucket,
        key=f"{upload.prefix}artifacts/{artifact.id}/{relative_path}",
        filename=relative_path.rsplit("/", 1)[-1],
        artifact_kind="pipeline_artifact",
        media_type=cast(str, selected.get("media_type") or "application/octet-stream"),
    )


@router.get(
    "/pipeline-runs/{run_id}/stages/{stage_run_id}/artifacts/{artifact_id}",
    response_model=PipelineArtifactDetailV1,
)
async def get_pipeline_artifact(
    request: Request,
    sc: SessionAndCtx,
    run_id: UUID,
    stage_run_id: UUID,
    artifact_id: UUID,
) -> dict[str, Any]:
    team_id, user_id = _team_and_user(sc, mutation=False)
    resolved = await resolve_public_artifact(
        sc[0],
        team_id=team_id,
        artifact_id=artifact_id,
        user_id=user_id,
        role=sc[1].role,
        platform_admin=is_admin(sc[1]),
        run_id=run_id,
        stage_run_id=stage_run_id,
    )
    await validate_public_artifact(
        resolved,
        client=request.app.state.minio_client,
        bucket=request.app.state.settings.artifacts_bucket,
    )
    return public_artifact_projection(resolved)


async def _read_pipeline_artifact_file(
    request: Request,
    sc: SessionAndCtx,
    artifact_id: UUID,
    file_index: int,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    if_range: Annotated[str | None, Header(alias="If-Range")] = None,
) -> Response:
    team_id, user_id = _team_and_user(sc, mutation=False)
    resolved = await resolve_public_artifact(
        sc[0],
        team_id=team_id,
        artifact_id=artifact_id,
        user_id=user_id,
        role=sc[1].role,
        platform_admin=is_admin(sc[1]),
    )
    return await stream_public_artifact_file(
        resolved,
        file_index=file_index,
        method=request.method,
        range_header=range_header,
        if_none_match=if_none_match,
        if_range=if_range,
        client=request.app.state.minio_client,
        bucket=request.app.state.settings.artifacts_bucket,
    )


@router.get(
    "/pipeline-artifacts/{artifact_id}/files/{file_index}",
    response_class=Response,
    responses=_ARTIFACT_FILE_RESPONSES,
)
async def get_pipeline_artifact_file(
    request: Request,
    sc: SessionAndCtx,
    artifact_id: UUID,
    file_index: int,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    if_range: Annotated[str | None, Header(alias="If-Range")] = None,
) -> Response:
    return await _read_pipeline_artifact_file(
        request,
        sc,
        artifact_id,
        file_index,
        range_header,
        if_none_match,
        if_range,
    )


@router.head(
    "/pipeline-artifacts/{artifact_id}/files/{file_index}",
    response_class=Response,
    responses=_ARTIFACT_FILE_RESPONSES,
)
async def head_pipeline_artifact_file(
    request: Request,
    sc: SessionAndCtx,
    artifact_id: UUID,
    file_index: int,
    range_header: Annotated[str | None, Header(alias="Range")] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    if_range: Annotated[str | None, Header(alias="If-Range")] = None,
) -> Response:
    return await _read_pipeline_artifact_file(
        request,
        sc,
        artifact_id,
        file_index,
        range_header,
        if_none_match,
        if_range,
    )


def _recipe_projection(item: Any) -> dict[str, Any]:
    return {
        "name": item.name,
        "version": item.version,
        "digest": item.digest,
        "submission_policy": item.submission_policy,
        "parameter_contract_digest": item.parameter_contract_digest,
        "source_lock_digest": item.source_lock_digest,
        "renderer_locks": [
            {"name": lock.name, "version": lock.version, "digest": canonical_digest(lock)}
            for lock in item.renderer_locks
        ],
    }


def _graph_topology(graph: dict[str, Any]) -> dict[str, tuple[int, list[str]]]:
    """Compute the UI-safe immutable node topology without returning raw graph JSON."""

    raw_nodes = graph.get("nodes") if isinstance(graph, dict) else None
    if not isinstance(raw_nodes, list):
        return {}
    needs_by_key: dict[str, list[str]] = {}
    for raw in raw_nodes:
        if not isinstance(raw, dict) or not isinstance(raw.get("node_key"), str):
            continue
        raw_needs = raw.get("needs", [])
        if not isinstance(raw_needs, list) or not all(
            isinstance(value, str) for value in raw_needs
        ):
            continue
        needs_by_key[raw["node_key"]] = sorted(set(raw_needs), key=lambda value: value.encode())

    levels: dict[str, int] = {}

    def resolve(key: str, active: frozenset[str] = frozenset()) -> int:
        if key in levels:
            return levels[key]
        if key in active:
            return 0
        upstream = needs_by_key.get(key, [])
        level = 0 if not upstream else 1 + max(resolve(value, active | {key}) for value in upstream)
        levels[key] = level
        return level

    return {key: (resolve(key), needs) for key, needs in needs_by_key.items()}


def _resource_projection(item: PipelineStageRun) -> tuple[str | None, str]:
    if item.node_kind == "gate":
        return None, "controller"
    raw_profile = getattr(item, "resource_profile_json", None)
    profile = raw_profile if isinstance(raw_profile, dict) else {}
    if isinstance(profile.get("name"), str) and isinstance(profile.get("version"), int):
        name = f"{profile['name']}@{profile['version']}"
    elif isinstance(profile.get("resource_profile"), str):
        name = cast(str, profile["resource_profile"])
    else:
        name = None
    variants = profile.get("execution_variants")
    has_gpu = bool(
        isinstance(variants, list)
        and any(
            isinstance(variant, dict)
            and isinstance(variant.get("gpu_count_exact"), int)
            and variant["gpu_count_exact"] > 0
            for variant in variants
        )
    )
    if not has_gpu and name is not None and "gpu" in name:
        has_gpu = True
    return name, "gpu" if has_gpu else "cpu"


def _stage_summary_projection(
    item: PipelineStageRun,
    *,
    topology: dict[str, tuple[int, list[str]]],
    run_state: str,
) -> dict[str, Any]:
    level, upstream = topology.get(item.node_key, (0, []))
    resource_name, resource_class = _resource_projection(item)
    retry_allowed = item.state == "failed" and run_state == "finished"
    ineligible = None
    if not retry_allowed:
        ineligible = "run_not_retryable" if item.state == "failed" else "stage_not_failed"
    return {
        "id": str(item.id),
        "node_key": item.node_key,
        "shard_key": item.shard_key,
        "node_kind": item.node_kind,
        "topological_level": level,
        "upstream_node_keys": upstream,
        "state": item.state,
        "domain_outcome": item.domain_outcome,
        "reason_code": item.reason_code,
        "attempt_count": item.attempt_count,
        "resource_profile_name": resource_name,
        "resource_class": resource_class,
        "retry_allowed": retry_allowed,
        "retry_ineligible_reason": ineligible,
    }


def _stage_detail_projection(
    item: PipelineStageRun,
    *,
    topology: dict[str, tuple[int, list[str]]],
    run_state: str,
    artifacts: list[Artifact],
) -> dict[str, Any]:
    return {
        **_stage_summary_projection(item, topology=topology, run_state=run_state),
        "pipeline_run_id": str(item.pipeline_run_id),
        "execution_spec_digest": item.execution_spec_digest,
        "input_bindings_digest": item.resolved_input_bindings_digest,
        "resource_profile_digest": item.resource_profile_digest,
        "request_renderer_digest": item.request_renderer_digest,
        "latest_checkpoint_artifact_id": str(item.latest_checkpoint_artifact_id)
        if item.latest_checkpoint_artifact_id
        else None,
        "live_preview_eligible": is_stage1_live_preview_eligible(item.resolved_execution_spec_json),
        "artifacts": [_artifact_projection(artifact) for artifact in artifacts],
    }


def _attempt_projection(
    item: ExecutionAttempt, *, worker_pool_class: str | None = None
) -> dict[str, Any]:
    def iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    return {
        "id": str(item.id),
        "attempt_number": item.attempt_number,
        "state": item.state,
        "worker_id": str(item.worker_id) if getattr(item, "worker_id", None) else None,
        "worker_pool_class": worker_pool_class,
        "queued_at": iso(getattr(item, "queued_at", None)),
        "claimed_at": iso(getattr(item, "claimed_at", None)),
        "started_at": iso(getattr(item, "started_at", None)),
        "finished_at": iso(getattr(item, "finished_at", None)),
        "exit_code": getattr(item, "exit_code", None),
        "retry_class": getattr(item, "retry_class", None),
        "reason_code": getattr(item, "reason_code", None),
        "stage_request_digest": item.stage_request_digest,
        "result_manifest_digest": item.result_manifest_digest,
        "resumed_checkpoint_artifact_id": str(item.resumed_checkpoint_artifact_id)
        if getattr(item, "resumed_checkpoint_artifact_id", None)
        else None,
        "cancellation_observed_at": iso(getattr(item, "cancellation_observed_at", None)),
        "cancellation_outcome": getattr(item, "cancellation_outcome", None),
        "cleanup_acknowledged_at": iso(getattr(item, "cleanup_acknowledged_at", None)),
        "cleanup_proof_digest": getattr(item, "cleanup_proof_digest", None),
    }


def _artifact_projection(item: Artifact) -> dict[str, Any]:
    pipeline_run_id = getattr(item, "pipeline_run_id", None)
    pipeline_stage_run_id = getattr(item, "pipeline_stage_run_id", None)
    return {
        "id": str(item.id),
        "name": item.name,
        "artifact_type": item.artifact_type,
        "content_sha256": item.content_hash,
        "manifest_sha256": item.manifest_sha256,
        "stored_size_bytes": item.stored_size_bytes,
        "file_count": item.file_count,
        "safety_state": item.safety_state,
        "visibility": getattr(item, "visibility", "team"),
        "share_status": getattr(item, "share_status", "pending_scan"),
        "access_class": getattr(item, "access_class", None) or "team_runtime",
        "download_path": f"/api/v1/pipeline-artifacts/{item.id}/download",
        "detail_path": (
            f"/pipelines/{pipeline_run_id}/stages/{pipeline_stage_run_id}/artifacts/{item.id}"
        ),
        "pipeline_run_id": str(pipeline_run_id) if pipeline_run_id else None,
        "pipeline_stage_run_id": str(pipeline_stage_run_id) if pipeline_stage_run_id else None,
        "execution_attempt_id": str(item.execution_attempt_id)
        if getattr(item, "execution_attempt_id", None)
        else None,
        "producer_kind": getattr(item, "producer_kind", None),
    }


def _artifact_read_allowed_for_context(
    item: Artifact,
    *,
    run: PipelineRun,
    sc: SessionAndCtx,
) -> bool:
    return artifact_read_allowed(
        getattr(item, "access_class", None),
        run_created_by_user_id=getattr(run, "created_by_user_id", None),
        requesting_user_id=sc[1].user_id,
        requesting_role=sc[1].role,
        platform_admin=is_admin(sc[1]),
    )


def _counter(limit: int, reserved: int, settled: int) -> dict[str, int]:
    return {
        "limit": limit,
        "reserved": reserved,
        "settled": settled,
        "remaining": limit - reserved - settled,
    }


def _budget_projection(
    item: PipelineBudgetLedger | None, run: PipelineRun
) -> dict[str, Any] | None:
    if item is None:
        return None
    source = getattr(run, "budget_json", {})
    wall_limit = int(source.get("max_wall_seconds", 1))
    wall_settled = 0
    if run.started_at is not None:
        end = run.finished_at or datetime.now(run.started_at.tzinfo)
        wall_settled = max(0, int((end - run.started_at).total_seconds()))
    wall_deadline_at = getattr(item, "wall_deadline_at", None)
    return {
        "max_wall_seconds": _counter(wall_limit, 0, wall_settled),
        "max_gpu_seconds": _counter(
            item.gpu_limit_seconds, item.gpu_reserved_seconds, item.gpu_settled_seconds
        ),
        "max_provider_cost_usd": _counter(
            item.provider_limit_microusd,
            item.provider_reserved_microusd,
            item.provider_settled_microusd,
        ),
        "max_artifact_bytes": _counter(
            item.artifact_limit_bytes,
            item.artifact_reserved_bytes,
            item.artifact_settled_bytes,
        ),
        "max_stage_runs": _counter(
            getattr(item, "stage_run_limit", max(1, item.stage_runs_created)),
            0,
            item.stage_runs_created,
        ),
        "max_attempts_total": _counter(
            getattr(item, "attempt_limit", max(1, item.attempts_created)),
            0,
            item.attempts_created,
        ),
        "wall_deadline_at": wall_deadline_at.isoformat() if wall_deadline_at else None,
        "terminal_cause": item.terminal_cause,
    }


def _run_list_projection(
    run: PipelineRun,
    stages: list[PipelineStageRun],
    ledger: PipelineBudgetLedger | None,
) -> dict[str, Any]:
    terminal = {"succeeded", "failed", "cancelled", "skipped"}
    outcomes: dict[str, int] = {}
    for stage in stages:
        if stage.state == "succeeded" and stage.domain_outcome is not None:
            outcomes[stage.domain_outcome] = outcomes.get(stage.domain_outcome, 0) + 1
    return {
        "id": str(run.id),
        "display_name": run.display_name,
        "recipe": {
            "name": run.recipe_name,
            "version": run.recipe_version,
            "digest": run.recipe_digest,
        },
        "state": run.state,
        "result": run.result,
        "completed_stage_runs": sum(stage.state in terminal for stage in stages),
        "total_stage_runs": len(stages),
        "domain_outcomes": dict(sorted(outcomes.items(), key=lambda pair: pair[0].encode())),
        "budget": _budget_projection(ledger, run) if ledger else None,
        "created_at": run.created_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }
