"""Team-safe REST surface for official Recipe Pipeline runs."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import Field, StringConstraints, model_validator
from sqlalchemy import and_, or_, select

from loom.db.schema import (
    Artifact,
    ArtifactUploadSession,
    ExecutionAttempt,
    PipelineBudgetLedger,
    PipelineEvent,
    PipelineRun,
    PipelineStageRun,
    TeamMembership,
)
from loom.pipeline.artifact_commit import PartReceiptV1
from loom.pipeline.keys import canonical_digest
from loom.pipeline.public_api import (
    PipelineRunCancelRequestV1,
    PipelineRunEventsQueryV1,
    PipelineRunListQueryV1,
    PipelineRunRetryRequestV1,
    PipelineRunSubmitRequestV1,
    validate_idempotency_key,
)
from loom.pipeline.recipes import OfficialRecipeRegistry
from loom.pipeline.spec import PipelineModel
from loom_service.auth_guards import is_admin, require_scope, require_submitting_user
from loom_service.dependencies import SessionAndCtx
from loom_service.pipeline_api_service import (
    PipelineApiError,
    create_public_run,
    create_retry_run,
    decode_pipeline_cursor,
    encode_pipeline_cursor,
    request_user_cancellation,
    run_projection,
)
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter(tags=["pipeline"])


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


@router.get("/pipeline-runs")
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
    return {"items": [run_projection(item) for item in rows], "next_cursor": next_cursor}


@router.get("/pipeline-runs/{run_id}")
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
    body["stages"] = [_stage_projection(item) for item in stages]
    body["artifacts"] = [_artifact_projection(item) for item in artifacts]
    body["budget"] = _budget_projection(ledger)
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


@router.get("/pipeline-runs/{run_id}/events")
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


@router.get("/pipeline-stage-runs/{stage_run_id}")
async def get_pipeline_stage_run(sc: SessionAndCtx, stage_run_id: UUID) -> dict[str, Any]:
    return _stage_projection(await _stage_for_team(sc, stage_run_id))


@router.get("/pipeline-stage-runs/{stage_run_id}/attempts")
async def list_pipeline_stage_attempts(sc: SessionAndCtx, stage_run_id: UUID) -> dict[str, Any]:
    stage = await _stage_for_team(sc, stage_run_id)
    rows = list(
        (
            await sc[0].execute(
                select(ExecutionAttempt)
                .where(ExecutionAttempt.stage_run_id == stage.id)
                .order_by(ExecutionAttempt.attempt_number)
            )
        ).scalars()
    )
    return {"items": [_attempt_projection(item) for item in rows]}


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
    manifest: dict[str, Any]
    recipe: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{0,127}@[1-9][0-9]{0,9}$")]


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


@router.post("/pipeline-recipes/{name}/{version}/materialize-inputs")
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


def _stage_projection(item: PipelineStageRun) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "pipeline_run_id": str(item.pipeline_run_id),
        "node_key": item.node_key,
        "shard_key": item.shard_key,
        "node_kind": item.node_kind,
        "state": item.state,
        "domain_outcome": item.domain_outcome,
        "reason": item.reason_code,
        "execution_spec_digest": item.execution_spec_digest,
        "input_bindings_digest": item.resolved_input_bindings_digest,
        "resource_profile_digest": item.resource_profile_digest,
        "request_renderer_digest": item.request_renderer_digest,
        "attempt_count": item.attempt_count,
        "latest_checkpoint_artifact_id": str(item.latest_checkpoint_artifact_id)
        if item.latest_checkpoint_artifact_id
        else None,
        "retry_eligible": item.state == "failed",
        "retry_ineligibility_reason": None if item.state == "failed" else "stage_not_failed",
    }


def _attempt_projection(item: ExecutionAttempt) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "attempt_number": item.attempt_number,
        "state": item.state,
        "worker_id": str(item.worker_id) if item.worker_id else None,
        "started_at": item.started_at.isoformat() if item.started_at else None,
        "finished_at": item.finished_at.isoformat() if item.finished_at else None,
        "exit_code": item.exit_code,
        "retry_class": item.retry_class,
        "reason": item.reason_code,
        "stage_request_digest": item.stage_request_digest,
        "result_manifest_digest": item.result_manifest_digest,
        "resumed_checkpoint_artifact_id": str(item.resumed_checkpoint_artifact_id)
        if item.resumed_checkpoint_artifact_id
        else None,
        "cancellation_observed_at": item.cancellation_observed_at.isoformat()
        if item.cancellation_observed_at
        else None,
        "cancellation_outcome": item.cancellation_outcome,
        "cleanup_proof_digest": item.cleanup_proof_digest,
    }


def _artifact_projection(item: Artifact) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "name": item.name,
        "artifact_type": item.artifact_type,
        "content_sha256": item.content_hash,
        "manifest_sha256": item.manifest_sha256,
        "stored_size_bytes": item.stored_size_bytes,
        "file_count": item.file_count,
        "safety_state": item.safety_state,
        "download_path": f"/api/v1/pipeline-artifacts/{item.id}/download",
    }


def _budget_projection(item: PipelineBudgetLedger | None) -> dict[str, Any] | None:
    if item is None:
        return None
    return {
        "provider": {
            "limit_microusd": item.provider_limit_microusd,
            "reserved_microusd": item.provider_reserved_microusd,
            "settled_microusd": item.provider_settled_microusd,
        },
        "gpu": {
            "limit_seconds": item.gpu_limit_seconds,
            "reserved_seconds": item.gpu_reserved_seconds,
            "settled_seconds": item.gpu_settled_seconds,
        },
        "artifact": {
            "limit_bytes": item.artifact_limit_bytes,
            "reserved_bytes": item.artifact_reserved_bytes,
            "settled_bytes": item.artifact_settled_bytes,
        },
        "stage_runs_created": item.stage_runs_created,
        "attempts_created": item.attempts_created,
        "terminal_cause": item.terminal_cause,
    }
