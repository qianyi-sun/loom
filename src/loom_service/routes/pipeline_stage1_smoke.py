"""Hidden, signature-gated BEHAVIOR Stage 1 live-smoke mutations."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, TypeVar, cast

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.exc import IntegrityError

from loom.pipeline.keys import canonical_document
from loom.pipeline.public_api import validate_idempotency_key
from loom.pipeline.spec import PipelineModel
from loom.pipeline.stage1_smoke import (
    Stage1SmokeAuthorizationV1,
    Stage1SmokeCandidateV1,
    Stage1SmokeCleanupV1,
    Stage1SmokePreflightV1,
)
from loom_service.pipeline_stage1_smoke_service import (
    Stage1SmokeCleanupAuthorityV1,
    Stage1SmokeEvidenceAuthorityV1,
    Stage1SmokeEvidenceV1,
    Stage1SmokeExecutionPreflightAuthorityV1,
    Stage1SmokeServiceError,
    Stage1SmokeSignatureVerifier,
    cleanup_signature_payload,
    cleanup_stage1_smoke,
    evidence_signature_payload,
    execute_signature_payload,
    execute_stage1_smoke,
    get_stage1_smoke_replay,
    record_stage1_smoke_evidence,
    stage1_smoke_request_digest,
)

router = APIRouter(include_in_schema=False)
_REPO_ROOT = Path(__file__).resolve().parents[3]


class Stage1SmokeExecuteV1(PipelineModel):
    candidate: Stage1SmokeCandidateV1
    authorization: Stage1SmokeAuthorizationV1
    preflight: Stage1SmokePreflightV1


_ModelT = TypeVar("_ModelT", bound=BaseModel)


def _decode_json_model(payload: dict[str, Any], model: type[_ModelT]) -> _ModelT:
    """Preserve strict Python models while accepting canonical JSON scalars."""

    try:
        return model.model_validate_json(canonical_document(payload))
    except (TypeError, ValueError, ValidationError) as exc:
        raise HTTPException(status_code=422, detail="stage1_smoke_request_invalid") from exc


def _verifier(request: Request) -> Stage1SmokeSignatureVerifier:
    value = getattr(request.app.state, "pipeline_stage1_smoke_verifier", None)
    if not isinstance(value, Stage1SmokeSignatureVerifier):
        raise HTTPException(status_code=503, detail="stage1_smoke_authority_unavailable")
    return value


def _error(exc: Stage1SmokeServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.reason_code)


def _validate_idempotency(value: str) -> None:
    try:
        validate_idempotency_key(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="invalid_idempotency_key") from exc


@router.post("/pipeline-stage1-smoke/execute", status_code=201)
async def execute(
    raw_payload: dict[str, Any],
    request: Request,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    signature_key_id: Annotated[str, Header(alias="X-Loom-Stage1-Signature-Key-Id")],
    signature: Annotated[
        str, Header(alias="X-Loom-Stage1-Signature"), Field(min_length=128, max_length=128)
    ],
) -> dict[str, object]:
    payload = _decode_json_model(raw_payload, Stage1SmokeExecuteV1)
    _validate_idempotency(idempotency_key)
    now = datetime.now(UTC)
    signed = execute_signature_payload(
        candidate=payload.candidate,
        authorization=payload.authorization,
        preflight=payload.preflight,
        idempotency_key=idempotency_key,
    )
    try:
        signature_sha256 = _verifier(request).verify(
            key_id=signature_key_id,
            payload=signed,
            signature=signature,
            observed_at=payload.authorization.authorized_at,
            now=now,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="stage1_smoke_signature_invalid") from exc
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None:
        raise HTTPException(status_code=503, detail="stage1_smoke_authority_unavailable")
    preflight_authority = getattr(
        request.app.state, "pipeline_stage1_execution_preflight_authority", None
    )
    if preflight_authority is None or not hasattr(preflight_authority, "verify_preflight"):
        raise HTTPException(status_code=503, detail="stage1_smoke_preflight_authority_unavailable")
    try:
        async with session_factory() as session:
            body, replay = await execute_stage1_smoke(
                session,
                candidate=payload.candidate,
                authorization=payload.authorization,
                preflight=payload.preflight,
                idempotency_key=idempotency_key,
                signature_key_id=signature_key_id,
                signature_sha256=signature_sha256,
                preflight_authority=cast(
                    Stage1SmokeExecutionPreflightAuthorityV1, preflight_authority
                ),
                repo_root=_REPO_ROOT,
                now=now,
            )
            await session.commit()
    except Stage1SmokeServiceError as exc:
        raise _error(exc) from exc
    except IntegrityError as exc:
        try:
            async with session_factory() as session:
                replay_body = await get_stage1_smoke_replay(
                    session,
                    team_id=payload.candidate.team_id,
                    idempotency_key=idempotency_key,
                    request_digest=stage1_smoke_request_digest(
                        candidate=payload.candidate,
                        authorization=payload.authorization,
                        preflight=payload.preflight,
                        signature_key_id=signature_key_id,
                    ),
                )
        except Stage1SmokeServiceError as replay_exc:
            raise _error(replay_exc) from replay_exc
        if replay_body is None:
            raise HTTPException(status_code=409, detail="stage1_smoke_concurrent_conflict") from exc
        body = replay_body
        replay = True
    if replay:
        response.status_code = 200
        response.headers["Idempotent-Replay"] = "true"
    return body


@router.post("/pipeline-stage1-smoke/evidence")
async def record_evidence(
    raw_payload: dict[str, Any],
    request: Request,
    response: Response,
    signature_key_id: Annotated[str, Header(alias="X-Loom-Stage1-Signature-Key-Id")],
    signature: Annotated[
        str, Header(alias="X-Loom-Stage1-Signature"), Field(min_length=128, max_length=128)
    ],
) -> dict[str, object]:
    payload = _decode_json_model(raw_payload, Stage1SmokeEvidenceV1)
    now = datetime.now(UTC)
    try:
        _verifier(request).verify(
            key_id=signature_key_id,
            payload=evidence_signature_payload(payload),
            signature=signature,
            observed_at=payload.observed_at,
            now=now,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="stage1_smoke_signature_invalid") from exc
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None:
        raise HTTPException(status_code=503, detail="stage1_smoke_authority_unavailable")
    evidence_authority = getattr(request.app.state, "pipeline_stage1_evidence_authority", None)
    if evidence_authority is None or not hasattr(evidence_authority, "verify_evidence"):
        raise HTTPException(status_code=503, detail="stage1_smoke_evidence_authority_unavailable")
    try:
        async with session_factory() as session:
            body, replay = await record_stage1_smoke_evidence(
                session,
                evidence=payload,
                authority=cast(Stage1SmokeEvidenceAuthorityV1, evidence_authority),
                now=now,
            )
            await session.commit()
    except Stage1SmokeServiceError as exc:
        raise _error(exc) from exc
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    return body


@router.post("/pipeline-stage1-smoke/cleanup")
async def cleanup(
    raw_payload: dict[str, Any],
    request: Request,
    response: Response,
    signature_key_id: Annotated[str, Header(alias="X-Loom-Stage1-Signature-Key-Id")],
    signature: Annotated[
        str, Header(alias="X-Loom-Stage1-Signature"), Field(min_length=128, max_length=128)
    ],
) -> dict[str, object]:
    payload = _decode_json_model(raw_payload, Stage1SmokeCleanupV1)
    now = datetime.now(UTC)
    try:
        _verifier(request).verify(
            key_id=signature_key_id,
            payload=cleanup_signature_payload(payload),
            signature=signature,
            observed_at=payload.cleaned_at,
            now=now,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="stage1_smoke_signature_invalid") from exc
    session_factory = getattr(request.app.state, "session_factory", None)
    if session_factory is None:
        raise HTTPException(status_code=503, detail="stage1_smoke_authority_unavailable")
    cleanup_authority = getattr(request.app.state, "pipeline_stage1_cleanup_authority", None)
    if cleanup_authority is None or not hasattr(cleanup_authority, "verify_cleanup"):
        raise HTTPException(status_code=503, detail="stage1_smoke_cleanup_authority_unavailable")
    try:
        async with session_factory() as session:
            body, replay = await cleanup_stage1_smoke(
                session,
                cleanup=payload,
                authority=cast(Stage1SmokeCleanupAuthorityV1, cleanup_authority),
                now=now,
            )
            await session.commit()
    except Stage1SmokeServiceError as exc:
        raise _error(exc) from exc
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    return body
