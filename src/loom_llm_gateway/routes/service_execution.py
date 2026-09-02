"""Credential-free Pod broker and durable output upload routes."""

from __future__ import annotations

import hashlib
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from starlette.responses import Response, StreamingResponse

from loom.db.schema import ServiceExecutionLease
from loom.pipeline.artifact_commit import ArtifactCommitError
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    ServiceExecutionFileCompleteV1,
    ServiceExecutionOutputCommitV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionPeerV1,
    ServiceExecutionTokenRequestV1,
    authorize_service_execution_peer,
    mint_service_execution_peer_token,
    resolve_service_execution_input,
)

router = APIRouter(prefix="/internal/service-execution", tags=["service-execution"])
LeaseIdHeader = Annotated[UUID, Header(alias="X-Loom-Execution-Lease-Id")]
GenerationHeader = Annotated[int, Header(alias="X-Loom-Execution-Generation", gt=0)]
RoleHeader = Annotated[str, Header(alias="X-Loom-Execution-Role")]
UploadTokenHeader = Annotated[str, Header(alias="X-Loom-Upload-Token", min_length=32)]
ContentSha256Header = Annotated[
    str, Header(alias="X-Loom-Content-SHA256", pattern=r"^sha256:[0-9a-f]{64}$")
]


def _peer_ip(request: Request) -> str:
    if request.client is None or not request.client.host:
        raise HTTPException(status_code=403, detail="workload_peer_unavailable")
    return request.client.host


def _peer(lease_id: UUID, generation: int, role: str) -> ServiceExecutionPeerV1:
    try:
        return ServiceExecutionPeerV1(
            lease_id=lease_id,
            generation=generation,
            execution_role=role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="execution_identity_invalid") from exc


def _broker_http(exc: ServiceExecutionBrokerError) -> HTTPException:
    status = (
        503
        if exc.reason
        in {
            "workload_identity_not_observed",
            "execution_target_unavailable",
        }
        else 409
    )
    if exc.reason == "peer_ip_invalid":
        status = 403
    return HTTPException(status_code=status, detail=exc.reason)


def _artifact_http(exc: ArtifactCommitError) -> HTTPException:
    if exc.reason in {"upload_token_invalid", "upload_token_expired"}:
        return HTTPException(status_code=403, detail=exc.reason)
    if "object" in exc.reason or "multipart" in exc.reason:
        return HTTPException(status_code=503, detail=exc.reason)
    return HTTPException(status_code=409, detail=exc.reason)


async def _authorize(
    request: Request,
    identity: ServiceExecutionPeerV1,
    *,
    purpose: Literal["token", "input", "output"] = "output",
) -> ServiceExecutionLease:
    async with request.app.state.session_factory() as session:
        try:
            lease = await authorize_service_execution_peer(
                session,
                peer_ip=_peer_ip(request),
                identity=identity,
                purpose=purpose,
            )
            session.expunge(lease)
            return lease
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc


@router.post("/token")
async def issue_service_execution_token(
    payload: ServiceExecutionTokenRequestV1,
    request: Request,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        try:
            lease = await authorize_service_execution_peer(
                session,
                peer_ip=_peer_ip(request),
                identity=payload,
                lock=True,
            )
            token, expires_at, step_jwt_id = await mint_service_execution_peer_token(
                session,
                lease=lease,
                ttl_seconds=payload.ttl_seconds,
                signing_key=request.app.state.settings.step_jwt_signing_key.get_secret_value(),
            )
            await session.commit()
        except ServiceExecutionBrokerError as exc:
            await session.rollback()
            raise _broker_http(exc) from exc
    return {
        "schema_version": "loom.service-execution-token.v1",
        "token": token,
        "expires_at": expires_at.isoformat(),
        "step_jwt_id": str(step_jwt_id),
    }


@router.get("/inputs/manifest")
async def get_service_execution_input_manifest(
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
) -> Response:
    identity = _peer(lease_id, generation, role)
    lease = await _authorize(request, identity, purpose="input")
    async with request.app.state.session_factory() as session:
        try:
            resolved = await resolve_service_execution_input(
                session,
                lease=lease,
                store=request.app.state.artifact_store,
                artifacts_bucket=request.app.state.settings.artifacts_bucket,
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
    return Response(
        content=resolved.manifest_body,
        media_type="application/json",
        headers={
            "X-Loom-Content-SHA256": "sha256:"
            + hashlib.sha256(resolved.manifest_body).hexdigest()
        },
    )


@router.get("/inputs/files/{file_index}")
async def get_service_execution_input_file(
    file_index: int,
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
) -> StreamingResponse:
    identity = _peer(lease_id, generation, role)
    lease = await _authorize(request, identity, purpose="input")
    async with request.app.state.session_factory() as session:
        try:
            resolved = await resolve_service_execution_input(
                session,
                lease=lease,
                store=request.app.state.artifact_store,
                artifacts_bucket=request.app.state.settings.artifacts_bucket,
            )
        except ServiceExecutionBrokerError as exc:
            raise _broker_http(exc) from exc
    if file_index < 0 or file_index >= len(resolved.manifest.files):
        raise HTTPException(status_code=404, detail="task_input_file_not_found")
    item = resolved.manifest.files[file_index]
    key = resolved.prefix + item.relative_path
    try:
        facts = await request.app.state.artifact_store.stat_object(
            bucket=resolved.bucket,
            key=key,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="task_input_object_unavailable") from exc
    if facts.content_length != item.size_bytes:
        raise HTTPException(status_code=409, detail="task_input_object_drift")
    return StreamingResponse(
        request.app.state.artifact_store.stream_object(
            bucket=resolved.bucket,
            key=key,
            chunk_size=1024 * 1024,
        ),
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(item.size_bytes),
            "X-Loom-Content-SHA256": item.sha256,
        },
    )


@router.post("/outputs/prepare", status_code=201)
async def prepare_service_execution_output(
    payload: ServiceExecutionOutputPrepareV1,
    request: Request,
) -> dict[str, Any]:
    lease = await _authorize(request, payload)
    try:
        return cast(
            dict[str, Any],
            await request.app.state.service_execution_output_service.prepare(
                lease=lease,
                request=payload,
            ),
        )
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc
    except ArtifactCommitError as exc:
        raise _artifact_http(exc) from exc


@router.put("/outputs/{session_id}/files/{file_index}/parts/{part_number}")
async def upload_service_execution_output_part(
    session_id: UUID,
    file_index: int,
    part_number: int,
    request: Request,
    lease_id: LeaseIdHeader,
    generation: GenerationHeader,
    role: RoleHeader,
    upload_token: UploadTokenHeader,
    content_sha256: ContentSha256Header,
    content_length: int = Header(alias="Content-Length", ge=0),
) -> dict[str, Any]:
    identity = _peer(lease_id, generation, role)
    lease = await _authorize(request, identity)
    try:
        return cast(
            dict[str, Any],
            await request.app.state.service_execution_output_service.put_part(
                lease=lease,
                session_id=session_id,
                file_index=file_index,
                part_number=part_number,
                content_length=content_length,
                content_sha256=content_sha256,
                upload_token=upload_token,
                body=request.stream(),
            ),
        )
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc
    except ArtifactCommitError as exc:
        raise _artifact_http(exc) from exc


@router.post("/outputs/{session_id}/files/{file_index}/complete")
async def complete_service_execution_output_file(
    session_id: UUID,
    file_index: int,
    payload: ServiceExecutionFileCompleteV1,
    request: Request,
    upload_token: UploadTokenHeader,
) -> dict[str, Any]:
    lease = await _authorize(request, payload)
    try:
        return cast(
            dict[str, Any],
            await request.app.state.service_execution_output_service.complete_file(
                lease=lease,
                session_id=session_id,
                file_index=file_index,
                ordered_parts=payload.ordered_parts,
                upload_token=upload_token,
            ),
        )
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc
    except ArtifactCommitError as exc:
        raise _artifact_http(exc) from exc


@router.post("/outputs/{session_id}/commit")
async def commit_service_execution_output(
    session_id: UUID,
    payload: ServiceExecutionOutputCommitV1,
    request: Request,
    upload_token: UploadTokenHeader,
) -> dict[str, Any]:
    lease = await _authorize(request, payload)
    try:
        return cast(
            dict[str, Any],
            await request.app.state.service_execution_output_service.commit(
                lease=lease,
                session_id=session_id,
                upload_token=upload_token,
            ),
        )
    except ServiceExecutionBrokerError as exc:
        raise _broker_http(exc) from exc
    except ArtifactCommitError as exc:
        raise _artifact_http(exc) from exc


__all__ = ["router"]
