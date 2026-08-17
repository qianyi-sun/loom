"""Exclusive-builder API for durable task-image materializations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import verify_bearer_token
from loom.db.schema import TaskImageMaterialization
from loom.task_image_materialization import validate_task_image_registry_images
from loom_control_plane.task_image_materializations import (
    TaskImageCompletionError,
    TaskImageLeaseConflictError,
    claim_task_image_materialization,
    claim_task_image_registry_gc,
    complete_task_image_materialization,
    complete_task_image_registry_gc,
    fail_task_image_materialization,
    heartbeat_task_image_materialization,
    record_task_image_publication,
    start_task_image_materialization,
    task_image_materialization_payload,
)

router = APIRouter(prefix="/api/v1/internal/task-image-materializations")
BuilderID = Annotated[str, Field(min_length=1, max_length=128)]
LeaseEpoch = Annotated[int, Field(gt=0)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ClaimRequest(_StrictModel):
    builder_id: BuilderID
    cpu_arch: Literal["x86_64", "arm64"]


class LeaseRequest(_StrictModel):
    builder_id: BuilderID
    lease_epoch: LeaseEpoch


class CompleteRequest(LeaseRequest):
    registry_images: dict[str, str]


class FailRequest(LeaseRequest):
    retryable: bool
    failure_reason: Annotated[str, Field(min_length=1, max_length=128)]
    failure_message: Annotated[str, Field(min_length=1, max_length=4000)]
    registry_images: dict[str, str] = Field(default_factory=dict)


class PublicationRequest(LeaseRequest):
    component: Annotated[str, Field(min_length=1, max_length=256)]
    registry_image: Annotated[str, Field(min_length=1, max_length=2048)]


class RegistryGCClaimRequest(_StrictModel):
    gc_id: BuilderID


class RegistryGCLeaseRequest(RegistryGCClaimRequest):
    lease_epoch: LeaseEpoch


async def _verify_scoped_request(
    request: Request,
    authorization: str | None,
    *,
    required_scope: str,
    credential_name: str,
) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or set(ctx.scopes) != {required_scope}:
        raise HTTPException(status_code=401, detail=f"{credential_name} token required")


def _validate_registry_images(
    registry_images: dict[str, str], *, require_nonempty: bool = True
) -> None:
    try:
        validate_task_image_registry_images(
            registry_images,
            require_nonempty=require_nonempty,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/claim", response_model=None)
async def claim_route(
    request: Request,
    payload: ClaimRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any] | Response:
    await _verify_scoped_request(
        request,
        authorization,
        required_scope="task-image:build",
        credential_name="task image builder",
    )
    async with request.app.state.session_factory() as session, session.begin():
        row = await claim_task_image_materialization(
            session,
            builder_id=payload.builder_id,
            cpu_arch=payload.cpu_arch,
            lease_seconds=request.app.state.settings.task_image_builder_lease_seconds,
        )
    if row is None:
        return Response(status_code=204)
    return task_image_materialization_payload(row)


@router.post("/registry-gc/claim", response_model=None)
async def registry_gc_claim_route(
    request: Request,
    payload: RegistryGCClaimRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any] | Response:
    await _verify_scoped_request(
        request,
        authorization,
        required_scope="task-image:gc",
        credential_name="task image registry GC",
    )
    async with request.app.state.session_factory() as session, session.begin():
        row = await claim_task_image_registry_gc(
            session,
            gc_id=payload.gc_id,
            grace_hours=request.app.state.settings.task_image_registry_grace_hours,
            lease_seconds=request.app.state.settings.task_image_builder_lease_seconds,
        )
    if row is None:
        return Response(status_code=204)
    return task_image_materialization_payload(row)


async def _run_lease_mutation(
    request: Request,
    authorization: str | None,
    operation: Callable[[AsyncSession], Awaitable[TaskImageMaterialization]],
    *,
    required_scope: str,
    credential_name: str,
) -> dict[str, Any]:
    await _verify_scoped_request(
        request,
        authorization,
        required_scope=required_scope,
        credential_name=credential_name,
    )
    try:
        async with request.app.state.session_factory() as session, session.begin():
            row = await operation(session)
    except TaskImageCompletionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except TaskImageLeaseConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return task_image_materialization_payload(row)


@router.post("/registry-gc/{materialization_id}/complete")
async def registry_gc_complete_route(
    materialization_id: UUID,
    request: Request,
    payload: RegistryGCLeaseRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: complete_task_image_registry_gc(
            session,
            materialization_id=materialization_id,
            gc_id=payload.gc_id,
            lease_epoch=payload.lease_epoch,
        ),
        required_scope="task-image:gc",
        credential_name="task image registry GC",
    )


@router.post("/{materialization_id}/start")
async def start_route(
    materialization_id: UUID,
    request: Request,
    payload: LeaseRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: start_task_image_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
            lease_seconds=request.app.state.settings.task_image_builder_lease_seconds,
        ),
        required_scope="task-image:build",
        credential_name="task image builder",
    )


@router.post("/{materialization_id}/heartbeat")
async def heartbeat_route(
    materialization_id: UUID,
    request: Request,
    payload: LeaseRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: heartbeat_task_image_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
            lease_seconds=request.app.state.settings.task_image_builder_lease_seconds,
        ),
        required_scope="task-image:build",
        credential_name="task image builder",
    )


@router.post("/{materialization_id}/complete")
async def complete_route(
    materialization_id: UUID,
    request: Request,
    payload: CompleteRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_registry_images(payload.registry_images)
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: complete_task_image_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
            registry_images=payload.registry_images,
        ),
        required_scope="task-image:build",
        credential_name="task image builder",
    )


@router.post("/{materialization_id}/publication")
async def publication_route(
    materialization_id: UUID,
    request: Request,
    payload: PublicationRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_registry_images({payload.component: payload.registry_image})
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: record_task_image_publication(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
            component=payload.component,
            registry_image=payload.registry_image,
        ),
        required_scope="task-image:build",
        credential_name="task image builder",
    )


@router.post("/{materialization_id}/fail")
async def fail_route(
    materialization_id: UUID,
    request: Request,
    payload: FailRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _validate_registry_images(payload.registry_images, require_nonempty=False)
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: fail_task_image_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
            retryable=payload.retryable,
            failure_reason=payload.failure_reason,
            failure_message=payload.failure_message,
            registry_images=payload.registry_images,
        ),
        required_scope="task-image:build",
        credential_name="task image builder",
    )
