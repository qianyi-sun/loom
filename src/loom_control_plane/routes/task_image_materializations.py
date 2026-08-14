"""Exclusive-builder API for durable task-image materializations."""

from __future__ import annotations

import re
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from loom.auth import verify_bearer_token
from loom_control_plane.task_image_materializations import (
    TaskImageLeaseConflictError,
    claim_task_image_materialization,
    complete_task_image_materialization,
    fail_task_image_materialization,
    heartbeat_task_image_materialization,
    start_task_image_materialization,
    task_image_materialization_payload,
)

router = APIRouter(prefix="/api/v1/internal/task-image-materializations")
_IMMUTABLE_IMAGE_RE = re.compile(r".+@sha256:[0-9a-f]{64}$")
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


async def _verify_builder_request(
    request: Request,
    authorization: str | None,
) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "task-image:build" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="task image builder token required")


def _validate_registry_images(registry_images: dict[str, str]) -> None:
    if not registry_images:
        raise HTTPException(status_code=400, detail="registry_images must not be empty")
    if any(
        not component or _IMMUTABLE_IMAGE_RE.fullmatch(image) is None
        for component, image in registry_images.items()
    ):
        raise HTTPException(
            status_code=400,
            detail="registry_images must map component names to immutable digest references",
        )


@router.post("/claim", response_model=None)
async def claim_route(
    request: Request,
    payload: ClaimRequest,
    authorization: str | None = Header(default=None),
):  # type: ignore[no-untyped-def]
    await _verify_builder_request(request, authorization)
    async with request.app.state.session_factory() as session, session.begin():
        row = await claim_task_image_materialization(
            session,
            builder_id=payload.builder_id,
            cpu_arch=payload.cpu_arch,
        )
    if row is None:
        return Response(status_code=204)
    return task_image_materialization_payload(row)


async def _run_lease_mutation(
    request: Request,
    authorization: str | None,
    operation,  # type: ignore[no-untyped-def]
):  # type: ignore[no-untyped-def]
    await _verify_builder_request(request, authorization)
    try:
        async with request.app.state.session_factory() as session, session.begin():
            row = await operation(session)
    except TaskImageLeaseConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return task_image_materialization_payload(row)


@router.post("/{materialization_id}/start")
async def start_route(
    materialization_id: UUID,
    request: Request,
    payload: LeaseRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: start_task_image_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
        ),
    )


@router.post("/{materialization_id}/heartbeat")
async def heartbeat_route(
    materialization_id: UUID,
    request: Request,
    payload: LeaseRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    return await _run_lease_mutation(
        request,
        authorization,
        lambda session: heartbeat_task_image_materialization(
            session,
            materialization_id=materialization_id,
            builder_id=payload.builder_id,
            lease_epoch=payload.lease_epoch,
        ),
    )


@router.post("/{materialization_id}/complete")
async def complete_route(
    materialization_id: UUID,
    request: Request,
    payload: CompleteRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
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
    )


@router.post("/{materialization_id}/fail")
async def fail_route(
    materialization_id: UUID,
    request: Request,
    payload: FailRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
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
        ),
    )
