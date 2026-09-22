"""Authenticated environment control; callers cannot supply provider authority."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Request

from loom.nebius_environment_contract import (
    EnvironmentCreateRequestV1,
    EnvironmentOperationV1,
    EnvironmentRegistrationV1,
    EnvironmentStatusV1,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.environment_management.manager import EnvironmentManager
from loom_service.environment_management.registry import ManagementError

router = APIRouter()


def manager(request: Request) -> EnvironmentManager:
    value = getattr(request.app.state, "environment_manager", None)
    if not isinstance(value, EnvironmentManager):
        raise ManagementError("environment_management_not_configured", 503)
    return value


@router.post("/environments", status_code=202)
async def create_environment(
    request: Request, payload: EnvironmentCreateRequestV1, sc: SessionAndCtx,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")],
) -> EnvironmentOperationV1:
    return await manager(request).create(sc[1], payload, idempotency_key=idempotency_key)


@router.get("/environments")
async def list_environments(request: Request, sc: SessionAndCtx) -> dict[str, list[EnvironmentRegistrationV1]]:
    return {"items": await manager(request).registry.list_environments(principal=sc[1])}


@router.get("/environments/{environment_id}")
async def environment_status(request: Request, environment_id: UUID, sc: SessionAndCtx) -> EnvironmentStatusV1:
    return await manager(request).registry.status(environment_id, principal=sc[1])


@router.get("/environment-operations/{operation_id}")
async def operation_status(request: Request, operation_id: UUID, sc: SessionAndCtx) -> EnvironmentOperationV1:
    return await manager(request).registry.get_operation(operation_id, principal=sc[1])
