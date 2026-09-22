"""Authenticated environment control; callers cannot supply provider authority."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request

from loom.auth import AuthContext
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


async def management_principal(sc: SessionAndCtx) -> AuthContext:
    # Authentication has completed its membership and CSRF checks. Finish the
    # session touch before taking an independent registry connection or waiting
    # on publication. Otherwise same-session polls block on UserSession while
    # filling the pool the create request still needs to commit its operation.
    session, principal = sc
    await session.commit()
    await session.close()
    return principal


ManagementPrincipal = Annotated[AuthContext, Depends(management_principal)]


def manager(request: Request) -> EnvironmentManager:
    value = getattr(request.app.state, "environment_manager", None)
    if not isinstance(value, EnvironmentManager):
        raise ManagementError("environment_management_not_configured", 503)
    return value


@router.post("/environments", status_code=202)
async def create_environment(
    request: Request, payload: EnvironmentCreateRequestV1, principal: ManagementPrincipal,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")],
) -> EnvironmentOperationV1:
    return await manager(request).create(principal, payload, idempotency_key=idempotency_key)


@router.get("/environments")
async def list_environments(request: Request, principal: ManagementPrincipal) -> dict[str, list[EnvironmentRegistrationV1]]:
    return {"items": await manager(request).registry.list_environments(principal=principal)}


@router.get("/environments/{environment_id}")
async def environment_status(request: Request, environment_id: UUID, principal: ManagementPrincipal) -> EnvironmentStatusV1:
    return await manager(request).registry.status(environment_id, principal=principal)


@router.get("/environment-operations/{operation_id}")
async def operation_status(request: Request, operation_id: UUID, principal: ManagementPrincipal) -> EnvironmentOperationV1:
    return await manager(request).registry.get_operation(operation_id, principal=principal)
