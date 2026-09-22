"""Transactional ownership/name/platform admission; no external resource writes.

The caller must resolve a protected approved candidate and render its exact plan.
HTTP payloads must never supply RenderedEnvironment, owner IDs or resource costs.
All provider operations occur after the committed registration transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.auth import AuthContext, is_admin
from loom.db.nebius_environment_schema import (
    NebiusEnvironment,
    NebiusEnvironmentNamespace,
    NebiusEnvironmentOperation,
    NebiusPlatformBudget,
    NebiusPlatformReservation,
)
from loom.nebius_environment_contract import EnvironmentOperationV1, EnvironmentRegistrationV1
from loom.nebius_environment_render import RenderedEnvironment


class ManagementError(ValueError):
    def __init__(self, code: str, status_code: int = 409, *, details: dict[str, Any] | None = None):
        super().__init__(code)
        self.code = code
        self.status_code = status_code
        self.details = details or {}


def owner_identity(principal: AuthContext, *, mutation: bool = False) -> tuple[UUID, UUID]:
    if principal.user_id is None or principal.team_id is None or principal.type not in {"team", "admin"}:
        raise ManagementError("user_identity_required", 403)
    if ("submit" if mutation else "read:own") not in principal.scopes and not is_admin(principal):
        raise ManagementError("environment_scope_required", 403)
    return principal.user_id, principal.team_id


def operation_view(row: NebiusEnvironmentOperation) -> EnvironmentOperationV1:
    return EnvironmentOperationV1.model_validate({
        "operation_id": row.operation_id, "environment_id": row.environment_id,
        "deployment_generation": row.deployment_generation, "action": row.action,
        "phase": row.phase, "error_code": row.error_code,
    })


def registration_view(row: NebiusEnvironment) -> EnvironmentRegistrationV1:
    return EnvironmentRegistrationV1.model_validate({
        field: getattr(row, field) for field in EnvironmentRegistrationV1.model_fields if field != "schema_version"
    })


class EnvironmentRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def create(
        self, *, principal: AuthContext, idempotency_key: str, prepared: RenderedEnvironment,
    ) -> EnvironmentOperationV1:
        owner, team = owner_identity(principal, mutation=True)
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", idempotency_key) is None:
            raise ManagementError("invalid_idempotency_key", 422)
        row = prepared.registration
        if (row.owner_user_id, row.owner_team_id) != (owner, team):
            raise ManagementError("environment_owner_mismatch", 403)
        if (row.scope != "personal" or row.binding_mode != "generated" or row.candidate_id is None
                or row.deployment_generation != 1 or row.desired_state != "active" or prepared.execution_enabled):
            raise ManagementError("invalid_create_plan", 422)
        request_sha256 = hashlib.sha256(json.dumps({
            "action": "create", "slug": row.slug, "candidate_id": str(row.candidate_id),
            "owner_team_id": str(team), "cluster_id": row.cluster_id,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        needed = asdict(prepared.platform_envelope)
        if any(type(value) is not int or value < 0 for value in needed.values()):
            raise ManagementError("invalid_platform_envelope", 422)
        try:
            async with self.session_factory.begin() as session:
                # Creation/resume/release use the same cluster row lock. No cloud
                # call is made under it; independent environments provision later.
                budget = (await session.execute(select(NebiusPlatformBudget).where(
                    NebiusPlatformBudget.cluster_id == row.cluster_id,
                ).with_for_update())).scalar_one_or_none()
                if budget is None:
                    raise ManagementError("platform_budget_not_configured", 503)
                replay = (await session.execute(select(NebiusEnvironmentOperation).where(
                    NebiusEnvironmentOperation.owner_user_id == owner,
                    NebiusEnvironmentOperation.idempotency_key == idempotency_key,
                ))).scalar_one_or_none()
                if replay is not None:
                    if replay.request_sha256 != request_sha256:
                        raise ManagementError("idempotency_conflict")
                    return operation_view(replay)
                used = (await session.execute(select(*[
                    func.coalesce(func.sum(getattr(NebiusPlatformReservation, name)), 0).label(name)
                    for name in needed
                ]).where(NebiusPlatformReservation.cluster_id == row.cluster_id))).mappings().one()
                available = {name: max(0, getattr(budget, name) - int(used[name])) for name in needed}
                if any(needed[name] > available[name] for name in needed):
                    raise ManagementError("platform_capacity_exhausted", details={
                        "needed": needed, "available": available,
                    })
                session.add(NebiusEnvironment(**row.model_dump(exclude={"schema_version"})))
                await session.flush()
                session.add_all([NebiusEnvironmentNamespace(
                    cluster_id=row.cluster_id, namespace_name=name,
                    environment_id=row.environment_id, role=role,
                ) for role, name in zip(("application", "execution", "build"), row.namespaces, strict=True)])
                session.add(NebiusPlatformReservation(
                    environment_id=row.environment_id, cluster_id=row.cluster_id, **needed,
                ))
                operation = NebiusEnvironmentOperation(
                    operation_id=uuid4(), environment_id=row.environment_id, owner_user_id=owner,
                    idempotency_key=idempotency_key, request_sha256=request_sha256,
                    deployment_generation=1, action="create", phase="pending",
                    plan_json={"registration": row.model_dump(mode="json"),
                               "files": prepared.files, "config": prepared.config},
                )
                session.add(operation)
                await session.flush()
                result = operation_view(operation)
            return result
        except IntegrityError as exc:
            raise ManagementError("environment_name_conflict") from exc

    async def get_operation(self, operation_id: UUID, *, principal: AuthContext) -> EnvironmentOperationV1:
        owner, team = owner_identity(principal)
        async with self.session_factory() as session:
            operation = (await session.execute(select(NebiusEnvironmentOperation).join(
                NebiusEnvironment, NebiusEnvironment.environment_id == NebiusEnvironmentOperation.environment_id,
            ).where(
                NebiusEnvironmentOperation.operation_id == operation_id,
                NebiusEnvironment.owner_user_id == owner, NebiusEnvironment.owner_team_id == team,
            ))).scalar_one_or_none()
            if operation is None:
                raise ManagementError("environment_forbidden", 403)
            return operation_view(operation)

    async def list_environments(self, *, principal: AuthContext) -> list[EnvironmentRegistrationV1]:
        owner, team = owner_identity(principal)
        async with self.session_factory() as session:
            rows = (await session.execute(select(NebiusEnvironment).where(
                NebiusEnvironment.owner_user_id == owner, NebiusEnvironment.owner_team_id == team,
                NebiusEnvironment.purged_at.is_(None),
            ).order_by(NebiusEnvironment.created_at, NebiusEnvironment.environment_id).limit(1000))).scalars()
            return [registration_view(row) for row in rows]
