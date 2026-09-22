"""Transactional ownership/name/platform admission; no external resource writes.

The caller must resolve a protected approved candidate and render its exact plan.
HTTP payloads must never supply RenderedEnvironment, owner IDs or resource costs.
All provider operations occur after the committed registration transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.auth import AuthContext, is_admin
from loom.db.nebius_environment_schema import (
    NebiusEnvironment,
    NebiusEnvironmentNamespace,
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
    NebiusPlatformBudget,
    NebiusPlatformReservation,
)
from loom.nebius_environment_contract import (
    EnvironmentCreateRequestV1,
    EnvironmentOperationV1,
    EnvironmentRegistrationV1,
    EnvironmentStatusV1,
)
from loom.nebius_environment_render import RenderedEnvironment
from loom_service.environment_management.steps import ProvisioningStep, StepKind, creation_steps


class ManagementError(ValueError):
    def __init__(self, code: str, status_code: int = 409, *, details: dict[str, Any] | None = None):
        super().__init__(code)
        self.code = code
        self.status_code = status_code
        self.details = details or {}


def owner_identity(principal: AuthContext, *, mutation: bool = False) -> tuple[UUID, UUID]:
    if principal.user_id is None or principal.team_id is None or principal.type not in {"user", "team", "admin"}:
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


@dataclass(frozen=True)
class OperationLease:
    operation_id: UUID
    environment_id: UUID
    deployment_generation: int
    runner_epoch: int
    lease_token: UUID


class EnvironmentRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @staticmethod
    def _create_fingerprint(*, team: UUID, cluster_id: str, slug: str, candidate_id: UUID) -> str:
        return hashlib.sha256(json.dumps({
            "action": "create", "slug": slug, "candidate_id": str(candidate_id),
            "owner_team_id": str(team), "cluster_id": cluster_id,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    async def replay_create(
        self, *, principal: AuthContext, idempotency_key: str,
        request: EnvironmentCreateRequestV1, cluster_id: str,
    ) -> EnvironmentOperationV1 | None:
        owner, team = owner_identity(principal, mutation=True)
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", idempotency_key) is None:
            raise ManagementError("invalid_idempotency_key", 422)
        async with self.session_factory() as session:
            row = (await session.execute(select(NebiusEnvironmentOperation).where(
                NebiusEnvironmentOperation.owner_user_id == owner,
                NebiusEnvironmentOperation.idempotency_key == idempotency_key,
            ))).scalar_one_or_none()
            if row is None:
                return None
            if row.request_sha256 != self._create_fingerprint(
                team=team, cluster_id=cluster_id, slug=request.slug, candidate_id=request.candidate_id,
            ):
                raise ManagementError("idempotency_conflict")
            return operation_view(row)

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
        request_sha256 = self._create_fingerprint(
            team=team, cluster_id=row.cluster_id, slug=row.slug, candidate_id=row.candidate_id,
        )
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
                session.add_all([NebiusEnvironmentResource(
                    operation_id=operation.operation_id, resource_key=step.key, sequence=sequence,
                    kind=step.kind, payload_json=step.payload, phase="planned",
                ) for sequence, step in enumerate(creation_steps(prepared))])
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

    async def status(self, environment_id: UUID, *, principal: AuthContext) -> EnvironmentStatusV1:
        owner, team = owner_identity(principal)
        async with self.session_factory() as session:
            row = (await session.execute(select(NebiusEnvironment).where(
                NebiusEnvironment.environment_id == environment_id,
                NebiusEnvironment.owner_user_id == owner, NebiusEnvironment.owner_team_id == team,
            ))).scalar_one_or_none()
            if row is None:
                raise ManagementError("environment_forbidden", 403)
            operation = (await session.execute(select(NebiusEnvironmentOperation).where(
                NebiusEnvironmentOperation.environment_id == row.environment_id,
                NebiusEnvironmentOperation.deployment_generation == row.deployment_generation,
            ))).scalar_one_or_none()
            return EnvironmentStatusV1(
                registration=registration_view(row),
                operation=operation_view(operation) if operation is not None else None,
            )

    async def _locked_operation(
        self, session: AsyncSession, operation_id: UUID,
    ) -> tuple[NebiusEnvironmentOperation, NebiusEnvironment, datetime]:
        environment_id = await session.scalar(select(NebiusEnvironmentOperation.environment_id).where(
            NebiusEnvironmentOperation.operation_id == operation_id,
        ))
        if environment_id is None:
            raise ManagementError("operation_not_found", 404)
        # Always lock environment before operation; every lifecycle mutation uses
        # this order. The owner binding cannot be replaced during a lease check.
        environment = (await session.execute(select(NebiusEnvironment).where(
            NebiusEnvironment.environment_id == environment_id,
        ).with_for_update())).scalar_one()
        operation = (await session.execute(select(NebiusEnvironmentOperation).where(
            NebiusEnvironmentOperation.operation_id == operation_id,
        ).with_for_update())).scalar_one()
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        return operation, environment, now

    async def _leased_operation(
        self, session: AsyncSession, lease: OperationLease,
    ) -> tuple[NebiusEnvironmentOperation, datetime]:
        operation, environment, now = await self._locked_operation(session, lease.operation_id)
        if (operation.phase != "running" or operation.lease_token != lease.lease_token
                or operation.runner_epoch != lease.runner_epoch or operation.environment_id != lease.environment_id
                or operation.deployment_generation != lease.deployment_generation
                or environment.deployment_generation != lease.deployment_generation
                or operation.lease_expires_at is None or operation.lease_expires_at <= now):
            raise ManagementError("stale_operation_lease")
        return operation, now

    @staticmethod
    def _lease_duration(seconds: int) -> timedelta:
        if type(seconds) is not int or not 1 <= seconds <= 300:
            raise ValueError("operation lease must be between 1 and 300 seconds")
        return timedelta(seconds=seconds)

    async def claim(self, operation_id: UUID, *, lease_seconds: int = 60) -> OperationLease | None:
        duration = self._lease_duration(lease_seconds)
        async with self.session_factory.begin() as session:
            operation, environment, now = await self._locked_operation(session, operation_id)
            if operation.phase == "completed":
                return None
            if environment.deployment_generation != operation.deployment_generation:
                raise ManagementError("stale_operation_generation")
            if operation.lease_expires_at is not None and operation.lease_expires_at > now:
                return None
            token = uuid4()
            operation.runner_epoch += 1
            operation.phase = "running"
            operation.error_code = None
            operation.lease_token = token
            operation.lease_expires_at = now + duration
            return OperationLease(operation_id, environment.environment_id,
                                  operation.deployment_generation, operation.runner_epoch, token)

    async def renew(self, lease: OperationLease, *, lease_seconds: int = 60) -> None:
        duration = self._lease_duration(lease_seconds)
        async with self.session_factory.begin() as session:
            operation, now = await self._leased_operation(session, lease)
            operation.lease_expires_at = now + duration

    async def next_step(self, lease: OperationLease) -> ProvisioningStep | None:
        async with self.session_factory.begin() as session:
            await self._leased_operation(session, lease)
            row = (await session.execute(select(NebiusEnvironmentResource).where(
                NebiusEnvironmentResource.operation_id == lease.operation_id,
                NebiusEnvironmentResource.phase == "planned",
            ).order_by(NebiusEnvironmentResource.sequence).limit(1))).scalar_one_or_none()
            if row is None:
                return None
            return ProvisioningStep(row.resource_key, cast(StepKind, row.kind), row.payload_json)

    async def confirm_step(self, lease: OperationLease, key: str, *, provider_identity: str) -> None:
        if not provider_identity or len(provider_identity) > 512 or any(ord(c) < 32 for c in provider_identity):
            raise ManagementError("invalid_provider_identity", 422)
        async with self.session_factory.begin() as session:
            await self._leased_operation(session, lease)
            row = await session.get(NebiusEnvironmentResource, (lease.operation_id, key))
            if row is None:
                raise ManagementError("resource_intent_missing")
            if row.provider_identity is not None:
                if row.provider_identity != provider_identity:
                    raise ManagementError("resource_identity_conflict")
                return
            earlier = await session.scalar(select(func.count()).select_from(NebiusEnvironmentResource).where(
                NebiusEnvironmentResource.operation_id == lease.operation_id,
                NebiusEnvironmentResource.phase == "planned", NebiusEnvironmentResource.sequence < row.sequence,
            ))
            if earlier:
                raise ManagementError("resource_step_out_of_order")
            row.provider_identity = provider_identity
            row.phase = "applied"

    async def complete(self, lease: OperationLease) -> None:
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased_operation(session, lease)
            rows = (await session.execute(select(NebiusEnvironmentResource).where(
                NebiusEnvironmentResource.operation_id == lease.operation_id,
            ).order_by(NebiusEnvironmentResource.sequence))).scalars().all()
            if not rows or rows[-1].kind != "application_ready" or any(row.phase != "applied" for row in rows):
                raise ManagementError("operation_resources_incomplete")
            operation.phase = "completed"
            operation.error_code = None
            operation.lease_token = None
            operation.lease_expires_at = None
