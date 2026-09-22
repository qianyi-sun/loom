"""Owner requests resolve protected publications before rendering or reservation."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from loom.auth import AuthContext
from loom.nebius_environment_contract import (
    EnvironmentCreateRequestV1,
    EnvironmentOperationV1,
    EnvironmentRegistrationV1,
    FoundationBinding,
    new_environment_registration,
)
from loom.nebius_environment_render import RenderedEnvironment, render_environment
from loom_service.environment_management.child_client import ChildEnvironmentClient
from loom_service.environment_management.provider import ProviderError
from loom_service.environment_management.registry import (
    EnvironmentRegistry,
    ManagementError,
    owner_identity,
)


@dataclass(frozen=True)
class CandidateBundle:
    """Returned only after the protected publication adapter verifies approval."""

    candidate_id: UUID
    candidate: dict[str, Any]
    profile: dict[str, Any]


class CandidateCatalog(Protocol):
    async def resolve(self, candidate_id: UUID) -> CandidateBundle: ...


class EnvironmentPlanFactory:
    def __init__(self, foundation: FoundationBinding, catalog: CandidateCatalog, *,
                 keyring: dict[str, Any], repo_root: Path):
        self.foundation = foundation
        self.catalog = catalog
        self.keyring = keyring
        self.repo_root = repo_root

    async def prepare(self, principal: AuthContext, request: EnvironmentCreateRequestV1) -> RenderedEnvironment:
        owner, team = owner_identity(principal, mutation=True)
        bundle = await self.catalog.resolve(request.candidate_id)
        if bundle.candidate_id != request.candidate_id:
            raise ManagementError("candidate_identity_mismatch", 503)
        registration = new_environment_registration(
            self.foundation, environment_id=uuid4(), incarnation=uuid4(), owner_user_id=owner,
            owner_team_id=team, slug=request.slug,
        )
        registration = EnvironmentRegistrationV1.model_validate({
            **registration.model_dump(), "candidate_id": request.candidate_id,
        })
        return render_environment(registration, bundle.candidate, self.foundation,
                                  profile=bundle.profile, keyring=self.keyring, repo_root=self.repo_root)


class EnvironmentManager:
    def __init__(self, registry: EnvironmentRegistry, plans: EnvironmentPlanFactory, *, child: ChildEnvironmentClient | None = None):
        self.registry = registry
        self.plans = plans
        self.child = child

    async def login(self, principal: AuthContext, environment_id: UUID) -> dict[str, Any]:
        row, material = await self.registry.ready_access(environment_id, principal=principal)
        if self.child is None:
            raise ManagementError("child_login_not_configured", 503)
        try:
            token = tomllib.loads(material["loom-admin-secret"]["secrets.toml"])["admin"]["token"]
            if not isinstance(token, str) or not token:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ManagementError("environment_credentials_unavailable", 503) from None
        try:
            return await self.child.login(row, admin_token=token)
        except ProviderError as exc:
            raise ManagementError(exc.code, 503) from None

    async def create(self, principal: AuthContext, request: EnvironmentCreateRequestV1, *,
                     idempotency_key: str) -> EnvironmentOperationV1:
        replay = await self.registry.replay_create(
            principal=principal, idempotency_key=idempotency_key, request=request,
            cluster_id=self.plans.foundation.platform_config["cluster_id"],
        )
        if replay is not None:
            return replay
        prepared = await self.plans.prepare(principal, request)
        return await self.registry.create(principal=principal, idempotency_key=idempotency_key, prepared=prepared)
