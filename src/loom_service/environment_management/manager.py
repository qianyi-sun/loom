"""Owner requests resolve protected publications before rendering or reservation."""

from __future__ import annotations

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
    def __init__(self, registry: EnvironmentRegistry, plans: EnvironmentPlanFactory):
        self.registry = registry
        self.plans = plans

    async def create(self, principal: AuthContext, request: EnvironmentCreateRequestV1, *,
                     idempotency_key: str) -> EnvironmentOperationV1:
        prepared = await self.plans.prepare(principal, request)
        return await self.registry.create(principal=principal, idempotency_key=idempotency_key, prepared=prepared)
