"""Live, candidate-aware preparation runtime for personal dev generations."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from loom.dev_instance import DevInstanceIdentity, derive_identity
from loom.dev_instance_manifest import (
    DevInstanceManifestConfig,
    PersonalDevManifestBinding,
)
from loom.dev_instance_provision import provisioning_plan
from loom.dev_instance_provisioner import (
    AccessBootstrap,
    BucketEnsurer,
    ObjectStoreTenant,
    OwnerAccessSnapshot,
    SqlExecutor,
    dev_buckets,
)
from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom.personal_dev_reconciler import (
    PersonalDevReadinessObservation,
    personal_dev_candidate_images,
)


class ReusablePersonalDevSecretVault(Protocol):
    async def database_password(self, identity: DevInstanceIdentity) -> str | None: ...

    async def store(self, identity: DevInstanceIdentity, password: str) -> str: ...


class CandidateGenerationProvisioner(Protocol):
    async def prepare(
        self,
        identity: DevInstanceIdentity,
        config: DevInstanceManifestConfig,
    ) -> PersonalDevReadinessObservation: ...


@dataclass(frozen=True, slots=True)
class PersonalDevRuntimeConfig:
    """Operator-owned values shared by all personal candidate generations."""

    minio_endpoint: str
    minio_region: str = "us-east-1"
    ingress_class_name: str = "nginx"
    ingress_cert_manager_cluster_issuer: str = "letsencrypt-prod"
    image_pull_policy: str = "IfNotPresent"

    def __post_init__(self) -> None:
        if not self.minio_endpoint.startswith(("http://", "https://")):
            raise ValueError("personal-dev MinIO endpoint must be an HTTP(S) URL")
        if self.image_pull_policy not in {"Always", "IfNotPresent", "Never"}:
            raise ValueError("personal-dev image pull policy is invalid")


@dataclass(slots=True)
class PersonalDevPreparationRuntime:
    """Idempotently prepare fixtures and one non-current candidate generation."""

    config: PersonalDevRuntimeConfig
    sql: SqlExecutor
    buckets: BucketEnsurer
    vault: ReusablePersonalDevSecretVault
    object_store_tenant: ObjectStoreTenant
    cluster: CandidateGenerationProvisioner
    access: AccessBootstrap
    password_factory: Callable[[], str] = field(default=lambda: secrets.token_hex(10))

    async def prepare(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        access: OwnerAccessSnapshot,
    ) -> PersonalDevReadinessObservation:
        operation = claim.operation
        if operation.kind not in {"create", "update"}:
            raise ValueError("personal-dev preparation requires a create or update operation")
        if access.user_id != operation.owner_user_id or access.team_id != operation.owner_team_id:
            raise RuntimeError("personal-dev bootstrap owner changed during reconciliation")
        images = personal_dev_candidate_images(claim)
        identity = derive_identity(operation.environment_name)
        manifest_config = DevInstanceManifestConfig(
            image_tag="",
            candidate_sha=operation.candidate_sha,
            deployment_generation=operation.deployment_generation,
            container_registry="",
            minio_endpoint=self.config.minio_endpoint,
            minio_region=self.config.minio_region,
            ingress_class_name=self.config.ingress_class_name,
            ingress_cert_manager_cluster_issuer=(self.config.ingress_cert_manager_cluster_issuer),
            image_pull_policy=self.config.image_pull_policy,
            image_references=images,
            lifecycle_binding=PersonalDevManifestBinding(
                subject_id=operation.subject_id,
                subject_incarnation=operation.subject_incarnation,
                operation_id=operation.id,
                attempt_id=claim.attempt.id,
                operation_epoch=operation.operation_epoch,
            ),
        )
        password = await self.vault.database_password(identity)
        if password is None:
            if operation.kind == "update":
                raise RuntimeError("personal-dev update has no existing fixture credential")
            password = self.password_factory()
        plan = provisioning_plan(identity.name, password)
        await self.sql.apply_role_and_database(
            identity,
            role_sql=str(plan["role_sql"]),
            create_database_sql=str(plan["create_database_sql"]),
        )
        await self.buckets.ensure_buckets(identity, dev_buckets(identity))
        await self.vault.store(identity, password)
        await self.object_store_tenant.converge(identity)
        return await self.cluster.prepare(identity, manifest_config)

    async def bootstrap_access(
        self,
        claim: PersonalDevReconciliationClaim,
        *,
        access: OwnerAccessSnapshot,
    ) -> None:
        """Install only a freshly revalidated attempt-bound owner credential."""
        operation = claim.operation
        if access.user_id != operation.owner_user_id or access.team_id != operation.owner_team_id:
            raise RuntimeError("personal-dev bootstrap owner changed during reconciliation")
        identity = derive_identity(operation.environment_name)
        password = await self.vault.database_password(identity)
        if password is None:
            raise RuntimeError("personal-dev fixture credential disappeared before bootstrap")
        await self.access.bootstrap(identity, password=password, access=access)


__all__ = [
    "CandidateGenerationProvisioner",
    "PersonalDevPreparationRuntime",
    "PersonalDevRuntimeConfig",
    "ReusablePersonalDevSecretVault",
]
