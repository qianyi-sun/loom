"""Live, candidate-aware preparation runtime for personal dev generations."""

from __future__ import annotations

import json
import re
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

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
from loom.personal_dev_capacity import PersonalDevCapacityManagerBinding
from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom.personal_dev_reconciler import (
    PersonalDevReadinessObservation,
    personal_dev_candidate_images,
)

_ACCEPTANCE_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_ACCEPTANCE_BINDING_MAX_BYTES = 16 * 1024
_ACCEPTANCE_BINDING_FIELDS = {
    "acceptance_plan_sha256",
    "expires_at",
    "manager",
    "schema_version",
    "started_at",
}
_ACCEPTANCE_MANAGER_FIELDS = {
    "authority_incarnation",
    "configuration_epoch",
    "executable_new_capacity_ceiling",
    "execution_epoch",
    "execution_state",
    "observer_principal_id",
}
_OPERATIONAL_BINDING_FIELDS = {
    "acceptance_result_sha256",
    "manager",
    "operational_plan_sha256",
    "schema_version",
}

PersonalDevAcceptanceBlockerCode = Literal[
    "acceptance-binding-invalid",
    "acceptance-time-invalid",
    "acceptance-window-not-open",
    "acceptance-window-expired",
    "capacity-manager-unavailable",
    "capacity-manager-binding-drift",
]


class PersonalDevAcceptanceInterlockError(RuntimeError):
    """One stable secret-free blocker for the acceptance runtime gate."""

    def __init__(self, code: PersonalDevAcceptanceBlockerCode) -> None:
        self.code = code
        super().__init__(code)


PersonalDevOperationalBlockerCode = Literal[
    "operational-binding-invalid",
    "operational-time-invalid",
    "capacity-manager-unavailable",
    "capacity-manager-binding-drift",
]


class PersonalDevOperationalInterlockError(RuntimeError):
    """One stable secret-free blocker for durable personal development."""

    def __init__(self, code: PersonalDevOperationalBlockerCode) -> None:
        self.code = code
        super().__init__(code)


class PersonalDevManagerBindingReader(Protocol):
    async def current_manager_binding(self) -> PersonalDevCapacityManagerBinding: ...


def _unique_acceptance_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate acceptance binding field")
        result[key] = value
    return result


def _acceptance_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or len(value) != 20:
        raise ValueError("acceptance timestamp is invalid")
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise ValueError("acceptance timestamp is not canonical")
    return parsed


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceRuntimeBinding:
    """Parsed, canonical, secret-free acceptance runtime configuration."""

    acceptance_plan_sha256: str
    expected_manager: PersonalDevCapacityManagerBinding
    started_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class PersonalDevOperationalRuntimeBinding:
    """Parsed, canonical, non-expiring zero-capacity runtime configuration."""

    operational_plan_sha256: str
    acceptance_result_sha256: str
    expected_manager: PersonalDevCapacityManagerBinding


def parse_personal_dev_acceptance_runtime_binding(
    binding_json: str,
    expected_plan_sha256: str,
) -> PersonalDevAcceptanceRuntimeBinding:
    """Parse the canonical binding before opening any owned network client."""

    try:
        if (
            not isinstance(binding_json, str)
            or not 0 < len(binding_json.encode("ascii")) <= _ACCEPTANCE_BINDING_MAX_BYTES
            or _ACCEPTANCE_DIGEST_RE.fullmatch(expected_plan_sha256) is None
            or expected_plan_sha256 == "0" * 64
        ):
            raise ValueError
        document = json.loads(
            binding_json,
            object_pairs_hook=_unique_acceptance_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        canonical = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        if (
            not isinstance(document, dict)
            or set(document) != _ACCEPTANCE_BINDING_FIELDS
            or canonical != binding_json
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or document["acceptance_plan_sha256"] != expected_plan_sha256
        ):
            raise ValueError
        manager = document["manager"]
        if not isinstance(manager, dict) or set(manager) != _ACCEPTANCE_MANAGER_FIELDS:
            raise ValueError
        raw_authority = manager["authority_incarnation"]
        if not isinstance(raw_authority, str):
            raise ValueError
        authority = UUID(raw_authority)
        if str(authority) != raw_authority:
            raise ValueError
        expected_manager = PersonalDevCapacityManagerBinding(
            authority_incarnation=authority,
            observer_principal_id=manager["observer_principal_id"],
            configuration_epoch=manager["configuration_epoch"],
            execution_state=manager["execution_state"],
            execution_epoch=manager["execution_epoch"],
            executable_new_capacity_ceiling=manager["executable_new_capacity_ceiling"],
        )
        if (
            expected_manager.execution_state not in {"shadow", "prepared", "drain-only"}
            or expected_manager.executable_new_capacity_ceiling != 0
        ):
            raise ValueError
        started_at = _acceptance_timestamp(document["started_at"])
        expires_at = _acceptance_timestamp(document["expires_at"])
        if started_at >= expires_at:
            raise ValueError
    except (
        AttributeError,
        RecursionError,
        UnicodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ):
        raise PersonalDevAcceptanceInterlockError("acceptance-binding-invalid") from None
    return PersonalDevAcceptanceRuntimeBinding(
        acceptance_plan_sha256=expected_plan_sha256,
        expected_manager=expected_manager,
        started_at=started_at,
        expires_at=expires_at,
    )


def parse_personal_dev_operational_runtime_binding(
    binding_json: str,
    expected_plan_sha256: str,
) -> PersonalDevOperationalRuntimeBinding:
    """Parse a durable binding before opening any owned network client."""

    try:
        if (
            not isinstance(binding_json, str)
            or not 0 < len(binding_json.encode("ascii")) <= _ACCEPTANCE_BINDING_MAX_BYTES
            or _ACCEPTANCE_DIGEST_RE.fullmatch(expected_plan_sha256) is None
            or expected_plan_sha256 == "0" * 64
        ):
            raise ValueError
        document = json.loads(
            binding_json,
            object_pairs_hook=_unique_acceptance_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        canonical = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        if (
            not isinstance(document, dict)
            or set(document) != _OPERATIONAL_BINDING_FIELDS
            or canonical != binding_json
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or document["operational_plan_sha256"] != expected_plan_sha256
            or not isinstance(document["acceptance_result_sha256"], str)
            or _ACCEPTANCE_DIGEST_RE.fullmatch(document["acceptance_result_sha256"]) is None
            or document["acceptance_result_sha256"] == "0" * 64
        ):
            raise ValueError
        manager = document["manager"]
        if not isinstance(manager, dict) or set(manager) != _ACCEPTANCE_MANAGER_FIELDS:
            raise ValueError
        raw_authority = manager["authority_incarnation"]
        if not isinstance(raw_authority, str):
            raise ValueError
        authority = UUID(raw_authority)
        if str(authority) != raw_authority:
            raise ValueError
        expected_manager = PersonalDevCapacityManagerBinding(
            authority_incarnation=authority,
            observer_principal_id=manager["observer_principal_id"],
            configuration_epoch=manager["configuration_epoch"],
            execution_state=manager["execution_state"],
            execution_epoch=manager["execution_epoch"],
            executable_new_capacity_ceiling=manager["executable_new_capacity_ceiling"],
        )
        if (
            expected_manager.execution_state not in {"shadow", "prepared", "drain-only"}
            or expected_manager.executable_new_capacity_ceiling != 0
        ):
            raise ValueError
    except (
        AttributeError,
        RecursionError,
        UnicodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ):
        raise PersonalDevOperationalInterlockError("operational-binding-invalid") from None
    return PersonalDevOperationalRuntimeBinding(
        operational_plan_sha256=expected_plan_sha256,
        acceptance_result_sha256=document["acceptance_result_sha256"],
        expected_manager=expected_manager,
    )


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceInterlock:
    """Continuously bind personal operations to one zero-capacity manager."""

    projector: PersonalDevManagerBindingReader
    acceptance_plan_sha256: str
    expected_manager: PersonalDevCapacityManagerBinding
    started_at: datetime
    expires_at: datetime

    @classmethod
    def from_binding(
        cls,
        *,
        projector: PersonalDevManagerBindingReader,
        binding: PersonalDevAcceptanceRuntimeBinding,
    ) -> PersonalDevAcceptanceInterlock:
        if not callable(getattr(projector, "current_manager_binding", None)) or not isinstance(
            binding, PersonalDevAcceptanceRuntimeBinding
        ):
            raise PersonalDevAcceptanceInterlockError("acceptance-binding-invalid")
        return cls(
            projector=projector,
            acceptance_plan_sha256=binding.acceptance_plan_sha256,
            expected_manager=binding.expected_manager,
            started_at=binding.started_at,
            expires_at=binding.expires_at,
        )

    @classmethod
    def from_json(
        cls,
        *,
        projector: PersonalDevManagerBindingReader,
        binding_json: str,
        expected_plan_sha256: str,
    ) -> PersonalDevAcceptanceInterlock:
        return cls.from_binding(
            projector=projector,
            binding=parse_personal_dev_acceptance_runtime_binding(
                binding_json,
                expected_plan_sha256,
            ),
        )

    async def assert_ready(self, *, now: datetime) -> None:
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise PersonalDevAcceptanceInterlockError("acceptance-time-invalid")
        observed_at = now.astimezone(UTC)
        if observed_at < self.started_at:
            raise PersonalDevAcceptanceInterlockError("acceptance-window-not-open")
        if observed_at >= self.expires_at:
            raise PersonalDevAcceptanceInterlockError("acceptance-window-expired")
        try:
            observed = await self.projector.current_manager_binding()
        except Exception:
            raise PersonalDevAcceptanceInterlockError("capacity-manager-unavailable") from None
        if not observed.satisfies_acceptance_boundary(self.expected_manager):
            raise PersonalDevAcceptanceInterlockError("capacity-manager-binding-drift")


@dataclass(frozen=True, slots=True)
class PersonalDevOperationalInterlock:
    """Continuously bind durable personal operations to zero-capacity authority."""

    projector: PersonalDevManagerBindingReader
    operational_plan_sha256: str
    acceptance_result_sha256: str
    expected_manager: PersonalDevCapacityManagerBinding

    @classmethod
    def from_binding(
        cls,
        *,
        projector: PersonalDevManagerBindingReader,
        binding: PersonalDevOperationalRuntimeBinding,
    ) -> PersonalDevOperationalInterlock:
        if not callable(getattr(projector, "current_manager_binding", None)) or not isinstance(
            binding, PersonalDevOperationalRuntimeBinding
        ):
            raise PersonalDevOperationalInterlockError("operational-binding-invalid")
        return cls(
            projector=projector,
            operational_plan_sha256=binding.operational_plan_sha256,
            acceptance_result_sha256=binding.acceptance_result_sha256,
            expected_manager=binding.expected_manager,
        )

    @classmethod
    def from_json(
        cls,
        *,
        projector: PersonalDevManagerBindingReader,
        binding_json: str,
        expected_plan_sha256: str,
    ) -> PersonalDevOperationalInterlock:
        return cls.from_binding(
            projector=projector,
            binding=parse_personal_dev_operational_runtime_binding(
                binding_json,
                expected_plan_sha256,
            ),
        )

    async def assert_ready(self, *, now: datetime) -> None:
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise PersonalDevOperationalInterlockError("operational-time-invalid")
        try:
            observed = await self.projector.current_manager_binding()
        except Exception:
            raise PersonalDevOperationalInterlockError("capacity-manager-unavailable") from None
        if not observed.satisfies_acceptance_boundary(self.expected_manager):
            raise PersonalDevOperationalInterlockError("capacity-manager-binding-drift")


class ReusablePersonalDevSecretVault(Protocol):
    async def database_password(self, identity: DevInstanceIdentity) -> str | None: ...

    async def store(self, identity: DevInstanceIdentity, password: str) -> str: ...

    async def delete(self, identity: DevInstanceIdentity) -> None: ...


class CandidateGenerationProvisioner(Protocol):
    async def bootstrap(
        self,
        identity: DevInstanceIdentity,
        config: DevInstanceManifestConfig,
    ) -> None: ...

    async def prepare(
        self,
        identity: DevInstanceIdentity,
        config: DevInstanceManifestConfig,
    ) -> PersonalDevReadinessObservation: ...

    async def destroy(self, identity: DevInstanceIdentity) -> None: ...


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

    def _manifest_config(
        self,
        claim: PersonalDevReconciliationClaim,
    ) -> tuple[DevInstanceIdentity, DevInstanceManifestConfig]:
        operation = claim.operation
        images = personal_dev_candidate_images(claim)
        identity = derive_identity(operation.environment_name)
        return identity, DevInstanceManifestConfig(
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
        identity, manifest_config = self._manifest_config(claim)
        await self.cluster.bootstrap(identity, manifest_config)
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
        identity, manifest_config = self._manifest_config(claim)
        await self.cluster.bootstrap(identity, manifest_config)
        password = await self.vault.database_password(identity)
        if password is None:
            raise RuntimeError("personal-dev fixture credential disappeared before bootstrap")
        await self.access.bootstrap(identity, password=password, access=access)

    @staticmethod
    def _destroy_identity(claim: PersonalDevReconciliationClaim) -> DevInstanceIdentity:
        if claim.operation.kind != "destroy":
            raise ValueError("personal-dev cleanup requires a destroy operation")
        return derive_identity(claim.operation.environment_name)

    async def delete_namespace(self, claim: PersonalDevReconciliationClaim) -> None:
        await self.cluster.destroy(self._destroy_identity(claim))

    async def delete_buckets(self, claim: PersonalDevReconciliationClaim) -> None:
        identity = self._destroy_identity(claim)
        await self.buckets.remove_buckets(identity, dev_buckets(identity))

    async def delete_tenant(self, claim: PersonalDevReconciliationClaim) -> None:
        await self.object_store_tenant.delete(self._destroy_identity(claim))

    async def delete_credentials(self, claim: PersonalDevReconciliationClaim) -> None:
        await self.vault.delete(self._destroy_identity(claim))


__all__ = [
    "CandidateGenerationProvisioner",
    "PersonalDevAcceptanceBlockerCode",
    "PersonalDevAcceptanceInterlock",
    "PersonalDevAcceptanceInterlockError",
    "PersonalDevAcceptanceRuntimeBinding",
    "PersonalDevManagerBindingReader",
    "PersonalDevOperationalBlockerCode",
    "PersonalDevOperationalInterlock",
    "PersonalDevOperationalInterlockError",
    "PersonalDevOperationalRuntimeBinding",
    "PersonalDevPreparationRuntime",
    "PersonalDevRuntimeConfig",
    "ReusablePersonalDevSecretVault",
    "parse_personal_dev_acceptance_runtime_binding",
    "parse_personal_dev_operational_runtime_binding",
]
