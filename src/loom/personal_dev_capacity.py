"""Trusted personal-development registration with the global capacity manager."""

from __future__ import annotations

import hashlib
import re
import ssl
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, cast
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    canonical_manager_origin,
    read_owner_only_bearer_token,
)
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    DynamicDevelopmentSubjectProjectionV1,
    SubjectConfigurationV1,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_MAX_RESPONSE_BYTES = 64 * 1024


class PersonalDevCapacityProjectionError(RuntimeError):
    """The global manager did not verifiably accept a personal subject."""


class PersonalDevCapacityProjectionConflictError(PersonalDevCapacityProjectionError):
    """The global configuration epoch changed before projection."""


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityManagerCheckpoint:
    """Exact manager execution checkpoint used separately from application readiness."""

    configuration_epoch: int
    execution_state: Literal["shadow", "prepared", "active", "drain-only"]
    execution_epoch: int
    executable_new_capacity_ceiling: int

    def __post_init__(self) -> None:
        if self.execution_state not in {"shadow", "prepared", "active", "drain-only"}:
            raise ValueError("capacity manager checkpoint execution state is invalid")
        if type(self.configuration_epoch) is not int or self.configuration_epoch <= 0:
            raise ValueError("capacity manager checkpoint configuration epoch is invalid")
        if type(self.execution_epoch) is not int or self.execution_epoch < 0:
            raise ValueError("capacity manager checkpoint execution epoch is invalid")
        if (
            type(self.executable_new_capacity_ceiling) is not int
            or self.executable_new_capacity_ceiling < 0
        ):
            raise ValueError("capacity manager checkpoint ceiling is invalid")
        if self.execution_state == "shadow":
            coherent = self.execution_epoch == 0 and self.executable_new_capacity_ceiling == 0
        elif self.execution_state in {"prepared", "drain-only"}:
            coherent = self.execution_epoch > 0 and self.executable_new_capacity_ceiling == 0
        else:
            coherent = self.execution_epoch > 0 and self.executable_new_capacity_ceiling > 0
        if not coherent:
            raise ValueError("capacity manager checkpoint is internally inconsistent")


@dataclass(frozen=True, slots=True)
class PersonalDevCapacitySubjectStatus:
    """Fresh manager-owned physical intent evidence for one personal subject."""

    subject_id: UUID
    subject_incarnation: UUID
    deployment_generation: int
    checkpoint: PersonalDevCapacityManagerCheckpoint
    capacity_prepared: bool
    capacity_status: Literal["shadow", "prepared", "waiting", "available"]
    active_bindings: tuple[ExecutableIntentBindingV2, ...]

    def __post_init__(self) -> None:
        if not self.capacity_prepared:
            raise ValueError("manager subject status is not capacity prepared")
        if len({binding.intent_id for binding in self.active_bindings}) != len(
            self.active_bindings
        ):
            raise ValueError("manager subject status has duplicate active bindings")
        for binding in self.active_bindings:
            if (
                binding.subject_id != self.subject_id
                or binding.subject_incarnation != self.subject_incarnation
                or binding.deployment_generation != self.deployment_generation
                or binding.execution.execution_epoch != self.checkpoint.execution_epoch
            ):
                raise ValueError("manager active binding differs from subject status")


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityAvailability:
    """Non-authoritative presentation of fresh manager and guard evidence."""

    capacity_status: Literal["shadow", "prepared", "waiting", "available"]
    capacity_prepared: bool
    worker_available: bool

    def __post_init__(self) -> None:
        if self.capacity_status == "available" and not (
            self.capacity_prepared and self.worker_available
        ):
            raise ValueError("available capacity requires protected worker evidence")
        if self.worker_available != (self.capacity_status == "available"):
            raise ValueError("worker availability must match available capacity status")


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityManagerConnection:
    manager_origin: str
    bearer_token_file: Path
    tls_files: DemandReporterTLSFiles
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        canonical_manager_origin(self.manager_origin)
        if not isinstance(self.bearer_token_file, Path):
            raise TypeError("capacity lifecycle credential must be an explicit path")
        if not isinstance(self.tls_files, DemandReporterTLSFiles):
            raise TypeError("capacity lifecycle TLS files are invalid")
        if type(self.timeout_seconds) is not float or not 0 < self.timeout_seconds <= 60:
            raise ValueError("capacity lifecycle timeout must be a float between 0 and 60")


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityInstallation:
    """Secret-bearing result of converging candidate-independent local authority."""

    reporter_incarnation: UUID
    reporter_token: str = field(repr=False)
    protected_admission_sha256: str
    capacity_agent_installation_sha256: str
    supported_pool_ids: tuple[Literal["oldlab", "gb10"], ...]
    supported_architectures: tuple[Literal["x86_64", "arm64"], ...]

    def __post_init__(self) -> None:
        if (
            not self.reporter_token
            or len(self.reporter_token.encode("utf-8")) > 16 * 1024
            or not self.reporter_token.isascii()
            or any(not 0x21 <= ord(character) <= 0x7E for character in self.reporter_token)
        ):
            raise ValueError("capacity reporter token is invalid")
        if any(
            _DIGEST_RE.fullmatch(value) is None
            for value in (
                self.protected_admission_sha256,
                self.capacity_agent_installation_sha256,
            )
        ):
            raise ValueError("capacity installation evidence must use SHA-256 digests")
        if (
            not self.supported_pool_ids
            or self.supported_pool_ids != tuple(sorted(set(self.supported_pool_ids)))
            or not self.supported_architectures
            or self.supported_architectures != tuple(sorted(set(self.supported_architectures)))
        ):
            raise ValueError("capacity installation capabilities must be nonempty and canonical")

    @property
    def reporter_token_sha256(self) -> str:
        return hashlib.sha256(self.reporter_token.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityProjectionResult:
    """Exact secret-free manager acknowledgement retained by the lifecycle."""

    configuration_epoch: int
    configuration_digest: str
    subject_id: UUID
    subject_incarnation: UUID
    configuration_generation: int
    deployment_generation: int
    reporter_incarnation: UUID
    replayed: bool

    def __post_init__(self) -> None:
        if type(self.configuration_epoch) is not int or self.configuration_epoch <= 0:
            raise ValueError("capacity configuration epoch must be positive")
        if _DIGEST_RE.fullmatch(self.configuration_digest) is None:
            raise ValueError("capacity configuration digest must be a SHA-256 digest")
        if (
            type(self.configuration_generation) is not int
            or self.configuration_generation <= 0
            or type(self.deployment_generation) is not int
            or self.deployment_generation <= 0
        ):
            raise ValueError("capacity projection generations must be positive")
        if type(self.replayed) is not bool:
            raise ValueError("capacity projection replay marker must be boolean")


class PersonalDevCapacityInstaller(Protocol):
    async def converge(
        self,
        claim: PersonalDevReconciliationClaim,
    ) -> PersonalDevCapacityInstallation: ...

    async def verify_publishing(
        self,
        claim: PersonalDevReconciliationClaim,
        installation: PersonalDevCapacityInstallation,
    ) -> None: ...

    async def seal(self, claim: PersonalDevReconciliationClaim) -> None: ...

    async def destroy(self, claim: PersonalDevReconciliationClaim) -> None: ...


class PersonalDevCapacityProjector(Protocol):
    async def current_manager_checkpoint(self) -> PersonalDevCapacityManagerCheckpoint: ...

    async def project(
        self,
        request: DynamicDevelopmentSubjectProjectionV1,
        *,
        idempotency_key: UUID,
    ) -> PersonalDevCapacityProjectionResult: ...


class _ProjectionResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    configuration_epoch: int
    configuration_digest: str
    subject: SubjectConfigurationV1
    account: AccountPolicyV1
    replayed: bool


class _SubjectStatusResponseV2(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[2]
    subject_id: UUID
    subject_incarnation: UUID
    deployment_generation: int
    configuration_epoch: int
    execution_epoch: int
    execution_state: Literal["shadow", "prepared", "active", "drain-only"]
    executable_new_capacity_ceiling: int
    capacity_prepared: bool
    capacity_status: Literal["shadow", "prepared", "waiting", "available"]
    worker_available: bool
    active_capacity_intents: tuple[ExecutableIntentBindingV2, ...]
    active_capacity_intent_count: int
    active_capacity_slots: int
    quarantined_intent_count: int
    intent_state_counts: dict[str, int]
    blockers: tuple[str, ...]

    @model_validator(mode="after")
    def _coherent_status(self) -> _SubjectStatusResponseV2:
        state_counts = self.intent_state_counts
        if any(type(value) is not int or value < 0 for value in state_counts.values()):
            raise ValueError("capacity manager intent state counts are invalid")
        if self.quarantined_intent_count != state_counts.get("quarantined", 0):
            raise ValueError("capacity manager quarantine count is inconsistent")
        if self.active_capacity_intent_count != len(self.active_capacity_intents):
            raise ValueError("capacity manager active binding count is inconsistent")
        if self.active_capacity_slots != sum(
            binding.concurrency_slots for binding in self.active_capacity_intents
        ):
            raise ValueError("capacity manager active binding slots are inconsistent")
        if len({binding.intent_id for binding in self.active_capacity_intents}) != len(
            self.active_capacity_intents
        ):
            raise ValueError("capacity manager active bindings are duplicated")
        if self.execution_state == "shadow" and (
            self.execution_epoch != 0
            or self.executable_new_capacity_ceiling != 0
            or self.capacity_status != "shadow"
            or self.active_capacity_intents
        ):
            raise ValueError("capacity manager shadow status is inconsistent")
        if self.execution_state in {"prepared", "drain-only"} and (
            self.execution_epoch <= 0
            or self.executable_new_capacity_ceiling != 0
            or self.capacity_status != "prepared"
            or self.active_capacity_intents
        ):
            raise ValueError("capacity manager prepared status is inconsistent")
        if self.execution_state == "active" and (
            self.execution_epoch <= 0
            or self.executable_new_capacity_ceiling <= 0
            or self.capacity_status != "waiting"
        ):
            raise ValueError("capacity manager active status is inconsistent")
        return self


class CapacityManagerPersonalDevProjector:
    """Fail-closed mTLS client for lifecycle-owned subject projection."""

    def __init__(
        self,
        *,
        manager_origin: str,
        bearer_token: str,
        http_client: httpx.AsyncClient,
        owns_http_client: bool = False,
    ) -> None:
        if (
            not bearer_token
            or len(bearer_token.encode("utf-8")) > 16 * 1024
            or not bearer_token.isascii()
            or any(not 0x21 <= ord(character) <= 0x7E for character in bearer_token)
        ):
            raise ValueError("capacity lifecycle bearer credential is invalid")
        if not isinstance(http_client, httpx.AsyncClient):
            raise TypeError("capacity lifecycle HTTP client must be asynchronous")
        self._origin = canonical_manager_origin(manager_origin)
        self._token = bearer_token
        self._http = http_client
        self._owns_http = owns_http_client

    @classmethod
    def from_files(
        cls,
        connection: PersonalDevCapacityManagerConnection,
    ) -> CapacityManagerPersonalDevProjector:
        token = read_owner_only_bearer_token(connection.bearer_token_file)
        tls: ssl.SSLContext = build_reporter_tls_context(connection.tls_files)
        client = httpx.AsyncClient(
            verify=tls,
            timeout=httpx.Timeout(connection.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        )
        return cls(
            manager_origin=connection.manager_origin,
            bearer_token=token,
            http_client=client,
            owns_http_client=True,
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    @staticmethod
    def _bounded_json(response: httpx.Response) -> object:
        if len(response.content) > _MAX_RESPONSE_BYTES:
            raise PersonalDevCapacityProjectionError(
                "capacity manager response exceeds its size bound"
            )
        content_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
        if content_type != "application/json":
            raise PersonalDevCapacityProjectionError(
                "capacity manager response is not canonical JSON"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager returned invalid JSON"
            ) from exc

    async def current_manager_checkpoint(self) -> PersonalDevCapacityManagerCheckpoint:
        try:
            response = await self._http.get(f"{self._origin}/v1/status", headers=self._headers)
        except httpx.HTTPError as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager status request failed"
            ) from exc
        if response.status_code != 200:
            raise PersonalDevCapacityProjectionError(
                f"capacity manager status request returned {response.status_code}"
            )
        payload = self._bounded_json(response)
        if not isinstance(payload, dict):
            raise PersonalDevCapacityProjectionError("capacity manager status is invalid")
        try:
            return PersonalDevCapacityManagerCheckpoint(
                configuration_epoch=cast(int, payload.get("configuration_epoch")),
                execution_state=cast(
                    Literal["shadow", "prepared", "active", "drain-only"],
                    payload.get("execution_state"),
                ),
                execution_epoch=cast(int, payload.get("execution_epoch")),
                executable_new_capacity_ceiling=cast(
                    int, payload.get("executable_new_capacity_ceiling")
                ),
            )
        except (TypeError, ValueError) as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager checkpoint is invalid"
            ) from exc

    async def subject_status(
        self,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
        deployment_generation: int,
    ) -> PersonalDevCapacitySubjectStatus:
        """Read the bounded exact manager inventory for one stored identity."""

        if not isinstance(subject_id, UUID) or not isinstance(subject_incarnation, UUID):
            raise TypeError("capacity subject status requires exact UUID identity")
        if type(deployment_generation) is not int or deployment_generation <= 0:
            raise ValueError("capacity subject status deployment generation is invalid")
        try:
            response = await self._http.get(
                f"{self._origin}/v2/status/subjects/{subject_id}", headers=self._headers
            )
        except httpx.HTTPError as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager subject status request failed"
            ) from exc
        if response.status_code != 200:
            raise PersonalDevCapacityProjectionError(
                f"capacity manager subject status returned {response.status_code}"
            )
        try:
            self._bounded_json(response)
            parsed = _SubjectStatusResponseV2.model_validate_json(response.content)
            checkpoint = PersonalDevCapacityManagerCheckpoint(
                configuration_epoch=parsed.configuration_epoch,
                execution_state=parsed.execution_state,
                execution_epoch=parsed.execution_epoch,
                executable_new_capacity_ceiling=parsed.executable_new_capacity_ceiling,
            )
            result = PersonalDevCapacitySubjectStatus(
                subject_id=parsed.subject_id,
                subject_incarnation=parsed.subject_incarnation,
                deployment_generation=parsed.deployment_generation,
                checkpoint=checkpoint,
                capacity_prepared=parsed.capacity_prepared,
                capacity_status=parsed.capacity_status,
                active_bindings=parsed.active_capacity_intents,
            )
        except (TypeError, ValidationError, ValueError) as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager subject status is invalid"
            ) from exc
        if (
            result.subject_id != subject_id
            or result.subject_incarnation != subject_incarnation
            or result.deployment_generation != deployment_generation
            or parsed.worker_available
            or parsed.active_capacity_intent_count != len(result.active_bindings)
            or parsed.active_capacity_slots
            != sum(binding.concurrency_slots for binding in result.active_bindings)
            or parsed.quarantined_intent_count < 0
            or any(
                type(value) is not int or value < 0 for value in parsed.intent_state_counts.values()
            )
            or tuple(sorted(set(parsed.blockers))) != parsed.blockers
        ):
            raise PersonalDevCapacityProjectionError(
                "capacity manager subject status differs from the exact stored identity"
            )
        if checkpoint.execution_state == "shadow" and (
            result.capacity_status != "shadow" or result.active_bindings
        ):
            raise PersonalDevCapacityProjectionError("capacity manager shadow status is incoherent")
        if checkpoint.execution_state in {"prepared", "drain-only"} and (
            result.capacity_status != "prepared" or result.active_bindings
        ):
            raise PersonalDevCapacityProjectionError(
                "capacity manager prepared status is incoherent"
            )
        if checkpoint.execution_state == "active" and result.capacity_status != "waiting":
            raise PersonalDevCapacityProjectionError("capacity manager active status is incoherent")
        return result

    async def project(
        self,
        request: DynamicDevelopmentSubjectProjectionV1,
        *,
        idempotency_key: UUID,
    ) -> PersonalDevCapacityProjectionResult:
        if not isinstance(request, DynamicDevelopmentSubjectProjectionV1):
            raise TypeError("capacity projection must use the strict manager contract")
        try:
            response = await self._http.put(
                f"{self._origin}/v1/development-projections/{request.subject_id}",
                headers=self._headers
                | {
                    "Content-Type": "application/json",
                    "Idempotency-Key": str(idempotency_key),
                },
                content=canonical_bytes(request),
            )
        except httpx.HTTPError as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager projection request failed"
            ) from exc
        if response.status_code == 409:
            raise PersonalDevCapacityProjectionConflictError(
                "capacity manager configuration changed during projection"
            )
        if response.status_code != 200:
            raise PersonalDevCapacityProjectionError(
                f"capacity manager projection returned {response.status_code}"
            )
        try:
            self._bounded_json(response)
            parsed = _ProjectionResponseV1.model_validate_json(response.content)
        except (ValidationError, ValueError) as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager projection acknowledgement is invalid"
            ) from exc
        subject = parsed.subject
        retiring = request.operation_kind == "destroy"
        if (
            parsed.configuration_epoch != request.expected_configuration_epoch + 1
            or subject.subject_id != request.subject_id
            or subject.subject_incarnation != request.subject_incarnation
            or subject.display_name != f"dev-{request.environment_name}"
            or subject.tier_id != "development"
            or subject.lifecycle_state != ("disabled" if retiring else "active")
            or subject.min_slots != (0 if retiring else request.min_slots)
            or subject.max_slots != (0 if retiring else request.max_slots)
            or subject.candidate_generation != request.candidate_generation
            or subject.deployment_generation != request.deployment_generation
            or subject.configuration_generation != request.configuration_generation
            or subject.demand_reporter_incarnation != request.demand_reporter_incarnation
            or parsed.account.account_id != subject.account_id
            or parsed.account.kind != "owner"
            or parsed.account.owner_id != request.owner_id
        ):
            raise PersonalDevCapacityProjectionError(
                "capacity manager acknowledgement differs from the lifecycle request"
            )
        try:
            return PersonalDevCapacityProjectionResult(
                configuration_epoch=parsed.configuration_epoch,
                configuration_digest=parsed.configuration_digest,
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
                configuration_generation=subject.configuration_generation,
                deployment_generation=subject.deployment_generation,
                reporter_incarnation=subject.demand_reporter_incarnation,
                replayed=parsed.replayed,
            )
        except (TypeError, ValueError) as exc:
            raise PersonalDevCapacityProjectionError(
                "capacity manager projection acknowledgement is invalid"
            ) from exc

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


def personal_dev_capacity_projection(
    claim: PersonalDevReconciliationClaim,
    installation: PersonalDevCapacityInstallation,
    *,
    expected_configuration_epoch: int,
) -> DynamicDevelopmentSubjectProjectionV1:
    """Build and verify the one exact manager request for a durable checkpoint."""

    operation = claim.operation
    candidate = claim.candidate
    if operation.kind not in {"create", "update", "capacity"}:
        raise ValueError("personal-dev operation cannot project capacity")
    if (
        candidate.status != "ready"
        or candidate.publication_sha256 is None
        or candidate.publication_json is None
        or operation.local_activation_sha256 is None
    ):
        raise ValueError("personal-dev capacity projection prerequisites are incomplete")
    protocols = candidate.publication_json.get("protocol_versions")
    if not isinstance(protocols, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in protocols.items()
    ):
        raise ValueError("personal-dev candidate protocol publication is invalid")
    if type(expected_configuration_epoch) is not int or expected_configuration_epoch <= 0:
        raise ValueError("expected global capacity configuration epoch must be positive")
    request = DynamicDevelopmentSubjectProjectionV1(
        expected_configuration_epoch=expected_configuration_epoch,
        operation_kind=cast(Literal["create", "update", "capacity", "destroy"], operation.kind),
        operation_id=operation.id,
        operation_epoch=operation.operation_epoch,
        environment_name=operation.environment_name,
        subject_id=operation.subject_id,
        subject_incarnation=operation.subject_incarnation,
        owner_id=operation.owner_user_id,
        min_slots=operation.min_slots,
        max_slots=operation.max_slots,
        candidate_generation=operation.deployment_generation,
        candidate_sha256=operation.candidate_sha,
        candidate_publication_sha256=candidate.publication_sha256,
        deployment_generation=operation.deployment_generation,
        configuration_generation=operation.operation_epoch,
        demand_reporter_incarnation=installation.reporter_incarnation,
        demand_reporter_token_sha256=installation.reporter_token_sha256,
        local_activation_sha256=operation.local_activation_sha256,
        protected_admission_sha256=installation.protected_admission_sha256,
        capacity_agent_installation_sha256=(installation.capacity_agent_installation_sha256),
        supported_pool_ids=installation.supported_pool_ids,
        supported_architectures=installation.supported_architectures,
        protocol_versions=protocols,
    )
    persisted = {
        "capacity_reporter_incarnation": installation.reporter_incarnation,
        "capacity_reporter_token_sha256": installation.reporter_token_sha256,
        "protected_admission_sha256": installation.protected_admission_sha256,
        "capacity_agent_installation_sha256": (installation.capacity_agent_installation_sha256),
        "capacity_supported_pool_ids": installation.supported_pool_ids,
        "capacity_supported_architectures": installation.supported_architectures,
    }
    mismatches = tuple(
        field_name
        for field_name, value in persisted.items()
        if getattr(operation, field_name) is not None and getattr(operation, field_name) != value
    )
    if mismatches:
        raise PersonalDevCapacityProjectionError(
            "capacity installation changed after durable preparation: " + ", ".join(mismatches)
        )
    if (
        operation.capacity_projection_request_sha256 is not None
        and operation.capacity_projection_request_sha256 != canonical_digest(request)
    ):
        raise PersonalDevCapacityProjectionError(
            "capacity projection request changed after durable preparation"
        )
    return request


def personal_dev_capacity_retirement_projection(
    claim: PersonalDevReconciliationClaim,
    *,
    expected_configuration_epoch: int,
) -> DynamicDevelopmentSubjectProjectionV1:
    """Rebuild the exact active subject evidence as a manager retirement."""

    operation = claim.operation
    candidate = claim.candidate
    if operation.kind != "destroy":
        raise ValueError("personal-dev capacity retirement requires a destroy operation")
    required = (
        operation.local_activation_sha256,
        operation.capacity_reporter_incarnation,
        operation.capacity_reporter_token_sha256,
        operation.protected_admission_sha256,
        operation.capacity_agent_installation_sha256,
        operation.capacity_supported_pool_ids,
        operation.capacity_supported_architectures,
    )
    if (
        any(value is None for value in required)
        or candidate.status != "ready"
        or candidate.publication_sha256 is None
        or candidate.publication_json is None
    ):
        raise ValueError("personal-dev capacity retirement evidence is incomplete")
    if type(expected_configuration_epoch) is not int or expected_configuration_epoch <= 0:
        raise ValueError("expected global capacity configuration epoch must be positive")
    protocols = candidate.publication_json.get("protocol_versions")
    if not isinstance(protocols, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in protocols.items()
    ):
        raise ValueError("personal-dev candidate protocol publication is invalid")
    supported_pool_ids = operation.capacity_supported_pool_ids
    supported_architectures = operation.capacity_supported_architectures
    reporter_incarnation = operation.capacity_reporter_incarnation
    assert supported_pool_ids is not None
    assert supported_architectures is not None
    assert reporter_incarnation is not None
    request = DynamicDevelopmentSubjectProjectionV1(
        expected_configuration_epoch=expected_configuration_epoch,
        operation_kind="destroy",
        operation_id=operation.id,
        operation_epoch=operation.operation_epoch,
        environment_name=operation.environment_name,
        subject_id=operation.subject_id,
        subject_incarnation=operation.subject_incarnation,
        owner_id=operation.owner_user_id,
        min_slots=operation.min_slots,
        max_slots=operation.max_slots,
        candidate_generation=operation.deployment_generation,
        candidate_sha256=operation.candidate_sha,
        candidate_publication_sha256=candidate.publication_sha256,
        deployment_generation=operation.deployment_generation,
        configuration_generation=operation.operation_epoch,
        demand_reporter_incarnation=reporter_incarnation,
        demand_reporter_token_sha256=cast(str, operation.capacity_reporter_token_sha256),
        local_activation_sha256=cast(str, operation.local_activation_sha256),
        protected_admission_sha256=cast(str, operation.protected_admission_sha256),
        capacity_agent_installation_sha256=cast(str, operation.capacity_agent_installation_sha256),
        supported_pool_ids=supported_pool_ids,
        supported_architectures=supported_architectures,
        protocol_versions=protocols,
    )
    if (
        operation.capacity_projection_request_sha256 is not None
        and operation.capacity_projection_request_sha256 != canonical_digest(request)
    ):
        raise PersonalDevCapacityProjectionError(
            "capacity retirement request changed after durable preparation"
        )
    return request


__all__ = [
    "CapacityManagerPersonalDevProjector",
    "PersonalDevCapacityAvailability",
    "PersonalDevCapacityInstallation",
    "PersonalDevCapacityInstaller",
    "PersonalDevCapacityManagerCheckpoint",
    "PersonalDevCapacityManagerConnection",
    "PersonalDevCapacityProjectionConflictError",
    "PersonalDevCapacityProjectionError",
    "PersonalDevCapacityProjectionResult",
    "PersonalDevCapacityProjector",
    "PersonalDevCapacitySubjectStatus",
    "personal_dev_capacity_projection",
    "personal_dev_capacity_retirement_projection",
]
