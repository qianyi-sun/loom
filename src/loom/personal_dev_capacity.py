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
from pydantic import BaseModel, ConfigDict, ValidationError

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

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_MAX_RESPONSE_BYTES = 64 * 1024


class PersonalDevCapacityProjectionError(RuntimeError):
    """The global manager did not verifiably accept a personal subject."""


class PersonalDevCapacityProjectionConflictError(PersonalDevCapacityProjectionError):
    """The global configuration epoch changed before projection."""


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
            or self.supported_architectures
            != tuple(sorted(set(self.supported_architectures)))
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


class PersonalDevCapacityProjector(Protocol):
    async def current_configuration_epoch(self) -> int: ...

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

    async def current_configuration_epoch(self) -> int:
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
        epoch = payload.get("configuration_epoch")
        if type(epoch) is not int or epoch <= 0:
            raise PersonalDevCapacityProjectionError(
                "capacity manager has no active configuration epoch"
            )
        if payload.get("executable_new_capacity_ceiling") != 0:
            raise PersonalDevCapacityProjectionError(
                "capacity manager crossed the zero-execution lifecycle boundary"
            )
        return epoch

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
        if (
            parsed.configuration_epoch != request.expected_configuration_epoch + 1
            or subject.subject_id != request.subject_id
            or subject.subject_incarnation != request.subject_incarnation
            or subject.display_name != f"dev-{request.environment_name}"
            or subject.tier_id != "development"
            or subject.lifecycle_state != "active"
            or subject.min_slots != request.min_slots
            or subject.max_slots != request.max_slots
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
        operation_kind=cast(Literal["create", "update", "capacity"], operation.kind),
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


__all__ = [
    "CapacityManagerPersonalDevProjector",
    "PersonalDevCapacityInstallation",
    "PersonalDevCapacityInstaller",
    "PersonalDevCapacityManagerConnection",
    "PersonalDevCapacityProjectionConflictError",
    "PersonalDevCapacityProjectionError",
    "PersonalDevCapacityProjectionResult",
    "PersonalDevCapacityProjector",
    "personal_dev_capacity_projection",
]
