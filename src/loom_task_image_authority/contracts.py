"""Retained signed publication and credential provenance schemas; no build admission."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

import rfc8785
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    field_validator,
    model_validator,
)

from loom_task_image_authority.config import (
    _validate_https_origin,
    _validate_registry_identity,
)
from loom_task_image_authority.registry_token import (
    MAX_REGISTRY_BEARER_TOKEN_BYTES,
    publication_repository,
)

MAX_SIGNED_BIGINT = (1 << 63) - 1

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_MANIFEST_DIGEST_RE = re.compile(r"sha256:([0-9a-f]{64})")

CpuArchitecture = Literal["x86_64", "arm64"]
SlurmClusterId = Literal["oldlab", "gb10"]
BuildPurpose = Literal["production", "shadow"]
PositiveSignedBigint = Annotated[int, Field(gt=0, le=MAX_SIGNED_BIGINT)]
RegistryCredentialGeneration = Annotated[int, Field(gt=0, le=512)]
TaskImageComponent = Annotated[
    str,
    Field(
        min_length=1,
        max_length=136,
        pattern=r"^(?:task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$",
    ),
]
Identifier = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
]
SlurmJobId = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,31}$")]


def _nonzero_uuid(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("authority UUID must be nonzero")
    return value


def _nonzero_digest(value: str) -> str:
    if _DIGEST_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError("authority digest must be a nonzero lowercase SHA-256")
    return value


def _nonzero_manifest_digest(value: str) -> str:
    match = _MANIFEST_DIGEST_RE.fullmatch(value)
    if match is None or match.group(1) == "0" * 64:
        raise ValueError("authority manifest digest must be a nonzero lowercase SHA-256")
    return value


NonzeroUUID = Annotated[UUID, AfterValidator(_nonzero_uuid)]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$"), AfterValidator(_nonzero_digest)]
ManifestDigest = Annotated[
    str,
    Field(pattern=r"^sha256:[0-9a-f]{64}$"),
    AfterValidator(_nonzero_manifest_digest),
]


def _parse_timestamp(value: str) -> datetime | str:
    candidate = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return value


class StrictTaskImageAuthorityModel(BaseModel):
    """Frozen strict base for persisted task-image authority documents."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1

    @model_validator(mode="before")
    @classmethod
    def _restore_json_types(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        for name, item in normalized.items():
            if isinstance(item, list):
                normalized[name] = tuple(item)
            elif isinstance(item, str) and (name.endswith("_at") or name.endswith("_expires_at")):
                normalized[name] = _parse_timestamp(item)
        return normalized

    @field_validator("*", mode="after")
    @classmethod
    def _canonicalize_timestamps(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("authority timestamp must include a timezone")
            return value.astimezone(UTC)
        return value


def _validate_interval(
    issued_at: datetime,
    expires_at: datetime,
    *,
    maximum: timedelta,
    label: str,
) -> None:
    lifetime = expires_at - issued_at
    if lifetime <= timedelta(0) or lifetime > maximum:
        raise ValueError(f"authority {label} lifetime is invalid")


class _SecretBearingAuthorityModel(StrictTaskImageAuthorityModel):
    pass


class TaskImageRegistryCredentialV1(_SecretBearingAuthorityModel):
    """One exact short-lived Distribution bearer capability."""

    credential_id: NonzeroUUID
    request_id: NonzeroUUID
    grant_id: NonzeroUUID
    session_id: NonzeroUUID
    session_generation: PositiveSignedBigint
    attestation_generation: PositiveSignedBigint
    attestation_sha256: Digest
    materialization_id: NonzeroUUID
    attempt_id: NonzeroUUID
    attempt_number: PositiveSignedBigint
    lease_epoch: PositiveSignedBigint
    builder_id: Annotated[str, Field(pattern=r"^rootless:[0-9a-f]{32}$")]
    purpose: Literal["production"]
    shadow_campaign_id: None = None
    cpu_arch: CpuArchitecture
    platform: Literal["linux/amd64", "linux/arm64"]
    component: TaskImageComponent
    generation: RegistryCredentialGeneration
    predecessor_credential_id: NonzeroUUID | None = None
    predecessor_generation: RegistryCredentialGeneration | None = None
    lease_heartbeat_operation_id: NonzeroUUID | None = None
    registry_origin: Annotated[str, Field(min_length=9, max_length=2048)]
    registry_service: Annotated[
        str,
        Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9_.:-]{0,127}$"),
    ]
    registry_issuer: Annotated[
        str,
        Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9_.:-]{0,127}$"),
    ]
    repository: Annotated[str, Field(min_length=1, max_length=255)]
    actions: tuple[Literal["pull"], Literal["push"]]
    registry_key_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{43}$")]
    bearer_token: Annotated[
        str,
        Field(
            min_length=5,
            max_length=MAX_REGISTRY_BEARER_TOKEN_BYTES,
            pattern=r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$",
            repr=False,
        ),
    ]
    issued_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def _credential_is_exact(self) -> TaskImageRegistryCredentialV1:
        expected_platform = "linux/amd64" if self.cpu_arch == "x86_64" else "linux/arm64"
        if self.platform != expected_platform:
            raise ValueError("registry credential architecture and platform disagree")
        if self.repository != publication_repository(
            purpose=self.purpose,
            shadow_campaign_id=self.shadow_campaign_id,
            cpu_arch=self.cpu_arch,
            attempt_id=self.attempt_id,
            component=self.component,
        ):
            raise ValueError("registry credential repository binding is invalid")
        renewal_values = (
            self.predecessor_credential_id,
            self.predecessor_generation,
            self.lease_heartbeat_operation_id,
        )
        if self.generation == 1:
            if any(value is not None for value in renewal_values):
                raise ValueError("first registry credential has renewal evidence")
        elif (
            any(value is None for value in renewal_values)
            or self.predecessor_generation != self.generation - 1
            or self.predecessor_credential_id == self.credential_id
        ):
            raise ValueError("registry credential renewal chain is invalid")
        _validate_https_origin(self.registry_origin, label="registry origin")
        _validate_registry_identity(self.registry_service, label="registry service")
        _validate_registry_identity(self.registry_issuer, label="registry issuer")
        if self.issued_at.microsecond != 0 or self.expires_at.microsecond != 0:
            raise ValueError("registry credential times must be whole seconds")
        _validate_interval(
            self.issued_at,
            self.expires_at,
            maximum=timedelta(seconds=45),
            label="registry credential",
        )
        return self

    def public_binding(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json", exclude={"bearer_token"})
        payload["bearer_token_sha256"] = hashlib.sha256(
            self.bearer_token.encode("utf-8")
        ).hexdigest()
        return payload


class TaskImageBaseResolutionEvidenceV1(BaseModel):
    """Immutable same-solve observations; never publication authority."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        serialize_by_alias=True,
        revalidate_instances="always",
    )

    schema_name: Literal["loom.task-image-base-resolution/v1"] = Field(alias="schema")
    solve_ref: Annotated[str, Field(min_length=1, max_length=128)]
    platform: Literal["linux/amd64", "linux/arm64"]
    output_digest: ManifestDigest
    observed_base_digests: Annotated[tuple[ManifestDigest, ...], Field(max_length=128)]

    @model_validator(mode="wrap")
    @classmethod
    def _own_observations(
        cls, value: Any, handler: ModelWrapValidatorHandler[TaskImageBaseResolutionEvidenceV1]
    ) -> TaskImageBaseResolutionEvidenceV1:
        if isinstance(value, cls):
            # Revalidate model instances through the same exact wire keys as dicts.
            # Pydantic's internal field-name dict otherwise loses the schema alias.
            value = value.model_dump(mode="python", by_alias=True)
        if isinstance(value, dict) and isinstance(value.get("observed_base_digests"), list):
            value = dict(value)
            value["observed_base_digests"] = tuple(value["observed_base_digests"])
        return handler(value)

    @model_validator(mode="after")
    def _bounded_exact_record(self) -> TaskImageBaseResolutionEvidenceV1:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", self.solve_ref) is None:
            raise ValueError("base-resolution solve reference is invalid")
        if any(
            left >= right
            for left, right in zip(
                self.observed_base_digests, self.observed_base_digests[1:], strict=False
            )
        ):
            raise ValueError("base-resolution observations must be sorted and unique")
        if len(rfc8785.dumps(self.model_dump(mode="json"))) > 16 * 1024:
            raise ValueError("base-resolution evidence exceeds maximum byte size")
        return self
