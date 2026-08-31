"""Strict, canonical Package 2A protected-admission contracts.

These models can represent only disabled authority and queued, unassigned
attempts. They deliberately contain no grant, permit, claim, worker-token,
physical-pool mutation, or release contract.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Any, Literal, TypeVar, get_origin
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.models.capabilities import RequiredCapabilities

SCHEMA_VERSION = 1
MAX_SIGNED_BIGINT = (1 << 63) - 1
MAX_CONTRACT_BYTES = 8 * 1024 * 1024

GuardIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=128, pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$"),
]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
PositiveGeneration = Annotated[int, Field(gt=0, le=MAX_SIGNED_BIGINT)]
NonNegativeSequence = Annotated[int, Field(ge=0, le=MAX_SIGNED_BIGINT)]
NetworkPolicy = Literal["public", "no-network", "gateway-only", "allowlist"]


class CapacityGuardContractError(ValueError):
    """Raised when a protected contract cannot be encoded safely."""


class StrictGuardModel(BaseModel):
    """Frozen, strict base for every persisted guard schema-v1 document."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1

    @model_validator(mode="before")
    @classmethod
    def _json_arrays_to_tuples(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        for name, field in cls.model_fields.items():
            if get_origin(field.annotation) is tuple and isinstance(normalized.get(name), list):
                normalized[name] = tuple(normalized[name])
        return normalized


class SealedRequirementsV1(StrictGuardModel):
    """Immutable normalized requirements plus an optional physical-pool pin."""

    os: Literal["linux", "windows"]
    cpu_arch: Literal["x86_64", "arm64", "any"]
    gpu_vendor: Literal["none", "nvidia"]
    network_policies: Annotated[tuple[NetworkPolicy, ...], Field(max_length=3)]
    required_pool: Literal["oldlab", "gb10"] | None = None

    @field_validator("network_policies")
    @classmethod
    def _canonical_network_policies(
        cls, value: tuple[NetworkPolicy, ...]
    ) -> tuple[NetworkPolicy, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate network policy")
        return tuple(sorted(value))


class GuardFenceV1(StrictGuardModel):
    """Exact disabled authority binding for one environment incarnation."""

    environment_id: GuardIdentifier
    subject_id: UUID
    subject_incarnation: UUID
    authority_mode: Literal["disabled"] = "disabled"
    authority_incarnation: UUID
    reporter_incarnation: UUID
    reporter_high_water: NonNegativeSequence = 0
    allocation_epoch: Literal[0] = 0
    deployment_generation: PositiveGeneration
    configuration_generation: PositiveGeneration
    candidate_digest: Digest


class ProtectedAttemptV1(StrictGuardModel):
    """Queued, unassigned protected attempt identity for Package 2A."""

    trial_id: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements_digest: Digest
    claim_state: Literal["queued"] = "queued"
    assigned_pool: None = None
    assignment_epoch: None = None
    worker_id: None = None
    claim_epoch: None = None

    @model_validator(mode="after")
    def _distinct_identities(self) -> ProtectedAttemptV1:
        if self.trial_id == self.protected_attempt_id:
            raise ValueError("trial and protected attempt identities must be distinct")
        return self


_GuardModelT = TypeVar("_GuardModelT", bound=StrictGuardModel)


def seal_requirements(
    requirements: RequiredCapabilities,
    *,
    required_pool: Literal["oldlab", "gb10"] | None = None,
) -> SealedRequirementsV1:
    """Seal one already-normalized runtime requirement without topology inference."""

    if not isinstance(requirements, RequiredCapabilities):
        raise CapacityGuardContractError(
            "sealing requires a normalized RequiredCapabilities contract"
        )
    return SealedRequirementsV1(
        os=requirements.os,
        cpu_arch=requirements.cpu_arch,
        gpu_vendor=requirements.gpu_vendor,
        network_policies=tuple(sorted(requirements.network_policies)),
        required_pool=required_pool,
    )


def canonical_bytes(model: StrictGuardModel) -> bytes:
    """Encode an exact bounded canonical schema-v1 object."""

    if not isinstance(model, StrictGuardModel):
        raise CapacityGuardContractError("canonical encoding requires a protected schema-v1 model")
    payload = model.model_dump(mode="json", exclude_none=False)
    if not isinstance(payload, dict):  # pragma: no cover - BaseModel guarantee
        raise CapacityGuardContractError("protected contract payload must be an object")
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise CapacityGuardContractError("protected canonical contract exceeds maximum byte size")
    return encoded


def canonical_digest(model: StrictGuardModel) -> str:
    return hashlib.sha256(canonical_bytes(model)).hexdigest()


__all__ = [
    "MAX_CONTRACT_BYTES",
    "CapacityGuardContractError",
    "Digest",
    "GuardFenceV1",
    "GuardIdentifier",
    "ProtectedAttemptV1",
    "SealedRequirementsV1",
    "StrictGuardModel",
    "canonical_bytes",
    "canonical_digest",
    "seal_requirements",
]
