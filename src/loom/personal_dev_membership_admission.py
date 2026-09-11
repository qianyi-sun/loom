"""Separate, operator-pinned active acceptance authority for personal membership.

This contract cannot reinterpret a legacy zero-capacity acceptance certificate as
active evidence. Configuration authentication belongs to trusted management
provisioning; these validators establish consistency and current manager binding.
Operational promotion needs its own verified active acceptance evidence and is
deliberately not accepted by this bounded acceptance-window contract.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import field_validator, model_validator

from loom.personal_dev_membership_client import PersonalDevMembershipError
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    Digest,
    StrictV1Model,
    canonical_bytes,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationV3,
    PersonalMembershipCheckpointV1,
)


class PersonalDevMembershipAdmissionError(RuntimeError):
    """New admission is unavailable; this never forbids historical recovery."""


class PersonalDevMembershipAcceptanceBindingV1(StrictV1Model):
    capacity_mode: Literal["membership-v1"] = "membership-v1"
    purpose: Literal["acceptance"] = "acceptance"
    plan_sha256: Digest
    preparation: ExecutionPreparationV3
    execution: ExecutionAuthorityV2
    started_at: datetime
    expires_at: datetime

    @property
    def namespace_id(self) -> UUID:
        return self.preparation.personal_membership.namespace_id

    @property
    def management_principal_id(self) -> str:
        return self.preparation.personal_membership.management_principal_id

    @field_validator("started_at", "expires_at")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("membership acceptance time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _prepared_authority(self) -> PersonalDevMembershipAcceptanceBindingV1:
        execution, preparation = self.execution, self.preparation
        if (
            self.plan_sha256 == "0" * 64
            or self.started_at >= self.expires_at
            or execution.execution_state != "active"
            or execution.execution_manifest_sha256 != canonical_executable_digest(preparation)
            or execution.authority_incarnation != preparation.authority_incarnation
            or execution.writer_epoch != preparation.expected_writer_epoch
            or execution.configuration_epoch != preparation.configuration_epoch
            or execution.trusted_fleet_release_sha256 != preparation.trusted_fleet_release_sha256
            or execution.executable_new_capacity_ceiling != preparation.requested_ceiling
            or execution.executable_new_capacity_rate_per_minute != preparation.requested_rate_per_minute
        ):
            raise ValueError("membership acceptance differs from its prepared authority")
        return self


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate membership acceptance field")
        if key == "schema_version" and type(value) is not int:
            raise ValueError("membership acceptance schema versions must be exact integers")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError("membership acceptance contains a non-JSON constant")


def parse_membership_acceptance_binding(
    payload: bytes | str, *, expected_plan_sha256: str
) -> PersonalDevMembershipAcceptanceBindingV1:
    encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
    if not isinstance(encoded, bytes) or not 0 < len(encoded) <= MAX_CONTRACT_BYTES:
        raise ValueError("membership acceptance binding exceeds its byte bound")
    try:
        json.loads(encoded, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        binding = PersonalDevMembershipAcceptanceBindingV1.model_validate_json(encoded)
        if binding.plan_sha256 != expected_plan_sha256 or canonical_bytes(binding) != encoded:
            raise ValueError("membership acceptance is not the canonical reviewed plan binding")
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("membership acceptance binding is invalid") from exc
    return binding


class MembershipAdmissionClient(Protocol):
    async def membership_checkpoint(self) -> PersonalMembershipCheckpointV1: ...


@dataclass(frozen=True, slots=True)
class PersonalDevMembershipAdmissionInterlock:
    binding: PersonalDevMembershipAcceptanceBindingV1
    client: MembershipAdmissionClient

    def __post_init__(self) -> None:
        parse_membership_acceptance_binding(
            canonical_bytes(self.binding), expected_plan_sha256=self.binding.plan_sha256
        )

    async def assert_admission_ready(self, *, now: datetime) -> None:
        started = monotonic()
        if (
            now.tzinfo is None
            or now.utcoffset() is None
            or not self.binding.started_at <= now < self.binding.expires_at
        ):
            raise PersonalDevMembershipAdmissionError("membership acceptance window is not open")
        try:
            checkpoint = await self.client.membership_checkpoint()
        except PersonalDevMembershipError as exc:
            raise PersonalDevMembershipAdmissionError("membership admission authority unavailable") from exc
        if now + timedelta(seconds=monotonic() - started) >= self.binding.expires_at:
            raise PersonalDevMembershipAdmissionError("membership acceptance window expired during read")
        if (
            checkpoint.execution != self.binding.execution
            or checkpoint.namespace_id != self.binding.namespace_id
        ):
            raise PersonalDevMembershipAdmissionError("membership admission authority changed")
