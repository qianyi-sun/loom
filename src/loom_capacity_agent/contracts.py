"""Strict local contracts for the trusted, zero-executable demand agent."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_guard.contracts import (
    Digest,
    GuardIdentifier,
    NetworkPolicy,
    PositiveGeneration,
    SealedRequirementsV1,
    StrictGuardModel,
    canonical_digest,
)
from loom_capacity_manager.contracts import MAX_FIXED_CLAIMS_PER_REPORT, MAX_POOLS


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


class AgentRegistrationV1(StrictGuardModel):
    """Immutable binding for one candidate-independent reporter incarnation."""

    environment_id: GuardIdentifier
    subject_id: UUID
    subject_incarnation: UUID
    authority_incarnation: UUID
    agent_incarnation: UUID
    reporter_incarnation: UUID
    authority_mode: Literal["disabled"] = "disabled"
    allocation_epoch: Literal[0] = 0
    reporter_high_water: Literal[0] = 0
    candidate_digest: Digest
    deployment_generation: PositiveGeneration
    configuration_generation: PositiveGeneration

    @model_validator(mode="after")
    def _distinct_incarnations(self) -> AgentRegistrationV1:
        incarnations = {
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
        }
        if len(incarnations) != 4:
            raise ValueError("subject, authority, agent, and reporter incarnations must be distinct")
        return self


class AgentPoolCapabilityV1(StrictGuardModel):
    """One exact trusted capability offer in a physical pool; never a weight."""

    capability_id: GuardIdentifier
    pool_id: Literal["oldlab", "gb10"]
    operating_system: Literal["linux", "windows"]
    cpu_architecture: Literal["x86_64", "arm64"]
    gpu_vendor: Literal["none", "nvidia"]
    network_policies: Annotated[tuple[NetworkPolicy, ...], Field(max_length=3)]

    @field_validator("network_policies")
    @classmethod
    def _canonical_values(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate pool capability")
        return tuple(sorted(value))


class ReporterConfigurationV1(AgentRegistrationV1):
    """Trusted local mapping sealed outside the candidate deployment."""

    pool_capabilities: Annotated[
        tuple[AgentPoolCapabilityV1, ...], Field(min_length=1, max_length=MAX_POOLS)
    ]

    @field_validator("pool_capabilities")
    @classmethod
    def _canonical_pools(
        cls, value: tuple[AgentPoolCapabilityV1, ...]
    ) -> tuple[AgentPoolCapabilityV1, ...]:
        capability_ids = [item.capability_id for item in value]
        if len(capability_ids) != len(set(capability_ids)):
            raise ValueError("duplicate pool capability identity")
        return tuple(sorted(value, key=lambda item: item.capability_id))


class GuardDemandAttemptV1(StrictGuardModel):
    """One protected queued attempt as observed by the trusted capture function."""

    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements: SealedRequirementsV1
    requirements_digest: Digest
    claim_state: Literal["queued"] = "queued"
    assigned_pool: None = None
    assignment_epoch: None = None
    worker_id: None = None
    claim_epoch: None = None
    submit_priority: Annotated[int, Field(ge=0, le=(1 << 31) - 1)]
    submitted_at: datetime

    @field_validator("submitted_at", mode="before")
    @classmethod
    def _parse_time(cls, value: datetime | str) -> datetime | str:
        if isinstance(value, str):
            timestamp = f"{value[:-1]}+00:00" if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                return value
        return value

    @field_validator("submitted_at")
    @classmethod
    def _submitted_at_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def _exact_requirement_binding(self) -> GuardDemandAttemptV1:
        if canonical_digest(self.requirements) != self.requirements_digest:
            raise ValueError("requirements digest does not match sealed requirements")
        return self


class GuardDemandObservationV1(AgentRegistrationV1):
    """One complete bounded view of protected pending demand."""

    sequence: PositiveGeneration
    source_observed_at: datetime
    view_state: Literal["complete"] = "complete"
    attempts: Annotated[
        tuple[GuardDemandAttemptV1, ...], Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT)
    ]

    @field_validator("source_observed_at", mode="before")
    @classmethod
    def _parse_time(cls, value: datetime | str) -> datetime | str:
        if isinstance(value, str):
            timestamp = f"{value[:-1]}+00:00" if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                return value
        return value

    @field_validator("source_observed_at")
    @classmethod
    def _observed_at_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("attempts")
    @classmethod
    def _canonical_attempts(
        cls, value: tuple[GuardDemandAttemptV1, ...]
    ) -> tuple[GuardDemandAttemptV1, ...]:
        identities = [item.protected_attempt_id for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate protected attempt identity")
        return tuple(sorted(value, key=lambda item: item.protected_attempt_id.hex))


__all__ = [
    "AgentPoolCapabilityV1",
    "AgentRegistrationV1",
    "GuardDemandAttemptV1",
    "GuardDemandObservationV1",
    "ReporterConfigurationV1",
]
