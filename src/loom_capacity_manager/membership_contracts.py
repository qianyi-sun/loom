"""Versioned contracts for delegated personal application membership."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, TypeAdapter, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_SUBJECTS,
    AllocationInputV1,
    Digest,
    Identifier,
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    SubjectConfigurationV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    SubjectExecutionAcknowledgementV2,
)

_ZERO_DIGEST = "0" * 64


def _nonzero_uuid(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("membership identity must be nonzero")
    return value


class PersonalMembershipPolicyV1(StrictV1Model):
    """Owner-pinned authority and bounds for one personal namespace."""

    namespace_id: UUID
    management_principal_id: Identifier
    development_template_sha256: Digest
    max_subjects: Annotated[int, Field(ge=1, le=MAX_SUBJECTS)]
    managed_base_subject_ids: Annotated[tuple[UUID, ...], Field(max_length=MAX_SUBJECTS)] = ()

    _namespace_is_nonzero = field_validator("namespace_id")(_nonzero_uuid)

    @field_validator("managed_base_subject_ids")
    @classmethod
    def _canonical_base_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        if any(item.int == 0 for item in value):
            raise ValueError("managed base subject identity must be nonzero")
        if len(value) != len(set(value)):
            raise ValueError("duplicate managed base subject identity")
        return tuple(sorted(value, key=lambda item: item.int))

    @model_validator(mode="after")
    def _managed_base_bound(self) -> PersonalMembershipPolicyV1:
        if len(self.managed_base_subject_ids) > self.max_subjects:
            raise ValueError("managed base subjects exceed membership policy")
        return self


class ExecutionPreparationPolicyV3(ExecutionPreparationPolicyV2):
    """V2 execution policy extended by an exact personal-membership policy."""

    schema_version: Literal[3] = 3
    personal_membership: PersonalMembershipPolicyV1


class ExecutionPreparationV3(ExecutionPreparationV2):
    """V2 execution preparation extended by its personal-membership policy."""

    schema_version: Literal[3] = 3
    personal_membership: PersonalMembershipPolicyV1


class PersonalApplicationMemberV1(StrictV1Model):
    """One revisioned personal application configuration and acknowledgement."""

    revision: PositiveQuantity
    owner_id: UUID
    purpose: Literal["personal-application"] = "personal-application"
    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2

    _owner_is_nonzero = field_validator("owner_id")(_nonzero_uuid)

    @model_validator(mode="after")
    def _exact_acknowledgement(self) -> PersonalApplicationMemberV1:
        configuration = self.configuration
        acknowledgement = self.acknowledgement
        identities = (
            configuration.subject_id,
            configuration.subject_incarnation,
            configuration.demand_reporter_incarnation,
        )
        if any(identity.int == 0 for identity in identities):
            raise ValueError("personal application identity must be nonzero")
        if (
            acknowledgement.subject_id != configuration.subject_id
            or acknowledgement.subject_incarnation != configuration.subject_incarnation
            or acknowledgement.configuration_generation != configuration.configuration_generation
            or acknowledgement.deployment_generation != configuration.deployment_generation
            or acknowledgement.reporter_incarnation != configuration.demand_reporter_incarnation
        ):
            raise ValueError("personal application acknowledgement binding changed")
        if (
            acknowledgement.candidate.algorithm != "source-sha256"
            or acknowledgement.candidate.identity == _ZERO_DIGEST
            or acknowledgement.candidate.publication_sha256 == _ZERO_DIGEST
        ):
            raise ValueError("personal application candidate publication is invalid")
        return self


class PersonalMembershipSnapshotV1(StrictV1Model):
    """Canonical materialized membership log head for one namespace."""

    namespace_id: UUID
    revision: Quantity
    head_sha256: Digest
    members: Annotated[
        tuple[PersonalApplicationMemberV1, ...],
        Field(max_length=MAX_SUBJECTS),
    ] = ()

    _namespace_is_nonzero = field_validator("namespace_id")(_nonzero_uuid)

    @field_validator("members")
    @classmethod
    def _canonical_members(
        cls,
        value: tuple[PersonalApplicationMemberV1, ...],
    ) -> tuple[PersonalApplicationMemberV1, ...]:
        subject_ids = [item.configuration.subject_id for item in value]
        revisions = [item.revision for item in value]
        names = [item.configuration.display_name for item in value]
        if len(subject_ids) != len(set(subject_ids)):
            raise ValueError("duplicate personal application subject identity")
        if len(revisions) != len(set(revisions)):
            raise ValueError("duplicate personal application revision")
        if len(names) != len(set(names)):
            raise ValueError("duplicate personal application name")
        return tuple(sorted(value, key=lambda item: item.revision))

    @model_validator(mode="after")
    def _exact_head(self) -> PersonalMembershipSnapshotV1:
        if self.revision == 0:
            if self.head_sha256 != _ZERO_DIGEST or self.members:
                raise ValueError("empty personal membership must use the zero head")
        elif self.head_sha256 == _ZERO_DIGEST:
            raise ValueError("nonempty personal membership requires a nonzero head")
        if any(item.revision > self.revision for item in self.members):
            raise ValueError("personal application revision is ahead of the membership head")
        return self


class DelegatedAllocationInputV2(AllocationInputV1):
    """Allocator input overlaid with a bounded personal membership snapshot."""

    schema_version: Literal[2] = 2
    preparation: ExecutionPreparationV3
    managed_base_subjects: Annotated[
        tuple[SubjectConfigurationV1, ...],
        Field(max_length=MAX_SUBJECTS),
    ] = ()
    membership: PersonalMembershipSnapshotV1

    @field_validator("managed_base_subjects")
    @classmethod
    def _canonical_base_subjects(
        cls,
        value: tuple[SubjectConfigurationV1, ...],
    ) -> tuple[SubjectConfigurationV1, ...]:
        subject_ids = [item.subject_id for item in value]
        if any(item.int == 0 for item in subject_ids):
            raise ValueError("managed base subject identity must be nonzero")
        if len(subject_ids) != len(set(subject_ids)):
            raise ValueError("duplicate managed base subject identity")
        return tuple(sorted(value, key=lambda item: item.subject_id.int))

    @model_validator(mode="after")
    def _membership_subject_bound(self) -> DelegatedAllocationInputV2:
        subject_ids = {item.subject_id for item in self.managed_base_subjects}
        subject_ids.update(item.configuration.subject_id for item in self.membership.members)
        if len(subject_ids) > self.preparation.personal_membership.max_subjects:
            raise ValueError("delegated allocation exceeds membership subject bound")
        return self


_PREPARATION_ADAPTER = TypeAdapter(
    Annotated[
        ExecutionPreparationV2 | ExecutionPreparationV3,
        Field(discriminator="schema_version"),
    ]
)
_POLICY_ADAPTER = TypeAdapter(
    Annotated[
        ExecutionPreparationPolicyV2 | ExecutionPreparationPolicyV3,
        Field(discriminator="schema_version"),
    ]
)


def parse_execution_preparation(payload: str | bytes) -> ExecutionPreparationV2:
    """Strictly parse one supported execution-preparation schema."""

    return _PREPARATION_ADAPTER.validate_json(payload)


def parse_execution_preparation_policy(payload: str | bytes) -> ExecutionPreparationPolicyV2:
    """Strictly parse one supported execution-preparation-policy schema."""

    return _POLICY_ADAPTER.validate_json(payload)


__all__ = [
    "DelegatedAllocationInputV2",
    "ExecutionPreparationPolicyV3",
    "ExecutionPreparationV3",
    "PersonalApplicationMemberV1",
    "PersonalMembershipPolicyV1",
    "PersonalMembershipSnapshotV1",
    "parse_execution_preparation",
    "parse_execution_preparation_policy",
]
