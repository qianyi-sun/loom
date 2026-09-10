"""Versioned contracts for delegated personal application membership."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    TypeAdapter,
    field_validator,
    model_serializer,
    model_validator,
)

from loom_capacity_manager.contracts import (
    MAX_SUBJECTS,
    AllocationInputV1,
    ConfigurationGenerationRefV1,
    Digest,
    DynamicDevelopmentSubjectProjectionV1,
    Identifier,
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    SubjectConfigurationV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
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

    # Intentional wire-version replacement; the discriminated parser retains V2.
    schema_version: Literal[3] = 3  # type: ignore[assignment]
    personal_membership: PersonalMembershipPolicyV1


class ExecutionPreparationV3(ExecutionPreparationV2):
    """V2 execution preparation extended by its personal-membership policy."""

    # Keep inherited fields/validators without widening V2's accepted wire tag.
    schema_version: Literal[3] = 3  # type: ignore[assignment]
    personal_membership: PersonalMembershipPolicyV1


class PersonalReincarnationEvidenceV1(StrictV1Model):
    """Store-issued binding of a successor to its released predecessor.

    Structural validation is not authentication: the membership store must verify
    the immutable event chain and actual durable release witnesses.
    """

    model_config = ConfigDict(revalidate_instances="subclass-instances")

    @model_serializer(mode="wrap")
    def _serialize_evidence(self, handler: SerializerFunctionWrapHandler) -> Any:
        if self.schema_version != 1:
            raise ValueError("legacy evidence serialization cannot truncate a newer version")
        return handler(self)

    namespace_id: UUID
    execution_manifest_sha256: Digest
    origin: ConfigurationGenerationRefV1
    predecessor: SubjectConfigurationV1
    predecessor_revision: PositiveQuantity
    predecessor_head_sha256: Digest
    admission_revision: PositiveQuantity
    successor_incarnation: UUID
    release_set_sha256: Digest

    _identities_nonzero = field_validator("namespace_id", "successor_incarnation")(_nonzero_uuid)

    @field_validator("execution_manifest_sha256", "predecessor_head_sha256", "release_set_sha256")
    @classmethod
    def _nonzero_digest(cls, value: str) -> str:
        if value == _ZERO_DIGEST:
            raise ValueError("reincarnation evidence digest must be nonzero")
        return value

    @model_validator(mode="after")
    def _predecessor_binding(self) -> PersonalReincarnationEvidenceV1:
        self._require_predecessor_identity()
        if self.admission_revision <= self.predecessor_revision:
            raise ValueError("reincarnation predecessor binding changed")
        return self

    def _require_predecessor_identity(self) -> None:
        """Shared identity checks; each evidence version defines its own ordering."""
        predecessor = self.predecessor
        identities = (
            self.origin.subject_id,
            self.origin.subject_incarnation,
            predecessor.subject_id,
            predecessor.subject_incarnation,
            predecessor.demand_reporter_incarnation,
        )
        if any(value is None or value.int == 0 for value in identities):
            raise ValueError("reincarnation subject identities must be nonzero")
        if (
            self.origin.scope != "subject"
            or self.origin.subject_id != predecessor.subject_id
            or self.origin.digest == _ZERO_DIGEST
            or self.origin.generation > predecessor.configuration_generation
            or predecessor.lifecycle_state != "disabled"
            or predecessor.min_slots != 0
            or predecessor.max_slots != 0
            or self.successor_incarnation
            in (
                predecessor.subject_incarnation,
                self.origin.subject_incarnation,
            )
        ):
            raise ValueError("reincarnation predecessor binding changed")


class PersonalApplicationMemberV1(StrictV1Model):
    """One revisioned personal application configuration and acknowledgement."""

    revision: PositiveQuantity
    owner_id: UUID
    purpose: Literal["personal-application"] = "personal-application"
    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2
    reincarnation: PersonalReincarnationEvidenceV1 | None = None

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
        evidence = self.reincarnation
        if evidence is not None:
            predecessor = evidence.predecessor
            if (
                evidence.successor_incarnation != configuration.subject_incarnation
                or predecessor.subject_id != configuration.subject_id
                or predecessor.account_id != configuration.account_id
                or predecessor.display_name != configuration.display_name
                or predecessor.demand_reporter_incarnation
                == configuration.demand_reporter_incarnation
                or configuration.configuration_generation <= predecessor.configuration_generation
                or evidence.admission_revision > self.revision
                or (
                    evidence.admission_revision == self.revision
                    and (
                        configuration.candidate_generation != 1
                        or configuration.deployment_generation != 1
                    )
                )
            ):
                raise ValueError("reincarnation successor binding changed")
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


class PersonalApplicationMembershipMutationV1(StrictV1Model):
    """One fenced compare-and-swap against an active membership namespace."""

    execution: ExecutionAuthorityV2
    namespace_id: UUID
    expected_revision: Quantity
    projection: DynamicDevelopmentSubjectProjectionV1
    acknowledgement: SubjectExecutionAcknowledgementV2

    _namespace_is_nonzero = field_validator("namespace_id")(_nonzero_uuid)


class PersonalApplicationMembershipResultV1(StrictV1Model):
    """The immutable checkpoint appended for one membership mutation."""

    revision: PositiveQuantity
    head_sha256: Digest
    member: PersonalApplicationMemberV1
    replayed: bool


class PersonalMembershipCheckpointV1(StrictV1Model):
    """Exact active delegation checkpoint, independent of configuration epochs."""

    execution: ExecutionAuthorityV2
    namespace_id: UUID
    revision: Quantity
    head_sha256: Digest

    _namespace_is_nonzero = field_validator("namespace_id")(_nonzero_uuid)

    @model_validator(mode="after")
    def _active_checkpoint(self) -> PersonalMembershipCheckpointV1:
        if self.execution.execution_state != "active":
            raise ValueError("membership checkpoint requires active execution")
        if (self.revision == 0) != (self.head_sha256 == _ZERO_DIGEST):
            raise ValueError("membership checkpoint head does not match its revision")
        return self


class PersonalApplicationMembershipResponseV1(StrictV1Model):
    """A mutation's original checkpoint, including exact replay responses."""

    checkpoint: PersonalMembershipCheckpointV1
    result: PersonalApplicationMembershipResultV1

    @model_validator(mode="after")
    def _exact_result_checkpoint(self) -> PersonalApplicationMembershipResponseV1:
        if (
            self.checkpoint.revision != self.result.revision
            or self.checkpoint.head_sha256 != self.result.head_sha256
            or self.result.member.revision != self.result.revision
        ):
            raise ValueError("membership response checkpoint differs from its result")
        return self


class DelegatedAllocationInputV2(AllocationInputV1):
    """Allocator input overlaid with a bounded personal membership snapshot."""

    # The delegated subtype has a distinct wire version, not a widened V1 parser.
    schema_version: Literal[2] = 2  # type: ignore[assignment]
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


_PREPARATION_ADAPTER: TypeAdapter[ExecutionPreparationV2 | ExecutionPreparationV3] = TypeAdapter(
    Annotated[
        ExecutionPreparationV2 | ExecutionPreparationV3,
        Field(discriminator="schema_version"),
    ]
)
_POLICY_ADAPTER: TypeAdapter[ExecutionPreparationPolicyV2 | ExecutionPreparationPolicyV3] = (
    TypeAdapter(
        Annotated[
            ExecutionPreparationPolicyV2 | ExecutionPreparationPolicyV3,
            Field(discriminator="schema_version"),
        ]
    )
)


def _require_exact_v3_tag(payload: str | bytes) -> None:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, ValueError):
        return
    if (
        isinstance(value, dict)
        and value.get("schema_version") == 3
        and type(value.get("schema_version")) is not int
    ):
        raise ValueError("schema version 3 must be an integer JSON value")


def parse_execution_preparation(payload: str | bytes) -> ExecutionPreparationV2:
    """Strictly parse one supported execution-preparation schema."""

    _require_exact_v3_tag(payload)
    return _PREPARATION_ADAPTER.validate_json(payload)


def parse_execution_preparation_policy(payload: str | bytes) -> ExecutionPreparationPolicyV2:
    """Strictly parse one supported execution-preparation-policy schema."""

    _require_exact_v3_tag(payload)
    return _POLICY_ADAPTER.validate_json(payload)


__all__ = [
    "DelegatedAllocationInputV2",
    "ExecutionPreparationPolicyV3",
    "ExecutionPreparationV3",
    "PersonalApplicationMemberV1",
    "PersonalApplicationMembershipMutationV1",
    "PersonalApplicationMembershipResponseV1",
    "PersonalApplicationMembershipResultV1",
    "PersonalMembershipCheckpointV1",
    "PersonalMembershipPolicyV1",
    "PersonalMembershipSnapshotV1",
    "PersonalReincarnationEvidenceV1",
    "parse_execution_preparation",
    "parse_execution_preparation_policy",
]
