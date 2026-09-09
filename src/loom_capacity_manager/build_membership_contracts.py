"""Typed build-membership contracts, not yet admitted by execution stores/APIs.

Pure allocation composition can exercise these contracts before the complete
purpose-preserving execution, admission, recovery and cleanup bridge is enabled.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID, uuid5

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.application_origin_contracts import (
    ManagedApplicationOriginV1,
    validate_managed_application_origins,
)
from loom_capacity_manager.contracts import (
    MAX_SUBJECTS,
    AllocationInputV1,
    Digest,
    PositiveQuantity,
    ProfileReferenceV1,
    Quantity,
    StrictV1Model,
    SubjectConfigurationV1,
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMemberV1,
    PersonalMembershipPolicyV1,
    PersonalReincarnationEvidenceV1,
)


def _nonzero_uuid(value: UUID) -> UUID:
    if not isinstance(value, UUID) or value.int == 0:
        raise ValueError("build membership identity must be nonzero")
    return value


def personal_build_subject_id(namespace_id: UUID, owner_id: UUID) -> UUID:
    return uuid5(_nonzero_uuid(namespace_id), f"personal-build-worker:{_nonzero_uuid(owner_id).hex}")


def personal_build_subject_name(owner_id: UUID) -> str:
    return f"dev-build-{_nonzero_uuid(owner_id).hex}"


class _StrictBuildV1(StrictV1Model):
    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != cls.model_fields["schema_version"].default:
            raise ValueError("build membership schema requires its exact integer version")
        return value


def _runtime_candidate(value: CandidateBindingV2) -> CandidateBindingV2:
    if value.algorithm != "git-sha1" or value.identity == "0" * 40 or value.publication_sha256 == "0" * 64:
        raise ValueError("build membership requires a trusted runtime Git publication")
    return value


class PersonalBuildTemplateV1(_StrictBuildV1):
    """Operator-owned native profiles and runtime, using the existing owner account.

runtime_candidate comes from the approved builder-runtime publication loader.
Membership does not itself prove image/profile/certification joins or CI approval.
Those remain required before executable readiness and pool-local admission.
"""

    runtime_candidate: CandidateBindingV2
    profiles: Annotated[tuple[ProfileReferenceV1, ...], Field(min_length=2, max_length=2)]
    max_slots_per_subject: PositiveQuantity
    max_pending_slots_per_subject: Quantity
    max_pending_jobs_per_subject: Quantity

    _candidate_is_runtime = field_validator("runtime_candidate")(_runtime_candidate)

    @field_validator("profiles")
    @classmethod
    def _cold_native_profiles(cls, profiles: tuple[ProfileReferenceV1, ...]) -> tuple[ProfileReferenceV1, ...]:
        if {item.pool_id for item in profiles} != {"gb10", "oldlab"}:
            raise ValueError("build membership requires both native pool profiles")
        for profile in profiles:
            if canonical_digest_excluding(profile, "profile_digest") != profile.profile_digest:
                raise ValueError("build profile self-digest changed")
            if len(profile.worker_shapes) != 1:
                raise ValueError("build profile requires exactly one cold native shape")
            shape = profile.worker_shapes[0]
            architecture = "cpu_arch.arm64" if profile.pool_id == "gb10" else "cpu_arch.x86_64"
            if (
                shape.concurrency_slots != 1 or len(shape.node_resources) != 1
                or shape.warm_approved or shape.total_resources.cpu_millicores <= 0
                or shape.total_resources.memory_bytes <= 0
                or not {architecture, "personal-build-worker"} <= set(shape.capabilities)
                or {item for item in shape.capabilities if item.startswith("cpu_arch.")} != {architecture}
            ):
                raise ValueError("build shape must be one cold native slot with positive CPU/memory")
        return tuple(sorted(profiles, key=lambda item: item.pool_id))


class ExecutionPreparationPolicyV4(ExecutionPreparationPolicyV2):
    """Explicit direct-V2 successor; never routed as application-only V3."""

    schema_version: Literal[4] = 4  # type: ignore[assignment]
    personal_membership: PersonalMembershipPolicyV1
    personal_builds: PersonalBuildTemplateV1
    managed_application_origins: Annotated[tuple[ManagedApplicationOriginV1, ...], Field(max_length=MAX_SUBJECTS)] = ()

    @field_validator("managed_application_origins")
    @classmethod
    def _canonical_origins(cls, values: tuple[ManagedApplicationOriginV1, ...]) -> tuple[ManagedApplicationOriginV1, ...]:
        return tuple(sorted(values, key=lambda item: item.configuration.subject_id.int))

    @model_validator(mode="after")
    def _managed_origins(self) -> ExecutionPreparationPolicyV4:
        validate_managed_application_origins(self.managed_application_origins,
            self.personal_membership.managed_base_subject_ids, self.subject_acknowledgements)
        return self

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 4:
            raise ValueError("build execution policy schema must be integer 4")
        return value


class ExecutionPreparationV4(ExecutionPreparationV2):
    schema_version: Literal[4] = 4  # type: ignore[assignment]
    personal_membership: PersonalMembershipPolicyV1
    personal_builds: PersonalBuildTemplateV1
    managed_application_origins: Annotated[tuple[ManagedApplicationOriginV1, ...], Field(max_length=MAX_SUBJECTS)] = ()

    @field_validator("managed_application_origins")
    @classmethod
    def _canonical_origins(cls, values: tuple[ManagedApplicationOriginV1, ...]) -> tuple[ManagedApplicationOriginV1, ...]:
        return ExecutionPreparationPolicyV4._canonical_origins(values)

    @model_validator(mode="after")
    def _managed_origins(self) -> ExecutionPreparationV4:
        validate_managed_application_origins(self.managed_application_origins,
            self.personal_membership.managed_base_subject_ids, self.subject_acknowledgements,
            configuration_epoch=self.configuration_epoch)
        return self

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 4:
            raise ValueError("build execution preparation schema must be integer 4")
        return value


class PersonalBuildMemberV1(_StrictBuildV1):
    """One owner's distinct build service; not a personal-application subtype."""

    revision: PositiveQuantity
    owner_id: UUID
    purpose: Literal["personal-build-worker"] = "personal-build-worker"
    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2
    reincarnation: PersonalReincarnationEvidenceV1 | None = None

    _owner_nonzero = field_validator("owner_id")(_nonzero_uuid)

    @model_validator(mode="after")
    def _exact_binding(self) -> PersonalBuildMemberV1:
        config, ack = self.configuration, self.acknowledgement
        for identity in (config.subject_id, config.subject_incarnation, config.demand_reporter_incarnation):
            _nonzero_uuid(identity)
        _runtime_candidate(ack.candidate)
        if (
            config.account_id != f"dev-owner-{self.owner_id.hex}" or config.tier_id != "development"
            or config.lifecycle_state not in {"active", "disabled"}
            or config.min_slots != 0 or config.rollout_surge_slots != 0
            or (config.lifecycle_state == "disabled" and config.max_slots != 0)
            or ack.subject_id != config.subject_id or ack.subject_incarnation != config.subject_incarnation
            or ack.configuration_generation != config.configuration_generation
            or ack.deployment_generation != config.deployment_generation
            or ack.reporter_incarnation != config.demand_reporter_incarnation
        ):
            raise ValueError("build member acknowledgement or owner configuration changed")
        evidence = self.reincarnation
        if evidence is not None:
            predecessor = evidence.predecessor
            if (
                evidence.successor_incarnation != config.subject_incarnation
                or predecessor.subject_id != config.subject_id or predecessor.account_id != config.account_id
                or predecessor.display_name != config.display_name
                or predecessor.demand_reporter_incarnation == config.demand_reporter_incarnation
                or config.configuration_generation <= predecessor.configuration_generation
                or evidence.admission_revision > self.revision
                or (evidence.admission_revision == self.revision and (config.candidate_generation != 1 or config.deployment_generation != 1))
            ):
                raise ValueError("build reincarnation successor binding changed")
        return self


PersonalMemberV2 = Annotated[PersonalApplicationMemberV1 | PersonalBuildMemberV1, Field(discriminator="purpose")]


class PersonalMembershipSnapshotV2(_StrictBuildV1):
    """One combined revision/bound for application and build members."""

    schema_version: Literal[2] = 2  # type: ignore[assignment]
    namespace_id: UUID
    revision: Quantity
    head_sha256: Digest
    members: Annotated[tuple[PersonalMemberV2, ...], Field(max_length=MAX_SUBJECTS)] = ()

    _namespace_nonzero = field_validator("namespace_id")(_nonzero_uuid)

    @field_validator("members")
    @classmethod
    def _canonical_members(cls, members: tuple[PersonalMemberV2, ...]) -> tuple[PersonalMemberV2, ...]:
        ids = [item.configuration.subject_id for item in members]
        revisions = [item.revision for item in members]
        names = [item.configuration.display_name for item in members]
        build_owners = [item.owner_id for item in members if isinstance(item, PersonalBuildMemberV1)]
        if any(len(values) != len(set(values)) for values in (ids, revisions, names, build_owners)):
            raise ValueError("duplicate membership identity, revision, name or owner build service")
        return tuple(sorted(members, key=lambda item: item.revision))

    @model_validator(mode="after")
    def _exact_head(self) -> PersonalMembershipSnapshotV2:
        if (self.revision == 0) != (self.head_sha256 == "0" * 64):
            raise ValueError("build membership snapshot head differs from revision")
        if any(item.revision > self.revision for item in self.members):
            raise ValueError("member revision is ahead of the shared head")
        return self


class DelegatedAllocationInputV3(AllocationInputV1):
    """Pure V4 preparation composition; executable promotion remains disabled."""

    schema_version: Literal[3] = 3  # type: ignore[assignment]
    preparation: ExecutionPreparationV4
    managed_base_subjects: Annotated[tuple[SubjectConfigurationV1, ...], Field(max_length=MAX_SUBJECTS)] = ()
    membership: PersonalMembershipSnapshotV2

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("build allocation input schema must be integer 3")
        return value

    @field_validator("managed_base_subjects")
    @classmethod
    def _canonical_base(cls, values: tuple[SubjectConfigurationV1, ...]) -> tuple[SubjectConfigurationV1, ...]:
        ids = [item.subject_id for item in values]
        if any(item.int == 0 for item in ids) or len(ids) != len(set(ids)):
            raise ValueError("build allocation managed base identities are invalid")
        return tuple(sorted(values, key=lambda item: item.subject_id.int))

    @model_validator(mode="after")
    def _combined_bounds(self) -> DelegatedAllocationInputV3:
        ids = {item.subject_id for item in self.managed_base_subjects}
        ids.update(item.configuration.subject_id for item in self.membership.members)
        if len(ids) > self.preparation.personal_membership.max_subjects:
            raise ValueError("combined membership exceeds its subject bound")
        return self
