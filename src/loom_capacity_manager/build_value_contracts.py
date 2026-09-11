"""Leaf build values shared by membership commands and successor provenance.

No preparation, origin, allocation composition or persistence imports belong here.
The original modules re-export these classes without changing their wire format.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID, uuid5

from pydantic import ConfigDict, Discriminator, Field, Tag, field_validator, model_validator

from loom_capacity_manager.contracts import (
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
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.inherited_reincarnation_contracts import (
    PersonalInheritedReincarnationEvidenceV2,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMemberV1,
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


class PersonalBuildMemberV1(_StrictBuildV1):
    """One owner's distinct build service; not a personal-application subtype."""

    model_config = ConfigDict(revalidate_instances="subclass-instances")

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


class PersonalApplicationMemberV2(PersonalApplicationMemberV1):
    """Typed application carrier for explicit inherited predecessor evidence."""

    schema_version: Literal[2] = 2  # type: ignore[assignment]
    reincarnation: PersonalInheritedReincarnationEvidenceV2 = Field(...)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 2:
            raise ValueError("inherited application member schema must be integer 2")
        return value


class PersonalBuildMemberV2(PersonalBuildMemberV1):
    """Typed build carrier for explicit inherited predecessor evidence."""

    schema_version: Literal[2] = 2  # type: ignore[assignment]
    reincarnation: PersonalInheritedReincarnationEvidenceV2 = Field(...)


def _member_tag(value: object) -> str | None:
    if isinstance(value, dict):
        version, purpose = value.get("schema_version", 1), value.get("purpose")
    else:
        version, purpose = getattr(value, "schema_version", None), getattr(value, "purpose", None)
    if (type(version) is not int or version not in (1, 2)
        or not isinstance(purpose, str) or purpose not in {"personal-application", "personal-build-worker"}):
        return None
    return f"{purpose}:{version}"


PersonalMemberV2 = Annotated[
    Annotated[PersonalApplicationMemberV1, Tag("personal-application:1")]
    | Annotated[PersonalApplicationMemberV2, Tag("personal-application:2")]
    | Annotated[PersonalBuildMemberV1, Tag("personal-build-worker:1")]
    | Annotated[PersonalBuildMemberV2, Tag("personal-build-worker:2")],
    Discriminator(_member_tag),
]


def _nonzero_identity(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("typed membership identity must be nonzero")
    return value


class _StrictTypedV1(StrictV1Model):
    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != cls.model_fields["schema_version"].default:
            raise ValueError("typed membership schema requires its exact integer version")
        return value


class PersonalBuildProjectionV1(_StrictTypedV1):
    owner_id: UUID
    subject_incarnation: UUID
    operation_kind: Literal["create", "update", "capacity", "destroy"]
    operation_id: UUID
    operation_epoch: PositiveQuantity
    configuration_generation: PositiveQuantity
    candidate_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    demand_reporter_incarnation: UUID
    demand_reporter_token_sha256: Digest
    max_slots: Quantity

    _identities_nonzero = field_validator(
        "owner_id", "subject_incarnation", "operation_id", "demand_reporter_incarnation",
    )(_nonzero_identity)

    @model_validator(mode="after")
    def _service_generation(self) -> PersonalBuildProjectionV1:
        if self.configuration_generation != self.operation_epoch:
            raise ValueError("build configuration generation must match service operation epoch")
        if self.demand_reporter_token_sha256 == "0" * 64:
            raise ValueError("build reporter token digest must be nonzero")
        return self
