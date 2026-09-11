"""Pure typed membership commands; these do not authorize durable admission.

The store must authenticate the delegate/current execution under authority-first
locks, enforce lifecycle/replay/release rules and retain reporter evidence. Build
generations describe the trusted service, never an arbitrary feature build.
Legacy application wire documents and their immutable history remain unchanged.
"""

from __future__ import annotations

import json
from typing import Annotated, Literal, TypeVar
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
from loom_capacity_manager.build_membership_contracts import (
    ExecutionPreparationV4,
    PersonalBuildMemberV1,
    PersonalMemberV2,
    personal_build_subject_id,
    personal_build_subject_name,
)
from loom_capacity_manager.build_value_contracts import (
    PersonalApplicationMemberV2,
    PersonalBuildMemberV2,
    _nonzero_identity,
    _StrictTypedV1,
)
from loom_capacity_manager.build_value_contracts import (
    PersonalBuildProjectionV1 as PersonalBuildProjectionV1,
)
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    AccountPolicyV1,
    Digest,
    DynamicDevelopmentSubjectProjectionV1,
    FleetManifestV1,
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    SubjectConfigurationV1,
    canonical_bytes,
    canonical_digest,
    canonical_digest_excluding,
    checked_add,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_digest,
)
from loom_capacity_manager.inherited_reincarnation_contracts import (
    PersonalInheritedReincarnationEvidenceV2,
)
from loom_capacity_manager.membership import (
    _validate_build_configuration,
    _validate_build_template,
    _validate_personal_configuration,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMemberV1,
    PersonalReincarnationEvidenceV1,
)
from loom_capacity_manager.store import _derive_development_subject, _derive_owner_account
from loom_capacity_manager.successor_origin_contracts import (
    ManagedApplicationOriginV2,
    ManagedBuildOriginV1,
)


class PersonalApplicationCommandV2(_StrictTypedV1):
    schema_version: Literal[2] = 2  # type: ignore[assignment]
    purpose: Literal["personal-application"] = "personal-application"
    projection: DynamicDevelopmentSubjectProjectionV1
    acknowledgement: SubjectExecutionAcknowledgementV2


class PersonalBuildCommandV2(_StrictTypedV1):
    schema_version: Literal[2] = 2  # type: ignore[assignment]
    purpose: Literal["personal-build-worker"] = "personal-build-worker"
    projection: PersonalBuildProjectionV1
    acknowledgement: SubjectExecutionAcknowledgementV2


class PersonalMembershipMutationV2(_StrictTypedV1):
    schema_version: Literal[2] = 2  # type: ignore[assignment]
    execution: ExecutionAuthorityV2
    namespace_id: UUID
    expected_revision: Quantity
    command: Annotated[PersonalApplicationCommandV2 | PersonalBuildCommandV2, Field(discriminator="purpose")]

    _namespace_nonzero = field_validator("namespace_id")(_nonzero_identity)

    @model_validator(mode="after")
    def _single_execution_fence(self) -> PersonalMembershipMutationV2:
        _nonzero_identity(self.execution.authority_incarnation)
        if self.execution.execution_state != "active":
            raise ValueError("typed membership requires active execution")
        checked_add(self.expected_revision, 1)
        if isinstance(self.command, PersonalApplicationCommandV2) and (
            self.command.projection.expected_configuration_epoch != self.execution.configuration_epoch
        ):
            raise ValueError("application projection configuration fence differs from execution")
        return self


class PersonalMembershipResultV2(_StrictTypedV1):
    schema_version: Literal[2] = 2  # type: ignore[assignment]
    revision: PositiveQuantity
    head_sha256: Digest
    member: PersonalMemberV2
    replayed: bool

    @model_validator(mode="after")
    def _exact_revision(self) -> PersonalMembershipResultV2:
        if self.revision != self.member.revision or self.head_sha256 == "0" * 64:
            raise ValueError("typed membership result revision or head is invalid")
        return self


_Contract = TypeVar("_Contract", bound=StrictV1Model)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate typed membership JSON key")
        if key == "schema_version" and type(value) is not int:
            raise ValueError("typed membership wire versions must be exact integers")
        result[key] = value
    return result


def _parse(payload: str | bytes, model: type[_Contract]) -> _Contract:
    if not isinstance(payload, (str, bytes)) or len(payload) > MAX_CONTRACT_BYTES:
        raise ValueError("typed membership payload exceeds byte bound")
    encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("typed membership payload exceeds byte bound")
    try:
        value = json.loads(encoded, object_pairs_hook=_unique_object)
    except (RecursionError, UnicodeError) as exc:
        raise ValueError("invalid typed membership JSON") from exc
    if not isinstance(value, dict) or value.get("schema_version") != model.model_fields["schema_version"].default:
        raise ValueError("typed membership requires an explicit wire version")
    return model.model_validate_json(encoded)


def parse_typed_membership_mutation(payload: str | bytes) -> PersonalMembershipMutationV2:
    return _parse(payload, PersonalMembershipMutationV2)


def parse_typed_membership_result(payload: str | bytes) -> PersonalMembershipResultV2:
    return _parse(payload, PersonalMembershipResultV2)


def typed_membership_subject_id(request: PersonalMembershipMutationV2) -> UUID:
    """Derive the transport target, never trust a build acknowledgement's path."""
    request = parse_typed_membership_mutation(canonical_bytes(request))
    if isinstance(request.command, PersonalBuildCommandV2):
        return personal_build_subject_id(request.namespace_id, request.command.projection.owner_id)
    return request.command.projection.subject_id


def _checked_context(
    request: PersonalMembershipMutationV2, preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
) -> tuple[PersonalMembershipMutationV2, ExecutionPreparationV4, FleetManifestV1, AccountPolicyV1]:
    """Revalidate unchecked nested copies, then join immutable authority inputs.

This proves internal consistency, not that this supplied execution is current.
"""
    request = parse_typed_membership_mutation(canonical_bytes(request))
    preparation = ExecutionPreparationV4.model_validate_json(preparation.model_dump_json())
    fleet = FleetManifestV1.model_validate_json(fleet.model_dump_json())
    execution, policy = request.execution, preparation.personal_membership
    template = fleet.development_subject_template
    if (
        execution.authority_incarnation != preparation.authority_incarnation
        or execution.writer_epoch != preparation.expected_writer_epoch
        or execution.configuration_epoch != preparation.configuration_epoch
        or execution.execution_manifest_sha256 != canonical_executable_digest(preparation)
        or execution.trusted_fleet_release_sha256 != preparation.trusted_fleet_release_sha256
        or execution.executable_new_capacity_ceiling > preparation.requested_ceiling
        or execution.executable_new_capacity_rate_per_minute > preparation.requested_rate_per_minute
        or request.namespace_id != policy.namespace_id
        or preparation.fleet_generation != fleet.fleet_generation
        or preparation.fleet_digest != canonical_digest(fleet)
        or fleet.fleet_digest != canonical_digest_excluding(fleet, "fleet_digest")
        or template is None or policy.development_template_sha256 != canonical_digest(template)
    ):
        raise ValueError("typed membership preparation, execution or fleet binding changed")
    owner = _derive_owner_account(fleet, request.command.projection.owner_id)
    _validate_build_template(preparation.personal_builds, fleet, owner)
    return request, preparation, fleet, owner


def derive_build_member(
    request: PersonalMembershipMutationV2, preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
    *, reincarnation: PersonalReincarnationEvidenceV1 | None = None,
) -> PersonalBuildMemberV1:
    """Derive a build service from operator policy, without granting admission.

Only the durable store may supply authenticated reincarnation evidence; structural
validation here cannot prove predecessor cleanup or a historical release set.
"""
    request, preparation, _fleet, owner = _checked_context(request, preparation, fleet)
    if not isinstance(request.command, PersonalBuildCommandV2):
        raise ValueError("build derivation requires a build command")
    projection = request.command.projection
    template = preparation.personal_builds
    if projection.max_slots > template.max_slots_per_subject:
        raise ValueError("build projection maximum exceeds the template")
    retiring = projection.operation_kind == "destroy"
    configuration = SubjectConfigurationV1(
        subject_id=personal_build_subject_id(request.namespace_id, projection.owner_id),
        subject_incarnation=projection.subject_incarnation,
        display_name=personal_build_subject_name(projection.owner_id),
        account_id=owner.account_id, tier_id="development", min_slots=0,
        max_slots=0 if retiring else projection.max_slots, rollout_surge_slots=0,
        max_pending_slots=template.max_pending_slots_per_subject,
        max_pending_jobs=template.max_pending_jobs_per_subject,
        submission_rate_per_minute=owner.submission_rate_per_minute,
        lifecycle_state="disabled" if retiring else "active",
        candidate_generation=projection.candidate_generation,
        deployment_generation=projection.deployment_generation,
        configuration_generation=projection.configuration_generation,
        demand_reporter_incarnation=projection.demand_reporter_incarnation, profiles=template.profiles,
    )
    inherited = _inherited_recreation_context(request, preparation, reincarnation)
    member = (PersonalBuildMemberV2(
        revision=checked_add(request.expected_revision, 1), owner_id=projection.owner_id,
        configuration=configuration, acknowledgement=request.command.acknowledgement, reincarnation=inherited,
    ) if inherited is not None else PersonalBuildMemberV1(
        revision=checked_add(request.expected_revision, 1), owner_id=projection.owner_id,
        configuration=configuration, acknowledgement=request.command.acknowledgement, reincarnation=reincarnation,
    ))
    member = type(member).model_validate_json(member.model_dump_json())
    _validate_build_configuration(member, request.namespace_id, template, owner)
    if member.reincarnation is not None and (
        member.reincarnation.namespace_id != request.namespace_id
        or member.reincarnation.execution_manifest_sha256 != request.execution.execution_manifest_sha256
    ):
        raise ValueError("build reincarnation authority changed")
    return member


def derive_application_member(
    request: PersonalMembershipMutationV2, preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
    *, reincarnation: PersonalReincarnationEvidenceV1 | None = None,
) -> PersonalApplicationMemberV1:
    """Derive a typed application from pinned policy, not mutable materialization."""
    request, preparation, fleet, owner = _checked_context(request, preparation, fleet)
    if not isinstance(request.command, PersonalApplicationCommandV2):
        raise ValueError("application derivation requires an application command")
    projection, ack = request.command.projection, request.command.acknowledgement
    configuration = _derive_development_subject(fleet, projection)
    template = fleet.development_subject_template
    assert template is not None
    _validate_personal_configuration(configuration, projection.owner_id, template, owner)
    if (
        ack.candidate.algorithm != "source-sha256" or ack.candidate.identity != projection.candidate_sha256
        or ack.candidate.publication_sha256 != projection.candidate_publication_sha256
        or ack.protected_admission_sha256 != projection.protected_admission_sha256
        or (reincarnation is not None and (
            reincarnation.namespace_id != request.namespace_id
            or reincarnation.execution_manifest_sha256 != request.execution.execution_manifest_sha256
        ))
    ):
        raise ValueError("application command acknowledgement or reincarnation differs from projection")
    inherited = _inherited_recreation_context(request, preparation, reincarnation)
    if inherited is not None:
        return PersonalApplicationMemberV2(revision=checked_add(request.expected_revision, 1), owner_id=projection.owner_id,
            configuration=configuration, acknowledgement=ack, reincarnation=inherited)
    return PersonalApplicationMemberV1(
        revision=checked_add(request.expected_revision, 1), owner_id=projection.owner_id,
        configuration=configuration, acknowledgement=ack, reincarnation=reincarnation,
    )


def _inherited_recreation_context(
    request: PersonalMembershipMutationV2, preparation: ExecutionPreparationV4,
    evidence: PersonalReincarnationEvidenceV1 | None,
) -> PersonalInheritedReincarnationEvidenceV2 | None:
    """Join structural provenance; the store must authenticate graph and release."""
    if not isinstance(evidence, PersonalInheritedReincarnationEvidenceV2):
        return None
    evidence = PersonalInheritedReincarnationEvidenceV2.model_validate_json(evidence.model_dump_json())
    origins: tuple[ManagedApplicationOriginV1 | ManagedBuildOriginV1, ...] = (
        *preparation.managed_application_origins, *preparation.managed_build_origins)
    base = next((origin for origin in origins if origin.configuration.subject_id == evidence.predecessor.subject_id), None)
    if (not isinstance(base, (ManagedApplicationOriginV2, ManagedBuildOriginV1))
        or isinstance(request.command, PersonalBuildCommandV2) != isinstance(base, ManagedBuildOriginV1)
        or base.base_projection.owner_id != request.command.projection.owner_id
        or evidence.execution_epoch != request.execution.execution_epoch
        or evidence.execution_manifest_sha256 != request.execution.execution_manifest_sha256
        or evidence.namespace_id != request.namespace_id
        or evidence.source != preparation.retired_source
        or evidence.source != base.inherited.source
        or evidence.origin != base.inherited.original_origin
        or evidence.predecessor != base.configuration
        or evidence.predecessor_execution_epoch != base.inherited.anchor.execution_epoch
        or evidence.predecessor_execution_manifest_sha256 != base.inherited.anchor.execution_manifest_sha256
        or evidence.predecessor_revision != base.inherited.anchor.revision
        or evidence.predecessor_head_sha256 != base.inherited.anchor.head_sha256):
        raise ValueError("inherited recreation differs from pinned predecessor or current execution")
    return evidence


def validate_typed_membership_result(
    request: PersonalMembershipMutationV2, result: PersonalMembershipResultV2,
    preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
) -> None:
    """Validate persisted values, not chain authenticity or live admission.

The caller separately authenticates the head, replay identity, reincarnation and
all retained generation evidence. In particular, accepting replayed=True here
does not itself establish an idempotent retry.
"""
    request, preparation, fleet, _owner = _checked_context(request, preparation, fleet)
    result = parse_typed_membership_result(canonical_bytes(result))
    if result.revision != checked_add(request.expected_revision, 1):
        raise ValueError("typed membership result does not advance requested revision")
    evidence = result.member.reincarnation
    if evidence is not None and (
        evidence.namespace_id != request.namespace_id
        or evidence.execution_manifest_sha256 != request.execution.execution_manifest_sha256
    ):
        raise ValueError("typed membership reincarnation authority changed")
    expected: PersonalBuildMemberV1 | PersonalApplicationMemberV1
    if isinstance(request.command, PersonalBuildCommandV2):
        expected = derive_build_member(request, preparation, fleet, reincarnation=result.member.reincarnation)
    else:
        expected = derive_application_member(request, preparation, fleet, reincarnation=result.member.reincarnation)
    if result.member != expected:
        raise ValueError("typed membership result differs from derived command")
