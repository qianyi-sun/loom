"""Pure personal application membership resolution."""

from __future__ import annotations

import re
from typing import cast
from uuid import UUID

from pydantic import ValidationError

from loom_capacity_manager.build_membership_contracts import (
    DelegatedAllocationInputV3,
    PersonalBuildMemberV1,
    PersonalBuildTemplateV1,
    PersonalMembershipSnapshotV2,
    personal_build_subject_id,
    personal_build_subject_name,
)
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    AllocationInputV1,
    CapacityContractError,
    ConfigurationGenerationRefV1,
    DevelopmentSubjectTemplateV1,
    FleetManifestV1,
    SubjectConfigurationV1,
    canonical_digest,
    checked_sum,
)
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.fleet_state import validate_profile_narrowing
from loom_capacity_manager.inherited_reincarnation_contracts import (
    PersonalInheritedReincarnationEvidenceV2,
)
from loom_capacity_manager.membership_contracts import (
    DelegatedAllocationInputV2,
    PersonalApplicationMemberV1,
    PersonalMembershipPolicyV1,
    PersonalMembershipSnapshotV1,
)
from loom_capacity_manager.successor_origin_contracts import (
    ManagedApplicationOriginV2,
    ManagedBuildOriginV1,
)

_PERSONAL_NAME = re.compile(r"^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$")
_RESERVED_PERSONAL_NAMES = frozenset(
    {"dev", "development", "staging", "production", "prod", "local", "loom", "shared"}
)


class PersonalMembershipResolutionError(ValueError):
    """A delegated membership cannot be composed with its immutable base."""


def _invalid(message: str) -> PersonalMembershipResolutionError:
    return PersonalMembershipResolutionError(message)


def _owner_id(configuration: SubjectConfigurationV1) -> UUID:
    prefix = "dev-owner-"
    encoded = configuration.account_id.removeprefix(prefix)
    if not configuration.account_id.startswith(prefix) or len(encoded) != 32:
        raise _invalid("personal application owner account is not canonical")
    try:
        owner_id = UUID(hex=encoded)
    except ValueError:
        raise _invalid("personal application owner account is not canonical") from None
    if owner_id.int == 0 or configuration.account_id != f"dev-owner-{owner_id.hex}":
        raise _invalid("personal application owner account is not canonical")
    return owner_id


def _derived_owner_policy(
    owner_id: UUID,
    template_policy: AccountPolicyV1,
    accounts: dict[str, AccountPolicyV1],
) -> AccountPolicyV1:
    account_id = f"dev-owner-{owner_id.hex}"
    expected = template_policy.model_copy(
        update={"account_id": account_id, "kind": "owner", "owner_id": owner_id}
    )
    if accounts.get(account_id) != expected:
        raise _invalid("personal application owner policy is not derived from the fleet")
    return expected


def _validate_personal_configuration(
    configuration: SubjectConfigurationV1,
    owner_id: UUID,
    template: DevelopmentSubjectTemplateV1,
    owner_policy: AccountPolicyV1,
) -> None:
    if configuration.subject_id.int == 0 or configuration.subject_incarnation.int == 0:
        raise _invalid("personal application identity must be nonzero")
    name = configuration.display_name
    personal_name = name[4:] if name.startswith("dev-") else ""
    if _PERSONAL_NAME.fullmatch(personal_name) is None or personal_name in _RESERVED_PERSONAL_NAMES:
        raise _invalid("personal application name is invalid")
    if configuration.account_id != f"dev-owner-{owner_id.hex}":
        raise _invalid("personal application owner account changed")
    if configuration.tier_id != "development":
        raise _invalid("personal application tier is not development")
    if configuration.lifecycle_state not in {"active", "disabled"}:
        raise _invalid("personal application lifecycle is invalid")
    if configuration.max_slots > template.max_slots_per_subject:
        raise _invalid("personal application maximum exceeds the fleet template")
    if configuration.profiles != template.profiles:
        raise _invalid("personal application profiles differ from the fleet template")
    if configuration.rollout_surge_slots != template.rollout_surge_slots:
        raise _invalid("personal application surge differs from the fleet template")
    if configuration.max_pending_slots != template.max_pending_slots_per_subject:
        raise _invalid("personal application pending slots differ from the fleet template")
    if configuration.max_pending_jobs != template.max_pending_jobs_per_subject:
        raise _invalid("personal application pending jobs differ from the fleet template")
    if configuration.submission_rate_per_minute != owner_policy.submission_rate_per_minute:
        raise _invalid("personal application rate differs from its owner policy")
    if configuration.lifecycle_state == "disabled" and (
        configuration.min_slots != 0 or configuration.max_slots != 0
    ):
        raise _invalid("disabled personal application must have zero capacity")


def _member_reference(member: PersonalApplicationMemberV1 | PersonalBuildMemberV1) -> ConfigurationGenerationRefV1:
    configuration = member.configuration
    return ConfigurationGenerationRefV1(
        scope="subject",
        generation=configuration.configuration_generation,
        digest=canonical_digest(configuration),
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
    )


def _validate_build_template(template: PersonalBuildTemplateV1, fleet: FleetManifestV1, owner: AccountPolicyV1) -> None:
    if (
        template.max_slots_per_subject > owner.max_slots
        or template.max_pending_slots_per_subject > owner.max_pending_slots
        or template.max_pending_jobs_per_subject > owner.max_pending_jobs
    ):
        raise _invalid("build membership template exceeds the shared owner policy")
    for profile in template.profiles:
        try:
            validate_profile_narrowing(fleet, profile)
        except ValueError as exc:
            raise _invalid("build membership profile differs from fleet authority") from exc
        pool = next(item for item in fleet.pools if item.pool_id == profile.pool_id)
        expected_architecture = "arm64" if profile.pool_id == "gb10" else "x86_64"
        if any(domain.architecture != expected_architecture for domain in pool.resource_domains if domain.domain_id in profile.eligible_resource_domains):
            raise _invalid("build membership profile is not natively placed")


def _validate_build_configuration(
    member: PersonalBuildMemberV1, namespace_id: UUID, template: PersonalBuildTemplateV1, owner: AccountPolicyV1,
) -> None:
    configuration = member.configuration
    if (
        configuration.subject_id != personal_build_subject_id(namespace_id, member.owner_id)
        or configuration.display_name != personal_build_subject_name(member.owner_id)
        or configuration.profiles != template.profiles
        or member.acknowledgement.candidate != template.runtime_candidate
        or configuration.max_slots > template.max_slots_per_subject
        or configuration.max_pending_slots != template.max_pending_slots_per_subject
        or configuration.max_pending_jobs != template.max_pending_jobs_per_subject
        or configuration.submission_rate_per_minute != owner.submission_rate_per_minute
    ):
        raise _invalid("build membership identity, runtime or owner policy changed")


def resolved_subject_references(
    value: AllocationInputV1,
) -> tuple[ConfigurationGenerationRefV1, ...]:
    """Resolve a bounded delegated overlay without mutating its immutable base."""

    if not isinstance(value, (DelegatedAllocationInputV2, DelegatedAllocationInputV3)):
        return value.configuration.subjects

    membership: PersonalMembershipSnapshotV1 | PersonalMembershipSnapshotV2
    build_template: PersonalBuildTemplateV1 | None = None
    try:
        policy = PersonalMembershipPolicyV1.model_validate(
            value.preparation.personal_membership.model_dump(mode="python")
        )
        if isinstance(value, DelegatedAllocationInputV3):
            checked = DelegatedAllocationInputV3.model_validate_json(value.model_dump_json())
            membership = checked.membership
            build_template = checked.preparation.personal_builds
            for inherited_origin in (*checked.preparation.managed_application_origins, *checked.preparation.managed_build_origins):
                if isinstance(inherited_origin, (ManagedApplicationOriginV2, ManagedBuildOriginV1)) and isinstance(
                    inherited_origin.inherited.anchor.member.reincarnation, PersonalInheritedReincarnationEvidenceV2,
                ):
                    raise _invalid("cross-epoch recreation allocation is not yet connected")
        else:
            membership = PersonalMembershipSnapshotV1.model_validate(
                value.membership.model_dump(mode="python")
            )
    except ValidationError as exc:
        raise _invalid("personal membership contract is invalid") from exc

    preparation = value.preparation
    configuration = value.configuration
    fleet = value.fleet
    if preparation.configuration_epoch != configuration.configuration_epoch:
        raise _invalid("personal membership preparation configuration is stale")
    if (
        preparation.fleet_generation != configuration.fleet.generation
        or preparation.fleet_generation != fleet.fleet_generation
    ):
        raise _invalid("personal membership fleet generation changed")
    if (
        preparation.fleet_digest != configuration.fleet.digest
        or configuration.fleet.digest != canonical_digest(fleet)
    ):
        raise _invalid("personal membership fleet digest changed")
    if policy.namespace_id != membership.namespace_id:
        raise _invalid("personal membership namespace changed")

    template = fleet.development_subject_template
    if template is None or policy.development_template_sha256 != canonical_digest(template):
        raise _invalid("personal membership fleet template changed")
    template_policy = next(
        (
            account
            for account in fleet.account_policies
            if account.account_id == template.owner_account_template_id
            and account.kind == "owner_template"
        ),
        None,
    )
    if template_policy is None:
        raise _invalid("personal membership owner template is unavailable")
    if build_template is not None:
        _validate_build_template(build_template, fleet, template_policy)
    accounts = {
        account.account_id: account
        for account in (value.effective_account_policies or fleet.account_policies)
    }

    # Subject references have non-null identities under the snapshot contract.
    base_references = {cast(UUID, item.subject_id): item for item in configuration.subjects}
    managed_ids = set(policy.managed_base_subject_ids)
    managed_base = {item.subject_id: item for item in value.managed_base_subjects}
    build_origins: dict[UUID, ManagedBuildOriginV1] = {}
    original_roots = dict(base_references)
    if isinstance(value, DelegatedAllocationInputV3):
        for origin in value.preparation.managed_application_origins:
            original = managed_base.get(origin.configuration.subject_id)
            if original is None or canonical_digest(original) != canonical_digest(origin.configuration):
                raise _invalid("managed application origin differs from immutable base configuration")
            if isinstance(origin, ManagedApplicationOriginV2):
                original_roots[origin.configuration.subject_id] = origin.inherited.original_origin
        for build_origin in value.preparation.managed_build_origins:
            original = managed_base.get(build_origin.configuration.subject_id)
            if original is None or canonical_digest(original) != canonical_digest(build_origin.configuration):
                raise _invalid("managed build origin differs from immutable base configuration")
            build_origins[original.subject_id] = build_origin
            original_roots[original.subject_id] = build_origin.inherited.original_origin
    if set(managed_base) != managed_ids:
        raise _invalid("managed base subject payloads do not exactly cover policy")
    if not managed_ids <= set(base_references):
        raise _invalid("managed base subject is absent from the immutable manifest")

    owners_by_subject: dict[UUID, UUID] = {}
    for subject_id, original in managed_base.items():
        reference = base_references[subject_id]
        if (
            reference.subject_incarnation != original.subject_incarnation
            or reference.generation != original.configuration_generation
            or reference.digest != canonical_digest(original)
        ):
            raise _invalid("managed base payload differs from its immutable generation")
        owner_id = _owner_id(original)
        owner_policy = _derived_owner_policy(owner_id, template_policy, accounts)
        if subject_id in build_origins:
            if build_template is None:
                raise _invalid("managed build template is unavailable")
            build_member = build_origins[subject_id].inherited.anchor.member
            if not isinstance(build_member, PersonalBuildMemberV1):
                raise _invalid("managed build origin purpose changed")
            _validate_build_configuration(build_member, membership.namespace_id, build_template, owner_policy)
        else:
            _validate_personal_configuration(original, owner_id, template, owner_policy)
        owners_by_subject[subject_id] = owner_id

    members_by_subject = {item.configuration.subject_id: item for item in membership.members}
    if len(managed_ids | set(members_by_subject)) > policy.max_subjects:
        raise _invalid("personal membership exceeds its subject bound")

    resolved = dict(base_references)
    for subject_id, member in members_by_subject.items():
        if isinstance(member.reincarnation, PersonalInheritedReincarnationEvidenceV2):
            raise _invalid("cross-epoch recreation allocation is not yet connected")
        owner_policy = _derived_owner_policy(member.owner_id, template_policy, accounts)
        if isinstance(member, PersonalBuildMemberV1):
            if build_template is None:
                raise _invalid("build membership template is unavailable")
            _validate_build_configuration(member, membership.namespace_id, build_template, owner_policy)
        else:
            _validate_personal_configuration(member.configuration, member.owner_id, template, owner_policy)
        managed_original = managed_base.get(subject_id)
        if isinstance(member, PersonalBuildMemberV1) and subject_id in base_references and subject_id not in build_origins:
            raise _invalid("build membership cannot override an application or immutable base")
        if not isinstance(member, PersonalBuildMemberV1) and subject_id in build_origins:
            raise _invalid("application membership cannot override a managed build")
        if subject_id in base_references and managed_original is None:
            raise _invalid("personal membership cannot override a static base subject")
        evidence = member.reincarnation
        if evidence is not None and (
            evidence.namespace_id != membership.namespace_id
            or evidence.execution_manifest_sha256 != canonical_executable_digest(preparation)
            or (managed_original is not None and evidence.origin != original_roots[subject_id])
        ):
            raise _invalid("personal reincarnation authority or origin changed")
        if managed_original is not None and (
            (
                managed_original.subject_incarnation != member.configuration.subject_incarnation
                and evidence is None
            )
            or managed_original.display_name != member.configuration.display_name
            or _owner_id(managed_original) != member.owner_id
        ):
            raise _invalid("managed base subject identity changed")
        resolved[subject_id] = _member_reference(member)
        owners_by_subject[subject_id] = member.owner_id

    input_configurations = {
        item.configuration.subject_id: item.configuration for item in value.subjects
    }
    if set(input_configurations) != set(resolved):
        raise _invalid("resolved personal subject manifest is incomplete")
    for resolved_subject_id, reference in resolved.items():
        supplied = input_configurations[resolved_subject_id]
        if (
            reference.subject_incarnation != supplied.subject_incarnation
            or reference.generation != supplied.configuration_generation
            or reference.digest != canonical_digest(supplied)
        ):
            raise _invalid("resolved personal subject generation binding changed")

    for member in membership.members:
        member_configuration = member.configuration
        if any(
            subject_id != member_configuration.subject_id
            and supplied.display_name == member_configuration.display_name
            for subject_id, supplied in input_configurations.items()
        ):
            raise _invalid("personal application name collides with resolved subject")

    for owner_id in set(owners_by_subject.values()):
        owner_policy = _derived_owner_policy(owner_id, template_policy, accounts)
        owner_subjects = tuple(
            supplied
            for supplied in input_configurations.values()
            if supplied.account_id == owner_policy.account_id
            and supplied.lifecycle_state != "disabled"
        )
        if len(owner_subjects) > owner_policy.max_live_subjects:
            raise _invalid("personal owner exceeds max_live_subjects")
        try:
            minimum = checked_sum(tuple(subject.min_slots for subject in owner_subjects))
        except CapacityContractError as exc:
            raise _invalid("personal owner minimum aggregate is invalid") from exc
        if minimum > owner_policy.min_reservation_slots:
            raise _invalid("personal owner minimum aggregate exceeds its reservation")

    return tuple(
        sorted(
            resolved.values(),
            key=lambda item: item.subject_id.int if item.subject_id is not None else 0,
        )
    )


__all__ = ["PersonalMembershipResolutionError", "resolved_subject_references"]
