"""Versioned personal-membership contracts and allocator composition."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from importlib import import_module
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.allocator import ShadowAllocatorError, allocate_shadow
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    DevelopmentSubjectTemplateV1,
    SubjectConfigurationV1,
    canonical_bytes,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutionPreparationV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.execution_policy import load_execution_preparation_policy
from loom_capacity_manager.membership import (
    PersonalMembershipResolutionError,
    resolved_subject_references,
)
from loom_capacity_manager.membership_contracts import (
    DelegatedAllocationInputV2,
    ExecutionPreparationPolicyV3,
    ExecutionPreparationV3,
    PersonalApplicationMemberV1,
    PersonalMembershipPolicyV1,
    PersonalMembershipSnapshotV1,
    parse_execution_preparation,
    parse_execution_preparation_policy,
)
from tests.capacity_execution_fixtures import execution_policy
from tests.capacity_fixtures import allocator_input, allocator_subject

OWNER_A = UUID("10000000-0000-4000-8000-000000000001")
OWNER_B = UUID("10000000-0000-4000-8000-000000000002")
NAMESPACE_ID = UUID("20000000-0000-4000-8000-000000000001")
ZERO_UUID = UUID(int=0)
ZERO_DIGEST = "0" * 64


def _owner_account(template: AccountPolicyV1, owner_id: UUID) -> AccountPolicyV1:
    return template.model_copy(
        update={
            "account_id": f"dev-owner-{owner_id.hex}",
            "kind": "owner",
            "owner_id": owner_id,
        }
    )


def _personal_configuration(
    source: SubjectConfigurationV1,
    template: DevelopmentSubjectTemplateV1,
    owner_id: UUID,
    display_name: str,
    *,
    lifecycle_state: str = "active",
    min_slots: int = 0,
    max_slots: int = 2,
) -> SubjectConfigurationV1:
    return source.model_copy(
        update={
            "display_name": display_name,
            "account_id": f"dev-owner-{owner_id.hex}",
            "tier_id": "development",
            "min_slots": min_slots,
            "max_slots": max_slots,
            "rollout_surge_slots": template.rollout_surge_slots,
            "max_pending_slots": template.max_pending_slots_per_subject,
            "max_pending_jobs": template.max_pending_jobs_per_subject,
            "submission_rate_per_minute": 3,
            "lifecycle_state": lifecycle_state,
            "profiles": template.profiles,
        }
    )


def _member(
    configuration: SubjectConfigurationV1,
    owner_id: UUID,
    *,
    revision: int = 1,
) -> PersonalApplicationMemberV1:
    acknowledgement = SubjectExecutionAcknowledgementV2(
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
        configuration_generation=configuration.configuration_generation,
        deployment_generation=configuration.deployment_generation,
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="7" * 64,
            publication_sha256="8" * 64,
        ),
        reporter_incarnation=configuration.demand_reporter_incarnation,
        protected_admission_sha256="9" * 64,
        legacy_writer_high_water=0,
        acknowledgement_sha256="a" * 64,
    )
    return PersonalApplicationMemberV1(
        revision=revision,
        owner_id=owner_id,
        configuration=configuration,
        acknowledgement=acknowledgement,
    )


def _preparation(
    configuration: ConfigurationSnapshotV1,
    fleet_generation: int,
    personal_membership: PersonalMembershipPolicyV1,
) -> ExecutionPreparationV3:
    policy = execution_policy()
    return ExecutionPreparationV3(
        authority_incarnation=UUID("30000000-0000-4000-8000-000000000001"),
        expected_writer_epoch=2,
        configuration_epoch=configuration.configuration_epoch,
        fleet_generation=fleet_generation,
        fleet_digest=configuration.fleet.digest,
        trusted_fleet_release_sha256=policy.trusted_fleet_release_sha256,
        requested_ceiling=policy.executable_new_capacity_ceiling,
        requested_rate_per_minute=policy.executable_new_capacity_rate_per_minute,
        executors=policy.executors,
        subject_acknowledgements=(),
        legacy_writer_fences=policy.legacy_writer_fences,
        rollback_evidence_sha256=policy.rollback_evidence_sha256,
        personal_membership=personal_membership,
    )


def delegated_input_with_new_owner() -> DelegatedAllocationInputV2:
    raw_base = allocator_subject(90, account_id="placeholder", max_slots=2)
    raw_new = allocator_subject(
        91,
        account_id="placeholder-new",
        pending=(("new-owner-work", ("gb10", "oldlab"), ("cpu",)),),
        max_slots=2,
    )
    allocation = allocator_input((raw_base, raw_new), gb10_slots=2, oldlab_slots=2)
    initial_base = allocation.subjects[0]
    initial_new = allocation.subjects[1]
    owner_template = AccountPolicyV1(
        account_id="personal-development-owner",
        kind="owner_template",
        owner_id=None,
        min_reservation_slots=2,
        max_slots=4,
        max_surge_slots=1,
        max_pending_slots=4,
        max_pending_jobs=4,
        submission_rate_per_minute=3,
        max_live_subjects=2,
    )
    template = DevelopmentSubjectTemplateV1(
        owner_account_template_id=owner_template.account_id,
        max_slots_per_subject=2,
        rollout_surge_slots=1,
        max_pending_slots_per_subject=4,
        max_pending_jobs_per_subject=4,
        profiles=initial_base.configuration.profiles,
    )
    fleet = allocation.fleet.model_copy(
        update={
            "fleet_digest": "f" * 64,
            "account_policies": (owner_template,),
            "development_subject_template": template,
        }
    )
    fleet = fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
    )
    base_configuration = _personal_configuration(
        initial_base.configuration,
        template,
        OWNER_A,
        "dev-alice",
    )
    new_configuration = _personal_configuration(
        initial_new.configuration,
        template,
        OWNER_B,
        "dev-bob",
    )
    base_input = initial_base.model_copy(update={"configuration": base_configuration})
    new_input = initial_new.model_copy(update={"configuration": new_configuration})
    base_reference = ConfigurationGenerationRefV1(
        scope="subject",
        generation=base_configuration.configuration_generation,
        digest=canonical_digest(base_configuration),
        subject_id=base_configuration.subject_id,
        subject_incarnation=base_configuration.subject_incarnation,
    )
    configuration = ConfigurationSnapshotV1(
        configuration_epoch=allocation.configuration.configuration_epoch,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=fleet.fleet_generation,
            digest=canonical_digest(fleet),
        ),
        subjects=(base_reference,),
    )
    membership_policy = PersonalMembershipPolicyV1(
        namespace_id=NAMESPACE_ID,
        management_principal_id="personal-membership-manager",
        development_template_sha256=canonical_digest(template),
        max_subjects=4,
        managed_base_subject_ids=(base_configuration.subject_id,),
    )
    return DelegatedAllocationInputV2(
        configuration=configuration,
        fleet=fleet,
        effective_account_policies=(
            owner_template,
            _owner_account(owner_template, OWNER_A),
            _owner_account(owner_template, OWNER_B),
        ),
        subjects=(base_input, new_input),
        pools=allocation.pools,
        preparation=_preparation(
            configuration,
            fleet.fleet_generation,
            membership_policy,
        ),
        managed_base_subjects=(base_configuration,),
        membership=PersonalMembershipSnapshotV1(
            namespace_id=NAMESPACE_ID,
            revision=1,
            head_sha256="b" * 64,
            members=(_member(new_configuration, OWNER_B),),
        ),
    )


def _replace_member(
    value: DelegatedAllocationInputV2,
    **configuration_updates: Any,
) -> DelegatedAllocationInputV2:
    member = value.membership.members[0]
    configuration = member.configuration.model_copy(update=configuration_updates)
    changed = _member(configuration, member.owner_id, revision=member.revision)
    return value.model_copy(
        update={
            "membership": value.membership.model_copy(update={"members": (changed,)}),
            "subjects": tuple(
                subject.model_copy(update={"configuration": configuration})
                if subject.configuration.subject_id == configuration.subject_id
                else subject
                for subject in value.subjects
            ),
        }
    )


def _remove_managed_base_authority(
    value: DelegatedAllocationInputV2,
) -> DelegatedAllocationInputV2:
    policy = value.preparation.personal_membership.model_copy(
        update={"managed_base_subject_ids": ()}
    )
    changed = value.model_copy(
        update={
            "preparation": value.preparation.model_copy(update={"personal_membership": policy}),
            "managed_base_subjects": (),
        }
    )
    return DelegatedAllocationInputV2.model_validate_json(changed.model_dump_json())


def _replace_base(
    value: DelegatedAllocationInputV2,
    **configuration_updates: Any,
) -> DelegatedAllocationInputV2:
    changed = value.managed_base_subjects[0].model_copy(update=configuration_updates)
    reference = ConfigurationGenerationRefV1(
        scope="subject",
        generation=changed.configuration_generation,
        digest=canonical_digest(changed),
        subject_id=changed.subject_id,
        subject_incarnation=changed.subject_incarnation,
    )
    configuration = value.configuration.model_copy(update={"subjects": (reference,)})
    preparation = value.preparation.model_copy(
        update={
            "configuration_epoch": configuration.configuration_epoch,
            "fleet_digest": configuration.fleet.digest,
        }
    )
    return value.model_copy(
        update={
            "configuration": configuration,
            "preparation": preparation,
            "managed_base_subjects": (changed,),
            "subjects": (
                value.subjects[0].model_copy(update={"configuration": changed}),
                value.subjects[1],
            ),
        }
    )


def _move_new_member_to_owner_a(
    value: DelegatedAllocationInputV2,
    *,
    min_slots: int = 0,
    max_slots: int = 1,
) -> DelegatedAllocationInputV2:
    member = value.membership.members[0]
    changed = member.configuration.model_copy(
        update={
            "account_id": f"dev-owner-{OWNER_A.hex}",
            "min_slots": min_slots,
            "max_slots": max_slots,
        }
    )
    changed_member = _member(changed, OWNER_A)
    return value.model_copy(
        update={
            "effective_account_policies": tuple(
                account
                for account in value.effective_account_policies
                if account.owner_id != OWNER_B
            ),
            "subjects": (
                value.subjects[0],
                value.subjects[1].model_copy(update={"configuration": changed}),
            ),
            "membership": value.membership.model_copy(update={"members": (changed_member,)}),
        }
    )


def _replace_owner_template(
    value: DelegatedAllocationInputV2,
    **policy_updates: Any,
) -> DelegatedAllocationInputV2:
    source = next(
        account for account in value.fleet.account_policies if account.kind == "owner_template"
    )
    changed_source = source.model_copy(update=policy_updates)
    fleet = value.fleet.model_copy(
        update={"fleet_digest": "f" * 64, "account_policies": (changed_source,)}
    )
    fleet = fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
    )
    configuration = value.configuration.model_copy(
        update={
            "fleet": value.configuration.fleet.model_copy(
                update={"generation": fleet.fleet_generation, "digest": canonical_digest(fleet)}
            )
        }
    )
    owners = tuple(
        sorted(
            {
                OWNER_A,
                *(
                    (OWNER_B,)
                    if any(
                        account.owner_id == OWNER_B for account in value.effective_account_policies
                    )
                    else ()
                ),
            },
            key=lambda item: item.int,
        )
    )
    return value.model_copy(
        update={
            "fleet": fleet,
            "configuration": configuration,
            "preparation": value.preparation.model_copy(
                update={
                    "fleet_generation": fleet.fleet_generation,
                    "fleet_digest": canonical_digest(fleet),
                }
            ),
            "effective_account_policies": (
                changed_source,
                *tuple(_owner_account(changed_source, owner_id) for owner_id in owners),
            ),
        }
    )


def test_personal_membership_modules_are_available() -> None:
    assert importlib.util.find_spec("loom_capacity_manager.membership_contracts") is not None
    assert importlib.util.find_spec("loom_capacity_manager.membership") is not None


def test_versioned_membership_interfaces_are_exported() -> None:
    contracts = import_module("loom_capacity_manager.membership_contracts")
    resolution = import_module("loom_capacity_manager.membership")

    assert {
        "DelegatedAllocationInputV2",
        "ExecutionPreparationPolicyV3",
        "ExecutionPreparationV3",
        "PersonalApplicationMemberV1",
        "PersonalMembershipPolicyV1",
        "PersonalMembershipSnapshotV1",
        "parse_execution_preparation",
        "parse_execution_preparation_policy",
    } <= set(vars(contracts))
    assert "resolved_subject_references" in vars(resolution)


def test_v1_subject_references_are_returned_unchanged() -> None:
    subject = allocator_subject(1, account_id="shared-development")
    value = allocator_input((subject,), gb10_slots=1, oldlab_slots=1)

    assert resolved_subject_references(value) is value.configuration.subjects


def test_membership_contracts_canonicalize_ids_and_revisions() -> None:
    value = delegated_input_with_new_owner()
    base_id = value.managed_base_subjects[0].subject_id
    other_id = UUID(int=1)
    policy = value.preparation.personal_membership.model_copy(
        update={"managed_base_subject_ids": (base_id, other_id)}
    )
    first = value.membership.members[0].model_copy(update={"revision": 2})
    second_configuration = first.configuration.model_copy(
        update={
            "subject_id": UUID("00000000-0000-4000-8000-000000000099"),
            "display_name": "dev-carol",
        }
    )
    second = _member(second_configuration, OWNER_B, revision=1)

    canonical_policy = PersonalMembershipPolicyV1.model_validate(policy.model_dump(mode="python"))
    snapshot = PersonalMembershipSnapshotV1(
        namespace_id=NAMESPACE_ID,
        revision=2,
        head_sha256="c" * 64,
        members=(first, second),
    )

    assert canonical_policy.managed_base_subject_ids == (other_id, base_id)
    assert tuple(item.revision for item in snapshot.members) == (1, 2)


@pytest.mark.parametrize(
    "payload_update",
    [
        {"namespace_id": ZERO_UUID},
        {"managed_base_subject_ids": (ZERO_UUID,)},
        {"managed_base_subject_ids": (UUID(int=7), UUID(int=7))},
        {"max_subjects": 1, "managed_base_subject_ids": (UUID(int=7), UUID(int=8))},
    ],
)
def test_membership_policy_rejects_zero_duplicate_or_excessive_base_ids(
    payload_update: dict[str, object],
) -> None:
    policy = delegated_input_with_new_owner().preparation.personal_membership

    with pytest.raises(ValidationError):
        PersonalMembershipPolicyV1.model_validate(policy.model_dump(mode="python") | payload_update)


@pytest.mark.parametrize("collision", ["subject", "revision", "name"])
def test_snapshot_rejects_duplicate_member_identities(collision: str) -> None:
    value = delegated_input_with_new_owner()
    first = value.membership.members[0]
    configuration = first.configuration.model_copy(
        update={
            "subject_id": (
                first.configuration.subject_id if collision == "subject" else UUID(int=9_999)
            ),
            "display_name": (
                first.configuration.display_name if collision == "name" else "dev-carol"
            ),
        }
    )
    second = _member(
        configuration,
        first.owner_id,
        revision=first.revision if collision == "revision" else 2,
    )

    with pytest.raises(ValidationError):
        PersonalMembershipSnapshotV1(
            namespace_id=NAMESPACE_ID,
            revision=2,
            head_sha256="c" * 64,
            members=(first, second),
        )


@pytest.mark.parametrize(
    "revision,head,members_kind",
    [
        (0, "c" * 64, "none"),
        (0, ZERO_DIGEST, "one"),
        (1, ZERO_DIGEST, "one"),
        (1, "c" * 64, "ahead"),
    ],
)
def test_snapshot_rejects_contradictory_head_and_revision(
    revision: int,
    head: str,
    members_kind: str,
) -> None:
    member = delegated_input_with_new_owner().membership.members[0]
    if members_kind == "ahead":
        member = member.model_copy(update={"revision": 2})
    members = () if members_kind == "none" else (member,)

    with pytest.raises(ValidationError):
        PersonalMembershipSnapshotV1(
            namespace_id=NAMESPACE_ID,
            revision=revision,
            head_sha256=head,
            members=members,
        )


def test_member_rejects_build_purpose_and_bad_acknowledgement() -> None:
    member = delegated_input_with_new_owner().membership.members[0]
    payload = member.model_dump(mode="python")

    with pytest.raises(ValidationError):
        PersonalApplicationMemberV1.model_validate(payload | {"purpose": "build"})

    acknowledgement = member.acknowledgement.model_copy(
        update={"deployment_generation": member.configuration.deployment_generation + 1}
    )
    with pytest.raises(ValidationError):
        PersonalApplicationMemberV1.model_validate(payload | {"acknowledgement": acknowledgement})


@pytest.mark.parametrize("identity", ["owner", "subject"])
def test_member_rejects_zero_declared_or_nested_identity(identity: str) -> None:
    member = delegated_input_with_new_owner().membership.members[0]
    payload_update: dict[str, object]
    if identity == "owner":
        payload_update = {"owner_id": ZERO_UUID}
    else:
        payload_update = {
            "configuration": member.configuration.model_copy(update={"subject_id": ZERO_UUID})
        }

    with pytest.raises(ValidationError):
        PersonalApplicationMemberV1.model_validate(
            member.model_dump(mode="python") | payload_update
        )


def test_snapshot_rejects_zero_namespace_identity() -> None:
    with pytest.raises(ValidationError):
        PersonalMembershipSnapshotV1(
            namespace_id=ZERO_UUID,
            revision=0,
            head_sha256=ZERO_DIGEST,
        )


@pytest.mark.parametrize(
    "candidate",
    [
        CandidateBindingV2(
            algorithm="git-sha1",
            identity="1" * 40,
            publication_sha256="2" * 64,
        ),
        CandidateBindingV2(
            algorithm="source-sha256",
            identity="1" * 64,
            publication_sha256=ZERO_DIGEST,
        ),
        CandidateBindingV2(
            algorithm="source-sha256",
            identity=ZERO_DIGEST,
            publication_sha256="2" * 64,
        ),
    ],
)
def test_member_rejects_non_application_candidate(candidate: CandidateBindingV2) -> None:
    member = delegated_input_with_new_owner().membership.members[0]
    acknowledgement = member.acknowledgement.model_copy(update={"candidate": candidate})

    with pytest.raises(ValidationError):
        PersonalApplicationMemberV1.model_validate(
            member.model_dump(mode="python") | {"acknowledgement": acknowledgement}
        )


def test_v2_and_v3_execution_documents_dispatch_strictly_and_digest_differently() -> None:
    value = delegated_input_with_new_owner()
    v2_policy = execution_policy()
    v3_payload = v2_policy.model_dump(mode="python")
    v3_payload["schema_version"] = 3
    v3_policy = ExecutionPreparationPolicyV3(
        **v3_payload,
        personal_membership=value.preparation.personal_membership,
    )
    preparation_payload = value.preparation.model_dump(mode="python")
    preparation_payload.pop("personal_membership")
    preparation_payload["schema_version"] = 2
    v2_preparation = ExecutionPreparationV2.model_validate(preparation_payload)

    assert parse_execution_preparation_policy(canonical_executable_bytes(v2_policy)) == v2_policy
    assert parse_execution_preparation(canonical_executable_bytes(v2_preparation)) == v2_preparation
    assert isinstance(
        parse_execution_preparation_policy(canonical_executable_bytes(v3_policy)),
        ExecutionPreparationPolicyV3,
    )
    assert isinstance(
        parse_execution_preparation(canonical_executable_bytes(value.preparation)),
        ExecutionPreparationV3,
    )
    assert canonical_executable_digest(v3_policy) != canonical_executable_digest(v2_policy)
    assert canonical_executable_digest(value.preparation) != canonical_executable_digest(
        v2_preparation
    )

    for bad_version in (None, 1, 4):
        payload = v2_policy.model_dump(mode="json")
        if bad_version is None:
            payload.pop("schema_version")
        else:
            payload["schema_version"] = bad_version
        with pytest.raises(ValidationError):
            parse_execution_preparation_policy(json.dumps(payload))


def test_v3_dispatch_requires_an_exact_integer_json_tag() -> None:
    value = delegated_input_with_new_owner()
    preparation_payload = value.preparation.model_dump(mode="json")
    preparation_payload["schema_version"] = 3.0
    v2 = execution_policy()
    policy_payload = v2.model_dump(mode="json")
    policy_payload["schema_version"] = 3.0
    policy_payload["personal_membership"] = value.preparation.personal_membership.model_dump(
        mode="json"
    )

    with pytest.raises(ValueError, match="schema version"):
        parse_execution_preparation(json.dumps(preparation_payload))
    with pytest.raises(ValueError, match="schema version"):
        parse_execution_preparation_policy(json.dumps(policy_payload))


def test_v2_dispatch_preserves_the_existing_numeric_literal_semantics() -> None:
    value = delegated_input_with_new_owner()
    preparation_payload = value.preparation.model_dump(mode="json")
    preparation_payload.pop("personal_membership")
    preparation_payload["schema_version"] = 2.0
    policy = execution_policy()
    policy_payload = policy.model_dump(mode="json")
    policy_payload["schema_version"] = 2.0

    preparation = parse_execution_preparation(json.dumps(preparation_payload))
    parsed_policy = parse_execution_preparation_policy(json.dumps(policy_payload))

    assert preparation.schema_version == 2
    assert parsed_policy == policy


def test_pinned_policy_loader_accepts_the_concrete_v3_policy(tmp_path: Path) -> None:
    value = delegated_input_with_new_owner()
    v2 = execution_policy()
    policy_payload = v2.model_dump(mode="python")
    policy_payload["schema_version"] = 3
    policy = ExecutionPreparationPolicyV3(
        **policy_payload,
        personal_membership=value.preparation.personal_membership,
    )
    payload = canonical_executable_bytes(policy)
    path = tmp_path / "execution-policy.json"
    path.write_bytes(payload)
    path.chmod(0o600)

    loaded = load_execution_preparation_policy(path, hashlib.sha256(payload).hexdigest())

    assert loaded == policy
    assert isinstance(loaded, ExecutionPreparationPolicyV3)


def test_delegated_references_merge_new_member_and_retain_immutable_base() -> None:
    value = delegated_input_with_new_owner()
    original = value.configuration.subjects[0]
    member = value.membership.members[0]

    resolved = resolved_subject_references(value)

    assert resolved[0] is original
    assert {item.subject_id for item in resolved} == {
        original.subject_id,
        member.configuration.subject_id,
    }
    assert next(
        item for item in resolved if item.subject_id == member.configuration.subject_id
    ) == (
        ConfigurationGenerationRefV1(
            scope="subject",
            generation=member.configuration.configuration_generation,
            digest=canonical_digest(member.configuration),
            subject_id=member.configuration.subject_id,
            subject_incarnation=member.configuration.subject_incarnation,
        )
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "stale-configuration",
        "namespace",
        "template",
        "fleet-generation",
        "fleet-digest",
        "base-digest",
    ],
)
def test_delegation_rejects_stale_or_mismatched_authority(mutation: str) -> None:
    value = delegated_input_with_new_owner()
    if mutation == "stale-configuration":
        value = value.model_copy(
            update={
                "preparation": value.preparation.model_copy(
                    update={"configuration_epoch": value.configuration.configuration_epoch + 1}
                )
            }
        )
    elif mutation == "namespace":
        value = value.model_copy(
            update={
                "membership": value.membership.model_copy(update={"namespace_id": UUID(int=88)})
            }
        )
    elif mutation == "template":
        policy = value.preparation.personal_membership.model_copy(
            update={"development_template_sha256": "d" * 64}
        )
        value = value.model_copy(
            update={
                "preparation": value.preparation.model_copy(update={"personal_membership": policy})
            }
        )
    elif mutation == "fleet-generation":
        value = value.model_copy(
            update={
                "preparation": value.preparation.model_copy(
                    update={"fleet_generation": value.fleet.fleet_generation + 1}
                )
            }
        )
    elif mutation == "fleet-digest":
        value = value.model_copy(
            update={"preparation": value.preparation.model_copy(update={"fleet_digest": "d" * 64})}
        )
    else:
        base = value.managed_base_subjects[0].model_copy(update={"max_slots": 1})
        value = value.model_copy(update={"managed_base_subjects": (base,)})

    with pytest.raises(ValueError):
        resolved_subject_references(value)


def test_delegation_binds_fleet_digest_directly_not_only_through_base_reference() -> None:
    value = delegated_input_with_new_owner()
    changed_fleet = value.fleet.model_copy(update={"fleet_digest": "d" * 64})

    with pytest.raises(ValueError):
        resolved_subject_references(value.model_copy(update={"fleet": changed_fleet}))


def test_delegation_rejects_an_unmanaged_static_override() -> None:
    value = delegated_input_with_new_owner()
    base_configuration = value.managed_base_subjects[0]
    base_member = _member(base_configuration, OWNER_A, revision=2)
    snapshot = value.membership.model_copy(
        update={"revision": 2, "members": (*value.membership.members, base_member)}
    )
    policy = value.preparation.personal_membership.model_copy(
        update={"managed_base_subject_ids": ()}
    )
    value = value.model_copy(
        update={
            "preparation": value.preparation.model_copy(update={"personal_membership": policy}),
            "managed_base_subjects": (),
            "membership": snapshot,
        }
    )

    with pytest.raises(ValueError):
        resolved_subject_references(value)


@pytest.mark.parametrize("substitution", ["owner", "name", "incarnation"])
def test_managed_base_overlay_cannot_substitute_protected_identity(substitution: str) -> None:
    value = delegated_input_with_new_owner()
    base = value.managed_base_subjects[0]
    updates: dict[str, object] = {}
    owner_id = OWNER_A
    if substitution == "owner":
        owner_id = OWNER_B
        updates["account_id"] = f"dev-owner-{OWNER_B.hex}"
    elif substitution == "name":
        updates["display_name"] = "dev-carol"
    else:
        updates["subject_incarnation"] = UUID(int=55_555)
    changed = base.model_copy(update=updates)
    base_member = _member(changed, owner_id, revision=2)
    value = value.model_copy(
        update={
            "membership": value.membership.model_copy(
                update={"revision": 2, "members": (*value.membership.members, base_member)}
            ),
            "subjects": (
                value.subjects[0].model_copy(update={"configuration": changed}),
                value.subjects[1],
            ),
        }
    )

    with pytest.raises(ValueError, match="managed base subject identity changed"):
        resolved_subject_references(value)


def test_managed_base_overlay_can_change_unprotected_configuration() -> None:
    value = delegated_input_with_new_owner()
    base = value.managed_base_subjects[0]
    changed = base.model_copy(update={"max_slots": 1})
    base_member = _member(changed, OWNER_A, revision=2)
    value = value.model_copy(
        update={
            "membership": value.membership.model_copy(
                update={"revision": 2, "members": (*value.membership.members, base_member)}
            ),
            "subjects": (
                value.subjects[0].model_copy(update={"configuration": changed}),
                value.subjects[1],
            ),
        }
    )

    resolved = resolved_subject_references(value)
    result = allocate_shadow(value)

    assert next(item for item in resolved if item.subject_id == changed.subject_id).digest == (
        canonical_digest(changed)
    )
    assert result.configuration is value.configuration


@pytest.mark.parametrize(
    "configuration_updates",
    [
        {"tier_id": "staging"},
        {"display_name": "alice"},
        {"account_id": "shared-development"},
    ],
)
def test_managed_base_payload_must_remain_a_canonical_personal_application(
    configuration_updates: dict[str, object],
) -> None:
    value = _replace_base(delegated_input_with_new_owner(), **configuration_updates)

    with pytest.raises(ValueError):
        resolved_subject_references(value)


@pytest.mark.parametrize(
    "mutation,message",
    [
        ("maximum", "maximum exceeds"),
        ("profiles", "profiles differ"),
        ("disabled-maximum", "disabled personal application must have zero capacity"),
        ("disabled-minimum", "disabled personal application must have zero capacity"),
        ("rate", "rate differs"),
        ("surge", "surge differs"),
        ("pending", "pending jobs differ"),
        ("name", "name is invalid"),
    ],
)
def test_personal_member_rejects_limits_profiles_and_disabled_capacity(
    mutation: str,
    message: str,
) -> None:
    value = delegated_input_with_new_owner()
    if mutation == "maximum":
        updates: dict[str, object] = {"max_slots": 3}
    elif mutation == "profiles":
        profile = value.membership.members[0].configuration.profiles[0]
        changed_profile = profile.model_copy(
            update={
                "profile_generation": profile.profile_generation + 1,
                "profile_digest": "e" * 64,
            }
        )
        updates = {
            "profiles": (changed_profile, value.membership.members[0].configuration.profiles[1])
        }
    elif mutation == "disabled-maximum":
        updates = {"lifecycle_state": "disabled", "max_slots": 1, "min_slots": 0}
    elif mutation == "disabled-minimum":
        updates = {"lifecycle_state": "disabled", "max_slots": 1, "min_slots": 1}
    elif mutation == "rate":
        updates = {"submission_rate_per_minute": 2}
    elif mutation == "surge":
        updates = {"rollout_surge_slots": 0}
    elif mutation == "pending":
        updates = {"max_pending_jobs": 3}
    else:
        updates = {"display_name": "dev-development"}
    value = _replace_member(value, **updates)
    value = DelegatedAllocationInputV2.model_validate_json(value.model_dump_json())

    with pytest.raises(ValueError, match=message):
        resolved_subject_references(value)


def test_owner_max_live_subjects_counts_existing_managed_base() -> None:
    value = _move_new_member_to_owner_a(delegated_input_with_new_owner())
    value = _replace_owner_template(value, max_live_subjects=1)

    with pytest.raises(ValueError):
        resolved_subject_references(value)


def test_owner_minimum_aggregate_counts_existing_managed_base() -> None:
    value = _replace_base(delegated_input_with_new_owner(), min_slots=2)
    value = _move_new_member_to_owner_a(value, min_slots=1)

    with pytest.raises(ValueError):
        resolved_subject_references(value)


def test_owner_max_live_subjects_counts_unmanaged_resolved_base_in_allocator() -> None:
    value = _move_new_member_to_owner_a(delegated_input_with_new_owner())
    value = _replace_owner_template(value, max_live_subjects=1)
    value = _remove_managed_base_authority(value)

    with pytest.raises(ShadowAllocatorError, match="personal owner exceeds max_live_subjects"):
        allocate_shadow(value)


def test_owner_minimum_aggregate_counts_unmanaged_resolved_base_in_allocator() -> None:
    value = _replace_base(delegated_input_with_new_owner(), min_slots=2)
    value = _move_new_member_to_owner_a(value, min_slots=1)
    value = _remove_managed_base_authority(value)

    with pytest.raises(
        ShadowAllocatorError,
        match="personal owner minimum aggregate exceeds its reservation",
    ):
        allocate_shadow(value)


def test_unmanaged_resolved_owner_subjects_pass_within_both_owner_limits() -> None:
    value = _move_new_member_to_owner_a(delegated_input_with_new_owner())
    value = _remove_managed_base_authority(value)

    result = allocate_shadow(value)

    assert {item.subject_id for item in result.allocations} == {
        subject.configuration.subject_id for subject in value.subjects
    }


@pytest.mark.parametrize("base_kind", ["managed", "unmanaged", "disabled"])
def test_logged_name_cannot_collide_with_any_resolved_base_identity(base_kind: str) -> None:
    value = delegated_input_with_new_owner()
    if base_kind == "disabled":
        value = _replace_base(value, lifecycle_state="disabled", min_slots=0, max_slots=0)
    base_name = value.subjects[0].configuration.display_name
    value = _replace_member(value, display_name=base_name)
    if base_kind == "unmanaged":
        value = _remove_managed_base_authority(value)
    else:
        value = DelegatedAllocationInputV2.model_validate_json(value.model_dump_json())

    with pytest.raises(
        PersonalMembershipResolutionError,
        match="personal application name collides with resolved subject",
    ):
        resolved_subject_references(value)
    with pytest.raises(
        ShadowAllocatorError,
        match="personal application name collides with resolved subject",
    ):
        allocate_shadow(value)


def test_disabled_tombstone_is_counted_and_required_in_the_full_input() -> None:
    value = _replace_member(
        delegated_input_with_new_owner(),
        lifecycle_state="disabled",
        min_slots=0,
        max_slots=0,
    )
    disabled_configuration = value.membership.members[0].configuration
    value = value.model_copy(
        update={
            "subjects": (
                value.subjects[0],
                value.subjects[1].model_copy(update={"configuration": disabled_configuration}),
            )
        }
    )

    assert disabled_configuration.subject_id in {
        item.subject_id for item in resolved_subject_references(value)
    }

    policy = value.preparation.personal_membership.model_copy(update={"max_subjects": 1})
    bounded = value.model_copy(
        update={"preparation": value.preparation.model_copy(update={"personal_membership": policy})}
    )
    with pytest.raises(ValueError):
        resolved_subject_references(bounded)

    incomplete = value.model_copy(update={"subjects": (value.subjects[0],)})
    with pytest.raises(ValueError):
        resolved_subject_references(incomplete)


def test_delegated_contract_rejects_union_above_policy_max_subjects() -> None:
    value = delegated_input_with_new_owner()
    policy = value.preparation.personal_membership.model_copy(update={"max_subjects": 1})
    preparation = value.preparation.model_copy(update={"personal_membership": policy})

    with pytest.raises(ValidationError):
        DelegatedAllocationInputV2.model_validate(
            value.model_dump(mode="python") | {"preparation": preparation}
        )


def test_full_allocator_composes_membership_without_replacing_base_snapshot() -> None:
    value = delegated_input_with_new_owner()
    original = canonical_bytes(value.configuration)
    new_owner_id = value.membership.members[0].configuration.subject_id

    result = allocate_shadow(value)

    assert canonical_bytes(result.configuration) == original
    assert new_owner_id in {item.subject_id for item in result.allocations}


def test_allocator_translates_membership_resolution_failure() -> None:
    value = delegated_input_with_new_owner()
    value = value.model_copy(
        update={"membership": value.membership.model_copy(update={"namespace_id": UUID(int=88)})}
    )

    with pytest.raises(ShadowAllocatorError):
        allocate_shadow(value)
