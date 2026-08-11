from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_capacity_manager.allocator import ShadowAllocatorError, allocate_shadow
from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    AllocationInputV1,
    DevelopmentSubjectTemplateV1,
    DynamicDevelopmentSubjectProjectionV1,
    FleetManifestV1,
    ProfileReferenceV1,
    canonical_bytes,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.fleet_state import FleetStateError, validate_fleet_manifest_digests
from tests.capacity_fixtures import (
    allocator_input,
    allocator_subject,
    fleet_manifest,
    valid_profile_payload,
)

_OWNER_ID = UUID("00000000-0000-4000-8000-000000000100")
_SUBJECT_ID = UUID("00000000-0000-4000-8000-000000000101")
_SUBJECT_INCARNATION = UUID("00000000-0000-4000-8000-000000000102")
_REPORTER_INCARNATION = UUID("00000000-0000-4000-8000-000000000103")
_OPERATION_ID = UUID("00000000-0000-4000-8000-000000000104")


def fleet_with_development_template() -> FleetManifestV1:
    base = fleet_manifest()
    owner_template = AccountPolicyV1(
        account_id="personal-development-owner",
        kind="owner_template",
        owner_id=None,
        min_reservation_slots=4,
        max_slots=8,
        max_surge_slots=1,
        max_pending_slots=8,
        max_pending_jobs=8,
        max_live_subjects=2,
    )
    template = DevelopmentSubjectTemplateV1(
        owner_account_template_id=owner_template.account_id,
        max_slots_per_subject=8,
        rollout_surge_slots=0,
        max_pending_slots_per_subject=8,
        max_pending_jobs_per_subject=8,
        profiles=tuple(
            ProfileReferenceV1.model_validate(valid_profile_payload(base, pool_id=pool_id))
            for pool_id in ("gb10", "oldlab")
        ),
    )
    changed = base.model_copy(
        update={
            "fleet_digest": "f" * 64,
            "account_policies": tuple(
                sorted(
                    (*base.account_policies, owner_template),
                    key=lambda account: account.account_id,
                )
            ),
            "development_subject_template": template,
        }
    )
    return changed.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed, "fleet_digest")}
    )


def projection_request(**changes: object) -> DynamicDevelopmentSubjectProjectionV1:
    payload: dict[str, object] = {
        "expected_configuration_epoch": 1,
        "operation_kind": "create",
        "operation_id": _OPERATION_ID,
        "operation_epoch": 1,
        "environment_name": "alice",
        "subject_id": _SUBJECT_ID,
        "subject_incarnation": _SUBJECT_INCARNATION,
        "owner_id": _OWNER_ID,
        "min_slots": 0,
        "max_slots": 2,
        "candidate_generation": 1,
        "candidate_sha256": "a" * 64,
        "candidate_publication_sha256": "b" * 64,
        "deployment_generation": 1,
        "configuration_generation": 1,
        "demand_reporter_incarnation": _REPORTER_INCARNATION,
        "demand_reporter_token_sha256": "c" * 64,
        "local_activation_sha256": "d" * 64,
        "protected_admission_sha256": "e" * 64,
        "capacity_agent_installation_sha256": "f" * 64,
        "supported_pool_ids": ("gb10", "oldlab"),
        "supported_architectures": ("arm64", "x86_64"),
        "protocol_versions": {
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
        },
    }
    payload.update(changes)
    return DynamicDevelopmentSubjectProjectionV1.model_validate(payload)


def test_development_template_requires_both_exact_physical_profiles() -> None:
    fleet = fleet_with_development_template()
    template = fleet.development_subject_template

    assert template is not None
    assert tuple(profile.pool_id for profile in template.profiles) == ("gb10", "oldlab")

    with pytest.raises(ValidationError, match="gb10 and oldlab"):
        template.model_copy(update={"profiles": template.profiles[:1]}).model_validate(
            template.model_copy(update={"profiles": template.profiles[:1]}).model_dump()
        )


def test_development_template_cannot_redefine_a_pool_generation() -> None:
    fleet = fleet_with_development_template()
    template = fleet.development_subject_template
    assert template is not None
    changed_profile = template.profiles[0].model_copy(update={"pool_generation": 2})
    changed_profile = changed_profile.model_copy(
        update={
            "profile_digest": canonical_digest_excluding(
                changed_profile,
                "profile_digest",
            )
        }
    )
    changed_template = template.model_copy(
        update={"profiles": (changed_profile, template.profiles[1])}
    )
    changed_fleet = fleet.model_copy(
        update={
            "fleet_digest": "f" * 64,
            "development_subject_template": changed_template,
        }
    )
    changed_fleet = changed_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed_fleet, "fleet_digest")}
    )

    with pytest.raises(FleetStateError, match="pool generation"):
        validate_fleet_manifest_digests(changed_fleet)


def test_development_template_cannot_exceed_owner_account_policy() -> None:
    fleet = fleet_with_development_template()
    template = fleet.development_subject_template
    assert template is not None
    oversized = template.model_copy(update={"max_slots_per_subject": 9})

    with pytest.raises(ValidationError, match="owner account maximum"):
        FleetManifestV1.model_validate(
            fleet.model_copy(update={"development_subject_template": oversized}).model_dump()
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("environment_name", "staging", "environment name"),
        ("supported_pool_ids", ("oldlab",), "both physical pools"),
        ("supported_architectures", ("x86_64",), "both architectures"),
        ("protocol_versions", {"capacity-agent": "v1"}, "protocol"),
    ],
)
def test_dynamic_projection_rejects_incomplete_personal_bindings(
    field: str,
    value: object,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        projection_request(**{field: value})


def test_dynamic_projection_is_strict_and_zero_execution_has_no_override() -> None:
    request = projection_request()

    assert request.min_slots == 0
    assert request.max_slots == 2
    assert "executable" not in DynamicDevelopmentSubjectProjectionV1.model_fields
    with pytest.raises(ValidationError, match="extra"):
        DynamicDevelopmentSubjectProjectionV1.model_validate(
            {**request.model_dump(mode="python"), "executable": True}
        )


def test_dynamic_projection_allows_gapped_lifecycle_generations() -> None:
    request = projection_request(
        operation_epoch=7,
        candidate_generation=5,
        deployment_generation=5,
        configuration_generation=7,
    )

    assert request.operation_epoch == 7
    assert request.deployment_generation == 5
    assert request.configuration_generation == 7


def test_capacity_projection_still_binds_one_unchanged_deployment() -> None:
    request = projection_request(operation_kind="capacity")

    assert request.operation_kind == "capacity"
    with pytest.raises(ValidationError, match="candidate generation"):
        projection_request(
            operation_kind="capacity",
            candidate_generation=2,
            deployment_generation=1,
        )


def test_optional_schema_extensions_preserve_preprojection_allocation_bytes() -> None:
    value = allocator_input(
        (allocator_subject(1, account_id="owner-a"),),
        gb10_slots=1,
        oldlab_slots=1,
    )
    encoded = canonical_bytes(value)

    assert b"effective_account_policies" not in encoded
    assert b"development_subject_template" not in encoded


def _input_with_derived_owner_account(*, include_derived: bool) -> AllocationInputV1:
    subject = allocator_subject(
        1,
        account_id="dev-owner-00000000000040008000000000000100",
        pending=(("attempt-a", ("gb10", "oldlab"), ("cpu",)),),
    )
    value = allocator_input((subject,), gb10_slots=1, oldlab_slots=1)
    resolved_subject = value.subjects[0]
    original_account = value.fleet.account_policies[0]
    owner_template = original_account.model_copy(
        update={
            "account_id": "personal-development-owner",
            "kind": "owner_template",
            "owner_id": None,
        }
    )
    fleet = value.fleet.model_copy(
        update={
            "fleet_digest": "f" * 64,
            "account_policies": (owner_template,),
            "development_subject_template": DevelopmentSubjectTemplateV1(
                owner_account_template_id=owner_template.account_id,
                max_slots_per_subject=owner_template.max_slots,
                rollout_surge_slots=0,
                max_pending_slots_per_subject=owner_template.max_pending_slots,
                max_pending_jobs_per_subject=owner_template.max_pending_jobs,
                profiles=resolved_subject.configuration.profiles,
            ),
        }
    )
    fleet = fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
    )
    configuration = value.configuration.model_copy(
        update={
            "fleet": value.configuration.fleet.model_copy(
                update={"digest": canonical_digest(fleet)}
            )
        }
    )
    derived = original_account.model_copy(
        update={
            "account_id": resolved_subject.configuration.account_id,
            "kind": "owner",
            "owner_id": _OWNER_ID,
        }
    )
    payload = value.model_dump(mode="python")
    payload.update(
        fleet=fleet,
        configuration=configuration,
        effective_account_policies=(owner_template, derived) if include_derived else (),
    )
    return AllocationInputV1.model_validate(payload)


def test_allocator_uses_derived_owner_accounts_without_mutating_fleet_manifest() -> None:
    result = allocate_shadow(_input_with_derived_owner_account(include_derived=True))

    assert sum(item.desired_slots for item in result.allocations) == 1
    with pytest.raises(ShadowAllocatorError, match="unknown capacity account"):
        allocate_shadow(_input_with_derived_owner_account(include_derived=False))


def test_allocator_rejects_a_forged_derived_owner_limit() -> None:
    value = _input_with_derived_owner_account(include_derived=True)
    static = next(
        account for account in value.effective_account_policies if account.kind == "owner_template"
    )
    derived = next(
        account for account in value.effective_account_policies if account.kind == "owner"
    )
    forged = derived.model_copy(update={"max_slots": derived.max_slots + 1})

    with pytest.raises(ShadowAllocatorError, match="not derived from fleet policy"):
        allocate_shadow(value.model_copy(update={"effective_account_policies": (static, forged)}))
