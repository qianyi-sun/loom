"""A caller cannot replace the bundled schema reference with an observed digest."""

from dataclasses import replace

import pytest

from loom.application_schema_inventory import ApplicationSchemaInventory, ApplicationSchemaObject
from loom.application_schema_reference import (
    ApplicationSchemaReferenceError,
    application_schema_profile,
    application_schema_reference,
    require_application_schema_reference,
)


def test_bundled_reference_is_immutable_and_has_no_caller_digest() -> None:
    reference = application_schema_reference()
    assert (reference.application_head, reference.guard_head) == ("0149", "guard_0035")
    # A caller-created alternate value cannot change the module's pinned value.
    alternate = replace(reference, inventory_sha256="e" * 64)
    assert application_schema_reference() == reference
    assert alternate != application_schema_reference()


def test_unknown_reference_profile_is_rejected() -> None:
    with pytest.raises(ApplicationSchemaReferenceError, match="profile is invalid"):
        application_schema_reference(profile="live-snapshot")


@pytest.mark.parametrize(
    "profile",
    [
        "legacy-owner",
        "sealed-owner",
        "staging-readonly-legacy-owner",
        "staging-readonly-sealed-owner",
        "cnpg-staging-legacy-owner",
        "cnpg-staging-sealed-owner",
    ],
)
def test_reference_is_bound_to_postgres_major(profile: str) -> None:
    pg16 = application_schema_reference(profile=profile, postgres_major=16)
    pg17 = application_schema_reference(profile=profile, postgres_major=17)
    assert pg16 == application_schema_reference(profile=profile)
    assert pg16.postgres_major == 16
    assert pg17.postgres_major == 17
    assert pg16.postgres_image != pg17.postgres_image
    assert pg16.inventory_sha256 != pg17.inventory_sha256
    assert pg17.postgres_image == (
        "docker.io/library/postgres@sha256:"
        "304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad"
    )


@pytest.mark.parametrize("ownership", ["legacy-owner", "sealed-owner"])
@pytest.mark.parametrize("acl_profile", ["application-only", "staging-readonly"])
def test_acl_selection_uses_distinct_fixed_pins(ownership, acl_profile):
    selected = application_schema_profile(ownership=ownership, acl_profile=acl_profile)
    assert selected == (
        ownership if acl_profile == "application-only" else "staging-readonly-" + ownership
    )
    if acl_profile == "staging-readonly":
        assert (
            application_schema_reference(profile=selected).inventory_sha256
            != application_schema_reference(profile=ownership).inventory_sha256
        )


@pytest.mark.parametrize("acl_profile", [None, True, "live-snapshot", "f" * 64])
def test_acl_selection_rejects_unknown_profiles(acl_profile):
    with pytest.raises(ApplicationSchemaReferenceError, match="ACL profile is invalid"):
        application_schema_profile(ownership="legacy-owner", acl_profile=acl_profile)


@pytest.mark.parametrize("major", [15, 18, True, 16.0, "17"])
def test_unadmitted_postgres_major_is_rejected(major: object) -> None:
    with pytest.raises(ApplicationSchemaReferenceError, match="PostgreSQL major"):
        application_schema_reference(postgres_major=major)


@pytest.mark.parametrize("postgres_major", [15, 16, 17])
def test_arbitrary_observation_is_not_reference_authority(postgres_major: int) -> None:
    observed = ApplicationSchemaInventory(
        postgres_major,
        (ApplicationSchemaObject("routine", '["public","foreign"]', "f" * 64),),
    )
    with pytest.raises(ApplicationSchemaReferenceError, match="trusted reference"):
        require_application_schema_reference(observed)


@pytest.mark.parametrize("major", [16, 17])
@pytest.mark.parametrize("profile", ["legacy-owner", "sealed-owner", "staging-readonly-legacy-owner", "staging-readonly-sealed-owner"])
def test_supported_baseline_has_independent_revision_pins(major, profile):
    baseline = application_schema_reference(profile=profile, postgres_major=major, revision="0134/guard_0030")
    current = application_schema_reference(profile=profile, postgres_major=major)
    assert (baseline.application_head, baseline.guard_head) == ("0134", "guard_0030")
    assert baseline.inventory_sha256 != current.inventory_sha256
    assert baseline.object_count < current.object_count


@pytest.mark.parametrize("revision", [None, "head", "0134/guard_0035", "0142/guard_0030", "f" * 64])
def test_unreviewed_revision_pair_is_not_reference_authority(revision):
    with pytest.raises(ApplicationSchemaReferenceError, match="revision"):
        application_schema_reference(revision=revision)


@pytest.mark.parametrize("revision", ["0149/guard_0035", "0148/guard_0035", "0147/guard_0035", "0142/guard_0035", "0134/guard_0030"])
@pytest.mark.parametrize("major", [16, 17])
@pytest.mark.parametrize("ownership", ["legacy-owner", "sealed-owner"])
def test_cnpg_locale_is_distinct_from_personal_development(revision, major, ownership):
    profile = application_schema_profile(ownership=ownership, acl_profile="cnpg-staging")
    cnpg = application_schema_reference(profile=profile, postgres_major=major, revision=revision)
    readonly = application_schema_reference(profile="staging-readonly-" + ownership, postgres_major=major, revision=revision)
    assert cnpg.object_count == readonly.object_count
    assert cnpg.inventory_sha256 != readonly.inventory_sha256


@pytest.mark.parametrize("pair", [(None, None), ("0134", "guard_0035"), ("0142", "guard_0030"), ("head", "head")])
def test_checkpoint_selection_refuses_unreviewed_revision_pairs(pair):
    from loom.application_schema_reference import application_schema_revision
    with pytest.raises(ApplicationSchemaReferenceError, match="revision"):
        application_schema_revision(public_revision=pair[0], guard_revision=pair[1])
