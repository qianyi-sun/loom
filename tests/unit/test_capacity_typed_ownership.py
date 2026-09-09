"""Purpose and immutable membership provenance survive signed proof boundaries."""

import base64
import json
from importlib import import_module
from uuid import UUID

import pytest

from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, ConfigurationGenerationRefV1
from loom_capacity_manager.executable_contracts import (
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
)
from loom_capacity_manager.ownership import OwnershipKeyring, sign_executable_ownership
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


def _metadata():
    module = import_module("loom_capacity_manager.typed_ownership_contracts")
    context = launch_context_fixture()
    binding = context.binding.model_copy(update={
        "account_id": f"dev-owner-{UUID(int=51).hex}",
        "candidate": context.binding.candidate.model_copy(update={"algorithm": "git-sha1", "identity": "a" * 40}),
    })
    event = module.PersonalMembershipLaunchReferenceV3(
        namespace_id=UUID(int=50), owner_id=UUID(int=51), revision=7, head_sha256="b" * 64,
        execution_manifest_sha256=binding.execution.execution_manifest_sha256,
    )
    authority = module.ExecutableSubjectAuthorityV3(
        source="personal-membership", purpose="personal-build-worker",
        configuration=ConfigurationGenerationRefV1(
            scope="subject", generation=9, digest="c" * 64,
            subject_id=binding.subject_id, subject_incarnation=binding.subject_incarnation,
        ),
        acknowledgement_sha256="d" * 64, membership=event,
    )
    metadata = module.ExecutableOwnershipMetadataV3(
        binding=binding, subject_authority=authority, launch_profile_sha256="e" * 64,
        controller_authority_sha256=context.profile.controller_authority_sha256,
        trusted_launcher_sha256=context.profile.trusted_launcher_release_sha256,
        slurm_cluster=context.profile.slurm_cluster, submitter_identity=context.profile.submitter,
        association=context.profile.association, submitted_at=context.submitted_at,
    )
    return module, context, metadata


def _proof():
    module, context, metadata = _metadata()
    ownership = import_module("loom_capacity_manager.ownership")
    proof = ownership.sign_typed_executable_ownership(
        context.ownership_key.private_key, signing_key_id=context.ownership_key.signing_key_id, metadata=metadata,
    )
    keyring = OwnershipKeyring({context.ownership_key.signing_key_id: context.ownership_key.private_key.public_key()})
    return module, context, proof, keyring


def test_typed_proof_round_trip_keeps_exact_historical_provenance():
    module, context, proof, keyring = _proof()
    encoded = module.canonical_typed_ownership_bytes(proof)
    restored = module.parse_typed_executable_ownership(encoded)
    assert restored == proof
    assert restored.metadata.subject_authority.membership.revision == 7
    assert restored.metadata.subject_authority.configuration.generation == 9
    assert keyring.verify_typed_executable(restored, expected_public_key_sha256=context.ownership_key.public_key_sha256)
    # Verification is historical authenticity, not a latest-membership/currentness check.
    assert not isinstance(restored, SignedExecutableOwnershipProofV2)
    with pytest.raises(ValueError):
        SignedExecutableOwnershipProofV2.model_validate_json(encoded)


@pytest.mark.parametrize("boundary", (
    "purpose", "configuration_generation", "configuration_digest", "acknowledgement",
    "member_revision", "member_head", "namespace", "profile", "controller", "candidate",
))
def test_signature_commits_every_new_authority_coordinate(boundary):
    _module, context, proof, keyring = _proof()
    metadata = proof.metadata
    authority = metadata.subject_authority
    if boundary == "purpose":
        authority = authority.model_copy(update={"purpose": "application-worker"})
    elif boundary.startswith("configuration_"):
        field = boundary.removeprefix("configuration_")
        authority = authority.model_copy(update={"configuration": authority.configuration.model_copy(update={
            field: 10 if field == "generation" else "f" * 64,
        })})
    elif boundary == "acknowledgement":
        authority = authority.model_copy(update={"acknowledgement_sha256": "f" * 64})
    elif boundary in {"member_revision", "member_head", "namespace"}:
        field, value = {
            "member_revision": ("revision", 8), "member_head": ("head_sha256", "f" * 64),
            "namespace": ("namespace_id", UUID(int=99)),
        }[boundary]
        authority = authority.model_copy(update={"membership": authority.membership.model_copy(update={field: value})})
    elif boundary == "candidate":
        metadata = metadata.model_copy(update={"binding": metadata.binding.model_copy(update={
            "candidate": metadata.binding.candidate.model_copy(update={"publication_sha256": "f" * 64}),
        })})
    else:
        field = "launch_profile_sha256" if boundary == "profile" else "controller_authority_sha256"
        metadata = metadata.model_copy(update={field: "f" * 64})
    tampered = proof.model_copy(update={"metadata": metadata.model_copy(update={"subject_authority": authority})})
    assert not keyring.verify_typed_executable(tampered, expected_public_key_sha256=context.ownership_key.public_key_sha256)


@pytest.mark.parametrize("boundary", ("subject", "incarnation", "owner", "manifest", "release", "build_source", "surge", "nodes"))
def test_signer_rejects_unchecked_cross_bound_metadata(boundary):
    _module, context, metadata = _metadata()
    if boundary in {"subject", "incarnation"}:
        field = "subject_id" if boundary == "subject" else "subject_incarnation"
        metadata = metadata.model_copy(update={"binding": metadata.binding.model_copy(update={field: UUID(int=99)})})
    elif boundary == "owner":
        metadata = metadata.model_copy(update={"binding": metadata.binding.model_copy(update={"account_id": "dev-owner-" + UUID(int=99).hex})})
    elif boundary == "release":
        metadata = metadata.model_copy(update={"trusted_launcher_sha256": "f" * 64})
    elif boundary == "manifest":
        authority = metadata.subject_authority
        event = authority.membership.model_copy(update={"execution_manifest_sha256": "f" * 64})
        metadata = metadata.model_copy(update={"subject_authority": authority.model_copy(update={"membership": event})})
    else:
        binding = metadata.binding
        if boundary == "build_source":
            binding = binding.model_copy(update={"candidate": launch_context_fixture().binding.candidate})
        elif boundary == "surge":
            binding = binding.model_copy(update={"rollout_surge_slots": 1, "old_shape_backing_id": "old-shape"})
        else:
            binding = binding.model_copy(update={"node_ids": ("oldlab-5", "oldlab-6")})
        metadata = metadata.model_copy(update={"binding": binding})
    with pytest.raises(ValueError):
        import_module("loom_capacity_manager.ownership").sign_typed_executable_ownership(
            context.ownership_key.private_key, signing_key_id=context.ownership_key.signing_key_id, metadata=metadata,
        )


@pytest.mark.parametrize("source,purpose,has_event,valid", (
    ("personal-membership", "personal-build-worker", False, False),
    ("personal-membership", "application-worker", False, False),
    ("immutable-base", "personal-build-worker", False, False),
    ("immutable-base", "application-worker", True, False),
    ("immutable-base", "application-worker", False, True),
))
def test_membership_provenance_cannot_be_silently_omitted(source, purpose, has_event, valid):
    module, _context, metadata = _metadata()
    authority = metadata.subject_authority.model_copy(update={
        "source": source, "purpose": purpose, "membership": metadata.subject_authority.membership if has_event else None,
    })
    if valid:
        assert module.ExecutableSubjectAuthorityV3.model_validate_json(authority.model_dump_json()) == authority
    else:
        with pytest.raises(ValueError):
            module.ExecutableSubjectAuthorityV3.model_validate_json(authority.model_dump_json())


def test_old_verifier_rejects_typed_payload_even_with_valid_old_domain_signature():
    _module, context, proof, keyring = _proof()
    forged = proof.model_copy(update={"signature_base64": base64.b64encode(
        context.ownership_key.private_key.sign(canonical_executable_bytes(proof.metadata)),
    ).decode("ascii")})
    assert not keyring.verify_executable(forged, expected_public_key_sha256=context.ownership_key.public_key_sha256)
    assert not keyring.verify_typed_executable(forged, expected_public_key_sha256=context.ownership_key.public_key_sha256)
    with pytest.raises(ValueError):
        sign_executable_ownership(context.ownership_key.private_key, signing_key_id=context.ownership_key.signing_key_id, metadata=proof.metadata)


def test_old_proof_cannot_enter_typed_verifier_and_wrong_keys_fail():
    _module, context, _proof_value, keyring = _proof()
    from loom_capacity_executor.launch_renderer import render_signed_launch
    old = render_signed_launch(context).ownership_proof
    assert not keyring.verify_typed_executable(old, expected_public_key_sha256=context.ownership_key.public_key_sha256)
    assert not keyring.verify_typed_executable(_proof_value, expected_public_key_sha256="f" * 64)


def test_signing_key_id_is_signed_even_when_another_keyring_accepts_its_alias():
    _module, context, proof, _keyring = _proof()
    alias = "oldlab-key-alias"
    keyring = OwnershipKeyring({alias: context.ownership_key.private_key.public_key()})
    changed = proof.model_copy(update={"signing_key_id": alias})
    assert keyring.matches(alias, context.ownership_key.public_key_sha256)
    assert not keyring.verify_typed_executable(changed, expected_public_key_sha256=context.ownership_key.public_key_sha256)


@pytest.mark.parametrize("version", (3.0, "3", True, 2))
def test_new_wire_version_is_exact_at_nested_and_top_levels(version):
    module, context, proof, keyring = _proof()
    for malformed in (proof.model_copy(update={"schema_version": version}), proof.model_copy(update={
        "metadata": proof.metadata.model_copy(update={"schema_version": version}),
    })):
        assert not keyring.verify_typed_executable(malformed, expected_public_key_sha256=context.ownership_key.public_key_sha256)
        with pytest.raises(ValueError):
            module.parse_typed_executable_ownership(malformed.model_dump_json().encode())


def test_parser_rejects_noncanonical_duplicate_and_oversized_bytes():
    module, _context, proof, _keyring = _proof()
    raw = module.canonical_typed_ownership_bytes(proof)
    for malformed in (b" " + raw, raw.replace(b'"schema_version":3', b'"schema_version":3,"schema_version":3', 1),
                      json.dumps(json.loads(raw), indent=2).encode(), b" " * (MAX_CONTRACT_BYTES + 1)):
        with pytest.raises(ValueError):
            module.parse_typed_executable_ownership(malformed)
