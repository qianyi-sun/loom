from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
    PersonalDevActivationSigner,
    PersonalDevActivationVerifier,
    load_personal_dev_activation_signer,
    load_personal_dev_activation_verifier,
)

_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)


def _keys() -> tuple[bytes, bytes]:
    private = Ed25519PrivateKey.generate()
    return (
        private.private_bytes_raw(),
        private.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ),
    )


def _ack() -> PersonalDevActivationAcknowledgement:
    return PersonalDevActivationAcknowledgement(
        environment_name="alice",
        subject_id=UUID("00000000-0000-0000-0000-000000000001"),
        subject_incarnation=UUID("00000000-0000-0000-0000-000000000002"),
        operation_id=UUID("00000000-0000-0000-0000-000000000003"),
        operation_epoch=5,
        attempt_id=UUID("00000000-0000-0000-0000-000000000004"),
        candidate_id=UUID("00000000-0000-0000-0000-000000000005"),
        candidate_sha="a" * 64,
        deployment_generation=8,
        readiness_evidence_sha256="b" * 64,
        local_activation_sha256="c" * 64,
        agent_key_id="personal-dev-agent-v1",
        observed_at=_NOW,
    )


def test_activation_acknowledgement_is_canonical_signed_and_tamper_evident() -> None:
    private_key, public_key = _keys()
    signer = PersonalDevActivationSigner(
        keys={"personal-dev-agent-v1": private_key},
    )
    verifier = PersonalDevActivationVerifier(
        keys={"personal-dev-agent-v1": public_key},
        max_age_seconds=300,
    )
    acknowledgement = _ack()
    signature = signer.sign(acknowledgement)

    verified = verifier.verify(acknowledgement, signature=signature, now=_NOW)

    assert verified.acknowledgement == acknowledgement
    assert len(verified.payload_sha256) == 64
    assert len(verified.signature_sha256) == 64
    with pytest.raises(ValueError, match="signature"):
        verifier.verify(
            replace(acknowledgement, deployment_generation=9),
            signature=signature,
            now=_NOW,
        )


def test_activation_acknowledgement_rejects_stale_or_unknown_agent_evidence() -> None:
    private_key, public_key = _keys()
    signer = PersonalDevActivationSigner(
        keys={"personal-dev-agent-v1": private_key},
    )
    verifier = PersonalDevActivationVerifier(
        keys={"personal-dev-agent-v1": public_key},
        max_age_seconds=300,
    )
    signature = signer.sign(_ack())
    with pytest.raises(ValueError, match="freshness"):
        verifier.verify(_ack(), signature=signature, now=_NOW + timedelta(seconds=301))
    with pytest.raises(ValueError, match="key"):
        verifier.verify(
            replace(_ack(), agent_key_id="unknown-agent"),
            signature=signature,
            now=_NOW,
        )


def test_activation_key_loaders_separate_public_verification_from_private_signing(
    tmp_path,
) -> None:
    private_key, public_key = _keys()
    public_file = tmp_path / "activation.pub"
    public_file.write_bytes(public_key)
    public_file.chmod(0o644)
    verifier = load_personal_dev_activation_verifier(
        public_file,
        key_id="personal-dev-agent-v1",
        max_age_seconds=300,
    )
    private_file = tmp_path / "activation.key"
    private_file.write_bytes(private_key)
    private_file.chmod(0o600)
    signer = load_personal_dev_activation_signer(
        private_file,
        key_id="personal-dev-agent-v1",
    )
    signature = signer.sign(_ack())
    assert verifier.verify(_ack(), signature=signature, now=_NOW)

    private_file.chmod(0o640)
    with pytest.raises(RuntimeError, match="owner-only"):
        load_personal_dev_activation_signer(
            private_file,
            key_id="personal-dev-agent-v1",
        )


def test_activation_key_loader_rejects_linked_or_executable_authority(tmp_path) -> None:
    _private_key, public_key = _keys()
    key_file = tmp_path / "activation.pub"
    key_file.write_bytes(public_key)
    key_file.chmod(0o600)
    symlink = tmp_path / "activation-link.key"
    symlink.symlink_to(key_file)
    with pytest.raises(RuntimeError, match="available regular file"):
        load_personal_dev_activation_verifier(
            symlink,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )

    symlink.unlink()
    hardlink = tmp_path / "activation-hardlink.key"
    os.link(key_file, hardlink)
    with pytest.raises(RuntimeError, match="regular read-only file"):
        load_personal_dev_activation_verifier(
            key_file,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )

    hardlink.unlink()
    key_file.chmod(0o755)
    with pytest.raises(RuntimeError, match="regular read-only file"):
        load_personal_dev_activation_verifier(
            key_file,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )
