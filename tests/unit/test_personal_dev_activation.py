from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
    PersonalDevActivationVerifier,
    load_personal_dev_activation_verifier,
)

_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)


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
    verifier = PersonalDevActivationVerifier(
        keys={"personal-dev-agent-v1": b"k" * 32},
        max_age_seconds=300,
    )
    acknowledgement = _ack()
    signature = verifier.sign(acknowledgement)

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
    verifier = PersonalDevActivationVerifier(
        keys={"personal-dev-agent-v1": b"k" * 32},
        max_age_seconds=300,
    )
    signature = verifier.sign(_ack())
    with pytest.raises(ValueError, match="freshness"):
        verifier.verify(_ack(), signature=signature, now=_NOW + timedelta(seconds=301))
    with pytest.raises(ValueError, match="key"):
        verifier.verify(
            replace(_ack(), agent_key_id="unknown-agent"),
            signature=signature,
            now=_NOW,
        )


def test_activation_key_loader_requires_owner_only_bounded_material(tmp_path) -> None:
    key_file = tmp_path / "activation.key"
    key_file.write_bytes(b"k" * 32 + b"\n")
    key_file.chmod(0o600)
    verifier = load_personal_dev_activation_verifier(
        key_file,
        key_id="personal-dev-agent-v1",
        max_age_seconds=300,
    )
    assert verifier.sign(_ack())

    key_file.chmod(0o644)
    with pytest.raises(RuntimeError, match="owner-only"):
        load_personal_dev_activation_verifier(
            key_file,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )


def test_activation_key_loader_rejects_linked_or_executable_authority(tmp_path) -> None:
    key_file = tmp_path / "activation.key"
    key_file.write_bytes(b"k" * 32)
    key_file.chmod(0o600)
    symlink = tmp_path / "activation-link.key"
    symlink.symlink_to(key_file)
    with pytest.raises(RuntimeError, match="regular owner-only file"):
        load_personal_dev_activation_verifier(
            symlink,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )

    symlink.unlink()
    hardlink = tmp_path / "activation-hardlink.key"
    os.link(key_file, hardlink)
    with pytest.raises(RuntimeError, match="regular owner-only file"):
        load_personal_dev_activation_verifier(
            key_file,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )

    hardlink.unlink()
    key_file.chmod(0o700)
    with pytest.raises(RuntimeError, match="regular owner-only file"):
        load_personal_dev_activation_verifier(
            key_file,
            key_id="personal-dev-agent-v1",
            max_age_seconds=300,
        )
