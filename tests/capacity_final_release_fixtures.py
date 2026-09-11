"""Small wire fixtures, not proof of a real manager release."""

from datetime import UTC, datetime

from loom_capacity_manager.executable_contracts import (
    ExecutableFinalReleaseWitnessV2,
    ExecutableProtectedReleaseV2,
    ExecutableReleasedShapeV2,
    canonical_executable_digest,
)


def final_release_witness(binding, reporter):
    protected = ExecutableProtectedReleaseV2(binding=binding,
        reporter_incarnation=reporter, bootstrap_registration_epoch=1,
        protected_registration_epoch=2, bootstrap_revoked=True, protected_release_sha256="b" * 64)
    return ExecutableFinalReleaseWitnessV2(
        release=ExecutableReleasedShapeV2(binding=binding, inventory_sequence=1,
            terminal_kind="unused", terminal_identity="unused-shape", terminal_evidence_sha256="a" * 64,
            protected_registration_epoch=2, bootstrap_revoked=True, protected_release_sha256="b" * 64),
        protected_release=protected, protected_acknowledgement_sha256=canonical_executable_digest(protected),
        command_sequence=4, command_request_sha256="c" * 64, released_at=datetime.now(UTC))
