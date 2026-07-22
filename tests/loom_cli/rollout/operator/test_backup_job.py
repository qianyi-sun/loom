from __future__ import annotations

from datetime import UTC, datetime

import pytest

from loom_cli.rollout.lifecycle_protocol import LifecycleAction, LifecyclePhase
from loom_cli.rollout.operator.backup_job import (
    BackupJobEnvelope,
    BackupJobState,
    PreflightBackupJobEnvelope,
    transition_backup_job,
    validate_job_binding,
)

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)


def _envelope() -> BackupJobEnvelope:
    return BackupJobEnvelope(
        job_id="job-12345678",
        request_id="req-12345678",
        payload_id="payload-12345678",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        preflight_attestation_sha256="c" * 64,
        mutation_epoch=8,
        environment="staging",
        namespace="loom-staging",
        bundle_name="20260719T210000Z-req-12345678",
        created_at=NOW,
    )


def test_job_identity_binds_candidate_epoch_attestation_and_bundle() -> None:
    envelope = _envelope()

    assert envelope.to_dict()["mutation_epoch"] == 8
    assert envelope.to_dict()["candidate_tree"] == "b" * 40
    assert len(envelope.evidence_digest) == 64


def test_preflight_job_binds_assessment_registry_coverage_and_round_trips() -> None:
    envelope = PreflightBackupJobEnvelope(
        job_id="job-12345678",
        request_id="req-12345678",
        payload_id="payload-12345678",
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        preflight_assessment_sha256="c" * 64,
        preflight_registry_sha256="d" * 64,
        preflight_coverage_sha256="e" * 64,
        mutation_epoch=8,
        environment="staging",
        namespace="loom-staging",
        bundle_name="20260719T210000Z-req-12345678",
        created_at=NOW,
    )

    assert PreflightBackupJobEnvelope.from_dict(envelope.to_dict()) == envelope
    assert len(envelope.evidence_digest) == 64


def test_worker_success_uses_shared_lifecycle_and_exact_verified_digests() -> None:
    state = BackupJobState(job_id="job-12345678", request_id="req-12345678")
    state = transition_backup_job(state, LifecycleAction.START_BACKUP, updated_at=NOW)
    state = transition_backup_job(
        state,
        LifecycleAction.VERIFY_BACKUP,
        updated_at=NOW,
        manifest_sha256="d" * 64,
        lease_digest="e" * 64,
        preflight_attestation_sha256="f" * 64,
    )
    state = transition_backup_job(state, LifecycleAction.PUBLISH_LAUNCH, updated_at=NOW)

    assert state.phase is LifecyclePhase.LAUNCH_PENDING
    assert state.sequence == 3
    assert state.manifest_sha256 == "d" * 64
    assert state.lease_digest == "e" * 64
    assert state.preflight_attestation_sha256 == "f" * 64


def test_cancel_is_short_state_transition_and_cannot_publish_launch() -> None:
    state = BackupJobState(job_id="job-12345678", request_id="req-12345678")
    state = transition_backup_job(state, LifecycleAction.START_BACKUP, updated_at=NOW)
    state = transition_backup_job(state, LifecycleAction.REQUEST_CANCEL, updated_at=NOW)

    assert state.phase is LifecyclePhase.BACKUP_CANCEL_REQUESTED
    with pytest.raises(ValueError, match="not authorized"):
        transition_backup_job(state, LifecycleAction.PUBLISH_LAUNCH, updated_at=NOW)


def test_cancel_and_worker_failure_seal_normalized_failure_code() -> None:
    state = BackupJobState(job_id="job-12345678", request_id="req-12345678")
    state = transition_backup_job(state, LifecycleAction.START_BACKUP, updated_at=NOW)
    state = transition_backup_job(state, LifecycleAction.REQUEST_CANCEL, updated_at=NOW)
    state = transition_backup_job(
        state,
        LifecycleAction.SEAL_CANCELLED,
        updated_at=NOW,
        failure_code="backup_cancelled",
    )

    assert state.phase is LifecyclePhase.BACKUP_FAILED
    assert state.failure_code == "backup_cancelled"


def test_verification_without_exact_manifest_lease_and_attestation_fails_closed() -> None:
    state = BackupJobState(job_id="job-12345678", request_id="req-12345678")
    state = transition_backup_job(state, LifecycleAction.START_BACKUP, updated_at=NOW)

    with pytest.raises(ValueError, match="manifest, lease and attestation"):
        transition_backup_job(state, LifecycleAction.VERIFY_BACKUP, updated_at=NOW)


def test_mutable_state_cannot_cross_immutable_job_or_request() -> None:
    state = BackupJobState(job_id="job-other0000", request_id="req-12345678")

    with pytest.raises(ValueError, match="immutable envelope"):
        validate_job_binding(_envelope(), state)
