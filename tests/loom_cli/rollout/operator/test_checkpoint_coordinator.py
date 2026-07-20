from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_cli.rollout.operator.backup import VerifiedBackup
from loom_cli.rollout.operator.backup_job import PreflightBackupJobEnvelope
from loom_cli.rollout.operator.backup_rotation import (
    BackupRetirementRecord,
    begin_candidate,
    fail_candidate,
)
from loom_cli.rollout.operator.checkpoint_coordinator import (
    CheckpointCoordinatorError,
    DetachedCheckpointCoordinator,
)
from loom_cli.rollout.operator.checkpoint_lease import (
    CriticalCheckpointEvidence,
    RestoreVerificationEvidence,
)
from loom_cli.rollout.operator.config import APPROVED_REMOTE_URL
from loom_cli.rollout.operator.model import (
    CallerIdentity,
    CandidateBinding,
    PreflightRequest,
)
from loom_cli.rollout.operator.store import RequestStore

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)
SHA = "a" * 40
TREE = "b" * 40
BASE = "c" * 40
MANIFEST = "d" * 64


class FakeCreator:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = manifest_path
        self.calls: list[tuple[PreflightRequest, datetime | None]] = []

    def create(
        self,
        request: PreflightRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        self.calls.append((request, created_at))
        return VerifiedBackup(self.manifest_path, MANIFEST)


def _request() -> PreflightRequest:
    return PreflightRequest(
        request_id="req-checkpoint1",
        rollout_id="staging-aaaaaaaa",
        caller=CallerIdentity("qianyi", 501),
        candidate=CandidateBinding(
            remote_url=APPROVED_REMOTE_URL,
            target_ref="origin/dev",
            resolved_sha=SHA,
            image_tag="staging-aaaaaaa",
            fetched_at=NOW.isoformat(),
            source_mode="sealed-cumulative",
            resolved_tree=TREE,
            approved_base_sha=BASE,
        ),
        candidate_tree=TREE,
        requested_at=NOW.isoformat(),
        runner_config_sha256="1" * 64,
        preflight_assessment_sha256="2" * 64,
        preflight_registry_sha256="3" * 64,
        preflight_coverage_sha256="4" * 64,
        mutation_epoch=17,
        environment="staging",
        namespace="loom-staging",
    )


def _job(bundle_name: str) -> PreflightBackupJobEnvelope:
    return PreflightBackupJobEnvelope(
        job_id="job-checkpoint01",
        request_id="req-checkpoint1",
        payload_id="payload-checkpoint1",
        candidate_sha=SHA,
        candidate_tree=TREE,
        preflight_assessment_sha256="2" * 64,
        preflight_registry_sha256="3" * 64,
        preflight_coverage_sha256="4" * 64,
        mutation_epoch=17,
        environment="staging",
        namespace="loom-staging",
        bundle_name=bundle_name,
        created_at=NOW,
    )


def _checkpoint(path: Path) -> CriticalCheckpointEvidence:
    return CriticalCheckpointEvidence(
        request_id="req-checkpoint1",
        manifest_path=path,
        manifest_sha256=MANIFEST,
        component_sha256={
            "k8s_secrets": "5" * 64,
            "object_inventory": "6" * 64,
            "postgres": "7" * 64,
        },
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        db_snapshot_identity="pgdump-sha256:" + "7" * 64,
        schema_revision="0067",
        object_inventory_root="8" * 64,
        created_at=NOW,
    )


def _restore(checkpoint: CriticalCheckpointEvidence) -> RestoreVerificationEvidence:
    return RestoreVerificationEvidence(
        verification_id="restore-checkpoint1",
        request_id=checkpoint.request_id,
        checkpoint_evidence_sha256=checkpoint.evidence_digest,
        manifest_sha256=checkpoint.manifest_sha256,
        db_snapshot_identity=checkpoint.db_snapshot_identity,
        object_inventory_root=checkpoint.object_inventory_root,
        mutation_epoch=checkpoint.mutation_epoch,
        schema_revision=checkpoint.schema_revision,
        environment=checkpoint.environment,
        namespace=checkpoint.namespace,
        report_sha256="9" * 64,
        verified_at=NOW + timedelta(minutes=3),
    )


def _coordinator(
    tmp_path: Path,
    *,
    verify_restore=None,
    retired: list[BackupRetirementRecord] | None = None,
) -> tuple[DetachedCheckpointCoordinator, FakeCreator, RequestStore, PreflightBackupJobEnvelope]:
    manifest_path = tmp_path / "20260719T210000Z-req-checkpoint1" / "backup-manifest.json"
    creator = FakeCreator(manifest_path)
    store = RequestStore(tmp_path / "state")
    checkpoint = _checkpoint(manifest_path)
    verifier = verify_restore or (lambda found, _request, _cancelled: _restore(found))
    coordinator = DetachedCheckpointCoordinator(
        creator=creator,
        store=store,
        inspect_checkpoint=lambda _backup, _request: checkpoint,
        verify_restore=verifier,
        publish_attestation=lambda _checkpoint, _lease, _request: "a" * 64,
        now=lambda: NOW + timedelta(minutes=4),
        lease_ttl=timedelta(hours=4),
        retire_payload=(retired.append if retired is not None else None),
    )
    return coordinator, creator, store, _job(manifest_path.parent.name)


def test_detached_checkpoint_promotes_only_after_restore_verified_lease(tmp_path: Path) -> None:
    coordinator, creator, store, job = _coordinator(tmp_path)

    verified = coordinator(_request(), job, lambda: False)

    state = store.read_backup_rotation()
    assert creator.calls == [(_request(), NOW)]
    assert verified.manifest_sha256 == MANIFEST
    assert state.active is not None
    assert state.active.payload_id == job.payload_id
    assert state.active.lease is not None
    assert state.active.lease.evidence_digest == verified.lease_digest
    assert state.candidate is None
    assert store.read_backup_lease(verified.lease_digest) == state.active.lease


def test_detached_checkpoint_accepts_exact_short_lock_reservation(tmp_path: Path) -> None:
    coordinator, creator, store, job = _coordinator(tmp_path)
    current = store.read_backup_rotation()
    reservation = begin_candidate(
        current,
        payload_id=job.payload_id,
        request_id=job.request_id,
        bundle_name=job.bundle_name,
        created_at=job.created_at,
    )
    store.replace_backup_rotation(
        reservation.state,
        expected_generation=current.generation,
    )

    coordinator(_request(), job, lambda: False)

    state = store.read_backup_rotation()
    assert creator.calls == [(_request(), NOW)]
    assert state.active is not None
    assert state.active.payload_id == job.payload_id


def test_restore_failure_preserves_old_active_and_never_publishes_new_lease(
    tmp_path: Path,
) -> None:
    retired: list[BackupRetirementRecord] = []
    coordinator, _creator, store, job = _coordinator(tmp_path, retired=retired)
    coordinator(_request(), job, lambda: False)
    old = store.read_backup_rotation().active
    assert old is not None

    second_path = tmp_path / "20260719T220000Z-req-checkpoint2" / "backup-manifest.json"
    second_creator = FakeCreator(second_path)
    second_request = replace(_request(), request_id="req-checkpoint2")
    second_job = replace(
        job,
        job_id="job-checkpoint02",
        request_id="req-checkpoint2",
        payload_id="payload-checkpoint2",
        bundle_name=second_path.parent.name,
        created_at=NOW + timedelta(hours=1),
    )
    second_checkpoint = replace(
        _checkpoint(second_path),
        request_id="req-checkpoint2",
        created_at=NOW + timedelta(hours=1),
    )

    def fail_restore(*_args: object) -> RestoreVerificationEvidence:
        raise CheckpointCoordinatorError("isolated restore failed")

    failed = DetachedCheckpointCoordinator(
        creator=second_creator,
        store=store,
        inspect_checkpoint=lambda _backup, _request: second_checkpoint,
        verify_restore=fail_restore,
        publish_attestation=lambda _checkpoint, _lease, _request: "a" * 64,
        now=lambda: NOW + timedelta(hours=1, minutes=4),
        lease_ttl=timedelta(hours=4),
        retire_payload=retired.append,
    )
    with pytest.raises(CheckpointCoordinatorError, match="isolated restore"):
        failed(second_request, second_job, lambda: False)

    state = store.read_backup_rotation()
    assert state.active == old
    assert state.candidate is None
    assert [record.payload_id for record in retired] == [second_job.payload_id]


def test_successful_replacement_acknowledges_persisted_old_payload_retirement(
    tmp_path: Path,
) -> None:
    retired: list[BackupRetirementRecord] = []
    coordinator, _creator, store, job = _coordinator(tmp_path, retired=retired)
    coordinator(_request(), job, lambda: False)

    second_path = tmp_path / "20260719T220000Z-req-checkpoint2" / "backup-manifest.json"
    second_request = replace(_request(), request_id="req-checkpoint2")
    second_job = replace(
        job,
        job_id="job-checkpoint02",
        request_id="req-checkpoint2",
        payload_id="payload-checkpoint2",
        bundle_name=second_path.parent.name,
        created_at=NOW + timedelta(hours=1),
    )
    second_checkpoint = replace(
        _checkpoint(second_path),
        request_id="req-checkpoint2",
        created_at=NOW + timedelta(hours=1),
    )
    replacement = DetachedCheckpointCoordinator(
        creator=FakeCreator(second_path),
        store=store,
        inspect_checkpoint=lambda _backup, _request: second_checkpoint,
        verify_restore=lambda found, _request, _cancelled: replace(
            _restore(found),
            verified_at=NOW + timedelta(hours=1, minutes=3),
        ),
        publish_attestation=lambda _checkpoint, _lease, _request: "a" * 64,
        now=lambda: NOW + timedelta(hours=1, minutes=4),
        lease_ttl=timedelta(hours=4),
        retire_payload=retired.append,
    )

    replacement(second_request, second_job, lambda: False)

    state = store.read_backup_rotation()
    assert state.active is not None
    assert state.active.payload_id == second_job.payload_id
    assert state.retirements == ()
    assert state.payload_count == 1
    assert [record.payload_id for record in retired] == [job.payload_id]


def test_failed_latest_retirement_is_kept_until_replacement_activates(
    tmp_path: Path,
) -> None:
    coordinator, _creator, store, job = _coordinator(tmp_path)
    current = store.read_backup_rotation()
    old = begin_candidate(
        current,
        payload_id="payload-failed00",
        request_id="req-failed000",
        bundle_name="20260719T200000Z-req-failed000",
        created_at=NOW - timedelta(hours=1),
    ).state
    store.replace_backup_rotation(old, expected_generation=current.generation)
    old = fail_candidate(
        old,
        payload_id="payload-failed00",
        failure_code="rehearsal_failed",
    ).state
    store.replace_backup_rotation(old, expected_generation=old.generation - 1)
    reserved = begin_candidate(
        old,
        payload_id=job.payload_id,
        request_id=job.request_id,
        bundle_name=job.bundle_name,
        created_at=job.created_at,
    ).state
    store.replace_backup_rotation(reserved, expected_generation=old.generation)
    activated: list[str] = []
    retired: list[str] = []

    def retire(record: BackupRetirementRecord) -> None:
        if not activated:
            raise RuntimeError("legacy latest is still protected")
        retired.append(record.payload_id)

    coordinator.retire_payload = retire
    coordinator.activate_payload = lambda record: activated.append(record.payload_id)

    coordinator(_request(), job, lambda: False)

    state = store.read_backup_rotation()
    assert activated == [job.payload_id]
    assert retired == ["payload-failed00"]
    assert state.active is not None
    assert state.active.payload_id == job.payload_id
    assert state.payload_count == 1
    assert state.retirements == ()


def test_replacement_failure_compacts_only_the_nonlatest_candidate(
    tmp_path: Path,
) -> None:
    def fail_restore(*_args: object) -> RestoreVerificationEvidence:
        raise CheckpointCoordinatorError("isolated restore failed")

    coordinator, _creator, store, job = _coordinator(
        tmp_path,
        verify_restore=fail_restore,
    )
    current = store.read_backup_rotation()
    old = begin_candidate(
        current,
        payload_id="payload-failed00",
        request_id="req-failed000",
        bundle_name="20260719T200000Z-req-failed000",
        created_at=NOW - timedelta(hours=1),
    ).state
    store.replace_backup_rotation(old, expected_generation=current.generation)
    old = fail_candidate(
        old,
        payload_id="payload-failed00",
        failure_code="rehearsal_failed",
    ).state
    store.replace_backup_rotation(old, expected_generation=old.generation - 1)
    reserved = begin_candidate(
        old,
        payload_id=job.payload_id,
        request_id=job.request_id,
        bundle_name=job.bundle_name,
        created_at=job.created_at,
    ).state
    store.replace_backup_rotation(reserved, expected_generation=old.generation)
    retired: list[str] = []

    def retire(record: BackupRetirementRecord) -> None:
        if record.payload_id == "payload-failed00":
            raise RuntimeError("legacy latest is still protected")
        retired.append(record.payload_id)

    coordinator.retire_payload = retire

    with pytest.raises(CheckpointCoordinatorError, match="isolated restore"):
        coordinator(_request(), job, lambda: False)

    state = store.read_backup_rotation()
    assert retired == [job.payload_id]
    assert state.active is None
    assert state.candidate is None
    assert state.payload_count == 1
    assert tuple(record.payload_id for record in state.retirements) == ("payload-failed00",)


def test_binding_drift_and_early_cancel_do_not_reserve_payload(tmp_path: Path) -> None:
    coordinator, creator, store, job = _coordinator(tmp_path)

    with pytest.raises(CheckpointCoordinatorError, match="binding drifted"):
        coordinator(_request(), replace(job, mutation_epoch=18), lambda: False)
    with pytest.raises(CheckpointCoordinatorError, match="cancelled before reservation"):
        coordinator(_request(), job, lambda: True)

    assert creator.calls == []
    assert store.read_backup_rotation().payload_count == 0


def test_cancel_after_manifest_seals_candidate_without_promotion(tmp_path: Path) -> None:
    calls = 0

    def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    coordinator, _creator, store, job = _coordinator(tmp_path)

    with pytest.raises(CheckpointCoordinatorError, match="before restore rehearsal"):
        coordinator(_request(), job, cancelled)

    assert store.read_backup_rotation().active is None
    assert store.read_backup_rotation().candidate is None
