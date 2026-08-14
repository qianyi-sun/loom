from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.lifecycle_protocol import LifecyclePhase
from loom_cli.rollout.operator import worker as worker_module
from loom_cli.rollout.operator.backup import BackupCreator, BackupError
from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.backup_retirement import (
    BackupPayloadActivator,
    BackupPayloadRetirer,
)
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
    BackupRotationState,
    backup_rotation_admission_blockers,
    begin_candidate,
    record_manifest_verified,
)
from loom_cli.rollout.operator.installed_backup_retention import (
    InstalledBackupRecoveryError,
    InstalledBackupRecoveryService,
    InstalledBackupRetentionError,
    InstalledBackupRetentionService,
    converge_verified_backup_candidate,
)
from loom_cli.rollout.operator.model import ActivePointer
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError
from loom_cli.rollout.operator.worker import VerifiedBackupJob, WorkerDependencies, run_backup_job
from tests.loom_cli.rollout.operator.test_backup import make_config
from tests.loom_cli.rollout.operator.test_store import (
    make_assessment,
    make_preflight_backup_job,
    make_preflight_request,
)


def _service(tmp_path: Path) -> tuple[InstalledBackupRetentionService, tuple[Path, Path]]:
    config = replace(
        make_config(tmp_path),
        source_mode="sealed-cumulative",
        source_commit_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )


    store = RequestStore(config.state_root)
    backups = config.rollout_root / "backups"
    backups.mkdir(mode=0o700)
    records: list[BackupRetirementRecord] = []
    roots: list[Path] = []
    for index in (1, 2):
        request_id = f"req-failed000{index}"
        bundle_name = f"20260719T1{index}0000Z-{request_id}"
        root = backups / bundle_name
        root.mkdir(mode=0o700)
        roots.append(root)
        manifest_sha256: str | None = None
        if index == 1:
            manifest = root / "backup-manifest.json"
            manifest.write_bytes(b"restore-verified latest\n")
            manifest.chmod(0o600)
            manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
            store.publish_backup_lease(
                BackupLease(
                    lease_id="lease-failed010000",
                    source_request_id=request_id,
                    manifest_sha256=manifest_sha256,
                    component_sha256={"postgres": "1" * 64},
                    environment="staging",
                    namespace="loom-staging",
                    mutation_epoch=17,
                    db_snapshot_identity="pgdump-sha256:" + "1" * 64,
                    schema_revision="0072",
                    object_inventory_root="2" * 64,
                    created_at=datetime(2026, 7, 19, 11, tzinfo=UTC),
                    restore_verified_at=datetime(2026, 7, 19, 11, 5, tzinfo=UTC),
                    expires_at=datetime(2026, 7, 20, 11, tzinfo=UTC),
                )
            )
        else:
            partial = root / "partial.bin"
            partial.write_bytes(b"partial-2")
            partial.chmod(0o600)
        records.append(
            BackupRetirementRecord(
                payload_id=f"payload-failed0{index}",
                request_id=request_id,
                bundle_name=bundle_name,
                reason="failed",
                manifest_sha256=manifest_sha256,
            )
        )
    state = BackupRotationState(generation=1, retirements=tuple(records))
    store.replace_backup_rotation(state, expected_generation=0)
    (backups / "latest").symlink_to(roots[0].name)
    creator = BackupCreator(config, service_uid=os.geteuid())
    return (
        InstalledBackupRetentionService(
            config=config,
            service_uid=os.geteuid(),
            store=store,
            retirer=BackupPayloadRetirer(creator=creator, store=store),
            activate_payload=lambda _record: None,
        ),
        (roots[0], roots[1]),
    )


def _verified_candidate_recovery_service(
    tmp_path: Path,
) -> tuple[InstalledBackupRecoveryService, BackupPayloadRecord, BackupPayloadRecord, list[str]]:
    config = replace(
        make_config(tmp_path),
        source_mode="sealed-cumulative",
        source_commit_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    store = RequestStore(config.state_root)
    assessment = make_assessment(tmp_path)
    request_id = "req-20260713-abcdef12"
    request = replace(
        make_preflight_request(),
        request_id=request_id,
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    job = replace(
        make_preflight_backup_job(),
        request_id=request_id,
        bundle_name=f"20260713T200000Z-{request_id}",
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    store.create_preflight_request(request)
    store.publish_preflight_assessment(request.request_id, assessment)
    store.publish_preflight_backup_job(job)

    old_lease = BackupLease(
        lease_id="lease-old0000000",
        source_request_id="req-old000000",
        manifest_sha256="1" * 64,
        component_sha256={"postgres": "2" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=3,
        db_snapshot_identity="pgdump-sha256:" + "3" * 64,
        schema_revision="0092",
        object_inventory_root="4" * 64,
        created_at=datetime(2026, 7, 13, 19, tzinfo=UTC),
        restore_verified_at=datetime(2026, 7, 13, 19, 5, tzinfo=UTC),
        expires_at=datetime(2026, 7, 14, 19, tzinfo=UTC),
    )
    old_active = BackupPayloadRecord(
        payload_id="payload-old00000",
        request_id=old_lease.source_request_id,
        bundle_name="20260713T190000Z-req-old000000",
        phase=BackupPayloadPhase.ACTIVE,
        created_at=old_lease.created_at,
        manifest_sha256=old_lease.manifest_sha256,
        lease=old_lease,
    )
    store.publish_backup_lease(old_lease)
    active_state = BackupRotationState(generation=1, active=old_active)
    store.replace_backup_rotation(active_state, expected_generation=0)
    reserved = begin_candidate(
        active_state,
        payload_id=job.payload_id,
        request_id=job.request_id,
        bundle_name=job.bundle_name,
        created_at=job.created_at,
    ).state
    store.replace_backup_rotation(reserved, expected_generation=active_state.generation)

    manifest_sha256 = "d" * 64
    manifested = record_manifest_verified(
        reserved,
        payload_id=job.payload_id,
        manifest_sha256=manifest_sha256,
    ).state
    store.replace_backup_rotation(manifested, expected_generation=reserved.generation)
    lease = BackupLease(
        lease_id="lease-candidate01",
        source_request_id=job.request_id,
        manifest_sha256=manifest_sha256,
        component_sha256={"postgres": "5" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=job.mutation_epoch,
        db_snapshot_identity="pgdump-sha256:" + "6" * 64,
        schema_revision="0092",
        object_inventory_root="7" * 64,
        created_at=job.created_at,
        restore_verified_at=datetime(2026, 7, 13, 20, 5, tzinfo=UTC),
        expires_at=datetime(2026, 7, 14, 20, tzinfo=UTC),
    )
    store.publish_backup_lease(lease)

    class LaunchLifecycle:
        def launch(self, envelope):  # type: ignore[no-untyped-def]
            return ActivePointer(
                request_id=envelope.request_id,
                attempt_number=envelope.attempt_number,
                unit_name=f"loom-staging-rollout-{envelope.request_id}-1.service",
                status="pending",
            )

    verified = VerifiedBackupJob(
        manifest_path=tmp_path / job.bundle_name / "backup-manifest.json",
        manifest_sha256=manifest_sha256,
        lease_digest=lease.evidence_digest,
        preflight_attestation_sha256="f" * 64,
    )
    dependencies = WorkerDependencies(
        store=store,
        lifecycle=LaunchLifecycle(),
        run_driver=lambda _path, _resume: 0,
        run_backup=lambda _request, _job, _cancelled: verified,
        finalize_backup=lambda found, result: worker_module._finalize_verified_backup(  # type: ignore[attr-defined]
            config,
            store,
            found,
            result,
        ),
        now=lambda: "2026-07-13T20:06:00Z",
        stderr=SimpleNamespace(write=lambda _value: None),
    )
    assert run_backup_job(job, dependencies) == 0
    assert store.read_preflight_backup_job_state(job.request_id).phase is LifecyclePhase.LAUNCH_RUNNING
    assert store.read_backup_rotation() == manifested

    activated: list[str] = []
    service = InstalledBackupRecoveryService(
        config=config,
        service_uid=os.geteuid(),
        store=store,
        activate_payload=lambda record: activated.append(record.payload_id),
    )
    candidate = manifested.candidate
    assert candidate is not None
    recovered = replace(
        candidate,
        phase=BackupPayloadPhase.ACTIVE,
        lease=lease,
    )
    return service, old_active, recovered, activated


def test_verified_candidate_recovery_promotes_without_deleting_payloads(
    tmp_path: Path,
) -> None:
    service, old_active, recovered, activated = _verified_candidate_recovery_service(tmp_path)

    plan = service.inventory()
    result = service.apply(service.load_claim(plan.plan_digest))
    replayed = service.apply(service.load_claim(plan.plan_digest))

    assert plan.candidate_before.phase is BackupPayloadPhase.MANIFEST_VERIFIED
    assert plan.recovered_active == recovered
    assert plan.previous_active == old_active
    assert result == replayed
    assert result["recovered_payload_id"] == recovered.payload_id
    assert result["queued_retirement_payload_ids"] == [old_active.payload_id]
    state = service.store.read_backup_rotation()
    assert state.active == recovered
    assert state.candidate is None
    assert tuple(record.payload_id for record in state.retirements) == (
        old_active.payload_id,
    )
    assert activated == [recovered.payload_id, recovered.payload_id]
    assert service.store.read_backup_retention_claim() is None
    assert not service.store.has_backup_retirement_receipt(old_active.payload_id)


def test_worker_convergence_promotes_verified_candidate_before_launch(tmp_path: Path) -> None:
    service, old_active, recovered, activated = _verified_candidate_recovery_service(tmp_path)
    plan = service.inventory()

    result = converge_verified_backup_candidate(
        service.store,
        request_id=plan.candidate_before.request_id,
        payload_id=plan.candidate_before.payload_id,
        bundle_name=plan.candidate_before.bundle_name,
        manifest_sha256=plan.candidate_before.manifest_sha256 or "",
        lease_digest=plan.lease_digest,
        activate_payload=service.activate_payload,
    )

    assert result == recovered
    state = service.store.read_backup_rotation()
    assert state.active == recovered
    assert state.candidate is None
    assert tuple(record.payload_id for record in state.retirements) == (
        old_active.payload_id,
    )
    assert activated == [recovered.payload_id]


def test_verified_candidate_recovery_rejects_cross_ledger_attestation_drift(
    tmp_path: Path,
) -> None:
    service, _old_active, _recovered, activated = _verified_candidate_recovery_service(tmp_path)
    request_id = service.inventory().candidate_before.request_id
    attempt = service.store.read_attempt_envelope(request_id, 1)
    attempt_path = (
        service.store.requests_root
        / attempt.request_id
        / "attempts"
        / "1"
        / "envelope.json"
    )
    payload = attempt.to_dict()
    payload["preflight_attestation_sha256"] = "0" * 64
    attempt_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    attempt_path.chmod(0o600)

    with pytest.raises(InstalledBackupRecoveryError, match="attempt authority drifted"):
        service.inventory()

    state = service.store.read_backup_rotation()
    assert state.candidate is not None
    assert state.candidate.phase is BackupPayloadPhase.MANIFEST_VERIFIED
    assert activated == []


def test_verified_candidate_recovery_plan_binds_entire_attempt_envelope(
    tmp_path: Path,
) -> None:
    service, _old_active, _recovered, activated = _verified_candidate_recovery_service(tmp_path)
    plan = service.inventory()
    attempt = service.store.read_attempt_envelope(plan.candidate_before.request_id, 1)
    attempt_path = (
        service.store.requests_root
        / attempt.request_id
        / "attempts"
        / "1"
        / "envelope.json"
    )
    payload = attempt.to_dict()
    payload["attempt_uid"] = 2003
    attempt_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
    attempt_path.chmod(0o600)

    with pytest.raises(InstalledBackupRecoveryError, match="job authority drifted"):
        service.load_claim(plan.plan_digest)

    assert activated == []


def test_verified_candidate_recovery_resumes_after_promotion_before_activation(
    tmp_path: Path,
) -> None:
    service, old_active, recovered, activated = _verified_candidate_recovery_service(tmp_path)
    fail_once = True

    def activate(record: BackupPayloadRecord) -> None:
        nonlocal fail_once
        activated.append(record.payload_id)
        if fail_once:
            fail_once = False
            raise RuntimeError("activation interrupted")

    service.activate_payload = activate
    plan = service.inventory()
    loaded = service.load_claim(plan.plan_digest)

    with pytest.raises(InstalledBackupRecoveryError, match="activation did not complete"):
        service.apply(loaded)

    stranded = service.store.read_backup_rotation()
    assert stranded.active == recovered
    assert stranded.candidate is None
    assert tuple(record.payload_id for record in stranded.retirements) == (
        old_active.payload_id,
    )
    assert service.store.read_backup_retention_claim() == (plan.plan_digest, ())

    result = service.apply(service.load_claim(plan.plan_digest))

    assert result["recovered_payload_id"] == recovered.payload_id
    assert activated == [recovered.payload_id, recovered.payload_id]
    assert service.store.read_backup_retention_claim() is None


def _stranded_activation_service(
    tmp_path: Path,
    *,
    activate_payload,
    include_retirement: bool = True,
) -> tuple[InstalledBackupRetentionService, Path, Path, Path]:
    config = replace(
        make_config(tmp_path),
        source_mode="sealed-cumulative",
        source_commit_sha="a" * 40,
        source_tree_sha="b" * 40,
        source_base_sha="c" * 40,
    )
    store = RequestStore(config.state_root)
    backups = config.rollout_root / "backups"
    backups.mkdir(mode=0o700)
    old_bundle = "20260719T110000Z-req-old000000"
    new_bundle = "20260719T120000Z-req-new000000"
    roots: list[Path] = []
    digests: list[str] = []
    for bundle in (old_bundle, new_bundle):
        root = backups / bundle
        root.mkdir(mode=0o700)
        manifest = root / "backup-manifest.json"
        manifest.write_bytes((bundle + "\n").encode())
        manifest.chmod(0o600)
        roots.append(root)
        digests.append(hashlib.sha256(manifest.read_bytes()).hexdigest())
    lease = BackupLease(
        lease_id="lease-new0000000",
        source_request_id="req-new000000",
        manifest_sha256=digests[1],
        component_sha256={"postgres": "1" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        db_snapshot_identity="pgdump-sha256:" + "1" * 64,
        schema_revision="0072",
        object_inventory_root="2" * 64,
        created_at=datetime(2026, 7, 19, 12, tzinfo=UTC),
        restore_verified_at=datetime(2026, 7, 19, 12, 5, tzinfo=UTC),
        expires_at=datetime(2026, 7, 20, 12, tzinfo=UTC),
    )
    store.publish_backup_lease(lease)
    state = BackupRotationState(
        generation=1,
        active=BackupPayloadRecord(
            payload_id="payload-new00000",
            request_id="req-new000000",
            bundle_name=new_bundle,
            phase=BackupPayloadPhase.ACTIVE,
            created_at=datetime(2026, 7, 19, 12, tzinfo=UTC),
            manifest_sha256=digests[1],
            lease=lease,
        ),
        retirements=(
            (
                BackupRetirementRecord(
                    payload_id="payload-old00000",
                    request_id="req-old000000",
                    bundle_name=old_bundle,
                    reason="superseded",
                    manifest_sha256=digests[0],
                ),
            )
            if include_retirement
            else ()
        ),
    )
    store.replace_backup_rotation(state, expected_generation=0)
    latest = backups / "latest"
    if include_retirement:
        latest.symlink_to(old_bundle)
    creator = BackupCreator(config, service_uid=os.geteuid())
    return (
        InstalledBackupRetentionService(
            config=config,
            service_uid=os.geteuid(),
            store=store,
            retirer=BackupPayloadRetirer(creator=creator, store=store),
            activate_payload=activate_payload,
        ),
        latest,
        roots[0],
        roots[1],
    )


def test_digest_approved_rotation_retirement_preserves_compact_evidence(
    tmp_path: Path,
) -> None:
    service, roots = _service(tmp_path)

    plan = service.inventory()
    loaded = service.load_claim(plan.plan_digest)
    result = service.apply(loaded)
    retried = service.apply(loaded)

    assert loaded == plan
    assert plan.to_dict()["schema_version"] == 3
    assert plan.active_action == "recover-and-verify"
    assert plan.recovered_active is not None
    assert plan.recovered_active.payload_id == "payload-failed01"
    assert plan.desired_latest_bundle == roots[0].name
    assert result["recovered_payload_id"] == "payload-failed01"
    assert result["retired_payload_ids"] == ["payload-failed02"]
    assert result["retained_payload_ids"] == []
    assert retried == result
    assert roots[0].is_dir()
    assert not roots[1].exists()
    rotation = service.store.read_backup_rotation()
    assert rotation.payload_count == 1
    assert rotation.active is not None
    assert rotation.active.payload_id == "payload-failed01"
    assert rotation.retirements == ()
    assert not service.store.has_backup_retirement_receipt("payload-failed01")
    assert service.store.has_backup_retirement_receipt("payload-failed02")
    evidence = service.store.backup_retirements_root / "payload-failed02.json"
    receipt = service.store.backup_retirements_root / "payload-failed02.deleted.json"
    assert evidence.is_file()
    assert receipt.is_file()


def test_latest_failed_retirement_recovery_requires_one_exact_immutable_lease(
    tmp_path: Path,
) -> None:
    service, roots = _service(tmp_path)
    lease_paths = tuple(service.store.backup_leases_root.glob("*.json"))
    assert len(lease_paths) == 1
    lease_paths[0].unlink()

    with pytest.raises(
        InstalledBackupRetentionError,
        match="lease authority is unavailable",
    ):
        service.inventory()

    assert all(root.is_dir() for root in roots)
    state = service.store.read_backup_rotation()
    assert state.active is None
    assert state.payload_count == 2


def test_latest_failed_retirement_recovery_resumes_after_rotation_cas(
    tmp_path: Path,
) -> None:
    service, roots = _service(tmp_path)
    plan = service.inventory()
    original_retirer = service.retirer

    def stop_before_incomplete_delete(record: BackupRetirementRecord) -> None:
        raise RuntimeError(f"stop before deleting {record.payload_id}")

    service.retirer = stop_before_incomplete_delete  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="payload-failed02"):
        service.apply(plan)

    interrupted = service.store.read_backup_rotation()
    assert interrupted.active == plan.recovered_active
    assert tuple(record.payload_id for record in interrupted.retirements) == ("payload-failed02",)
    assert all(root.is_dir() for root in roots)
    assert service.store.read_backup_retention_claim() is not None

    service.retirer = original_retirer
    result = service.apply(service.load_claim(plan.plan_digest))

    assert result["recovered_payload_id"] == "payload-failed01"
    assert result["retired_payload_ids"] == ["payload-failed02"]
    assert service.store.read_backup_rotation().payload_count == 1
    assert roots[0].is_dir() and not roots[1].exists()


def test_same_retention_claim_cannot_execute_concurrently(tmp_path: Path) -> None:
    service, roots = _service(tmp_path)
    plan = service.inventory()
    service.claim(plan)

    with service._execution_guard():
        with pytest.raises(InstalledBackupRetentionError, match="already running"):
            service.apply(plan)

    assert all(root.is_dir() for root in roots)
    assert service.store.read_backup_retention_claim() is not None
    result = service.apply(plan)
    assert result["retired_payload_ids"] == ["payload-failed02"]
    assert service.store.read_backup_retention_claim() is None


def test_retention_clear_cannot_leave_a_reclaimed_stale_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()
    original_clear = service.store.clear_backup_retention_claim
    competing_errors: list[str] = []
    attempted = False

    def clear_then_compete(digest: str) -> bool:
        nonlocal attempted
        cleared = original_clear(digest)
        if not attempted:
            attempted = True
            try:
                service.apply(plan)
            except InstalledBackupRetentionError as exc:
                competing_errors.append(str(exc))
        return cleared

    monkeypatch.setattr(service.store, "clear_backup_retention_claim", clear_then_compete)

    result = service.apply(plan)

    assert result["retired_payload_ids"] == ["payload-failed02"]
    assert competing_errors == ["backup retention execution is already running"]
    assert service.store.read_backup_retention_claim() is None


def test_retention_rejects_unbound_applied_evidence_without_deleting(
    tmp_path: Path,
) -> None:
    service, roots = _service(tmp_path)
    plan = service.inventory()
    applied = service.evidence_root / f"{plan.plan_digest}.applied.json"
    applied.write_text('{"bogus":true}\n')
    applied.chmod(0o600)

    with pytest.raises(InstalledBackupRetentionError, match="applied evidence is invalid"):
        service.apply(plan)

    assert all(root.is_dir() for root in roots)
    assert service.store.read_backup_retention_claim() is not None


def test_retention_execution_lock_rejects_hardlink_alias(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    with service._execution_guard():
        pass
    lock = service.evidence_root / ".apply.lock"
    os.link(lock, service.evidence_root / ".apply.lock.alias")

    with pytest.raises(InstalledBackupRetentionError, match="execution lock is unsafe"):
        with service._execution_guard():
            pass


def test_rotation_retirement_rejects_unapproved_or_drifted_claim(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()

    with pytest.raises(InstalledBackupRetentionError, match="approval"):
        service.load_claim("0" * 64)

    current = service.store.read_backup_rotation()
    drifted = BackupRotationState(
        generation=current.generation + 1,
        retirements=current.retirements[:-1],
    )
    service.store.replace_backup_rotation(drifted, expected_generation=current.generation)

    with pytest.raises(InstalledBackupRetentionError, match="receipt"):
        service.load_claim(plan.plan_digest)


def test_rotation_retention_rejects_generation_rollback(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()
    current = service.store.read_backup_rotation()
    rolled_back = replace(current, generation=plan.rotation_generation - 1)

    with pytest.raises(InstalledBackupRetentionError, match="generation or digest drifted"):
        service._validate_current(plan, state=rolled_back)


def test_rotation_retention_rejects_unproven_forward_generation(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()
    current = service.store.read_backup_rotation()
    jumped = replace(current, generation=plan.rotation_generation + 99)

    with pytest.raises(InstalledBackupRetentionError, match="generation or digest drifted"):
        service._validate_current(plan, state=jumped)


def test_rotation_retention_rejects_legacy_deletion_only_claim(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()
    legacy = plan.to_dict()
    legacy.pop("active_action")
    legacy.pop("desired_latest_bundle")
    legacy["schema_version"] = 1
    payload = json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    path = service.evidence_root / f"{digest}.plan.json"
    path.write_bytes(payload)
    path.chmod(0o600)

    with pytest.raises(InstalledBackupRetentionError, match="approval is unavailable"):
        service.load_claim(digest)


def test_rotation_retirement_rejects_latest_drift_after_inventory(tmp_path: Path) -> None:
    service, roots = _service(tmp_path)
    plan = service.inventory()
    latest = service.config.rollout_root / "backups" / "latest"

    latest.unlink()
    latest.symlink_to(roots[1].name)

    with pytest.raises(InstalledBackupRetentionError, match="authority drifted"):
        service.load_claim(plan.plan_digest)


@pytest.mark.parametrize("target", ["/tmp/backup", "../backup", "unknown-backup"])
def test_rotation_retirement_rejects_unsafe_or_unknown_latest(
    tmp_path: Path,
    target: str,
) -> None:
    service, _roots = _service(tmp_path)
    latest = service.config.rollout_root / "backups" / "latest"
    latest.unlink()
    latest.symlink_to(target)

    with pytest.raises(InstalledBackupRetentionError, match="outside rotation"):
        service.inventory()


def test_rotation_retirement_rejects_candidate_before_inventory(tmp_path: Path) -> None:
    service, _roots = _service(tmp_path)
    current = service.store.read_backup_rotation()
    reduced = BackupRotationState(
        generation=current.generation + 1,
        retirements=current.retirements[:1],
    )
    service.store.replace_backup_rotation(reduced, expected_generation=current.generation)
    reserved = begin_candidate(
        reduced,
        payload_id="payload-candidate1",
        request_id="req-candidate1",
        bundle_name="20260719T140000Z-req-candidate1",
        created_at=datetime(2026, 7, 19, 14, tzinfo=UTC),
    )
    service.store.replace_backup_rotation(
        reserved.state,
        expected_generation=reduced.generation,
    )

    with pytest.raises(InstalledBackupRetentionError, match="candidate"):
        service.inventory()


def test_retention_reconciles_stranded_activation_before_clearing_admission(
    tmp_path: Path,
) -> None:
    activation_attempts: list[str] = []
    fail_first = True
    latest_holder: list[Path] = []

    def activate(record: BackupPayloadRecord) -> None:
        nonlocal fail_first
        activation_attempts.append(record.payload_id)
        if fail_first:
            fail_first = False
            raise RuntimeError("latest activation failed")
        latest = latest_holder[0]
        latest.unlink()
        latest.symlink_to(record.bundle_name)

    service, latest, old_root, new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=activate,
    )
    latest_holder.append(latest)
    plan = service.inventory()
    assert plan.active_action == "activate-and-verify"
    assert plan.desired_latest_bundle == new_root.name
    loaded = service.load_claim(plan.plan_digest)

    with pytest.raises(InstalledBackupRetentionError, match="activation did not complete"):
        service.apply(loaded)

    failed = service.store.read_backup_rotation()
    assert latest.readlink() == Path(old_root.name)
    assert old_root.is_dir() and new_root.is_dir()
    assert tuple(record.payload_id for record in failed.retirements) == ("payload-old00000",)
    assert not service.store.has_backup_retirement_receipt("payload-old00000")
    assert backup_rotation_admission_blockers(failed) == {"transient-limit": "reached"}
    assert service.store.read_backup_retention_claim() == (
        plan.plan_digest,
        ("payload-old00000",),
    )
    with pytest.raises(RequestStoreError, match="retention maintenance"):
        service.store.set_active(ActivePointer("req-blocked", 1, "unit-blocked", "pending"))

    restarted = replace(service, activate_payload=activate)
    result = restarted.apply(restarted.load_claim(plan.plan_digest))
    retried = restarted.apply(restarted.load_claim(plan.plan_digest))

    recovered = restarted.store.read_backup_rotation()
    assert activation_attempts == [
        "payload-new00000",
        "payload-new00000",
        "payload-new00000",
    ]
    assert result == retried
    assert result["retired_payload_ids"] == ["payload-old00000"]
    assert latest.readlink() == Path(new_root.name)
    assert not old_root.exists() and new_root.is_dir()
    assert recovered.retirements == ()
    assert recovered.payload_count == 1
    assert backup_rotation_admission_blockers(recovered) == {}
    assert restarted.store.has_backup_retirement_receipt("payload-old00000")
    assert restarted.store.read_backup_retention_claim() is None


def test_retention_revalidates_already_active_payload_before_retirement(tmp_path: Path) -> None:
    activation_attempts: list[str] = []
    service, latest, old_root, new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=lambda record: activation_attempts.append(record.payload_id),
    )
    plan = service.inventory()

    latest.unlink()
    latest.symlink_to(new_root.name)

    loaded_after_restart = service.load_claim(plan.plan_digest)
    result = service.apply(loaded_after_restart)

    assert result["retired_payload_ids"] == ["payload-old00000"]
    assert activation_attempts == ["payload-new00000"]
    assert latest.readlink() == Path(new_root.name)
    assert not old_root.exists()
    assert service.store.read_backup_rotation().payload_count == 1


def test_retention_does_not_delete_old_when_current_active_payload_is_corrupt(
    tmp_path: Path,
) -> None:
    service, latest, old_root, new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=lambda _record: None,
    )
    latest.unlink()
    latest.symlink_to(new_root.name)
    plan = service.inventory()
    (new_root / "backup-manifest.json").write_bytes(b"corrupt\n")
    (new_root / "backup-manifest.json").chmod(0o600)
    service.activate_payload = BackupPayloadActivator(
        creator=service.retirer.creator,
        enforce_freshness=False,
    )

    with pytest.raises(InstalledBackupRetentionError, match="activation did not complete"):
        service.apply(service.load_claim(plan.plan_digest))

    state = service.store.read_backup_rotation()
    assert latest.readlink() == Path(new_root.name)
    assert old_root.is_dir() and new_root.is_dir()
    assert tuple(record.payload_id for record in state.retirements) == ("payload-old00000",)
    assert not service.store.has_backup_retirement_receipt("payload-old00000")
    assert service.store.read_backup_retention_claim() is not None


def test_retention_rejects_latest_regression_after_activation(tmp_path: Path) -> None:
    service, latest, old_root, new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=lambda record: (
            latest.unlink(),
            latest.symlink_to(record.bundle_name),
        ),
    )

    def regress_latest(_record: BackupRetirementRecord) -> None:
        latest.unlink()
        latest.symlink_to(old_root.name)

    service.retirer = regress_latest  # type: ignore[assignment]
    plan = service.inventory()

    with pytest.raises(InstalledBackupRetentionError, match="latest pointer drifted"):
        service.apply(plan)

    assert latest.readlink() == Path(old_root.name)
    assert old_root.is_dir() and new_root.is_dir()
    assert not service.store.has_backup_retirement_receipt("payload-old00000")
    assert service.store.read_backup_retention_claim() is not None


@pytest.mark.parametrize(
    "phase",
    [
        LifecyclePhase.BACKUP_PENDING,
        LifecyclePhase.BACKUP_RUNNING,
        LifecyclePhase.BACKUP_CANCEL_REQUESTED,
        LifecyclePhase.BACKUP_VERIFIED,
        LifecyclePhase.LAUNCH_PENDING,
    ],
)
def test_retention_inventory_rejects_nonterminal_detached_backup_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: LifecyclePhase,
) -> None:
    service, _latest, _old_root, _new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=lambda _record: None,
    )
    active = service.store.read_backup_rotation().active
    assert active is not None
    monkeypatch.setattr(
        service.store,
        "read_preflight_backup_job",
        lambda _request_id: SimpleNamespace(
            payload_id=active.payload_id,
            bundle_name=active.bundle_name,
        ),
    )
    monkeypatch.setattr(
        service.store,
        "read_preflight_backup_job_state",
        lambda _request_id: SimpleNamespace(phase=phase),
    )

    with pytest.raises(InstalledBackupRetentionError, match="nonterminal detached backup"):
        service.inventory()


def test_retention_rejects_existing_active_envelope_with_missing_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _latest, _old_root, _new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=lambda _record: None,
    )
    active = service.store.read_backup_rotation().active
    assert active is not None
    monkeypatch.setattr(
        service.store,
        "read_preflight_backup_job",
        lambda _request_id: SimpleNamespace(
            payload_id=active.payload_id,
            bundle_name=active.bundle_name,
        ),
    )

    def missing_state(_request_id: str) -> object:
        raise RequestStoreError("preflight backup job state does not exist")

    monkeypatch.setattr(service.store, "read_preflight_backup_job_state", missing_state)

    with pytest.raises(InstalledBackupRetentionError, match="authority is unreadable"):
        service.inventory()


def test_retention_recovers_first_active_when_latest_was_never_published(tmp_path: Path) -> None:
    latest_holder: list[Path] = []
    activation_attempts: list[str] = []
    fail_first = True

    def activate(record: BackupPayloadRecord) -> None:
        nonlocal fail_first
        activation_attempts.append(record.payload_id)
        if fail_first:
            fail_first = False
            raise RuntimeError("latest activation failed")
        latest_holder[0].unlink(missing_ok=True)
        latest_holder[0].symlink_to(record.bundle_name)

    service, latest, old_root, new_root = _stranded_activation_service(
        tmp_path,
        activate_payload=activate,
        include_retirement=False,
    )
    latest_holder.append(latest)

    plan = service.inventory()
    loaded = service.load_claim(plan.plan_digest)
    with pytest.raises(InstalledBackupRetentionError, match="activation did not complete"):
        service.apply(loaded)

    failed = service.store.read_backup_rotation()
    assert not latest.exists()
    assert failed.active is not None
    assert failed.active.payload_id == "payload-new00000"
    assert failed.retirements == ()

    restarted = replace(service, activate_payload=activate)
    result = restarted.apply(restarted.load_claim(plan.plan_digest))
    latest.unlink()
    retried = restarted.apply(restarted.load_claim(plan.plan_digest))

    assert result == retried
    assert result["retired_payload_ids"] == []
    assert result["retained_payload_ids"] == []
    assert activation_attempts == [
        "payload-new00000",
        "payload-new00000",
        "payload-new00000",
    ]
    assert latest.readlink() == Path(new_root.name)
    assert old_root.is_dir() and new_root.is_dir()
    assert service.store.read_backup_rotation().payload_count == 1
    assert backup_rotation_admission_blockers(service.store.read_backup_rotation()) == {}


def test_applied_replay_removes_exact_recreated_retired_payload(tmp_path: Path) -> None:
    service, roots = _service(tmp_path)
    plan = service.inventory()
    result = service.apply(plan)
    recreated = roots[1]
    recreated.mkdir(mode=0o700)
    partial = recreated / "partial.bin"
    partial.write_bytes(b"recreated")
    partial.chmod(0o600)

    replayed = service.apply(service.load_claim(plan.plan_digest))

    assert replayed == result
    assert not recreated.exists()
    assert service.store.read_backup_retention_claim() is None


def test_applied_replay_fails_closed_on_mismatched_recreated_payload(tmp_path: Path) -> None:
    service, roots = _service(tmp_path)
    plan = service.inventory()
    service.apply(plan)
    recreated = roots[1]
    recreated.mkdir(mode=0o700)
    manifest = recreated / "backup-manifest.json"
    manifest.write_bytes(b"unexpected manifest\n")
    manifest.chmod(0o600)

    with pytest.raises(BackupError, match="backup_cleanup_failed"):
        service.apply(service.load_claim(plan.plan_digest))

    assert recreated.is_dir()
    assert service.store.read_backup_retention_claim() is not None


def test_applied_replay_rejects_self_consistent_wrong_retirement_evidence(
    tmp_path: Path,
) -> None:
    service, _roots = _service(tmp_path)
    plan = service.inventory()
    service.apply(plan)
    payload_id = "payload-failed02"
    wrong_record = replace(plan.retirements[1], request_id="req-wrong00000")
    evidence = {
        "manifest_size": None,
        "record": wrong_record.to_dict(),
        "schema_version": 1,
    }
    evidence_payload = (
        json.dumps(evidence, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    receipt = {
        "evidence_sha256": hashlib.sha256(evidence_payload).hexdigest(),
        "payload_id": payload_id,
        "schema_version": 1,
    }
    evidence_path = service.store.backup_retirements_root / f"{payload_id}.json"
    receipt_path = service.store.backup_retirements_root / f"{payload_id}.deleted.json"
    evidence_path.write_bytes(evidence_payload)
    evidence_path.chmod(0o600)
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
    )
    receipt_path.chmod(0o600)

    with pytest.raises(RequestStoreError, match="evidence identity drifted"):
        service.apply(service.load_claim(plan.plan_digest))

    assert service.store.read_backup_retention_claim() is not None
