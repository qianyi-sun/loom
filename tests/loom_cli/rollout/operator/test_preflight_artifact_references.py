from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.lifecycle_protocol import LifecyclePhase
from loom_cli.rollout.manifest_ownership_journal import ManifestOwnershipJournal
from loom_cli.rollout.operator import preflight_artifact_references as reference_module
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
    BackupRotationState,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.final_admission_store import FinalAdmissionStore
from loom_cli.rollout.operator.final_gate_store import FinalGateExecutionStore
from loom_cli.rollout.operator.model import (
    ActivePointer,
    CallerIdentity,
    CandidateBinding,
    RequestEvent,
    RolloutRequest,
)
from loom_cli.rollout.operator.preflight_artifact_references import (
    InstalledMaintenanceReferenceInventory,
    InstalledPreflightArtifactReferenceInventory,
    InstalledResumeEligibility,
    PreflightArtifactReferenceInventoryError,
)
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError
from loom_cli.rollout.preflight_artifact_reference import PreflightArtifactReference
from loom_cli.rollout.preflight_artifact_retention import PreflightArtifactProtection
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    PreflightAttestation,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import (
    PreflightAssessment,
    PreflightPipeline,
    _assessment_digest,
)
from tests.loom_cli.rollout.operator.test_broker import make_config
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    NOW as FINAL_NOW,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _attestation as _final_attestation,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _envelope as _final_envelope
from tests.loom_cli.rollout.operator.test_final_gate_plan import _lease as _final_lease
from tests.loom_cli.rollout.operator.test_lifecycle_capacity_job import (
    _plan as _capacity_plan,
)
from tests.loom_cli.rollout.operator.test_protected_apply_baseline import (
    _baseline_executions,
)
from tests.loom_cli.rollout.operator.test_store import (
    REQUEST_ID,
    make_event,
    make_preflight_request,
)
from tests.loom_cli.rollout.test_preflight_runtime import _runtime

NOW = datetime(2026, 8, 26, 12, tzinfo=UTC)
RESUME_NOW = FINAL_NOW + timedelta(minutes=1)
RESUME_REQUEST_ID = "req-alpha000"


def _assessment(tmp_path: Path, bundle_digest: str) -> PreflightAssessment:
    runtime = _runtime(tmp_path / bundle_digest[:4])
    plan = runtime.prebackup_plan(runtime.candidate)
    base = PreflightPipeline(
        registry=plan.registry,
        store=PreflightAttestationStore(tmp_path / bundle_digest[:4] / "attestations"),
        now=lambda: NOW,
    ).assess(context=plan.context)
    executions = []
    for execution in base.executions:
        if execution.check_id != "artifacts.publish":
            executions.append(execution)
            continue
        evidence = {**execution.evidence, "bundle-digest": bundle_digest}
        executions.append(
            replace(
                execution,
                evidence=evidence,
                evidence_hash=hashlib.sha256(
                    json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            )
        )
    stable = tuple(executions)
    return replace(
        base,
        executions=stable,
        assessment_digest=_assessment_digest(
            through_tier=base.through_tier,
            registry_digest=base.registry_digest,
            coverage_digest=base.coverage_digest,
            executions=stable,
        ),
    )


def _request(request_id: str, assessment: PreflightAssessment, *, preview: bool = False):
    return replace(
        make_preflight_request(),
        request_id=request_id,
        rollout_id="staging-" + request_id[-8:],
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
        status="preview" if preview else "pending",
    )


def _event(request_id: str, *, event: str, occurred_at: str, attempt_number: int = 1):
    return replace(
        make_event(
            event=event,
            occurred_at=occurred_at,
            attempt_number=attempt_number,
            status=(
                "done"
                if event == "attempt_done"
                else "failed"
                if event in {"attempt_failed", "launch_failed"}
                else "cancelled"
            ),
        ),
        request_id=request_id,
    )


class _Store:
    def __init__(self) -> None:
        self.preflight_requests: dict[str, object] = {}
        self.assessments: dict[str, PreflightAssessment] = {}
        self.job_phases: dict[str, LifecyclePhase] = {}
        self.events: dict[str, list[RequestEvent]] = {}
        self.promoted: set[str] = set()
        self.attempts: dict[str, tuple[int, ...]] = {}
        self.active: ActivePointer | None = None
        self.rotation = BackupRotationState()
        self.retention_claim: tuple[str, tuple[str, ...]] | None = None

    def request_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.preflight_requests))

    def read_preflight_request(self, request_id: str):  # type: ignore[no-untyped-def]
        return self.preflight_requests[request_id]

    def read_preflight_assessment(self, request_id: str) -> PreflightAssessment:
        return self.assessments[request_id]

    def read_preflight_backup_job_state(self, request_id: str):  # type: ignore[no-untyped-def]
        try:
            phase = self.job_phases[request_id]
        except KeyError as exc:
            raise RequestStoreError("preflight backup job directory does not exist") from exc
        return SimpleNamespace(phase=phase)

    def read_active(self) -> ActivePointer | None:
        return self.active

    def read_backup_rotation(self) -> BackupRotationState:
        return self.rotation

    def read_backup_retention_claim(self) -> tuple[str, tuple[str, ...]] | None:
        return self.retention_claim

    def read_request(self, request_id: str):  # type: ignore[no-untyped-def]
        if request_id not in self.promoted:
            raise RequestStoreError("rollout request is not promoted")
        return SimpleNamespace(status="pending")

    def attempt_numbers(self, request_id: str) -> tuple[int, ...]:
        return self.attempts.get(request_id, ())

    def read_events(self, request_id: str) -> list[RequestEvent]:
        return self.events.get(request_id, [])


def _inventory(
    tmp_path: Path,
    store: _Store,
    *,
    config: OperatorConfig | None = None,
    resume_eligible=None,  # type: ignore[no-untyped-def]
    maintenance_references=None,  # type: ignore[no-untyped-def]
) -> InstalledPreflightArtifactReferenceInventory:
    return InstalledPreflightArtifactReferenceInventory(
        config=config or make_config(tmp_path),
        service_uid=2001,
        store=store,
        resume_eligible=resume_eligible or (lambda _request_id, _now: False),
        maintenance_references=maintenance_references or (lambda: ()),
    )


def _resume_authority(
    tmp_path: Path,
    *,
    lease_expires_at: datetime | None = None,
) -> tuple[
    InstalledResumeEligibility,
    RequestStore,
    PreflightAttestation,
    tuple[CheckExecution, ...],
]:
    base_lease = _final_lease()
    lease = replace(
        base_lease,
        source_request_id=RESUME_REQUEST_ID,
        expires_at=lease_expires_at or base_lease.expires_at,
    )
    baseline = _baseline_executions()
    tier0 = replace(
        baseline[0],
        check_id="candidate.identity",
        failure_code="candidate.identity.failed",
        tier=0,
        stage=StageCapability.STATIC,
    )
    config = make_config(tmp_path)
    bindings = replace(
        _final_attestation().bindings,
        runner_config_hash="3" * 64,
        backup_lease_id=lease.lease_id,
        backup_lease_digest=lease.evidence_digest,
        backup_manifest_sha256=lease.manifest_sha256,
        backup_component_set_digest=component_set_digest(lease.component_sha256),
        db_snapshot_identity=lease.db_snapshot_identity,
        schema_revision=lease.schema_revision,
        object_inventory_root=lease.object_inventory_root,
    )
    attestation = PreflightAttestation.issue(
        bindings=bindings,
        executions=(tier0, *baseline),
        issued_at=FINAL_NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )
    envelope = replace(
        _final_envelope(attestation),
        request_id=RESUME_REQUEST_ID,
        resolved_tree=attestation.bindings.candidate_tree,
    )
    config = replace(
        config,
        config_sha256=envelope.runner_config_sha256,
        cluster_config_path=Path(envelope.cluster_config_path),
        rollout_root=Path(envelope.rollout_root),
        admin_token_source=envelope.admin_token_source,
        worker_token_source=envelope.worker_token_source,
        service_token_source=envelope.service_token_source,
        expect_admin_token_fingerprint=envelope.expect_admin_token_fingerprint,
        smoke_on_behalf_username=envelope.smoke_on_behalf_username,
        smoke_on_behalf_team_id=envelope.smoke_on_behalf_team_id,
        scope=envelope.scope,
        gb10_prep_concurrency=envelope.gb10_prep_concurrency,
    )
    request = RolloutRequest(
        request_id=envelope.request_id,
        rollout_id=envelope.rollout_id,
        caller=CallerIdentity(envelope.initiating_operator, envelope.initiating_uid),
        candidate=CandidateBinding(
            remote_url=envelope.remote_url,
            target_ref=envelope.target_ref,
            resolved_sha=envelope.resolved_sha,
            image_tag=envelope.image_tag,
            fetched_at=envelope.fetched_at,
            source_mode=envelope.source_mode,
            resolved_tree=envelope.resolved_tree,
            approved_base_sha=envelope.approved_base_sha,
        ),
        requested_at="2026-07-19T20:00:00Z",
        runner_config_sha256=envelope.runner_config_sha256,
        preflight_attestation_sha256=envelope.preflight_attestation_sha256,
        preflight_registry_sha256=envelope.preflight_registry_sha256,
        preflight_coverage_sha256=envelope.preflight_coverage_sha256,
    )
    store = RequestStore(config.state_root)
    store.create_request(request)
    store.publish_attempt_envelope(envelope)
    store.publish_backup_lease(lease)
    attestation_store = PreflightAttestationStore(config.state_root)
    attestation_store.publish(attestation)
    return (
        InstalledResumeEligibility(
            config=config,
            service_uid=os.geteuid(),
            store=store,
            attestation_store=attestation_store,
            read_mutation_epoch=lambda: 7,
        ),
        store,
        attestation,
        (tier0, *baseline),
    )


def _manifest_inventory(bundle_digest: str) -> dict[str, object]:
    dry_run = "d" * 64
    plan = "e" * 64
    inventory_digest = hashlib.sha256(
        json.dumps(
            {
                "artifact_bundle_sha256": bundle_digest,
                "dry_run_sha256": dry_run,
                "plan_sha256": plan,
                "version": "v2",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return {
        "schema_version": 2,
        "action": "inventory",
        "artifact_bundle_sha256": bundle_digest,
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "rendered_manifest_sha256": "c" * 64,
        "mutation_epoch": 7,
        "plan_sha256": plan,
        "dry_run_sha256": dry_run,
        "inventory_sha256": inventory_digest,
        "resources": [
            {
                "identity": "v1|ConfigMap|loom-staging|example",
                "uid": "11111111-1111-4111-8111-111111111111",
                "resource_version": "42",
                "generation": None,
                "live_sha256": "1" * 64,
                "managed_fields_sha256": "2" * 64,
                "desired_sha256": "3" * 64,
                "overlay_sha256": "4" * 64,
            }
        ],
    }


def _legacy_manifest_inventory() -> dict[str, object]:
    inventory = _manifest_inventory("7" * 64)
    inventory.pop("artifact_bundle_sha256")
    inventory["schema_version"] = 1
    inventory["inventory_sha256"] = hashlib.sha256(
        json.dumps(
            {
                "dry_run_sha256": inventory["dry_run_sha256"],
                "plan_sha256": inventory["plan_sha256"],
                "version": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return inventory


def _append_manifest_events(
    journal: ManifestOwnershipJournal,
    request_id: str,
    events: tuple[tuple[str, dict[str, object]], ...],
) -> None:
    for offset, (event, evidence) in enumerate(events):
        journal.append(
            request_id,
            {
                "event": event,
                "observed_at": (NOW + timedelta(seconds=offset)).isoformat(),
                "evidence": evidence,
            },
        )


def _write_private_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    for directory in (path.parent.parent, path.parent):
        directory.chmod(0o700)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_reference_inventory_merges_active_nonterminal_and_cleanup_authority(
    tmp_path: Path,
) -> None:
    store = _Store()
    active_assessment = _assessment(tmp_path, "1" * 64)
    failed_assessment = _assessment(tmp_path, "2" * 64)
    cleaned_assessment = _assessment(tmp_path, "3" * 64)
    for request_id, assessment in (
        (REQUEST_ID, active_assessment),
        ("stg-20260713-bcdef234", failed_assessment),
        ("stg-20260713-cdef3456", cleaned_assessment),
    ):
        store.preflight_requests[request_id] = _request(request_id, assessment)
        store.assessments[request_id] = assessment
    store.active = ActivePointer(REQUEST_ID, 1, "unit-active", "running")
    store.job_phases[REQUEST_ID] = LifecyclePhase.BACKUP_RUNNING
    store.job_phases["stg-20260713-bcdef234"] = LifecyclePhase.BACKUP_FAILED
    store.job_phases["stg-20260713-cdef3456"] = LifecyclePhase.BACKUP_FAILED
    store.events["stg-20260713-cdef3456"] = [
        replace(
            make_event(event="backup_cleanup_done", status="done"),
            request_id="stg-20260713-cdef3456",
        )
    ]

    protections = _inventory(tmp_path, store).collect(now=NOW)

    assert protections == (
        PreflightArtifactProtection(
            "1" * 64,
            ("active-rollout", "nonterminal-preflight-backup"),
        ),
        PreflightArtifactProtection("2" * 64, ("backup-cleanup-pending",)),
    )


def test_reference_inventory_preserves_only_latest_success_and_eligible_resume(
    tmp_path: Path,
) -> None:
    store = _Store()
    older = "stg-20260713-bcdef234"
    latest = "stg-20260713-cdef3456"
    failed = "stg-20260713-def45678"
    preview = "stg-20260713-ef567890"
    for index, request_id in enumerate((older, latest, failed, preview), start=4):
        assessment = _assessment(tmp_path, str(index) * 64)
        store.preflight_requests[request_id] = _request(
            request_id,
            assessment,
            preview=request_id == preview,
        )
        store.assessments[request_id] = assessment
        store.job_phases[request_id] = LifecyclePhase.LAUNCH_RUNNING
    store.promoted.update({older, latest, failed})
    store.attempts.update({older: (1,), latest: (1,), failed: (1,)})
    store.events[older] = [_event(older, event="attempt_done", occurred_at="2026-08-24T12:00:00Z")]
    store.events[latest] = [
        _event(latest, event="attempt_done", occurred_at="2026-08-25T12:00:00Z")
    ]
    store.events[failed] = [
        _event(failed, event="attempt_failed", occurred_at="2026-08-25T13:00:00Z")
    ]

    protections = _inventory(
        tmp_path,
        store,
        resume_eligible=lambda request_id, _now: request_id == failed,
    ).collect(now=NOW)

    assert protections == (
        PreflightArtifactProtection("5" * 64, ("current-release",)),
        PreflightArtifactProtection("6" * 64, ("resume-eligible",)),
    )


def test_installed_resume_requires_exact_config_attestation_and_fresh_lease(
    tmp_path: Path,
) -> None:
    eligibility, _store, attestation, _executions = _resume_authority(tmp_path)

    assert eligibility(RESUME_REQUEST_ID, RESUME_NOW)
    assert not eligibility(RESUME_REQUEST_ID, attestation.expires_at)
    assert not replace(
        eligibility,
        config=replace(eligibility.config, config_sha256="f" * 64),
    )(RESUME_REQUEST_ID, RESUME_NOW)
    assert not replace(eligibility, read_mutation_epoch=lambda: 8)(
        RESUME_REQUEST_ID,
        RESUME_NOW,
    )

    lease_expiry = FINAL_NOW + timedelta(minutes=5)
    lease_limited, _store, limited_attestation, _executions = _resume_authority(
        tmp_path / "lease-limited",
        lease_expires_at=lease_expiry,
    )
    assert limited_attestation.expires_at > lease_expiry
    assert not lease_limited(RESUME_REQUEST_ID, lease_expiry)


def test_installed_resume_preserves_expired_post_protected_apply_chain(
    tmp_path: Path,
) -> None:
    eligibility, _store, attestation, executions = _resume_authority(tmp_path)
    after_expiry = attestation.expires_at + timedelta(seconds=1)
    assert not eligibility(RESUME_REQUEST_ID, after_expiry)
    FinalAdmissionStore(
        eligibility.config.state_root,
        request_id=RESUME_REQUEST_ID,
        attempt_number=1,
        service_uid=os.geteuid(),
    ).publish(
        FinalAttestationAdmission(
            attestation,
            (executions[0],),
            executions[1:],
        )
    )
    evidence = MappingProxyType(
        {
            "ready": True,
            "candidate-sha": attestation.bindings.candidate_sha,
            "attestation-digest": attestation.attestation_digest,
            "observed-epoch": attestation.bindings.staging_mutation_epoch + 1,
            "protected-mutation": True,
            "blockers": {},
        }
    )
    protected_apply = CheckExecution(
        check_id="final.protected-apply",
        failure_code="final.protected-apply.failed",
        tier=4,
        stage=StageCapability.FINAL_ONLY,
        operation=CheckOperation.APPLY,
        outcome=CheckOutcome.PASS,
        input_fingerprint="1" * 64,
        implementation_digest="2" * 64,
        evidence=evidence,
        evidence_hash=hashlib.sha256(
            json.dumps(dict(evidence), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        started_at=FINAL_NOW,
        finished_at=FINAL_NOW,
        expires_at=attestation.expires_at,
        remediation=None,
    )
    FinalGateExecutionStore(
        eligibility.config.state_root,
        request_id=RESUME_REQUEST_ID,
        attempt_number=1,
        service_uid=os.geteuid(),
    ).publish(protected_apply)

    assert replace(eligibility, read_mutation_epoch=lambda: 8)(
        RESUME_REQUEST_ID,
        after_expiry,
    )


def test_reference_inventory_merges_inflight_maintenance_pins(tmp_path: Path) -> None:
    store = _Store()
    assessment = _assessment(tmp_path, "7" * 64)
    store.preflight_requests[REQUEST_ID] = _request(REQUEST_ID, assessment)
    store.assessments[REQUEST_ID] = assessment

    protections = _inventory(
        tmp_path,
        store,
        maintenance_references=lambda: (
            PreflightArtifactProtection("7" * 64, ("manifest-ownership-claim",)),
            PreflightArtifactProtection("8" * 64, ("lifecycle-capacity-claim",)),
        ),
    ).collect(now=NOW)

    assert protections == (
        PreflightArtifactProtection("7" * 64, ("manifest-ownership-claim",)),
        PreflightArtifactProtection("8" * 64, ("lifecycle-capacity-claim",)),
    )


def test_installed_maintenance_inventory_pins_only_inflight_exact_claims(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    manifest_bundle = "7" * 64
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    manifest_inventory = _manifest_inventory(manifest_bundle)
    journal.publish_inventory(request_id, manifest_inventory)
    journal.append(
        request_id,
        {
            "event": "inventory-approved",
            "observed_at": NOW.isoformat(),
            "evidence": {
                "inventory_sha256": manifest_inventory["inventory_sha256"],
                "plan_sha256": manifest_inventory["plan_sha256"],
                "starting_epoch": 7,
            },
        },
    )
    maintenance = InstalledMaintenanceReferenceInventory(
        config=config,
        service_uid=service_uid,
    )

    assert maintenance() == (
        PreflightArtifactProtection(manifest_bundle, ("manifest-ownership-claim",)),
    )

    journal.append(
        request_id,
        {
            "event": "failed",
            "observed_at": (NOW + timedelta(seconds=1)).isoformat(),
            "evidence": {
                "failure_class": "ManifestOwnershipAdoptionError",
                "failure_code": "manifest_ownership.epoch-claim.failed",
            },
        },
    )
    capacity_plan = _capacity_plan()
    claim_path = (
        config.state_root / "lifecycle-capacity-jobs" / f"{capacity_plan.plan_digest}.claim.json"
    )
    _write_private_json(
        claim_path,
        {
            "approved_plan_digest": capacity_plan.plan_digest,
            "claimed_at": NOW.isoformat(),
            "plan": capacity_plan.to_dict(),
            "schema_version": 1,
        },
    )

    assert maintenance() == (
        PreflightArtifactProtection(
            capacity_plan.artifact_bundle_sha256,
            ("lifecycle-capacity-claim",),
        ),
    )

    capacity_model = StagingCapacity(
        object_count=1,
        bytes_used=1,
        disk_free_percent=99.0,
        inode_free_percent=99.0,
    )
    result: dict[str, object] = {
        "capacity": {
            "admission_allowed": True,
            "bytes_used": 1,
            "disk_free_percent": 99.0,
            "evidence_sha256": capacity_model.evidence_digest,
            "gc_required": False,
            "inode_free_percent": 99.0,
            "object_count": 1,
            "observed_at": NOW.isoformat(),
            "policy_sha256": staging_capacity_policy_digest(),
        },
        "database_evidence_sha256": "a" * 64,
        "job_uid": "11111111-1111-4111-8111-111111111111",
        "mutation_epoch": capacity_plan.mutation_epoch,
        "plan_digest": capacity_plan.plan_digest,
        "schema_version": 1,
    }
    result["evidence_sha256"] = hashlib.sha256(
        json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    _write_private_json(
        claim_path.with_name(f"{capacity_plan.plan_digest}.result.json"),
        result,
    )

    assert maintenance() == ()
    capacity = result["capacity"]
    assert isinstance(capacity, dict)
    capacity["object_count"] = "1"
    result_without_digest = dict(result)
    result_without_digest.pop("evidence_sha256")
    result["evidence_sha256"] = hashlib.sha256(
        json.dumps(result_without_digest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    _write_private_json(
        claim_path.with_name(f"{capacity_plan.plan_digest}.result.json"),
        result,
    )
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="result is invalid"):
        maintenance()


def test_installed_maintenance_inventory_accepts_terminal_v2_subset_cleanup(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    inventory = _manifest_inventory("7" * 64)
    resources = inventory["resources"]
    assert isinstance(resources, list)
    second = dict(resources[0])
    second["identity"] = "v1|ConfigMap|loom-staging|second"
    second["uid"] = "22222222-2222-4222-8222-222222222222"
    resources.append(second)
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, inventory)
    _append_manifest_events(
        journal,
        request_id,
        (
            (
                "inventory-approved",
                {
                    "inventory_sha256": inventory["inventory_sha256"],
                    "plan_sha256": inventory["plan_sha256"],
                    "starting_epoch": 7,
                },
            ),
            ("epoch-claimed", {"observed_epoch": 8}),
            ("ownership-adopted", {"adoption_sha256": "a" * 64}),
            (
                "managed-fields-cleaned",
                {"cleanup_count": 1, "cleanup_sha256": "b" * 64},
            ),
            ("network-policies-converged", {"network_sha256": "c" * 64}),
            (
                "live-state-verified",
                {"attempts": 1, "post_apply_sha256": "d" * 64},
            ),
            (
                "completed",
                {"final_dry_run_sha256": "e" * 64, "observed_epoch": 8},
            ),
        ),
    )

    assert (
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()
        == ()
    )


@pytest.mark.parametrize(
    "events",
    (
        (
            ("inventory-approved", {}),
            ("failed", {"failure_class": "ValueError"}),
        ),
        (
            ("inventory-approved", {}),
            ("epoch-claimed", {"observed_epoch": 8}),
            ("ownership-adopted", {"adoption_sha256": "a" * 64}),
            ("network-policies-converged", {"network_sha256": "b" * 64}),
            ("live-state-verified", {"post_apply_sha256": "c" * 64}),
            (
                "failed",
                {
                    "failure_class": "RuntimeError",
                    "failure_code": "manifest_ownership.final-no-force-dry-run.failed",
                },
            ),
        ),
        (
            ("inventory-approved", {}),
            ("epoch-claimed", {"observed_epoch": 8}),
            ("ownership-adopted", {"adoption_sha256": "a" * 64}),
            ("network-policies-converged", {"network_sha256": "b" * 64}),
            (
                "live-state-verified",
                {"attempts": 1, "post_apply_sha256": "c" * 64},
            ),
            (
                "completed",
                {"final_dry_run_sha256": "d" * 64, "observed_epoch": 8},
            ),
        ),
        (
            ("inventory-approved", {}),
            ("epoch-claimed", {"observed_epoch": 8}),
            ("ownership-adopted", {"adoption_sha256": "a" * 64}),
            (
                "managed-fields-cleaned",
                {"cleanup_count": 1, "cleanup_sha256": "b" * 64},
            ),
            ("network-policies-converged", {"network_sha256": "c" * 64}),
            (
                "live-state-verified",
                {"attempts": 1, "post_apply_sha256": "d" * 64},
            ),
            (
                "completed",
                {"final_dry_run_sha256": "e" * 64, "observed_epoch": 8},
            ),
        ),
    ),
    ids=(
        "initial-failure-evidence",
        "stage-coded-failure",
        "retry-aware-completion",
        "managed-fields-completion",
    ),
)
def test_installed_maintenance_inventory_ignores_exact_terminal_v1_journals(
    tmp_path: Path,
    events: tuple[tuple[str, dict[str, object]], ...],
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    inventory = _legacy_manifest_inventory()
    bound_events = (
        (
            events[0][0],
            {
                **events[0][1],
                "inventory_sha256": inventory["inventory_sha256"],
                "plan_sha256": inventory["plan_sha256"],
                "starting_epoch": 7,
            },
        ),
        *events[1:],
    )
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, inventory)
    _append_manifest_events(journal, request_id, bound_events)

    assert (
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()
        == ()
    )


@pytest.mark.parametrize("publish_approval", (False, True))
def test_installed_maintenance_inventory_rejects_nonterminal_v1_journal(
    tmp_path: Path,
    publish_approval: bool,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    inventory = _legacy_manifest_inventory()
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, inventory)
    if publish_approval:
        _append_manifest_events(
            journal,
            request_id,
            (
                (
                    "inventory-approved",
                    {
                        "inventory_sha256": inventory["inventory_sha256"],
                        "plan_sha256": inventory["plan_sha256"],
                        "starting_epoch": 7,
                    },
                ),
            ),
        )

    with pytest.raises(PreflightArtifactReferenceInventoryError):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()


@pytest.mark.parametrize("case", ("digest-drift", "unknown-field"))
def test_installed_maintenance_inventory_rejects_malformed_v1_inventory(
    tmp_path: Path,
    case: str,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    inventory = _legacy_manifest_inventory()
    if case == "digest-drift":
        inventory["inventory_sha256"] = "f" * 64
    else:
        inventory["unknown"] = "field"
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, inventory)
    _append_manifest_events(
        journal,
        request_id,
        (
            (
                "inventory-approved",
                {
                    "inventory_sha256": inventory["inventory_sha256"],
                    "plan_sha256": inventory["plan_sha256"],
                    "starting_epoch": 7,
                },
            ),
            ("failed", {"failure_class": "ValueError"}),
        ),
    )

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="inventory"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()


def test_installed_maintenance_inventory_rejects_unknown_v1_event_profile(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    inventory = _legacy_manifest_inventory()
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, inventory)
    _append_manifest_events(
        journal,
        request_id,
        (
            (
                "inventory-approved",
                {
                    "inventory_sha256": inventory["inventory_sha256"],
                    "plan_sha256": inventory["plan_sha256"],
                    "starting_epoch": 7,
                },
            ),
            ("epoch-claimed", {"observed_epoch": 8}),
            ("ownership-adopted", {"adoption_sha256": "a" * 64}),
            (
                "managed-fields-cleaned",
                {"cleanup_count": 1, "cleanup_sha256": "b" * 64},
            ),
            ("network-policies-converged", {"network_sha256": "c" * 64}),
            ("live-state-verified", {"post_apply_sha256": "d" * 64}),
            (
                "completed",
                {"final_dry_run_sha256": "e" * 64, "observed_epoch": 8},
            ),
        ),
    )

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="events"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()


@pytest.mark.parametrize("schema_version", (True, 1.0))
def test_installed_maintenance_inventory_rejects_noninteger_manifest_schema(
    tmp_path: Path,
    schema_version: object,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    inventory = _legacy_manifest_inventory()
    inventory["schema_version"] = schema_version
    inventory["inventory_sha256"] = hashlib.sha256(
        json.dumps(
            {
                "dry_run_sha256": inventory["dry_run_sha256"],
                "plan_sha256": inventory["plan_sha256"],
                "version": f"v{schema_version}",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, inventory)
    _append_manifest_events(
        journal,
        request_id,
        (
            (
                "inventory-approved",
                {
                    "inventory_sha256": inventory["inventory_sha256"],
                    "plan_sha256": inventory["plan_sha256"],
                    "starting_epoch": 7,
                },
            ),
            ("failed", {"failure_class": "ValueError"}),
        ),
    )

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="inventory is invalid"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()


def test_installed_maintenance_inventory_rejects_unknown_or_changing_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    request_id = "req-manifest-ownership-12345678"
    journal = ManifestOwnershipJournal(config.state_root, service_uid=service_uid)
    journal.publish_inventory(request_id, _manifest_inventory("8" * 64))
    maintenance = InstalledMaintenanceReferenceInventory(
        config=config,
        service_uid=service_uid,
    )
    unknown = journal.root / "unknown"
    unknown.write_text("unsafe\n", encoding="utf-8")
    unknown.chmod(0o600)
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="unknown"):
        maintenance()
    unknown.unlink()

    original = reference_module.read_trusted_file
    changed = False

    def change_after_read(path: Path, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal changed
        value = original(path, **kwargs)
        if path.name == "inventory.json" and not changed:
            changed = True
            marker = path.parent / "changed"
            marker.write_text("changed\n", encoding="utf-8")
            marker.chmod(0o600)
        return value

    monkeypatch.setattr(reference_module, "read_trusted_file", change_after_read)
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="changed"):
        maintenance()


def test_installed_maintenance_inventory_rejects_malformed_manifest_identity(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    inventory = _manifest_inventory("9" * 64)
    inventory["candidate_sha"] = "z" * 40
    ManifestOwnershipJournal(config.state_root, service_uid=service_uid).publish_inventory(
        "req-manifest-ownership-12345678",
        inventory,
    )

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="inventory is invalid"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("generation", 0),
        ("identity", "invalid"),
        ("resource_version", ""),
        ("uid", ""),
    ),
)
def test_installed_maintenance_inventory_rejects_impossible_manifest_resource(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    inventory = _manifest_inventory("9" * 64)
    resources = inventory["resources"]
    assert isinstance(resources, list)
    resource = resources[0]
    assert isinstance(resource, dict)
    resource[field] = value
    ManifestOwnershipJournal(
        config.state_root,
        service_uid=os.geteuid(),
    ).publish_inventory("req-manifest-ownership-12345678", inventory)

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="inventory is invalid"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=os.geteuid(),
        )()


@pytest.mark.parametrize(
    "case",
    (
        "failure-before-approval",
        "wrong-failure-stage",
        "wrong-claimed-epoch",
        "wrong-cleanup-count",
        "wrong-completed-epoch",
    ),
)
def test_installed_maintenance_inventory_rejects_producer_impossible_events(
    tmp_path: Path,
    case: str,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    request_id = "req-manifest-ownership-12345678"
    inventory = _manifest_inventory("9" * 64)
    events: list[tuple[str, dict[str, object]]] = [
        (
            "inventory-approved",
            {
                "inventory_sha256": inventory["inventory_sha256"],
                "plan_sha256": inventory["plan_sha256"],
                "starting_epoch": 7,
            },
        ),
        ("epoch-claimed", {"observed_epoch": 8}),
        ("ownership-adopted", {"adoption_sha256": "a" * 64}),
        (
            "managed-fields-cleaned",
            {"cleanup_count": 1, "cleanup_sha256": "b" * 64},
        ),
        ("network-policies-converged", {"network_sha256": "c" * 64}),
        (
            "live-state-verified",
            {"attempts": 1, "post_apply_sha256": "d" * 64},
        ),
        (
            "completed",
            {"final_dry_run_sha256": "e" * 64, "observed_epoch": 8},
        ),
    ]
    if case == "failure-before-approval":
        events = [
            (
                "failed",
                {
                    "failure_class": "ManifestOwnershipAdoptionError",
                    "failure_code": "manifest_ownership.epoch-claim.failed",
                },
            )
        ]
    elif case == "wrong-failure-stage":
        events = [
            *events[:1],
            (
                "failed",
                {
                    "failure_class": "ManifestOwnershipAdoptionError",
                    "failure_code": "manifest_ownership.live-state-verification.failed",
                },
            ),
        ]
    elif case == "wrong-claimed-epoch":
        events = events[:2]
        events[-1][1]["observed_epoch"] = 9
    elif case == "wrong-cleanup-count":
        events = events[:4]
        events[-1][1]["cleanup_count"] = 2
    elif case == "wrong-completed-epoch":
        events[-1][1]["observed_epoch"] = 9
    journal = ManifestOwnershipJournal(
        config.state_root,
        service_uid=os.geteuid(),
    )
    journal.publish_inventory(request_id, inventory)
    for offset, (event, evidence) in enumerate(events):
        journal.append(
            request_id,
            {
                "event": event,
                "observed_at": (NOW + timedelta(seconds=offset)).isoformat(),
                "evidence": evidence,
            },
        )

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="events"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=os.geteuid(),
        )()


def test_installed_maintenance_inventory_rejects_non_service_file_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path)
    config.state_root.mkdir(parents=True, mode=0o700)
    config.state_root.chmod(0o700)
    service_uid = os.geteuid()
    ManifestOwnershipJournal(config.state_root, service_uid=service_uid).publish_inventory(
        "req-manifest-ownership-12345678",
        _manifest_inventory("a" * 64),
    )
    original = reference_module.read_trusted_file

    def root_owned(path: Path, **kwargs):  # type: ignore[no-untyped-def]
        value = original(path, **kwargs)
        fields = list(value.metadata)
        fields[stat.ST_UID] = 0
        return replace(value, metadata=os.stat_result(fields))

    monkeypatch.setattr(reference_module, "read_trusted_file", root_owned)
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="owner"):
        InstalledMaintenanceReferenceInventory(
            config=config,
            service_uid=service_uid,
        )()


def test_reference_inventory_protects_rotation_and_claimed_backup_payloads(
    tmp_path: Path,
) -> None:
    store = _Store()
    active_request = "req-active0000"
    candidate_request = "req-candidate0000"
    retirement_request = "req-retirement0000"
    for index, request_id in enumerate(
        (active_request, candidate_request, retirement_request),
        start=1,
    ):
        assessment = _assessment(tmp_path, str(index) * 64)
        store.preflight_requests[request_id] = _request(request_id, assessment)
        store.assessments[request_id] = assessment
    lease = BackupLease(
        lease_id="lease-active000",
        source_request_id=active_request,
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="snapshot-active",
        schema_revision="0067",
        object_inventory_root="c" * 64,
        created_at=NOW - timedelta(hours=1),
        restore_verified_at=NOW - timedelta(minutes=55),
        expires_at=NOW + timedelta(hours=1),
    )
    active = BackupPayloadRecord(
        payload_id="payload-active000",
        request_id=active_request,
        bundle_name="20260826T100000Z-active",
        phase=BackupPayloadPhase.ACTIVE,
        created_at=NOW - timedelta(hours=1),
        manifest_sha256=lease.manifest_sha256,
        lease=lease,
    )
    candidate = BackupPayloadRecord(
        payload_id="payload-candidate000",
        request_id=candidate_request,
        bundle_name="20260826T110000Z-candidate",
        phase=BackupPayloadPhase.CREATING,
        created_at=NOW - timedelta(minutes=30),
    )
    retirement = BackupRetirementRecord(
        payload_id="payload-retirement000",
        request_id=retirement_request,
        bundle_name="20260825T120000Z-retirement",
        reason="failed",
    )
    store.rotation = BackupRotationState(
        generation=9,
        active=active,
        candidate=candidate,
        retirements=(retirement,),
    )
    store.retention_claim = ("f" * 64, (retirement.payload_id,))

    protections = _inventory(tmp_path, store).collect(now=NOW)

    assert protections == (
        PreflightArtifactProtection("1" * 64, ("backup-rotation-active",)),
        PreflightArtifactProtection("2" * 64, ("backup-rotation-candidate",)),
        PreflightArtifactProtection("3" * 64, ("backup-retention-claim",)),
    )


def test_reference_inventory_rejects_backup_claim_outside_rotation(tmp_path: Path) -> None:
    store = _Store()
    store.retention_claim = ("f" * 64, ("payload-unknown000",))

    with pytest.raises(
        PreflightArtifactReferenceInventoryError,
        match="claim is inconsistent",
    ):
        _inventory(tmp_path, store).collect(now=NOW)


def test_reference_inventory_protects_empty_verified_candidate_recovery_claim(
    tmp_path: Path,
) -> None:
    store = _Store()
    assessment = _assessment(tmp_path, "4" * 64)
    request_id = "req-recovery0000"
    store.preflight_requests[request_id] = _request(request_id, assessment)
    store.assessments[request_id] = assessment
    store.rotation = BackupRotationState(
        generation=4,
        candidate=BackupPayloadRecord(
            payload_id="payload-recovery000",
            request_id=request_id,
            bundle_name="20260826T110000Z-recovery",
            phase=BackupPayloadPhase.CREATING,
            created_at=NOW - timedelta(minutes=10),
        ),
    )
    store.retention_claim = ("e" * 64, ())

    assert _inventory(tmp_path, store).collect(now=NOW) == (
        PreflightArtifactProtection(
            "4" * 64,
            ("backup-recovery-claim", "backup-rotation-candidate"),
        ),
    )


def test_reference_inventory_fails_closed_on_missing_or_malformed_assessment(
    tmp_path: Path,
) -> None:
    store = _Store()
    assessment = _assessment(tmp_path, "9" * 64)
    store.preflight_requests[REQUEST_ID] = _request(REQUEST_ID, assessment)

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="assessment"):
        _inventory(tmp_path, store).collect(now=NOW)

    store.assessments[REQUEST_ID] = replace(assessment, executions=())
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="publication"):
        _inventory(tmp_path, store).collect(now=NOW)


def test_reference_inventory_does_not_mask_corruption_as_known_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    store.preflight_requests[REQUEST_ID] = object()

    def corrupt_request(_request_id: str) -> object:
        raise RequestStoreError("corrupt required field does not exist")

    monkeypatch.setattr(store, "read_preflight_request", corrupt_request)

    with pytest.raises(PreflightArtifactReferenceInventoryError, match="authority is unreadable"):
        _inventory(tmp_path, store).collect(now=NOW)


def test_reference_inventory_rejects_non_utc_clock(tmp_path: Path) -> None:
    with pytest.raises(PreflightArtifactReferenceInventoryError, match="clock"):
        _inventory(tmp_path, _Store()).collect(now=NOW.replace(tzinfo=None))


def test_assessment_fixture_has_exact_typed_publication(tmp_path: Path) -> None:
    assessment = _assessment(tmp_path, "a" * 64)
    assert PreflightArtifactReference.from_assessment(assessment).bundle_digest == "a" * 64
