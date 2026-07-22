from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pytest

from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    CheckContext,
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
    external_supervisor_transition_digest,
    external_supervisor_unit_set_digest,
)

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)


def _execution(check_id: str, evidence: dict[str, object]) -> CheckExecution:
    return CheckExecution(
        check_id=check_id,
        failure_code=f"{check_id}.failed",
        tier=0,
        stage=StageCapability.STATIC,
        operation=CheckOperation.PROBE,
        outcome=CheckOutcome.PASS,
        input_fingerprint="1" * 64,
        implementation_digest="2" * 64,
        evidence=MappingProxyType(evidence),  # type: ignore[arg-type]
        evidence_hash="3" * 64,
        started_at=NOW,
        finished_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
        remediation=None,
    )


def _executions() -> tuple[CheckExecution, ...]:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "a" * 64,
        "loom-autoscaler-gb10-staging.timer": "b" * 64,
    }
    target_units = {
        "loom-autoscaler-gb10-staging.service": "c" * 64,
        "loom-autoscaler-gb10-staging.timer": "d" * 64,
    }
    return (
        _execution(
            "candidate.identity",
            {"resolved-sha": "a" * 40, "resolved-tree": "b" * 40},
        ),
        _execution("runner.install", {"attestation-digest": "c" * 64}),
        _execution(
            "credentials.metadata",
            {"metadata-fingerprints": {"admin": "d" * 64}},
        ),
        _execution(
            "backup.lease-eligibility",
            {"source-request": "req-known-good"},
        ),
        _execution(
            "images.contract",
            {"image-digests": {"browser": f"sha256:{'e' * 64}"}},
        ),
        _execution("migration.plan", {"plan-digest": "f" * 64}),
        _execution("migration.manifest", {"artifact-digest": "0" * 64}),
        _execution(
            "systemd.render",
            {
                "supervisor-artifact-digest": "5" * 64,
                "supervisor-profile-sha256": "6" * 64,
                "supervisor-script-digests": {
                    "scripts/ops/worker_pool_autoscaler_external_once.py": "7" * 64
                },
                "supervisor-unit-digests": target_units,
                "supervisor-unit-set-digest": external_supervisor_unit_set_digest(target_units),
            },
        ),
        _execution(
            "external-supervisor.predecessor",
            {
                "authority-kind": "legacy-manifest",
                "authority-digest": "8" * 64,
                "pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
                "unit-digests": predecessor_units,
                "unit-set-digest": external_supervisor_unit_set_digest(predecessor_units),
                "live-evidence-digest": "9" * 64,
                "pending-transition-digest": "0" * 64,
                "transition-clear": True,
                "runtime-ready": True,
            },
        ),
        _execution("gb10.shared-mount", {"mount-digest": "2" * 64}),
        _execution("gb10.candidate-source", {"source-digest": "1" * 64}),
        _execution(
            "gb10.host-readiness",
            {"inventory-digest": "3" * 64, "boot-ids": {"gb10-1": "boot-1"}},
        ),
        _execution(
            "browser.runtime",
            {
                "image-id": f"sha256:{'e' * 64}",
                "report-schema-digest": "4" * 64,
            },
        ),
        _execution(
            "rehearsal.cleanup",
            {"cleanup-verified": True, "protected-mutation": False},
        ),
    )


def _context(*, candidate_sha: str = "a" * 40) -> CheckContext:
    return CheckContext(
        {
            "candidate.sha": candidate_sha,
            "candidate.tree": "b" * 40,
            "runner.config.sha256": "5" * 64,
            "staging.mutation-epoch": 7,
            "backup.lease.sha256": "6" * 64,
            "backup.manifest.sha256": "7" * 64,
            "backup.component-set.sha256": "8" * 64,
            "db.snapshot-identity": "lsn-1",
            "schema.revision": "0066",
            "object.inventory-root": "9" * 64,
            "environment": "staging",
            "namespace": "loom-staging",
            "route": "https://yylx.world/dev",
        }
    )


def test_derives_complete_bindings_only_from_exact_evidence() -> None:
    bindings = derive_attestation_bindings(_context(), _executions())
    assert bindings.candidate_sha == "a" * 40
    assert bindings.candidate_tree == "b" * 40
    assert bindings.browser_image_digest == f"sha256:{'e' * 64}"
    assert bindings.secret_metadata_fingerprints == {"admin": f"sha256:{'d' * 64}"}
    assert bindings.staging_mutation_epoch == 7
    assert bindings.backup_lease_id == "req-known-good"
    assert bindings.backup_lease_digest == "6" * 64
    assert bindings.backup_manifest_sha256 == "7" * 64
    assert bindings.backup_component_set_digest == "8" * 64
    assert bindings.object_inventory_root == "9" * 64
    assert bindings.supervisor_predecessor_kind == "legacy-manifest"
    assert bindings.supervisor_predecessor_unit_set_digest == (
        external_supervisor_unit_set_digest(bindings.supervisor_predecessor_unit_sha256)
    )
    systemd = next(
        execution for execution in _executions() if execution.check_id == "systemd.render"
    )
    assert bindings.supervisor_transition_digest == external_supervisor_transition_digest(
        candidate_sha=bindings.candidate_sha,
        candidate_tree=bindings.candidate_tree,
        environment=bindings.environment,
        predecessor_kind=bindings.supervisor_predecessor_kind,
        predecessor_digest=bindings.supervisor_predecessor_digest,
        predecessor_pointer_digest=bindings.supervisor_predecessor_pointer_digest,
        predecessor_unit_sha256=bindings.supervisor_predecessor_unit_sha256,
        predecessor_unit_set_digest=bindings.supervisor_predecessor_unit_set_digest,
        predecessor_live_evidence_digest=(bindings.supervisor_predecessor_live_evidence_digest),
        predecessor_pending_transition_digest=(
            bindings.supervisor_predecessor_pending_transition_digest
        ),
        target_artifact_digest=str(systemd.evidence["supervisor-artifact-digest"]),
        target_profile_sha256=str(systemd.evidence["supervisor-profile-sha256"]),
        target_script_sha256=systemd.evidence["supervisor-script-digests"],  # type: ignore[arg-type]
        target_unit_sha256=systemd.evidence["supervisor-unit-digests"],  # type: ignore[arg-type]
        target_unit_set_digest=str(systemd.evidence["supervisor-unit-set-digest"]),
    )


def test_restore_verified_lease_overrides_prebackup_reuse_evidence() -> None:
    lease = BackupLease(
        lease_id="lease-restored01",
        source_request_id="req-restored01",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64, "object_inventory": "c" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="pgdump-sha256:" + "b" * 64,
        schema_revision="0067",
        object_inventory_root="d" * 64,
        created_at=NOW,
        restore_verified_at=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=2),
    )

    bindings = derive_attestation_bindings(
        _context(),
        _executions(),
        backup_lease=lease,
    )

    assert bindings.backup_lease_id == lease.lease_id
    assert bindings.backup_lease_digest == lease.evidence_digest
    assert bindings.backup_manifest_sha256 == lease.manifest_sha256
    assert bindings.backup_component_set_digest == component_set_digest(lease.component_sha256)
    assert bindings.db_snapshot_identity == lease.db_snapshot_identity
    assert bindings.schema_revision == lease.schema_revision
    assert bindings.object_inventory_root == lease.object_inventory_root


def test_rejects_candidate_drift_and_incomplete_cleanup() -> None:
    with pytest.raises(ValueError, match="candidate evidence drifted"):
        derive_attestation_bindings(_context(candidate_sha="9" * 40), _executions())
    executions = list(_executions())
    executions[-1] = replace(
        executions[-1],
        evidence=MappingProxyType({"cleanup-verified": False, "protected-mutation": False}),
    )
    with pytest.raises(ValueError, match="cleanup evidence"):
        derive_attestation_bindings(_context(), executions)


def test_rejects_absent_external_supervisor_predecessor_evidence() -> None:
    executions = list(_executions())
    index = next(
        index
        for index, execution in enumerate(executions)
        if execution.check_id == "external-supervisor.predecessor"
    )
    executions[index] = replace(
        executions[index],
        evidence=MappingProxyType(
            {
                "authority-kind": "absent",
                "authority-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
                "pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
                "unit-digests": {},
                "unit-set-digest": "0" * 64,
                "live-evidence-digest": "9" * 64,
                "pending-transition-digest": "0" * 64,
                "transition-clear": True,
                "runtime-ready": True,
            }
        ),
    )

    with pytest.raises(ValueError, match="not authoritative"):
        derive_attestation_bindings(_context(), executions)
