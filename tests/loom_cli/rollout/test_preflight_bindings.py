from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pytest

from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.preflight_bindings import derive_attestation_bindings
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
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
        _execution("systemd.render", {"unit-set-digest": "1" * 64}),
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
