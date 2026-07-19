from __future__ import annotations

import json
import stat
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.final_gate_plan import (
    FinalGatePlan,
    FinalGatePlanError,
    FinalGatePlanStore,
)
from loom_cli.rollout.operator.model import DriverEnvelope, driver_envelope_sha256
from loom_cli.rollout.operator.protected_apply_baseline import ProtectedApplyBaseline
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactPublication
from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightAttestation,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)


def _lease() -> BackupLease:
    return BackupLease(
        lease_id="lease-1234567890abcdef",
        source_request_id="req-source01",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "d" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="snapshot-1",
        schema_revision="0066",
        object_inventory_root="c" * 64,
        created_at=NOW - timedelta(minutes=10),
        expires_at=NOW + timedelta(hours=1),
        restore_verified_at=NOW - timedelta(minutes=5),
    )


def _attestation() -> PreflightAttestation:
    check = RegisteredCheck(
        spec=CheckSpec(
            check_id="candidate.identity",
            failure_code="candidate.identity.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation="restore the exact candidate identity",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )
    executions = PreflightDag((check,)).run(
        CheckContext({"candidate.sha": "a" * 40}),
        now=lambda: NOW,
    )
    lease = _lease()
    bindings = AttestationBindings(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_digests={
            "api": "sha256:" + "1" * 64,
            "loom-control-plane": "sha256:" + f"{1:064x}",
            "loom-staging-admin-browser-smoke": "sha256:" + "8" * 64,
        },
        runner_source_sha="c" * 40,
        runner_source_tree="d" * 40,
        runner_install_hash="2" * 64,
        runner_config_hash="3" * 64,
        staging_mutation_epoch=7,
        backup_lease_id=lease.lease_id,
        backup_lease_digest=lease.evidence_digest,
        backup_manifest_sha256="a" * 64,
        backup_component_set_digest=component_set_digest(lease.component_sha256),
        db_snapshot_identity="snapshot-1",
        schema_revision="0066",
        object_inventory_root="c" * 64,
        migration_plan_digest="4" * 64,
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        secret_metadata_fingerprints={"admin": "sha256:abc len=32"},
        gb10_inventory_digest="5" * 64,
        gb10_boot_ids={"trt-gb10-1": "boot-1"},
        gb10_mount_digest="6" * 64,
        gb10_unit_digest="7" * 64,
        browser_image_digest="sha256:" + "8" * 64,
        browser_report_schema="8" * 64,
    )
    return PreflightAttestation.issue(
        bindings=bindings,
        executions=executions,
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )


def _envelope(attestation: PreflightAttestation) -> DriverEnvelope:
    return DriverEnvelope(
        schema_version=1,
        request_id="req-alpha",
        rollout_id="rollout-alpha",
        initiating_operator="qianyi",
        initiating_uid=501,
        attempt_number=1,
        attempt_operator="qianyi",
        attempt_uid=501,
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T21:00:00+00:00",
        backup_manifest_path="/data/loom-staging/backups/exact/backup-manifest.json",
        backup_manifest_sha256="a" * 64,
        runner_config_sha256="3" * 64,
        preflight_attestation_sha256=attestation.attestation_digest,
        preflight_registry_sha256="9" * 64,
        preflight_coverage_sha256="a" * 64,
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        cluster_config_path="/opt/loom-staging-runner/repo/deploy/staging.toml",
        rollout_root="/data/loom-staging",
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=32",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        resume=False,
    )


def _artifacts(tmp_path: Path) -> PreflightArtifactPublication:
    root = tmp_path / "preflight-artifacts" / ("e" * 64)
    return PreflightArtifactPublication(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        mutation_epoch=7,
        bundle_digest="e" * 64,
        descriptor_path=root / "artifact.json",
        rendered_manifest_path=root / "rendered.yaml",
        migration_manifest_path=root / "migration.yaml",
        image_artifact_sha256="1" * 64,
        manifest_artifact_sha256="2" * 64,
        rendered_manifest_sha256="3" * 64,
        migration_manifest_artifact_sha256="5" * 64,
        migration_manifest_sha256="6" * 64,
        migration_job_name="loom-migrate-staging-aaaaaaa-pf-123456789abc",
        migration_image_id="sha256:" + f"{1:064x}",
        migration_plan_sha256="4" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="8" * 64,
    )


def _baseline() -> ProtectedApplyBaseline:
    checks = (
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
        "staging.release-baseline",
    )
    return ProtectedApplyBaseline(
        schema_version=1,
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        readonly_principal="loom-staging-preflight-readonly",
        resource_digests={check_id: "b" * 64 for check_id in checks},
        implementation_digests={check_id: "c" * 64 for check_id in checks},
        evidence_hashes={check_id: "d" * 64 for check_id in checks},
        baseline_digest="e" * 64,
    )


def _plan(tmp_path: Path) -> FinalGatePlan:
    attestation = _attestation()
    return FinalGatePlan.build(
        _envelope(attestation), attestation, _artifacts(tmp_path), _lease(), _baseline()
    )


def test_final_gate_plan_binds_attestation_artifacts_and_checkpoint(tmp_path: Path) -> None:
    plan = _plan(tmp_path)

    assert FinalGatePlan.from_dict(plan.to_dict()) == plan
    assert plan.candidate_tree == "b" * 40
    assert plan.request_envelope_sha256 == driver_envelope_sha256(_envelope(_attestation()))
    assert plan.artifact_bundle_digest == "e" * 64
    assert plan.backup_lease_id == _lease().lease_id
    assert plan.backup_source_request_id == "req-source01"
    assert plan.image_digests["api"] == "sha256:" + "1" * 64
    assert plan.secret_metadata_fingerprints == {"admin": "sha256:abc len=32"}
    assert plan.protected_baseline_digest == _baseline().baseline_digest
    assert plan.protected_baseline_resource_digests == _baseline().resource_digests


def test_final_gate_plan_rejects_drift_or_content_tamper(tmp_path: Path) -> None:
    attestation = _attestation()
    envelope = _envelope(attestation)

    with pytest.raises(ValueError, match="inputs drifted"):
        FinalGatePlan.build(
            replace(envelope, backup_manifest_sha256="f" * 64),
            attestation,
            _artifacts(tmp_path),
            _lease(),
            _baseline(),
        )

    payload = _plan(tmp_path).to_dict()
    payload["starting_mutation_epoch"] = 8
    with pytest.raises(ValueError, match="content digest drifted"):
        FinalGatePlan.from_dict(payload)


def test_final_gate_plan_store_is_private_and_nonreplaceable(tmp_path: Path) -> None:
    attempt = tmp_path / "requests" / "req-alpha" / "attempts" / "1"
    attempt.mkdir(parents=True, mode=0o700)
    store = FinalGatePlanStore(tmp_path, request_id="req-alpha", attempt_number=1)
    plan = _plan(tmp_path)

    path = store.publish(plan)

    assert store.publish(plan) == path
    assert store.read() == plan
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.stat().st_nlink == 1

    with pytest.raises(FinalGatePlanError, match="cannot be replaced"):
        store.publish(replace(plan, db_snapshot_identity="snapshot-2"))

    raw = json.loads(path.read_text())
    raw["route"] = "https://example.invalid/dev"
    path.write_text(json.dumps(raw) + "\n")
    with pytest.raises(FinalGatePlanError, match="invalid"):
        store.read()
