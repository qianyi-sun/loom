from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from loom_cli.rollout.external_supervisor_controller import (
    ExternalSupervisorControllerBinding,
    encode_external_supervisor_controller_bindings,
    parse_external_supervisor_controller_bindings,
)
from loom_cli.rollout.external_supervisor_predecessor import (
    GB10_CANONICAL_UNIT_DIR,
    PROTECTED_CANONICAL_UNIT_DIR,
)
from loom_cli.rollout.external_supervisor_readiness import (
    protected_external_supervisor_script_paths_for_units,
)
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.final_gate_plan import (
    FinalGatePlan,
    FinalGatePlanError,
    FinalGatePlanStore,
)
from loom_cli.rollout.operator.model import DriverEnvelope, driver_envelope_sha256
from loom_cli.rollout.operator.protected_apply_baseline import ProtectedApplyBaseline
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisiteStore,
)
from loom_cli.rollout.operator.protected_execution_prerequisites import (
    ProtectedExecutionPrerequisiteArtifact,
)
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactPublication
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
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
    external_supervisor_transition_digest,
    external_supervisor_unit_set_digest,
    external_supervisor_unit_set_digest_or_empty,
)
from loom_cli.rollout.systemd_unit_readiness import UNIT_PATHS
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact as _artifact,
)

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)


def _lease() -> BackupLease:
    authority = DatabaseAuthorityEvidence(
        public_schema_revision="0066",
        capacity_guard_schema_revision="guard_0027",
        configuration_epoch=9,
        configuration_digest="9" * 64,
        authority_incarnation=UUID("00000000-0000-4000-8000-0000000000aa"),
        writer_epoch=4,
        execution_state="shadow",
        execution_epoch=0,
        execution_manifest_sha256=None,
        executable_new_capacity_ceiling=0,
        increase_freeze=True,
    )
    return BackupLease(
        lease_id="lease-1234567890abcdef",
        source_request_id="req-source01",
        manifest_sha256="a" * 64,
        component_sha256={
            "database_authority": authority.digest,
            "k8s_secrets": "e" * 64,
            "object_inventory": "c" * 64,
            "postgres": "d" * 64,
        },
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="pgdump-sha256:" + "d" * 64,
        schema_revision="0066",
        object_inventory_root="c" * 64,
        created_at=NOW - timedelta(minutes=10),
        expires_at=NOW + timedelta(hours=1),
        restore_verified_at=NOW - timedelta(minutes=5),
        checkpoint_schema_version=3,
        database_authority_digest=authority.digest,
        public_schema_revision=authority.public_schema_revision,
        capacity_guard_schema_revision=authority.capacity_guard_schema_revision,
        manager_configuration_epoch=authority.configuration_epoch,
        manager_configuration_digest=authority.configuration_digest,
        manager_authority_incarnation=authority.authority_incarnation,
        manager_writer_epoch=authority.writer_epoch,
        manager_execution_state=authority.execution_state,
        manager_execution_epoch=authority.execution_epoch,
        manager_execution_manifest_sha256=authority.execution_manifest_sha256,
        manager_executable_new_capacity_ceiling=authority.executable_new_capacity_ceiling,
        manager_increase_freeze=authority.increase_freeze,
        restore_report_sha256="f" * 64,
    )


def _attestation(
    execution_prerequisite: ProtectedExecutionPrerequisiteArtifact | None = None,
    *,
    execution_prerequisite_path: Path | None = None,
) -> PreflightAttestation:
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
    systemd = _systemd_evidence()
    predecessor = _predecessor_evidence()
    controller_bindings = _controller_bindings(systemd, predecessor)
    execution_bindings: dict[str, object] = {}
    if execution_prerequisite is not None:
        if execution_prerequisite_path is None:
            execution_prerequisite_path = (
                Path("/test/execution-prerequisites")
                / f"{execution_prerequisite.artifact_sha256}.json"
            )
        execution_bindings = {
            "execution_prerequisite_schema_version": 1,
            "execution_prerequisite_artifact_path": str(execution_prerequisite_path),
            "execution_prerequisite_artifact_sha256": (execution_prerequisite.artifact_sha256),
            "execution_core_artifact_bundle_sha256": (
                execution_prerequisite.core_artifact_bundle_sha256
            ),
            "execution_policy_sha256": execution_prerequisite.execution_policy_sha256,
            "executor_profile_seed_sha256": (execution_prerequisite.executor_profile_seed_sha256),
            "execution_manager_route_sha256": (execution_prerequisite.manager_route_sha256),
            "execution_access_metadata_sha256": (
                execution_prerequisite.credential_metadata_manifest_sha256
            ),
            "execution_coexistence_witness_sha256": (
                execution_prerequisite.witness_manifest_sha256
            ),
            "execution_legacy_writer_sha256": (
                execution_prerequisite.legacy_writer_manifest_sha256
            ),
            "execution_rollback_evidence_sha256": (execution_prerequisite.rollback_evidence_sha256),
        }
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
        db_snapshot_identity=lease.db_snapshot_identity,
        schema_revision="0066",
        object_inventory_root="c" * 64,
        migration_plan_digest="4" * 64,
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        secret_metadata_fingerprints={
            "admin": "sha256:abc len=32",
            "service": "sha256:" + "f" * 64,
        },
        gb10_inventory_digest="5" * 64,
        gb10_boot_ids={"trt-gb10-1": "boot-1"},
        gb10_mount_digest="6" * 64,
        gb10_unit_digest="7" * 64,
        browser_image_digest="sha256:" + "8" * 64,
        browser_report_schema="8" * 64,
        supervisor_predecessor_kind="legacy-manifest",
        supervisor_predecessor_digest=str(predecessor["authority-digest"]),
        supervisor_predecessor_pointer_digest=str(predecessor["pointer-digest"]),
        supervisor_predecessor_unit_sha256=dict(
            predecessor["unit-digests"]  # type: ignore[arg-type]
        ),
        supervisor_predecessor_unit_set_digest=str(predecessor["unit-set-digest"]),
        supervisor_predecessor_live_evidence_digest=str(predecessor["live-evidence-digest"]),
        supervisor_predecessor_pending_transition_digest=str(
            predecessor["pending-transition-digest"]
        ),
        supervisor_transition_digest=external_supervisor_transition_digest(
            unit_directory=GB10_CANONICAL_UNIT_DIR,
            candidate_sha="a" * 40,
            candidate_tree="b" * 40,
            environment="staging",
            predecessor_kind="legacy-manifest",
            predecessor_digest=str(predecessor["authority-digest"]),
            predecessor_pointer_digest=str(predecessor["pointer-digest"]),
            predecessor_unit_sha256=dict(
                predecessor["unit-digests"]  # type: ignore[arg-type]
            ),
            predecessor_unit_set_digest=str(predecessor["unit-set-digest"]),
            predecessor_live_evidence_digest=str(predecessor["live-evidence-digest"]),
            predecessor_pending_transition_digest=str(predecessor["pending-transition-digest"]),
            target_artifact_digest=str(systemd["supervisor-artifact-digest"]),
            target_profile_sha256=str(systemd["supervisor-profile-sha256"]),
            target_script_sha256=dict(
                systemd["supervisor-script-digests"]  # type: ignore[arg-type]
            ),
            target_unit_sha256=dict(
                systemd["supervisor-unit-digests"]  # type: ignore[arg-type]
            ),
            target_unit_set_digest=str(systemd["supervisor-unit-set-digest"]),
        ),
        supervisor_controller_bindings=controller_bindings,
        checkpoint_schema_version=lease.checkpoint_schema_version,
        checkpoint_component_sha256=lease.component_sha256,
        database_authority_digest=lease.database_authority_digest,
        public_schema_revision=lease.public_schema_revision,
        capacity_guard_schema_revision=lease.capacity_guard_schema_revision,
        manager_configuration_epoch=lease.manager_configuration_epoch,
        manager_configuration_digest=lease.manager_configuration_digest,
        manager_authority_incarnation=str(lease.manager_authority_incarnation),
        manager_writer_epoch=lease.manager_writer_epoch,
        manager_execution_state=lease.manager_execution_state,
        manager_execution_epoch=lease.manager_execution_epoch,
        manager_execution_manifest_sha256=lease.manager_execution_manifest_sha256,
        manager_executable_new_capacity_ceiling=lease.manager_executable_new_capacity_ceiling,
        manager_increase_freeze=lease.manager_increase_freeze,
        restore_report_sha256=lease.restore_report_sha256,
        **execution_bindings,  # type: ignore[arg-type]
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
        production_defaults_path=root / "production-defaults.json",
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
        production_defaults_sha256="7" * 64,
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


def _systemd_evidence() -> dict[str, object]:
    supervisor_units = {
        "loom-autoscaler-gb10-staging.service": "2" * 64,
        "loom-autoscaler-gb10-staging.timer": "3" * 64,
        "loom-autoscaler-oldlab-staging.service": "4" * 64,
        "loom-autoscaler-oldlab-staging.timer": "5" * 64,
    }
    gb10_units = {name: digest for name, digest in supervisor_units.items() if "gb10" in name}
    oldlab_units = {name: digest for name, digest in supervisor_units.items() if "oldlab" in name}
    units = {**{path: "1" * 64 for path in UNIT_PATHS}, **supervisor_units}
    return {
        "failed-units": {},
        "supervisor-artifact-digest": "4" * 64,
        "supervisor-profile-sha256": "5" * 64,
        "supervisor-script-digests": {
            "scripts/ops/worker_pool_autoscaler_external_once.py": "6" * 64,
        },
        "supervisor-unit-digests": supervisor_units,
        "supervisor-unit-set-digest": external_supervisor_unit_set_digest(supervisor_units),
        "supervisor-controller-artifact-digests": {
            "gx10-01c7": "7" * 64,
            "TRT-EAI-OLDLAB-1": "8" * 64,
        },
        "supervisor-controller-unit-digests": {
            **{f"gx10-01c7/{name}": digest for name, digest in gb10_units.items()},
            **{f"TRT-EAI-OLDLAB-1/{name}": digest for name, digest in oldlab_units.items()},
        },
        "supervisor-controller-unit-set-digests": {
            "gx10-01c7": external_supervisor_unit_set_digest(gb10_units),
            "TRT-EAI-OLDLAB-1": external_supervisor_unit_set_digest(oldlab_units),
        },
        "unit-count": len(units),
        "unit-digests": units,
        "unit-set-digest": hashlib.sha256(
            json.dumps(
                {"failed": {}, "units": units},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
    }


def _predecessor_evidence(
    *,
    pool_identity_digest: str = "b" * 64,
    primary_live_evidence_digest: str = "a" * 64,
) -> dict[str, object]:
    units = {
        "loom-autoscaler-gb10-staging.service": "7" * 64,
        "loom-autoscaler-gb10-staging.timer": "8" * 64,
    }
    oldlab_units = {
        "loom-autoscaler-oldlab-staging.service": "c" * 64,
        "loom-autoscaler-oldlab-staging.timer": "d" * 64,
    }
    controller_bindings = {
        "gx10-01c7/authority-kind": "legacy-manifest",
        "gx10-01c7/authority-digest": "9" * 64,
        "gx10-01c7/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        "gx10-01c7/unit-set-digest": external_supervisor_unit_set_digest(units),
        "gx10-01c7/live-evidence-digest": primary_live_evidence_digest,
        "gx10-01c7/pending-transition-digest": hashlib.sha256(b"{}").hexdigest(),
        "gx10-01c7/runtime-state": "ready",
        "gx10-01c7/unit-directory": GB10_CANONICAL_UNIT_DIR,
        **{f"gx10-01c7/unit/{name}": digest for name, digest in units.items()},
        "TRT-EAI-OLDLAB-1/authority-kind": "legacy-manifest",
        "TRT-EAI-OLDLAB-1/authority-digest": "e" * 64,
        "TRT-EAI-OLDLAB-1/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        "TRT-EAI-OLDLAB-1/unit-set-digest": external_supervisor_unit_set_digest(oldlab_units),
        "TRT-EAI-OLDLAB-1/live-evidence-digest": "f" * 64,
        "TRT-EAI-OLDLAB-1/pending-transition-digest": hashlib.sha256(b"{}").hexdigest(),
        "TRT-EAI-OLDLAB-1/runtime-state": "ready",
        "TRT-EAI-OLDLAB-1/unit-directory": PROTECTED_CANONICAL_UNIT_DIR,
        **{f"TRT-EAI-OLDLAB-1/unit/{name}": digest for name, digest in oldlab_units.items()},
    }
    runtime_suffixes = ("/live-evidence-digest", "/runtime-state")
    return {
        "authority-kind": "legacy-manifest",
        "authority-digest": "9" * 64,
        "pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        "unit-digests": units,
        "unit-set-digest": external_supervisor_unit_set_digest(units),
        "live-evidence-digest": primary_live_evidence_digest,
        "pending-transition-digest": hashlib.sha256(b"{}").hexdigest(),
        "transition-clear": True,
        "runtime-ready": True,
        "pool-identity-digest": pool_identity_digest,
        "controller-identity-bindings": {
            key: value
            for key, value in controller_bindings.items()
            if not key.endswith(runtime_suffixes)
        },
        "controller-runtime-observations": {
            key: value
            for key, value in controller_bindings.items()
            if key.endswith(runtime_suffixes)
        },
    }


def _controller_bindings(
    systemd: dict[str, object],
    predecessor: dict[str, object],
) -> dict[str, str]:
    artifacts = systemd["supervisor-controller-artifact-digests"]
    target_units = systemd["supervisor-controller-unit-digests"]
    target_sets = systemd["supervisor-controller-unit-set-digests"]
    predecessors = {
        **predecessor["controller-identity-bindings"],
        **predecessor["controller-runtime-observations"],
    }
    assert isinstance(artifacts, dict)
    assert isinstance(target_units, dict)
    assert isinstance(target_sets, dict)
    assert isinstance(predecessors, dict)
    bindings: dict[str, ExternalSupervisorControllerBinding] = {}
    for host in artifacts:
        prefix = f"{host}/"
        predecessor_units = {
            key.removeprefix(f"{prefix}unit/"): value
            for key, value in predecessors.items()
            if key.startswith(f"{prefix}unit/")
        }
        host_target_units = {
            key.removeprefix(prefix): value
            for key, value in target_units.items()
            if key.startswith(prefix)
        }
        bindings[host] = ExternalSupervisorControllerBinding.build(
            execution_host=host,
            candidate_sha="a" * 40,
            candidate_tree="b" * 40,
            environment="staging",
            predecessor_kind=predecessors[f"{prefix}authority-kind"],
            predecessor_digest=predecessors[f"{prefix}authority-digest"],
            predecessor_pointer_digest=predecessors[f"{prefix}pointer-digest"],
            predecessor_unit_sha256=predecessor_units,
            predecessor_unit_set_digest=predecessors[f"{prefix}unit-set-digest"],
            predecessor_live_evidence_digest=predecessors[f"{prefix}live-evidence-digest"],
            predecessor_pending_transition_digest=predecessors[
                f"{prefix}pending-transition-digest"
            ],
            predecessor_runtime_state=predecessors[f"{prefix}runtime-state"],
            unit_directory=predecessors[f"{prefix}unit-directory"],
            target_artifact_digest=artifacts[host],
            target_profile_sha256=systemd["supervisor-profile-sha256"],
            target_script_sha256=systemd["supervisor-script-digests"],
            target_unit_sha256=host_target_units,
            target_unit_set_digest=target_sets[host],
        )
    return encode_external_supervisor_controller_bindings(bindings)


def _plan(tmp_path: Path) -> FinalGatePlan:
    attestation = _attestation()
    return FinalGatePlan.build(
        _envelope(attestation),
        attestation,
        _artifacts(tmp_path),
        _lease(),
        _baseline(),
        _systemd_evidence(),
        _predecessor_evidence(),
    )


def _execution_plan(
    tmp_path: Path,
    *,
    access_metadata_override: str | None = None,
) -> FinalGatePlan:
    artifacts = _artifacts(tmp_path)
    lease = _lease()
    prerequisite = replace(
        _artifact(
            core_bundle_sha256=artifacts.bundle_digest,
            backup_lease_sha256=lease.evidence_digest,
        ),
        source_configuration_epoch=lease.manager_configuration_epoch,
        source_configuration_sha256=lease.manager_configuration_digest,
    )
    if access_metadata_override is not None:
        prerequisite = replace(
            prerequisite,
            credential_metadata_sha256={
                **prerequisite.credential_metadata_sha256,
                "manager-read": access_metadata_override,
            },
        )
    prerequisite_store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "execution-authority",
        service_uid=tmp_path.stat().st_uid,
    )
    publication = prerequisite_store.publish(prerequisite)
    attestation = _attestation(
        prerequisite,
        execution_prerequisite_path=publication.path,
    )
    return FinalGatePlan.build(
        _envelope(attestation),
        attestation,
        artifacts,
        lease,
        _baseline(),
        _systemd_evidence(),
        _predecessor_evidence(),
        execution_prerequisite_publication=publication,
        execution_prerequisite_store=prerequisite_store,
    )


def test_final_gate_plan_binds_attestation_artifacts_and_checkpoint(tmp_path: Path) -> None:
    plan = _plan(tmp_path)

    assert FinalGatePlan.from_dict(plan.to_dict()) == plan
    assert plan.schema_version == 6
    assert plan.checkpoint_schema_version == 3
    assert plan.checkpoint_component_sha256 == _lease().component_sha256
    assert plan.database_authority_digest == _lease().database_authority_digest
    assert plan.manager_configuration_digest == _lease().manager_configuration_digest
    assert plan.candidate_tree == "b" * 40
    assert plan.request_envelope_sha256 == driver_envelope_sha256(_envelope(_attestation()))
    assert plan.artifact_bundle_digest == "e" * 64
    assert plan.backup_lease_id == _lease().lease_id
    assert plan.backup_source_request_id == "req-source01"
    assert plan.image_digests["api"] == "sha256:" + "1" * 64
    assert plan.secret_metadata_fingerprints == {
        "admin": "sha256:abc len=32",
        "service": "sha256:" + "f" * 64,
    }
    assert plan.service_token_source == _envelope(_attestation()).service_token_source
    assert plan.protected_baseline_digest == _baseline().baseline_digest
    assert plan.protected_baseline_resource_digests == _baseline().resource_digests


def test_final_gate_plan_binds_exact_execution_prerequisite_publication(
    tmp_path: Path,
) -> None:
    artifacts = _artifacts(tmp_path)
    lease = _lease()
    prerequisite = replace(
        _artifact(
            core_bundle_sha256=artifacts.bundle_digest,
            backup_lease_sha256=lease.evidence_digest,
        ),
        source_configuration_epoch=lease.manager_configuration_epoch,
        source_configuration_sha256=lease.manager_configuration_digest,
    )
    prerequisite_store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "execution-authority",
        service_uid=tmp_path.stat().st_uid,
    )
    publication = prerequisite_store.publish(prerequisite)
    attestation = _attestation(
        prerequisite,
        execution_prerequisite_path=publication.path,
    )

    plan = FinalGatePlan.build(
        _envelope(attestation),
        attestation,
        artifacts,
        lease,
        _baseline(),
        _systemd_evidence(),
        _predecessor_evidence(),
        execution_prerequisite_publication=publication,
        execution_prerequisite_store=prerequisite_store,
    )

    assert plan.schema_version == 7
    assert plan.execution_prerequisite_artifact_path == str(publication.path)
    assert plan.execution_prerequisite_artifact_sha256 == prerequisite.artifact_sha256
    assert plan.execution_policy_sha256 == prerequisite.execution_policy_sha256
    assert plan.executor_profile_seed_sha256 == prerequisite.executor_profile_seed_sha256
    assert FinalGatePlan.from_dict(plan.to_dict()) == plan


def test_final_gate_plan_requires_bound_execution_prerequisite_publication(
    tmp_path: Path,
) -> None:
    artifacts = _artifacts(tmp_path)
    lease = _lease()
    prerequisite = replace(
        _artifact(
            core_bundle_sha256=artifacts.bundle_digest,
            backup_lease_sha256=lease.evidence_digest,
        ),
        source_configuration_epoch=lease.manager_configuration_epoch,
        source_configuration_sha256=lease.manager_configuration_digest,
    )
    attestation = _attestation(prerequisite)

    with pytest.raises(ValueError, match="authority is incomplete"):
        FinalGatePlan.build(
            _envelope(attestation),
            attestation,
            artifacts,
            lease,
            _baseline(),
            _systemd_evidence(),
            _predecessor_evidence(),
        )


def test_final_gate_plan_reader_rejects_prerequisite_path_digest_drift(
    tmp_path: Path,
) -> None:
    payload = _execution_plan(tmp_path).to_dict()
    payload["execution_prerequisite_artifact_path"] = "/safe/different.json"
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in payload.items() if key != "plan_digest"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

    with pytest.raises(ValueError, match="prerequisite binding is invalid"):
        FinalGatePlan.from_dict(payload)


@pytest.mark.parametrize(
    "drift",
    [
        "attestation",
        "publication-path",
        "core-bundle",
        "candidate-tree",
        "backup-lease",
        "manager-configuration",
    ],
)
def test_final_gate_plan_rejects_execution_prerequisite_authority_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    artifacts = _artifacts(tmp_path)
    lease = _lease()
    prerequisite = replace(
        _artifact(
            core_bundle_sha256=("0" * 64 if drift == "core-bundle" else artifacts.bundle_digest),
            backup_lease_sha256=("1" * 64 if drift == "backup-lease" else lease.evidence_digest),
        ),
        candidate_tree="2" * 40 if drift == "candidate-tree" else artifacts.candidate_tree,
        source_configuration_epoch=lease.manager_configuration_epoch,
        source_configuration_sha256=(
            "3" * 64 if drift == "manager-configuration" else lease.manager_configuration_digest
        ),
    )
    attested_prerequisite = prerequisite
    if drift == "attestation":
        attested_prerequisite = replace(
            prerequisite,
            credential_metadata_sha256={
                **prerequisite.credential_metadata_sha256,
                "manager-read": "4" * 64,
            },
        )
    prerequisite_store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "execution-authority",
        service_uid=tmp_path.stat().st_uid,
    )
    publication = prerequisite_store.publish(prerequisite)
    attestation = _attestation(
        attested_prerequisite,
        execution_prerequisite_path=(
            (
                tmp_path / "different-execution-authority" / "execution-prerequisites"
                if drift == "publication-path"
                else prerequisite_store.root
            )
            / f"{attested_prerequisite.artifact_sha256}.json"
        ),
    )

    with pytest.raises(ValueError, match="authority drifted"):
        FinalGatePlan.build(
            _envelope(attestation),
            attestation,
            artifacts,
            lease,
            _baseline(),
            _systemd_evidence(),
            _predecessor_evidence(),
            execution_prerequisite_publication=publication,
            execution_prerequisite_store=prerequisite_store,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("capacity_guard_schema_revision", "guard_0027"),
        ("manager_execution_manifest_sha256", "f" * 64),
        ("execution_policy_sha256", "f" * 64),
    ],
)
def test_historical_final_gate_constructor_rejects_schema_three_only_fields(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    historical = replace(
        _plan(tmp_path),
        schema_version=5,
        checkpoint_schema_version=None,
        checkpoint_component_sha256=None,
        database_authority_digest=None,
        public_schema_revision=None,
        capacity_guard_schema_revision=None,
        manager_configuration_epoch=None,
        manager_configuration_digest=None,
        manager_authority_incarnation=None,
        manager_writer_epoch=None,
        manager_execution_state=None,
        manager_execution_epoch=None,
        manager_execution_manifest_sha256=None,
        manager_executable_new_capacity_ceiling=None,
        manager_increase_freeze=None,
        restore_report_sha256=None,
    )

    with pytest.raises(ValueError, match="historical final gate plan"):
        replace(historical, **{field: value})


def test_final_gate_plan_reobserves_runtime_without_freezing_live_digest(
    tmp_path: Path,
) -> None:
    attestation = _attestation()
    plan = FinalGatePlan.build(
        _envelope(attestation),
        attestation,
        _artifacts(tmp_path),
        _lease(),
        _baseline(),
        _systemd_evidence(),
        _predecessor_evidence(primary_live_evidence_digest="0" * 64),
    )

    assert plan.supervisor_predecessor_live_evidence_digest == (
        attestation.bindings.supervisor_predecessor_live_evidence_digest
    )


def test_final_gate_plan_v4_remains_readable_with_historical_transition_digest(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    payload = plan.to_dict()
    payload.pop("plan_digest")
    payload["schema_version"] = 4
    for field in (
        "checkpoint_schema_version",
        "checkpoint_component_sha256",
        "database_authority_digest",
        "public_schema_revision",
        "capacity_guard_schema_revision",
        "manager_configuration_epoch",
        "manager_configuration_digest",
        "manager_authority_incarnation",
        "manager_writer_epoch",
        "manager_execution_state",
        "manager_execution_epoch",
        "manager_execution_manifest_sha256",
        "manager_executable_new_capacity_ceiling",
        "manager_increase_freeze",
        "restore_report_sha256",
    ):
        payload.pop(field)
    target_units = {
        name: digest for name, digest in plan.systemd_unit_digests.items() if name not in UNIT_PATHS
    }
    payload["supervisor_transition_digest"] = external_supervisor_transition_digest(
        unit_directory=GB10_CANONICAL_UNIT_DIR,
        candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree,
        environment=plan.environment,
        predecessor_kind=plan.supervisor_predecessor_kind,
        predecessor_digest=plan.supervisor_predecessor_digest,
        predecessor_pointer_digest=plan.supervisor_predecessor_pointer_digest,
        predecessor_unit_sha256=plan.supervisor_predecessor_unit_sha256,
        predecessor_unit_set_digest=plan.supervisor_predecessor_unit_set_digest,
        predecessor_live_evidence_digest=(plan.supervisor_predecessor_live_evidence_digest),
        predecessor_pending_transition_digest=(
            plan.supervisor_predecessor_pending_transition_digest
        ),
        target_artifact_digest=plan.supervisor_artifact_digest,
        target_profile_sha256=plan.supervisor_profile_sha256,
        target_script_sha256=plan.supervisor_script_digests,
        target_unit_sha256=target_units,
        target_unit_set_digest=external_supervisor_unit_set_digest(target_units),
        transition_schema_version=1,
    )
    historical_bindings: dict[str, ExternalSupervisorControllerBinding] = {}
    for host, binding in parse_external_supervisor_controller_bindings(
        plan.supervisor_controller_bindings
    ).items():
        prefix = f"{host}/"
        host_units = {
            key.removeprefix(prefix): digest
            for key, digest in plan.supervisor_controller_unit_digests.items()
            if key.startswith(prefix)
        }
        script_paths = protected_external_supervisor_script_paths_for_units(host_units)
        historical_bindings[host] = replace(
            binding,
            transition_digest=external_supervisor_transition_digest(
                unit_directory=binding.unit_directory,
                candidate_sha=plan.candidate_sha,
                candidate_tree=plan.candidate_tree,
                environment=plan.environment,
                predecessor_kind=binding.predecessor_kind,
                predecessor_digest=binding.predecessor_digest,
                predecessor_pointer_digest=binding.predecessor_pointer_digest,
                predecessor_unit_sha256=binding.predecessor_unit_sha256,
                predecessor_unit_set_digest=binding.predecessor_unit_set_digest,
                predecessor_live_evidence_digest=(binding.predecessor_live_evidence_digest),
                predecessor_pending_transition_digest=(
                    binding.predecessor_pending_transition_digest
                ),
                target_artifact_digest=plan.supervisor_controller_artifact_digests[host],
                target_profile_sha256=plan.supervisor_profile_sha256,
                target_script_sha256={
                    path: plan.supervisor_script_digests[path] for path in script_paths
                },
                target_unit_sha256=host_units,
                target_unit_set_digest=plan.supervisor_controller_unit_set_digests[host],
                transition_schema_version=1,
            ),
        )
    payload["supervisor_controller_bindings"] = encode_external_supervisor_controller_bindings(
        historical_bindings
    )
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    historical = FinalGatePlan.from_dict(payload)

    assert historical.schema_version == 4
    assert historical.to_dict() == payload


def test_final_gate_plan_preserves_independent_supervisor_controller_authority(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)

    assert set(plan.supervisor_controller_artifact_digests) == {
        "gx10-01c7",
        "TRT-EAI-OLDLAB-1",
    }
    assert set(plan.supervisor_controller_unit_set_digests) == {
        "gx10-01c7",
        "TRT-EAI-OLDLAB-1",
    }
    assert dict(plan.supervisor_controller_bindings) == dict(
        plan.to_dict()["supervisor_controller_bindings"]
    )
    controllers = parse_external_supervisor_controller_bindings(plan.supervisor_controller_bindings)
    assert controllers["gx10-01c7"].unit_directory == GB10_CANONICAL_UNIT_DIR
    assert controllers["TRT-EAI-OLDLAB-1"].unit_directory == PROTECTED_CANONICAL_UNIT_DIR

    gb10_only = {
        key: value
        for key, value in plan.supervisor_controller_bindings.items()
        if key.startswith("gx10-01c7/")
    }
    with pytest.raises(ValueError, match="controller binding set"):
        parse_external_supervisor_controller_bindings(gb10_only)


def test_controller_binding_rejects_repairable_noncanonical_predecessor(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    invalid = dict(plan.supervisor_controller_bindings)
    invalid["gx10-01c7/runtime-state"] = "repairable"

    with pytest.raises(ValueError, match="controller binding is invalid"):
        parse_external_supervisor_controller_bindings(invalid)


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
            _systemd_evidence(),
            _predecessor_evidence(),
        )

    drifted_systemd = _systemd_evidence()
    drifted_systemd["unit-set-digest"] = "0" * 64
    with pytest.raises(ValueError, match="systemd evidence is invalid"):
        FinalGatePlan.build(
            envelope,
            attestation,
            _artifacts(tmp_path),
            _lease(),
            _baseline(),
            drifted_systemd,
            _predecessor_evidence(),
        )

    unpaired_systemd = _systemd_evidence()
    unpaired_units = dict(unpaired_systemd["unit-digests"])  # type: ignore[arg-type]
    unpaired_units["loom-other.timer"] = unpaired_units.pop("loom-autoscaler-gb10-staging.timer")
    unpaired_systemd["unit-digests"] = unpaired_units
    unpaired_systemd["unit-set-digest"] = hashlib.sha256(
        json.dumps(
            {"failed": {}, "units": unpaired_units},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    with pytest.raises(ValueError, match="systemd evidence is invalid"):
        FinalGatePlan.build(
            envelope,
            attestation,
            _artifacts(tmp_path),
            _lease(),
            _baseline(),
            unpaired_systemd,
            _predecessor_evidence(),
        )

    payload = _plan(tmp_path).to_dict()
    payload["starting_mutation_epoch"] = 8
    with pytest.raises(ValueError, match="content digest drifted"):
        FinalGatePlan.from_dict(payload)

    payload = _plan(tmp_path).to_dict()
    payload["supervisor_transition_digest"] = "0" * 64
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in payload.items() if key != "plan_digest"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    with pytest.raises(ValueError, match="transition identity drifted"):
        FinalGatePlan.from_dict(payload)


def test_final_gate_plan_tolerates_pool_identity_outside_transition_identity(
    tmp_path: Path,
) -> None:
    # ``pool-identity-digest`` is a live worker-count field carried in the
    # external-supervisor.predecessor evidence. The plan must accept it (it is
    # part of the evidence schema) but must NOT fold it into the supervisor
    # transition identity, so two rehearsals whose only difference is the live
    # pool identity produce the same transition digest.
    attestation = _attestation()
    envelope = _envelope(attestation)

    def build(pool_identity_digest: str) -> FinalGatePlan:
        return FinalGatePlan.build(
            envelope,
            attestation,
            _artifacts(tmp_path),
            _lease(),
            _baseline(),
            _systemd_evidence(),
            _predecessor_evidence(pool_identity_digest=pool_identity_digest),
        )

    first = build("b" * 64)
    second = build("c" * 64)
    assert first.supervisor_transition_digest == second.supervisor_transition_digest

    # A missing or malformed pool-identity-digest is still rejected: the field is
    # required and must be a well-formed sha256.
    missing = _predecessor_evidence()
    del missing["pool-identity-digest"]
    with pytest.raises(ValueError, match="predecessor evidence is invalid"):
        FinalGatePlan.build(
            envelope,
            attestation,
            _artifacts(tmp_path),
            _lease(),
            _baseline(),
            _systemd_evidence(),
            missing,
        )

    with pytest.raises(ValueError, match="predecessor evidence is invalid"):
        FinalGatePlan.build(
            envelope,
            attestation,
            _artifacts(tmp_path),
            _lease(),
            _baseline(),
            _systemd_evidence(),
            _predecessor_evidence(pool_identity_digest="not-a-sha256"),
        )


def test_final_gate_plan_accepts_absent_supervisor_predecessor(tmp_path: Path) -> None:
    # First introduction of the external supervisor: the predecessor is "absent"
    # and legitimately carries NO units. The plan's map validation must not reject
    # the empty predecessor unit map (regression: it was lumped in with the
    # non-empty maps and every such deploy crashed at plan construction).
    payload = _plan(tmp_path).to_dict()
    payload["supervisor_predecessor_kind"] = "absent"
    payload["supervisor_predecessor_unit_sha256"] = {}
    payload["supervisor_predecessor_digest"] = EXTERNAL_SUPERVISOR_ABSENT_DIGEST
    payload["supervisor_predecessor_pointer_digest"] = EXTERNAL_SUPERVISOR_ABSENT_DIGEST
    payload["supervisor_predecessor_unit_set_digest"] = (
        external_supervisor_unit_set_digest_or_empty({})
    )
    target_unit_sha256 = {
        name: digest
        for name, digest in payload["systemd_unit_digests"].items()
        if name not in UNIT_PATHS
    }
    payload["supervisor_transition_digest"] = external_supervisor_transition_digest(
        unit_directory=GB10_CANONICAL_UNIT_DIR,
        candidate_sha=payload["candidate_sha"],
        candidate_tree=payload["candidate_tree"],
        environment=payload["environment"],
        predecessor_kind="absent",
        predecessor_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        predecessor_pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        predecessor_unit_sha256={},
        predecessor_unit_set_digest=external_supervisor_unit_set_digest_or_empty({}),
        predecessor_live_evidence_digest=payload["supervisor_predecessor_live_evidence_digest"],
        predecessor_pending_transition_digest=payload[
            "supervisor_predecessor_pending_transition_digest"
        ],
        target_artifact_digest=payload["supervisor_artifact_digest"],
        target_profile_sha256=payload["supervisor_profile_sha256"],
        target_script_sha256=payload["supervisor_script_digests"],
        target_unit_sha256=target_unit_sha256,
        target_unit_set_digest=external_supervisor_unit_set_digest(target_unit_sha256),
    )
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in payload.items() if key != "plan_digest"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

    plan = FinalGatePlan.from_dict(payload)

    assert plan.supervisor_predecessor_kind == "absent"
    assert dict(plan.supervisor_predecessor_unit_sha256) == {}


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
        store.publish(replace(plan, starting_mutation_epoch=8))

    raw = json.loads(path.read_text())
    raw["route"] = "https://example.invalid/dev"
    path.write_text(json.dumps(raw) + "\n")
    with pytest.raises(FinalGatePlanError, match="invalid"):
        store.read()
