from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightRehearsal
from loom_cli.rollout.preflight_registered_checks import build_rehearsal_checks
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS, RehearsalResult
from loom_cli.rollout.rehearsal_restore_evidence import build_restore_verification_evidence

NOW = datetime(2026, 7, 19, 22, tzinfo=UTC)


def _dependency(check_id: str) -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=check_id,
            failure_code=f"{check_id}.failed",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("runner.config.sha256",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=300,
            remediation=f"restore {check_id}",
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


def _checkpoint(tmp_path: Path) -> CriticalCheckpointEvidence:
    authority = DatabaseAuthorityEvidence(
        public_schema_revision="0067",
        capacity_guard_schema_revision="guard_0028",
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
    return CriticalCheckpointEvidence(
        request_id="req-restore001",
        manifest_path=tmp_path / "backup" / "backup-manifest.json",
        manifest_sha256="1" * 64,
        component_sha256={
            "k8s_secrets": "2" * 64,
            "object_inventory": "3" * 64,
            "postgres": "4" * 64,
            "database_authority": authority.digest,
        },
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=17,
        db_snapshot_identity="pgdump-sha256:" + "4" * 64,
        schema_revision="0067",
        object_inventory_root="5" * 64,
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
        created_at=NOW,
    )


def _rehearsal(
    checkpoint: CriticalCheckpointEvidence,
    *,
    cleanup: bool = True,
    include_execution_prerequisites: bool = False,
) -> tuple[PreflightRehearsal, CheckContext]:
    def result(check_id: str) -> RehearsalResult:
        return RehearsalResult(
            check_id=check_id,
            isolation_id="rehearsal-restore001",
            candidate_sha="6" * 40,
            mutation_epoch=checkpoint.mutation_epoch,
            evidence_digest=hashlib.sha256(check_id.encode()).hexdigest(),
            journal_digest=hashlib.sha256((check_id + "-journal").encode()).hexdigest(),
            protected_mutation=False,
            cleanup_verified=cleanup and check_id == "rehearsal.cleanup",
            blockers={},
        )

    checks = build_rehearsal_checks(
        {check_id: lambda check_id=check_id: result(check_id) for check_id in REHEARSAL_CHECK_IDS},
        isolation_id="rehearsal-restore001",
        candidate_sha="6" * 40,
        mutation_epoch=checkpoint.mutation_epoch,
        checkpoint_evidence_digest=checkpoint.evidence_digest,
        rehearsal_plan_digest="7" * 64,
    )
    external = sorted(
        {
            dependency
            for check in checks
            for dependency in check.spec.dependencies
            if not dependency.startswith("rehearsal.")
        }
    )
    context = CheckContext(
        {
            "candidate.sha": "6" * 40,
            "checkpoint.evidence.sha256": checkpoint.evidence_digest,
            "environment": checkpoint.environment,
            "namespace": checkpoint.namespace,
            "rehearsal.plan.sha256": "7" * 64,
            "runner.config.sha256": "8" * 64,
            "staging.mutation-epoch": checkpoint.mutation_epoch,
        }
    )
    execution_prerequisites = (
        (
            RegisteredCheck(
                spec=CheckSpec(
                    check_id="execution.prerequisites",
                    failure_code="execution.prerequisites.unavailable",
                    tier=3,
                    stage=StageCapability.ISOLATED_REHEARSAL,
                    dependencies=("rehearsal.cleanup",),
                    mutation_class=MutationClass.ISOLATED,
                    input_keys=("runner.config.sha256",),
                    evidence_schema=(EvidenceField("ready", "boolean"),),
                    timeout_seconds=5,
                    freshness_ttl_seconds=300,
                    remediation="restore execution prerequisites",
                    secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
                ),
                implementation_version="test-v1",
                operations={
                    CheckOperation.PROBE: lambda _context: CheckProbe(
                        passed=True,
                        evidence={"ready": True},
                    )
                },
            ),
        )
        if include_execution_prerequisites
        else ()
    )
    executions = PreflightDag(
        (*(_dependency(check_id) for check_id in external), *checks, *execution_prerequisites)
    ).run(context, through_tier=3, now=lambda: NOW)
    return (
        PreflightRehearsal.from_executions(
            registry_digest="9" * 64,
            coverage_digest="a" * 64,
            executions=executions,
        ),
        context,
    )


def test_restore_proof_binds_every_tier_three_action_and_cleanup(tmp_path: Path) -> None:
    checkpoint = _checkpoint(tmp_path)
    rehearsal, context = _rehearsal(checkpoint)

    evidence = build_restore_verification_evidence(
        checkpoint,
        rehearsal,
        context=context,
        verified_at=NOW,
    )

    assert evidence.request_id == checkpoint.request_id
    assert evidence.checkpoint_evidence_sha256 == checkpoint.evidence_digest
    assert evidence.db_snapshot_identity == checkpoint.db_snapshot_identity
    assert evidence.checkpoint_schema_version == 3
    assert evidence.component_sha256 == checkpoint.component_sha256
    assert evidence.database_authority_digest == checkpoint.database_authority_digest
    assert evidence.manager_configuration_digest == checkpoint.manager_configuration_digest
    assert len(evidence.report_sha256) == 64
    assert evidence.verification_id.startswith("restore-")


def test_restore_proof_allows_passing_non_rehearsal_tier_three_evidence(
    tmp_path: Path,
) -> None:
    checkpoint = _checkpoint(tmp_path)
    rehearsal, context = _rehearsal(
        checkpoint,
        include_execution_prerequisites=True,
    )

    evidence = build_restore_verification_evidence(
        checkpoint,
        rehearsal,
        context=context,
        verified_at=NOW,
    )

    assert evidence.checkpoint_evidence_sha256 == checkpoint.evidence_digest


def test_restore_proof_rejects_context_drift_and_incomplete_cleanup(tmp_path: Path) -> None:
    checkpoint = _checkpoint(tmp_path)
    rehearsal, context = _rehearsal(checkpoint)
    drifted = dict(context.bindings)
    drifted["staging.mutation-epoch"] = 18

    with pytest.raises(ValueError, match="context"):
        build_restore_verification_evidence(
            checkpoint,
            rehearsal,
            context=CheckContext(drifted),
            verified_at=NOW,
        )

    incomplete, context = _rehearsal(checkpoint, cleanup=False)
    with pytest.raises(ValueError, match="passing rehearsal"):
        build_restore_verification_evidence(
            checkpoint,
            incomplete,
            context=context,
            verified_at=NOW,
        )


def test_restore_proof_rejects_tampered_execution_evidence(tmp_path: Path) -> None:
    checkpoint = _checkpoint(tmp_path)
    rehearsal, context = _rehearsal(checkpoint)
    executions = list(rehearsal.executions)
    index = next(
        index
        for index, execution in enumerate(executions)
        if execution.check_id == "rehearsal.db-clone"
    )
    executions[index] = replace(
        executions[index],
        evidence={**executions[index].evidence, "observed-epoch": 18},
    )
    tampered = replace(rehearsal, executions=tuple(executions))

    with pytest.raises(ValueError, match="rehearsal authority"):
        build_restore_verification_evidence(
            checkpoint,
            tampered,
            context=context,
            verified_at=NOW,
        )
