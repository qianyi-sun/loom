from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.final_attestation_admission import validate_final_attestation
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightAttestation,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_registry import PreflightRegistry

NOW = datetime(2026, 7, 19, 22, tzinfo=UTC)


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T21:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )


def _check(
    check_id: str,
    evidence: dict[str, object],
    schema: tuple[EvidenceField, ...],
    *,
    policy: SecretRedactionPolicy = SecretRedactionPolicy.NO_SECRET_INPUTS,
) -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=check_id,
            failure_code=f"{check_id}.failed",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=schema,
            timeout_seconds=5,
            freshness_ttl_seconds=120,
            remediation=f"restore {check_id}",
            secret_redaction_policy=policy,
        ),
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence=evidence,  # type: ignore[arg-type]
            )
        },
    )


def _checks(*, boot_id: str = "boot-1") -> tuple[RegisteredCheck, ...]:
    return (
        _check(
            "candidate.identity",
            {"resolved-sha": "a" * 40, "resolved-tree": "b" * 40},
            (EvidenceField("resolved-sha", "string"), EvidenceField("resolved-tree", "string")),
        ),
        _check(
            "runner.install",
            {"attestation-digest": "2" * 64},
            (EvidenceField("attestation-digest", "sha256"),),
        ),
        _check(
            "credentials.metadata",
            {"metadata-fingerprints": {"admin": "abcd"}},
            (EvidenceField("metadata-fingerprints", "string-map"),),
            policy=SecretRedactionPolicy.METADATA_FINGERPRINTS_ONLY,
        ),
        _check(
            "gb10.shared-mount",
            {"mount-digest": "3" * 64},
            (EvidenceField("mount-digest", "sha256"),),
        ),
        _check(
            "gb10.host-readiness",
            {"inventory-digest": "4" * 64, "boot-ids": {"gb10-1": boot_id}},
            (EvidenceField("inventory-digest", "sha256"), EvidenceField("boot-ids", "string-map")),
        ),
    )


def _bindings() -> AttestationBindings:
    return AttestationBindings(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_digests={"control-plane": "sha256:" + "1" * 64},
        runner_source_sha="a" * 40,
        runner_source_tree="b" * 40,
        runner_install_hash="2" * 64,
        runner_config_hash="5" * 64,
        staging_mutation_epoch=7,
        backup_lease_id="lease-1",
        backup_lease_digest="6" * 64,
        backup_manifest_sha256="7" * 64,
        backup_component_set_digest="8" * 64,
        db_snapshot_identity="snapshot-1",
        schema_revision="0067",
        object_inventory_root="9" * 64,
        migration_plan_digest="a" * 64,
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        secret_metadata_fingerprints={"admin": "sha256:abcd"},
        gb10_inventory_digest="4" * 64,
        gb10_boot_ids={"gb10-1": "boot-1"},
        gb10_mount_digest="3" * 64,
        gb10_unit_digest="b" * 64,
        browser_image_digest="sha256:" + "c" * 64,
        browser_report_schema="v3",
    )


def _plan(checks: tuple[RegisteredCheck, ...]) -> CandidatePreflightPlan:
    candidate = _candidate()
    registry = PreflightRegistry(
        checks=checks,
        through_tier=3,
        coverage_digest="d" * 64,
        registry_digest="e" * 64,
    )
    return CandidatePreflightPlan(
        candidate=candidate,
        registry=registry,
        context=CheckContext(
            {
                "candidate.base.sha": candidate.approved_base_sha or "none",
                "candidate.sha": candidate.resolved_sha,
                "candidate.source-mode": candidate.source_mode,
                "candidate.tree": candidate.resolved_tree or "none",
                "environment": "staging",
                "namespace": "loom-staging",
                "route": "https://yylx.world/dev",
                "runner.config.sha256": "5" * 64,
                "staging.mutation-epoch": 7,
            }
        ),
    )


def _attestation(plan: CandidatePreflightPlan) -> PreflightAttestation:
    implementations = plan.registry.implementation_digests
    return PreflightAttestation(
        schema_version=1,
        bindings=_bindings(),
        registry_digest=plan.registry.registry_digest,
        coverage_digest=plan.registry.coverage_digest,
        check_implementation_digests=implementations,
        evidence_hashes={check_id: "f" * 64 for check_id in implementations},
        issued_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=30),
        attestation_digest="1" * 64,
    )


def test_final_admission_rechecks_exact_drift_sensitive_tier0() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    assert len(admission.tier0_executions) == 5
    assert all(execution.passed for execution in admission.tier0_executions)


def test_final_admission_rejects_host_boot_or_epoch_drift() -> None:
    drifted_plan = _plan(_checks(boot_id="boot-2"))
    with pytest.raises(ValueError, match="evidence changed"):
        validate_final_attestation(
            attestation=_attestation(drifted_plan),
            candidate=_candidate(),
            plan=drifted_plan,
            current_mutation_epoch=7,
            now=NOW,
        )

    plan = _plan(_checks())
    with pytest.raises(ValueError, match="identity drifted"):
        validate_final_attestation(
            attestation=_attestation(plan),
            candidate=_candidate(),
            plan=plan,
            current_mutation_epoch=8,
            now=NOW,
        )
