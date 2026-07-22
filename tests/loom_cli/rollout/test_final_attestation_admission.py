from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.final_attestation_admission import (
    validate_final_attestation,
    validate_post_apply_attestation_drift,
)
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
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
    external_supervisor_unit_set_digest,
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


def _baseline_check(
    check_id: str,
    *,
    dependencies: tuple[str, ...],
    resource_digest: str,
) -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=check_id,
            failure_code=f"{check_id}.failed",
            tier=2,
            stage=StageCapability.BASELINE_LIVE_READONLY,
            dependencies=dependencies,
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(
                EvidenceField("ready", "boolean"),
                EvidenceField("observed-epoch", "integer"),
                EvidenceField("readonly-principal", "string"),
                EvidenceField("resource-digest", "sha256"),
                EvidenceField("blockers", "string-map"),
            ),
            timeout_seconds=5,
            freshness_ttl_seconds=120,
            remediation=f"restore {check_id}",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={
                    "ready": True,
                    "observed-epoch": 7,
                    "readonly-principal": "system:serviceaccount:loom-staging:readonly",
                    "resource-digest": resource_digest,
                    "blockers": {},
                },
            )
        },
    )


def _checks(
    *,
    boot_id: str = "boot-1",
    baseline_digest: str = "6" * 64,
    predecessor_live_digest: str = "d" * 64,
    predecessor_pool_digest: str = "2" * 64,
) -> tuple[RegisteredCheck, ...]:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "e" * 64,
        "loom-autoscaler-gb10-staging.timer": "f" * 64,
    }
    tier0 = (
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
            "gb10.candidate-source",
            {"source-digest": "b" * 64},
            (EvidenceField("source-digest", "sha256"),),
        ),
        _check(
            "gb10.host-readiness",
            {"inventory-digest": "4" * 64, "boot-ids": {"gb10-1": boot_id}},
            (EvidenceField("inventory-digest", "sha256"), EvidenceField("boot-ids", "string-map")),
        ),
        _check(
            "external-supervisor.predecessor",
            {
                "authority-kind": "legacy-manifest",
                "authority-digest": "c" * 64,
                "pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
                "unit-digests": predecessor_units,
                "unit-set-digest": external_supervisor_unit_set_digest(predecessor_units),
                "live-evidence-digest": predecessor_live_digest,
                "pending-transition-digest": "1" * 64,
                "transition-clear": True,
                "runtime-ready": True,
                "pool-identity-digest": predecessor_pool_digest,
            },
            (
                EvidenceField("authority-kind", "string"),
                EvidenceField("authority-digest", "sha256"),
                EvidenceField("pointer-digest", "sha256"),
                EvidenceField("unit-digests", "string-map"),
                EvidenceField("unit-set-digest", "sha256"),
                EvidenceField("live-evidence-digest", "sha256"),
                EvidenceField("pending-transition-digest", "sha256"),
                EvidenceField("transition-clear", "boolean"),
                EvidenceField("runtime-ready", "boolean"),
                EvidenceField("pool-identity-digest", "sha256"),
            ),
        ),
    )
    tier2 = (
        _baseline_check(
            "staging.health",
            dependencies=(),
            resource_digest=baseline_digest,
        ),
        _baseline_check(
            "staging.auth",
            dependencies=("staging.health",),
            resource_digest="7" * 64,
        ),
        _baseline_check(
            "staging.catalog-task",
            dependencies=("staging.auth",),
            resource_digest="8" * 64,
        ),
        _baseline_check(
            "staging.storage-db",
            dependencies=("staging.health",),
            resource_digest="9" * 64,
        ),
        _baseline_check(
            "staging.network",
            dependencies=("staging.health",),
            resource_digest="a" * 64,
        ),
        _baseline_check(
            "staging.release-baseline",
            dependencies=(
                "staging.catalog-task",
                "staging.storage-db",
                "staging.network",
            ),
            resource_digest="b" * 64,
        ),
    )
    return tier0 + tier2


def _bindings() -> AttestationBindings:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "e" * 64,
        "loom-autoscaler-gb10-staging.timer": "f" * 64,
    }
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
        supervisor_predecessor_kind="legacy-manifest",
        supervisor_predecessor_digest="c" * 64,
        supervisor_predecessor_pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        supervisor_predecessor_unit_sha256=predecessor_units,
        supervisor_predecessor_unit_set_digest=external_supervisor_unit_set_digest(
            predecessor_units
        ),
        supervisor_predecessor_live_evidence_digest="d" * 64,
        supervisor_predecessor_pending_transition_digest="1" * 64,
        supervisor_transition_digest="2" * 64,
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
    executions = PreflightDag(
        tuple(check for check in plan.registry.checks if check.spec.tier in {0, 2})
    ).run(plan.context, through_tier=2, now=lambda: NOW - timedelta(minutes=1))
    return PreflightAttestation(
        schema_version=2,
        bindings=_bindings(),
        registry_digest=plan.registry.registry_digest,
        coverage_digest=plan.registry.coverage_digest,
        check_implementation_digests=implementations,
        evidence_hashes={execution.check_id: execution.evidence_hash for execution in executions},
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

    assert len(admission.tier0_executions) == 7
    assert all(execution.passed for execution in admission.tier0_executions)
    assert len(admission.tier2_executions) == 6
    assert all(execution.passed for execution in admission.tier2_executions)
    assert admission.preflight_plan is plan


def test_final_admission_rejects_pool_identity_drift() -> None:
    attested_plan = _plan(_checks(predecessor_pool_digest="2" * 64))
    drifted_plan = _plan(_checks(predecessor_pool_digest="3" * 64))

    with pytest.raises(ValueError, match="evidence changed"):
        validate_final_attestation(
            attestation=_attestation(attested_plan),
            candidate=_candidate(),
            plan=drifted_plan,
            current_mutation_epoch=7,
            now=NOW,
        )


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


def test_final_admission_rejects_tier2_baseline_drift() -> None:
    attested_plan = _plan(_checks())
    drifted_plan = _plan(_checks(baseline_digest="c" * 64))

    with pytest.raises(ValueError, match="Tier 2 baseline changed"):
        validate_final_attestation(
            attestation=_attestation(attested_plan),
            candidate=_candidate(),
            plan=drifted_plan,
            current_mutation_epoch=7,
            now=NOW,
        )


def test_final_admission_rejects_predecessor_drift_before_apply() -> None:
    attested_plan = _plan(_checks())
    drifted_plan = _plan(_checks(predecessor_live_digest="0" * 64))

    with pytest.raises(ValueError, match="evidence changed"):
        validate_final_attestation(
            attestation=_attestation(attested_plan),
            candidate=_candidate(),
            plan=drifted_plan,
            current_mutation_epoch=7,
            now=NOW,
        )


def test_post_apply_drift_reuses_exact_admission_without_baseline_replay() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    post_apply_plan = _plan(_checks(predecessor_live_digest="0" * 64))
    evidence = validate_post_apply_attestation_drift(
        admission=admission,
        plan=post_apply_plan,
        current_mutation_epoch=8,
        now=NOW,
    )

    assert evidence.observed_mutation_epoch == 8
    assert {execution.check_id for execution in evidence.executions} == {
        "candidate.identity",
        "runner.install",
        "credentials.metadata",
        "gb10.candidate-source",
        "gb10.shared-mount",
        "gb10.host-readiness",
    }
    assert len(evidence.evidence_digest) == 64


def test_post_apply_drift_rejects_wrong_epoch_or_expired_attestation() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    with pytest.raises(ValueError, match="mutation epoch"):
        validate_post_apply_attestation_drift(
            admission=admission,
            plan=plan,
            current_mutation_epoch=7,
            now=NOW,
        )
    with pytest.raises(ValueError, match="mutation epoch or attestation drifted"):
        validate_post_apply_attestation_drift(
            admission=admission,
            plan=plan,
            current_mutation_epoch=8,
            now=NOW + timedelta(minutes=31),
        )


def test_post_apply_drift_reruns_and_rejects_changed_host_identity() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )
    drifted_plan = _plan(_checks(boot_id="boot-2"))

    with pytest.raises(ValueError, match="drift-sensitive evidence changed"):
        validate_post_apply_attestation_drift(
            admission=admission,
            plan=drifted_plan,
            current_mutation_epoch=8,
            now=NOW,
        )


def test_post_apply_drift_rejects_route_or_config_context_drift() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )
    drifted_bindings = dict(plan.context.bindings)
    drifted_bindings["route"] = "https://yylx.world/other"
    drifted_plan = replace(plan, context=CheckContext(drifted_bindings))

    with pytest.raises(ValueError, match="context binding drifted"):
        validate_post_apply_attestation_drift(
            admission=admission,
            plan=drifted_plan,
            current_mutation_epoch=8,
            now=NOW,
        )
