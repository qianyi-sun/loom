from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.final_attestation_admission import (
    PostApplyDriftTransientError,
    validate_final_attestation,
    validate_post_apply_attestation_drift,
    validate_post_apply_resume_attestation,
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
    ready: bool = True,
    observed_epoch: int = 7,
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
                passed=ready,
                evidence={
                    "ready": ready,
                    "observed-epoch": observed_epoch,
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
    credential_metadata: dict[str, str] | None = None,
    predecessor_kind: str = "legacy-manifest",
    predecessor_pointer_digest: str = EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    predecessor_live_digest: str = "d" * 64,
    predecessor_runtime_state: str = "ready",
    predecessor_pending_transition_digest: str = "1" * 64,
    predecessor_transition_clear: bool = True,
    predecessor_runtime_ready: bool = True,
    oldlab_predecessor_live_digest: str = "7" * 64,
    controller_binding_overrides: dict[str, str] | None = None,
    predecessor_pool_digest: str = "2" * 64,
    gb10_inventory_digest: str = "4" * 64,
    baseline_ready: bool = True,
    baseline_epoch: int = 7,
) -> tuple[RegisteredCheck, ...]:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "e" * 64,
        "loom-autoscaler-gb10-staging.timer": "f" * 64,
    }
    oldlab_predecessor_units = {
        "loom-autoscaler-oldlab-staging.service": "3" * 64,
        "loom-autoscaler-oldlab-staging.timer": "4" * 64,
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
            {
                "metadata-fingerprints": (
                    {"admin": "abcd"} if credential_metadata is None else credential_metadata
                )
            },
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
            {"inventory-digest": gb10_inventory_digest, "boot-ids": {"gb10-1": boot_id}},
            (EvidenceField("inventory-digest", "sha256"), EvidenceField("boot-ids", "string-map")),
        ),
        _check(
            "external-supervisor.predecessor",
            {
                "authority-kind": predecessor_kind,
                "authority-digest": "c" * 64,
                "pointer-digest": predecessor_pointer_digest,
                "unit-digests": predecessor_units,
                "unit-set-digest": external_supervisor_unit_set_digest(predecessor_units),
                "live-evidence-digest": predecessor_live_digest,
                "pending-transition-digest": predecessor_pending_transition_digest,
                "transition-clear": predecessor_transition_clear,
                "runtime-ready": predecessor_runtime_ready,
                "pool-identity-digest": predecessor_pool_digest,
                "controller-bindings": {
                    "gx10-01c7/authority-kind": predecessor_kind,
                    "gx10-01c7/authority-digest": "c" * 64,
                    "gx10-01c7/pointer-digest": predecessor_pointer_digest,
                    "gx10-01c7/unit-set-digest": external_supervisor_unit_set_digest(
                        predecessor_units
                    ),
                    "gx10-01c7/live-evidence-digest": predecessor_live_digest,
                    "gx10-01c7/pending-transition-digest": (predecessor_pending_transition_digest),
                    "gx10-01c7/runtime-state": predecessor_runtime_state,
                    "gx10-01c7/unit-directory": "/var/lib/loom-rollout/.config/systemd/user",
                    **{
                        f"gx10-01c7/unit/{name}": digest
                        for name, digest in predecessor_units.items()
                    },
                    "TRT-EAI-OLDLAB-1/authority-kind": "legacy-manifest",
                    "TRT-EAI-OLDLAB-1/authority-digest": "6" * 64,
                    "TRT-EAI-OLDLAB-1/pointer-digest": (EXTERNAL_SUPERVISOR_ABSENT_DIGEST),
                    "TRT-EAI-OLDLAB-1/unit-set-digest": (
                        external_supervisor_unit_set_digest(oldlab_predecessor_units)
                    ),
                    "TRT-EAI-OLDLAB-1/live-evidence-digest": (oldlab_predecessor_live_digest),
                    "TRT-EAI-OLDLAB-1/pending-transition-digest": "1" * 64,
                    "TRT-EAI-OLDLAB-1/runtime-state": "ready",
                    "TRT-EAI-OLDLAB-1/unit-directory": (
                        "/var/lib/loom-staging-rollout/.config/systemd/user"
                    ),
                    **{
                        f"TRT-EAI-OLDLAB-1/unit/{name}": digest
                        for name, digest in oldlab_predecessor_units.items()
                    },
                    **(
                        {} if controller_binding_overrides is None else controller_binding_overrides
                    ),
                },
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
                EvidenceField("controller-bindings", "string-map"),
            ),
        ),
    )
    tier2 = (
        _baseline_check(
            "staging.health",
            dependencies=(),
            resource_digest=baseline_digest,
            ready=baseline_ready,
            observed_epoch=baseline_epoch,
        ),
        _baseline_check(
            "staging.auth",
            dependencies=("staging.health",),
            resource_digest="7" * 64,
            observed_epoch=baseline_epoch,
        ),
        _baseline_check(
            "staging.catalog-task",
            dependencies=("staging.auth",),
            resource_digest="8" * 64,
            observed_epoch=baseline_epoch,
        ),
        _baseline_check(
            "staging.storage-db",
            dependencies=("staging.health",),
            resource_digest="9" * 64,
            observed_epoch=baseline_epoch,
        ),
        _baseline_check(
            "staging.network",
            dependencies=("staging.health",),
            resource_digest="a" * 64,
            observed_epoch=baseline_epoch,
        ),
        _baseline_check(
            "staging.release-baseline",
            dependencies=(
                "staging.catalog-task",
                "staging.storage-db",
                "staging.network",
            ),
            resource_digest="b" * 64,
            observed_epoch=baseline_epoch,
        ),
    )
    return tier0 + tier2


def _bindings() -> AttestationBindings:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "e" * 64,
        "loom-autoscaler-gb10-staging.timer": "f" * 64,
    }
    oldlab_predecessor_units = {
        "loom-autoscaler-oldlab-staging.service": "3" * 64,
        "loom-autoscaler-oldlab-staging.timer": "4" * 64,
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
        supervisor_controller_bindings={
            "gx10-01c7/authority-kind": "legacy-manifest",
            "gx10-01c7/authority-digest": "c" * 64,
            "gx10-01c7/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            "gx10-01c7/unit-set-digest": external_supervisor_unit_set_digest(predecessor_units),
            "gx10-01c7/live-evidence-digest": "d" * 64,
            "gx10-01c7/pending-transition-digest": "1" * 64,
            "gx10-01c7/runtime-state": "ready",
            "gx10-01c7/unit-directory": "/var/lib/loom-rollout/.config/systemd/user",
            "gx10-01c7/transition-digest": "2" * 64,
            **{f"gx10-01c7/unit/{name}": digest for name, digest in predecessor_units.items()},
            "TRT-EAI-OLDLAB-1/authority-kind": "legacy-manifest",
            "TRT-EAI-OLDLAB-1/authority-digest": "6" * 64,
            "TRT-EAI-OLDLAB-1/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            "TRT-EAI-OLDLAB-1/unit-set-digest": external_supervisor_unit_set_digest(
                oldlab_predecessor_units
            ),
            "TRT-EAI-OLDLAB-1/live-evidence-digest": "7" * 64,
            "TRT-EAI-OLDLAB-1/pending-transition-digest": "1" * 64,
            "TRT-EAI-OLDLAB-1/runtime-state": "ready",
            "TRT-EAI-OLDLAB-1/unit-directory": (
                "/var/lib/loom-staging-rollout/.config/systemd/user"
            ),
            "TRT-EAI-OLDLAB-1/transition-digest": "8" * 64,
            **{
                f"TRT-EAI-OLDLAB-1/unit/{name}": digest
                for name, digest in oldlab_predecessor_units.items()
            },
        },
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


def _at_epoch(plan: CandidatePreflightPlan, mutation_epoch: int) -> CandidatePreflightPlan:
    bindings = dict(plan.context.bindings)
    bindings["staging.mutation-epoch"] = mutation_epoch
    return replace(plan, context=CheckContext(bindings))


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


def test_final_admission_tolerates_pool_identity_drift() -> None:
    # pool-identity-digest is a live count of external-supervisor worker rows per
    # pool; it shifts with ordinary worker registration between the restore
    # rehearsal and this final admission and can never re-match a fixed
    # attestation snapshot. It is deliberately NOT gated here: the drift-sensitive
    # authority/transition fields (rejected in
    # test_final_admission_rejects_predecessor_drift_before_apply) are re-checked
    # individually, so a pool-identity-only change must still admit.
    attested_plan = _plan(_checks(predecessor_pool_digest="2" * 64))
    drifted_plan = _plan(_checks(predecessor_pool_digest="3" * 64))

    admission = validate_final_attestation(
        attestation=_attestation(attested_plan),
        candidate=_candidate(),
        plan=drifted_plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    assert all(execution.passed for execution in admission.tier0_executions)


def test_final_admission_tolerates_supervisor_runtime_convergence() -> None:
    canonical_pointer_digest = "9" * 64
    attested_plan = _plan(
        _checks(
            predecessor_kind="canonical",
            predecessor_pointer_digest=canonical_pointer_digest,
            predecessor_live_digest="d" * 64,
            predecessor_runtime_state="repairable",
        )
    )
    attestation = _attestation(attested_plan)
    attested_controller_bindings = dict(attestation.bindings.supervisor_controller_bindings)
    attested_controller_bindings["gx10-01c7/authority-kind"] = "canonical"
    attested_controller_bindings["gx10-01c7/pointer-digest"] = canonical_pointer_digest
    attested_controller_bindings["gx10-01c7/runtime-state"] = "repairable"
    attestation = replace(
        attestation,
        bindings=replace(
            attestation.bindings,
            supervisor_predecessor_kind="canonical",
            supervisor_predecessor_pointer_digest=canonical_pointer_digest,
            supervisor_controller_bindings=attested_controller_bindings,
        ),
    )
    converged_plan = _plan(
        _checks(
            predecessor_kind="canonical",
            predecessor_pointer_digest=canonical_pointer_digest,
            predecessor_live_digest="0" * 64,
            predecessor_runtime_state="ready",
        )
    )

    admission = validate_final_attestation(
        attestation=attestation,
        candidate=_candidate(),
        plan=converged_plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    assert all(execution.passed for execution in admission.tier0_executions)


def test_final_admission_tolerates_other_controller_live_evidence_drift() -> None:
    attested_plan = _plan(_checks())
    drifted_plan = _plan(_checks(oldlab_predecessor_live_digest="9" * 64))

    admission = validate_final_attestation(
        attestation=_attestation(attested_plan),
        candidate=_candidate(),
        plan=drifted_plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    assert all(execution.passed for execution in admission.tier0_executions)


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    (
        ("authority-kind", "absent"),
        ("authority-digest", "0" * 64),
        ("pointer-digest", "0" * 64),
        ("unit/loom-autoscaler-gb10-staging.service", "0" * 64),
        ("unit-set-digest", "0" * 64),
        ("pending-transition-digest", "0" * 64),
        ("unit-directory", "/var/lib/loom-staging-rollout/.config/systemd/user"),
    ),
)
def test_final_admission_rejects_stable_controller_binding_drift(
    field: str,
    drifted_value: str,
) -> None:
    attested_plan = _plan(_checks())
    drifted_plan = _plan(
        _checks(
            controller_binding_overrides={f"gx10-01c7/{field}": drifted_value},
        )
    )

    with pytest.raises(ValueError, match="drift-sensitive evidence changed"):
        validate_final_attestation(
            attestation=_attestation(attested_plan),
            candidate=_candidate(),
            plan=drifted_plan,
            current_mutation_epoch=7,
            now=NOW,
        )


@pytest.mark.parametrize(
    ("transition_clear", "runtime_ready"),
    ((False, True), (True, False)),
)
def test_final_admission_rejects_unclear_transition_or_failed_runtime(
    transition_clear: bool,
    runtime_ready: bool,
) -> None:
    plan = _plan(
        _checks(
            predecessor_transition_clear=transition_clear,
            predecessor_runtime_ready=runtime_ready,
        )
    )

    with pytest.raises(ValueError, match="drift-sensitive evidence changed"):
        validate_final_attestation(
            attestation=_attestation(plan),
            candidate=_candidate(),
            plan=plan,
            current_mutation_epoch=7,
            now=NOW,
        )


def test_final_admission_tolerates_gb10_inventory_drift() -> None:
    # gb10.host-readiness inventory-digest folds in each host's node-agent
    # service/timer runtime state, which cycles between the restore rehearsal and
    # this re-check. It is deliberately NOT gated: fleet readiness (the check must
    # pass) and boot-ids (rejected in test_final_admission_rejects_host_boot_or_epoch_drift)
    # cover meaningful gb10 drift, so an inventory-digest-only change must admit.
    attested_plan = _plan(_checks(gb10_inventory_digest="4" * 64))
    drifted_plan = _plan(_checks(gb10_inventory_digest="5" * 64))

    admission = validate_final_attestation(
        attestation=_attestation(attested_plan),
        candidate=_candidate(),
        plan=drifted_plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    assert all(execution.passed for execution in admission.tier0_executions)


def test_final_and_post_apply_admission_tolerate_tokenrequest_kubeconfig_rotation() -> None:
    attested_metadata = {
        "admin": "abcd",
        "readonly-kubeconfig": "1" * 64,
        "rehearsal-kubeconfig": "2" * 64,
    }
    attested_plan = _plan(_checks(credential_metadata=attested_metadata))
    attestation = _attestation(attested_plan)
    attestation = replace(
        attestation,
        bindings=replace(
            attestation.bindings,
            secret_metadata_fingerprints={
                key: f"sha256:{value}" for key, value in attested_metadata.items()
            },
        ),
    )
    final_plan = _plan(
        _checks(
            credential_metadata={
                **attested_metadata,
                "readonly-kubeconfig": "3" * 64,
                "rehearsal-kubeconfig": "4" * 64,
            }
        )
    )

    admission = validate_final_attestation(
        attestation=attestation,
        candidate=_candidate(),
        plan=final_plan,
        current_mutation_epoch=7,
        now=NOW,
    )
    post_apply_plan = _at_epoch(
        _plan(
            _checks(
                credential_metadata={
                    **attested_metadata,
                    "readonly-kubeconfig": "5" * 64,
                    "rehearsal-kubeconfig": "6" * 64,
                }
            )
        ),
        8,
    )

    evidence = validate_post_apply_attestation_drift(
        admission=admission,
        plan=post_apply_plan,
        current_mutation_epoch=8,
        now=NOW,
    )

    assert evidence.observed_mutation_epoch == 8


def test_final_admission_still_rejects_stable_credential_or_source_set_drift() -> None:
    attested_metadata = {
        "admin": "abcd",
        "readonly-kubeconfig": "1" * 64,
        "rehearsal-kubeconfig": "2" * 64,
    }
    attested_plan = _plan(_checks(credential_metadata=attested_metadata))
    attestation = _attestation(attested_plan)
    attestation = replace(
        attestation,
        bindings=replace(
            attestation.bindings,
            secret_metadata_fingerprints={
                key: f"sha256:{value}" for key, value in attested_metadata.items()
            },
        ),
    )
    for drifted_metadata in (
        {**attested_metadata, "admin": "9" * 64},
        {
            name: fingerprint
            for name, fingerprint in attested_metadata.items()
            if name != "rehearsal-kubeconfig"
        },
    ):
        drifted_plan = _plan(_checks(credential_metadata=drifted_metadata))
        with pytest.raises(ValueError, match="drift-sensitive evidence changed"):
            validate_final_attestation(
                attestation=attestation,
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


def test_final_admission_tolerates_tier2_baseline_resource_drift() -> None:
    # A Tier 2 baseline's resource-digest is a live hash of the probed staging
    # resource (auth/release-baseline/storage-db) that shifts with ordinary
    # traffic between the restore rehearsal and this re-check. It is deliberately
    # NOT byte-matched against the attestation -- the baseline must only be
    # re-verified HEALTHY (ready/epoch/principal/unblocked), which
    # test_final_admission_rejects_unhealthy_tier2_baseline covers.
    attested_plan = _plan(_checks())
    drifted_plan = _plan(_checks(baseline_digest="c" * 64))

    admission = validate_final_attestation(
        attestation=_attestation(attested_plan),
        candidate=_candidate(),
        plan=drifted_plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    assert all(execution.passed for execution in admission.tier2_executions)


def test_final_admission_rejects_unhealthy_tier2_baseline() -> None:
    # A baseline that is no longer ready must still fail final admission even
    # though its resource-digest is not byte-matched.
    plan = _plan(_checks(baseline_ready=False))

    with pytest.raises(ValueError, match="Tier 0 drift check failed"):
        validate_final_attestation(
            attestation=_attestation(plan),
            candidate=_candidate(),
            plan=plan,
            current_mutation_epoch=7,
            now=NOW,
        )


def test_final_admission_rejects_predecessor_drift_before_apply() -> None:
    attested_plan = _plan(_checks())
    drifted_plan = _plan(_checks(predecessor_pending_transition_digest="0" * 64))

    with pytest.raises(ValueError, match="evidence changed"):
        validate_final_attestation(
            attestation=_attestation(attested_plan),
            candidate=_candidate(),
            plan=drifted_plan,
            current_mutation_epoch=7,
            now=NOW,
        )


def test_post_apply_drift_uses_fresh_epoch_plan_without_baseline_replay() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )

    post_apply_plan = _at_epoch(
        _plan(_checks(predecessor_live_digest="0" * 64)),
        8,
    )
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


def test_post_apply_drift_classifies_changed_host_identity_as_transient() -> None:
    plan = _plan(_checks())
    admission = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )
    drifted_plan = _at_epoch(_plan(_checks(boot_id="boot-2")), 8)

    with pytest.raises(
        PostApplyDriftTransientError,
        match="drift-sensitive evidence changed",
    ):
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
    drifted_bindings["staging.mutation-epoch"] = 8
    drifted_plan = replace(plan, context=CheckContext(drifted_bindings))

    with pytest.raises(ValueError, match="context binding drifted"):
        validate_post_apply_attestation_drift(
            admission=admission,
            plan=drifted_plan,
            current_mutation_epoch=8,
            now=NOW,
        )


def test_post_apply_resume_reuses_original_baseline_after_current_health_recheck() -> None:
    plan = _plan(_checks())
    prior = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )
    post_apply_plan = _at_epoch(
        _plan(
            _checks(
                baseline_epoch=8,
                predecessor_live_digest="0" * 64,
            )
        ),
        8,
    )

    resumed = validate_post_apply_resume_attestation(
        prior_admission=prior,
        candidate=_candidate(),
        plan=post_apply_plan,
        current_mutation_epoch=8,
        now=NOW + timedelta(hours=2),
    )

    assert resumed.attestation is prior.attestation
    assert resumed.tier0_executions == prior.tier0_executions
    assert resumed.tier2_executions == prior.tier2_executions
    assert resumed.preflight_plan is post_apply_plan
    assert resumed.post_apply_resume is True


def test_post_apply_resume_rejects_unhealthy_current_baseline() -> None:
    plan = _plan(_checks())
    prior = validate_final_attestation(
        attestation=_attestation(plan),
        candidate=_candidate(),
        plan=plan,
        current_mutation_epoch=7,
        now=NOW,
    )
    post_apply_plan = _at_epoch(
        _plan(_checks(baseline_epoch=8, baseline_ready=False)),
        8,
    )

    with pytest.raises(ValueError, match="post-apply resume baseline"):
        validate_post_apply_resume_attestation(
            prior_admission=prior,
            candidate=_candidate(),
            plan=post_apply_plan,
            current_mutation_epoch=8,
            now=NOW,
        )
