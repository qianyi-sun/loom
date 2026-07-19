from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckOperation,
    CheckOutcome,
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

NOW = datetime(2026, 7, 19, 16, tzinfo=UTC)


def _spec(
    check_id: str,
    *,
    dependencies: tuple[str, ...] = (),
    tier: int = 0,
    stage: StageCapability = StageCapability.STATIC,
    mutation_class: MutationClass = MutationClass.NONE,
    final_only_justification: str | None = None,
    run_after_failed_dependencies: bool = False,
) -> CheckSpec:
    return CheckSpec(
        check_id=check_id,
        failure_code=f"{check_id}.failed",
        tier=tier,
        stage=stage,
        dependencies=dependencies,
        mutation_class=mutation_class,
        input_keys=("candidate.sha",),
        evidence_schema=(EvidenceField("status.value", "string"),),
        timeout_seconds=5,
        freshness_ttl_seconds=600,
        remediation=f"repair {check_id}",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        final_only_justification=final_only_justification,
        run_after_failed_dependencies=run_after_failed_dependencies,
    )


def _check(
    check_id: str,
    *,
    passed: bool = True,
    dependencies: tuple[str, ...] = (),
    delay: float = 0,
) -> RegisteredCheck:
    def probe(_context: CheckContext) -> CheckProbe:
        if delay:
            time.sleep(delay)
        return CheckProbe(passed=passed, evidence={"status.value": "ready"})

    return RegisteredCheck(
        spec=_spec(check_id, dependencies=dependencies),
        implementation_version="v1",
        operations={CheckOperation.PROBE: probe},
    )


def _context() -> CheckContext:
    return CheckContext(bindings={"candidate.sha": "a" * 40})


def test_check_spec_rejects_mutation_before_isolated_stage() -> None:
    with pytest.raises(ValueError, match="mutation class"):
        _spec("unsafe.check", mutation_class=MutationClass.PROTECTED_STAGING)


def test_final_only_requires_technical_justification() -> None:
    with pytest.raises(ValueError, match="technical justification"):
        _spec(
            "final.check",
            tier=4,
            stage=StageCapability.FINAL_ONLY,
            mutation_class=MutationClass.PROTECTED_STAGING,
        )


def test_dag_runs_independent_checks_and_reports_all_failures() -> None:
    dag = PreflightDag(
        [
            _check("candidate.identity"),
            _check("browser.token", passed=False),
            _check("gb10.timer", passed=False),
            _check("browser.launch", dependencies=("browser.token",)),
        ],
        max_concurrency=4,
    )
    results = dag.run(_context(), now=lambda: NOW)
    outcomes = {result.check_id: result.outcome for result in results}
    assert outcomes == {
        "browser.launch": CheckOutcome.BLOCKED,
        "browser.token": CheckOutcome.FAIL,
        "candidate.identity": CheckOutcome.PASS,
        "gb10.timer": CheckOutcome.FAIL,
    }
    browser_launch = next(result for result in results if result.check_id == "browser.launch")
    assert browser_launch.blocked_by == ("browser.token",)


def test_dag_runs_isolated_cleanup_after_failed_dependencies() -> None:
    cleanup_calls: list[str] = []

    def cleanup(_context: CheckContext) -> CheckProbe:
        cleanup_calls.append("cleanup")
        return CheckProbe(passed=True, evidence={"status.value": "clean"})

    cleanup_check = RegisteredCheck(
        spec=_spec(
            "rehearsal.cleanup",
            dependencies=("rehearsal.browser",),
            tier=3,
            stage=StageCapability.ISOLATED_REHEARSAL,
            mutation_class=MutationClass.ISOLATED,
            run_after_failed_dependencies=True,
        ),
        implementation_version="v1",
        operations={CheckOperation.PROBE: cleanup},
    )
    browser_check = RegisteredCheck(
        spec=_spec(
            "rehearsal.browser",
            tier=3,
            stage=StageCapability.ISOLATED_REHEARSAL,
            mutation_class=MutationClass.ISOLATED,
        ),
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=False, evidence={"status.value": "blocked"}
            )
        },
    )

    results = PreflightDag([browser_check, cleanup_check]).run(_context(), through_tier=3)

    assert {result.check_id: result.outcome for result in results} == {
        "rehearsal.browser": CheckOutcome.FAIL,
        "rehearsal.cleanup": CheckOutcome.PASS,
    }
    assert cleanup_calls == ["cleanup"]


def test_failed_dependency_override_is_restricted_to_isolated_cleanup() -> None:
    with pytest.raises(ValueError, match="reserved for dependent isolated cleanup"):
        _spec(
            "unsafe.cleanup",
            dependencies=("candidate.identity",),
            run_after_failed_dependencies=True,
        )


def test_dag_executes_independent_wave_concurrently() -> None:
    dag = PreflightDag([_check("one.check", delay=0.08), _check("two.check", delay=0.08)])
    started = time.monotonic()
    results = dag.run(_context())
    elapsed = time.monotonic() - started
    assert all(result.passed for result in results)
    assert elapsed < 0.14


def test_dag_rejects_cycles_and_missing_dependencies() -> None:
    with pytest.raises(ValueError, match="unknown dependencies"):
        PreflightDag([_check("one.check", dependencies=("missing.check",))])
    with pytest.raises(ValueError, match="dependency cycle"):
        PreflightDag(
            [
                _check("one.check", dependencies=("two.check",)),
                _check("two.check", dependencies=("one.check",)),
            ]
        )


def test_final_dag_consumes_attested_dependencies_and_mixed_operations() -> None:
    calls: list[tuple[str, CheckOperation]] = []

    def final_check(
        check_id: str,
        *,
        dependency: str,
        mutation: MutationClass,
        operations: tuple[CheckOperation, ...],
    ) -> RegisteredCheck:
        implementations = {}
        for operation in operations:

            def execute(
                _context: CheckContext,
                *,
                operation: CheckOperation = operation,
                check_id: str = check_id,
            ) -> CheckProbe:
                calls.append((check_id, operation))
                return CheckProbe(passed=True, evidence={"status.value": "ready"})

            implementations[operation] = execute
        return RegisteredCheck(
            spec=_spec(
                check_id,
                dependencies=(dependency,),
                tier=4,
                stage=StageCapability.FINAL_ONLY,
                mutation_class=mutation,
                final_only_justification="Only protected live state can prove this invariant.",
            ),
            implementation_version="v1",
            operations=implementations,
        )

    apply = final_check(
        "final.apply",
        dependency="rehearsal.cleanup",
        mutation=MutationClass.PROTECTED_STAGING,
        operations=(CheckOperation.PROBE, CheckOperation.APPLY),
    )
    verify = final_check(
        "final.verify",
        dependency="final.apply",
        mutation=MutationClass.NONE,
        operations=(CheckOperation.PROBE, CheckOperation.VERIFY),
    )
    dag = PreflightDag(
        (apply, verify),
        attested_dependencies=frozenset({"rehearsal.cleanup"}),
    )

    results = dag.run(
        _context(),
        through_tier=4,
        operation={
            "final.apply": CheckOperation.APPLY,
            "final.verify": CheckOperation.VERIFY,
        },
        now=lambda: NOW,
    )

    assert all(result.passed for result in results)
    assert calls == [
        ("final.apply", CheckOperation.APPLY),
        ("final.verify", CheckOperation.VERIFY),
    ]


def test_attested_dependency_cannot_shadow_a_registered_check() -> None:
    with pytest.raises(ValueError, match="must be external"):
        PreflightDag(
            (_check("candidate.identity"),),
            attested_dependencies=frozenset({"candidate.identity"}),
        )


def test_final_dag_resumes_from_exact_persisted_apply_without_repeating_mutation() -> None:
    apply_calls: list[str] = []
    verify_calls: list[str] = []

    apply = RegisteredCheck(
        spec=_spec(
            "final.apply",
            dependencies=("rehearsal.cleanup",),
            tier=4,
            stage=StageCapability.FINAL_ONLY,
            mutation_class=MutationClass.PROTECTED_STAGING,
            final_only_justification="Only protected live state can prove this invariant.",
        ),
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True, evidence={"status.value": "ready"}
            ),
            CheckOperation.APPLY: lambda _context: (
                apply_calls.append("apply")
                or CheckProbe(passed=True, evidence={"status.value": "ready"})
            ),
        },
    )
    verify = RegisteredCheck(
        spec=_spec(
            "final.verify",
            dependencies=("final.apply",),
            tier=4,
            stage=StageCapability.FINAL_ONLY,
            final_only_justification="Only protected live state can prove this invariant.",
        ),
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True, evidence={"status.value": "ready"}
            ),
            CheckOperation.VERIFY: lambda _context: (
                verify_calls.append("verify")
                or CheckProbe(passed=True, evidence={"status.value": "ready"})
            ),
        },
    )
    dag = PreflightDag(
        (apply, verify),
        attested_dependencies=frozenset({"rehearsal.cleanup"}),
    )
    operations = {
        "final.apply": CheckOperation.APPLY,
        "final.verify": CheckOperation.VERIFY,
    }
    first = dag.run(_context(), operation=operations, through_tier=4, now=lambda: NOW)
    persisted_apply = {first[0].check_id: first[0]}
    apply_calls.clear()
    verify_calls.clear()

    resumed = dag.run(
        _context(),
        operation=operations,
        through_tier=4,
        now=lambda: NOW + timedelta(seconds=1),
        prior_executions=persisted_apply,
    )

    assert all(result.passed for result in resumed)
    assert apply_calls == []
    assert verify_calls == ["verify"]


def test_final_dag_refuses_drifted_prior_execution() -> None:
    check = _check("candidate.identity")
    result = PreflightDag((check,)).run(_context(), now=lambda: NOW)[0]

    with pytest.raises(ValueError, match="expired or drifted"):
        PreflightDag((check,)).run(
            CheckContext({"candidate.sha": "b" * 40}),
            now=lambda: NOW + timedelta(seconds=1),
            prior_executions={result.check_id: result},
        )


def test_input_fingerprint_rejects_raw_secret_fields() -> None:
    spec = CheckSpec(
        check_id="token.metadata",
        failure_code="token.metadata.failed",
        tier=0,
        stage=StageCapability.STATIC,
        dependencies=(),
        mutation_class=MutationClass.NONE,
        input_keys=("admin.token",),
        evidence_schema=(EvidenceField("status.value", "string"),),
        timeout_seconds=5,
        freshness_ttl_seconds=60,
        remediation="restore token metadata",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
    )
    check = RegisteredCheck(
        spec=spec,
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"status.value": "ready"},
            )
        },
    )
    with pytest.raises(ValueError, match="secret"):
        check.input_fingerprint(CheckContext(bindings={"admin.token": "raw-secret"}))


def test_check_contract_accepts_bounded_string_map_evidence() -> None:
    spec = CheckSpec(
        check_id="gb10.host-readiness",
        failure_code="gb10.host-readiness.failed",
        tier=0,
        stage=StageCapability.STATIC,
        dependencies=(),
        mutation_class=MutationClass.NONE,
        input_keys=("inventory.digest",),
        evidence_schema=(EvidenceField("boot-ids", "string-map"),),
        timeout_seconds=10,
        freshness_ttl_seconds=120,
        remediation="restore the exact GB10 host readiness contract",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
    )
    check = RegisteredCheck(
        spec=spec,
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"boot-ids": {"trt-gb10-1": "boot-a"}},
            )
        },
    )

    execution = PreflightDag((check,)).run(
        CheckContext({"inventory.digest": "a" * 64}),
        through_tier=0,
    )[0]

    assert execution.passed
    assert execution.evidence["boot-ids"] == {"trt-gb10-1": "boot-a"}


@pytest.mark.parametrize(
    "bad_map",
    [
        {"host": "token=raw-secret"},
        {"": "boot"},
        {"host": "line\nbreak"},
    ],
)
def test_check_contract_rejects_unsafe_string_map_evidence(
    bad_map: dict[str, str],
) -> None:
    spec = CheckSpec(
        check_id="gb10.host-readiness",
        failure_code="gb10.host-readiness.failed",
        tier=0,
        stage=StageCapability.STATIC,
        dependencies=(),
        mutation_class=MutationClass.NONE,
        input_keys=("inventory.digest",),
        evidence_schema=(EvidenceField("boot-ids", "string-map"),),
        timeout_seconds=10,
        freshness_ttl_seconds=120,
        remediation="restore the exact GB10 host readiness contract",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
    )
    check = RegisteredCheck(
        spec=spec,
        implementation_version="v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"boot-ids": bad_map},
            )
        },
    )

    with pytest.raises(ValueError):
        PreflightDag((check,)).run(
            CheckContext({"inventory.digest": "a" * 64}),
            through_tier=0,
        )


def _bindings(**overrides: object) -> AttestationBindings:
    values: dict[str, object] = {
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "image_digests": {"api": "sha256:" + "1" * 64},
        "runner_source_sha": "c" * 40,
        "runner_source_tree": "d" * 40,
        "runner_install_hash": "2" * 64,
        "runner_config_hash": "3" * 64,
        "staging_mutation_epoch": 7,
        "backup_lease_id": "lease-1",
        "backup_lease_digest": "9" * 64,
        "backup_manifest_sha256": "a" * 64,
        "backup_component_set_digest": "b" * 64,
        "db_snapshot_identity": "snapshot-1",
        "schema_revision": "0066",
        "object_inventory_root": "c" * 64,
        "migration_plan_digest": "4" * 64,
        "environment": "staging",
        "namespace": "loom-staging",
        "route": "https://yylx.world/dev",
        "secret_metadata_fingerprints": {"admin": "sha256:abc len=32"},
        "gb10_inventory_digest": "5" * 64,
        "gb10_boot_ids": {"trt-gb10-1": "boot-1"},
        "gb10_mount_digest": "6" * 64,
        "gb10_unit_digest": "7" * 64,
        "browser_image_digest": "sha256:" + "8" * 64,
        "browser_report_schema": "v3",
    }
    values.update(overrides)
    return AttestationBindings(**values)  # type: ignore[arg-type]


def test_attestation_binds_all_evidence_and_invalidates_drift() -> None:
    result = PreflightDag([_check("candidate.identity")]).run(
        _context(),
        now=lambda: NOW,
    )
    attestation = PreflightAttestation.issue(
        bindings=_bindings(),
        executions=result,
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )
    assert attestation.valid_for(_bindings(), now=NOW + timedelta(seconds=1))
    assert not attestation.valid_for(
        _bindings(staging_mutation_epoch=8),
        now=NOW + timedelta(seconds=1),
    )
    assert not attestation.valid_for(_bindings(), now=NOW + timedelta(minutes=11))


def test_attestation_round_trip_preserves_exact_digest_and_immutable_maps() -> None:
    result = PreflightDag([_check("candidate.identity")]).run(
        _context(),
        now=lambda: NOW,
    )
    attestation = PreflightAttestation.issue(
        bindings=_bindings(),
        executions=result,
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )

    decoded = PreflightAttestation.from_dict(attestation.to_dict())

    assert decoded == attestation
    assert decoded.attestation_digest == attestation.attestation_digest
    with pytest.raises(TypeError):
        decoded.evidence_hashes["other.check"] = "0" * 64  # type: ignore[index]


def test_attestation_round_trip_rejects_payload_and_digest_tampering() -> None:
    result = PreflightDag([_check("candidate.identity")]).run(
        _context(),
        now=lambda: NOW,
    )
    attestation = PreflightAttestation.issue(
        bindings=_bindings(),
        executions=result,
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )
    payload = attestation.to_dict()
    bindings = dict(payload["bindings"])  # type: ignore[arg-type]
    bindings["staging_mutation_epoch"] = 8
    payload["bindings"] = bindings

    with pytest.raises(ValueError, match="digest does not match"):
        PreflightAttestation.from_dict(payload)

    payload = attestation.to_dict()
    payload["attestation_digest"] = "f" * 64
    with pytest.raises(ValueError, match="digest does not match"):
        PreflightAttestation.from_dict(payload)


def test_attestation_rejects_incomplete_or_expired_results() -> None:
    failed = PreflightDag([_check("candidate.identity", passed=False)]).run(
        _context(),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="every non-final check"):
        PreflightAttestation.issue(
            bindings=_bindings(),
            executions=failed,
            issued_at=NOW,
            registry_digest="9" * 64,
            coverage_digest="a" * 64,
        )

    passed = PreflightDag([_check("candidate.identity")]).run(
        _context(),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="expired evidence"):
        PreflightAttestation.issue(
            bindings=_bindings(),
            executions=passed,
            issued_at=NOW + timedelta(minutes=11),
            registry_digest="9" * 64,
            coverage_digest="a" * 64,
        )
