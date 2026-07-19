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
        "db_snapshot_identity": "snapshot-1",
        "schema_revision": "0066",
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
    )
    assert attestation.valid_for(_bindings(), now=NOW + timedelta(seconds=1))
    assert not attestation.valid_for(
        _bindings(staging_mutation_epoch=8),
        now=NOW + timedelta(seconds=1),
    )
    assert not attestation.valid_for(_bindings(), now=NOW + timedelta(minutes=11))


def test_attestation_rejects_incomplete_or_expired_results() -> None:
    failed = PreflightDag([_check("candidate.identity", passed=False)]).run(
        _context(),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="every non-final check"):
        PreflightAttestation.issue(bindings=_bindings(), executions=failed, issued_at=NOW)

    passed = PreflightDag([_check("candidate.identity")]).run(
        _context(),
        now=lambda: NOW,
    )
    with pytest.raises(ValueError, match="expired evidence"):
        PreflightAttestation.issue(
            bindings=_bindings(),
            executions=passed,
            issued_at=NOW + timedelta(minutes=11),
        )
