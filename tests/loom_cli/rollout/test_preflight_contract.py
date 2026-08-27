from __future__ import annotations

import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event, Thread, current_thread

import pytest

import loom_cli.rollout.preflight_contract as preflight_contract
from loom_cli.rollout.external_supervisor_predecessor import (
    GB10_CANONICAL_UNIT_DIR,
    PROTECTED_CANONICAL_UNIT_DIR,
)
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    AttestationBindings,
    CheckContext,
    CheckExecution,
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
    external_supervisor_transition_digest,
    external_supervisor_unit_set_digest,
)

NOW = datetime(2026, 7, 19, 16, tzinfo=UTC)


def test_external_supervisor_transition_digest_binds_validated_controller_unit_directory() -> None:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "1" * 64,
        "loom-autoscaler-gb10-staging.timer": "2" * 64,
    }
    target_units = {
        "loom-autoscaler-gb10-staging.service": "3" * 64,
        "loom-autoscaler-gb10-staging.timer": "4" * 64,
    }
    arguments = {
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "environment": "staging",
        "predecessor_kind": "legacy-manifest",
        "predecessor_digest": "5" * 64,
        "predecessor_pointer_digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        "predecessor_unit_sha256": predecessor_units,
        "predecessor_unit_set_digest": external_supervisor_unit_set_digest(predecessor_units),
        "predecessor_live_evidence_digest": "6" * 64,
        "predecessor_pending_transition_digest": "7" * 64,
        "target_artifact_digest": "8" * 64,
        "target_profile_sha256": "9" * 64,
        "target_script_sha256": {"scripts/ops/autoscaler.py": "c" * 64},
        "target_unit_sha256": target_units,
        "target_unit_set_digest": external_supervisor_unit_set_digest(target_units),
    }

    gb10 = external_supervisor_transition_digest(
        **arguments,
        unit_directory=GB10_CANONICAL_UNIT_DIR,
    )
    oldlab = external_supervisor_transition_digest(
        **arguments,
        unit_directory=PROTECTED_CANONICAL_UNIT_DIR,
    )

    assert gb10 != oldlab
    with pytest.raises(ValueError, match="transition identity"):
        external_supervisor_transition_digest(
            **arguments,
            unit_directory="/tmp/candidate-controlled",
        )


def _spec(
    check_id: str,
    *,
    dependencies: tuple[str, ...] = (),
    tier: int = 0,
    stage: StageCapability = StageCapability.STATIC,
    mutation_class: MutationClass = MutationClass.NONE,
    final_only_justification: str | None = None,
    run_after_failed_dependencies: bool = False,
    freshness_ttl_seconds: int = 600,
    timeout_seconds: int = 5,
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
        timeout_seconds=timeout_seconds,
        freshness_ttl_seconds=freshness_ttl_seconds,
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
    tier: int = 0,
    stage: StageCapability = StageCapability.STATIC,
    freshness_ttl_seconds: int = 600,
) -> RegisteredCheck:
    def probe(_context: CheckContext) -> CheckProbe:
        if delay:
            time.sleep(delay)
        return CheckProbe(passed=passed, evidence={"status.value": "ready"})

    return RegisteredCheck(
        spec=_spec(
            check_id,
            dependencies=dependencies,
            tier=tier,
            stage=stage,
            freshness_ttl_seconds=freshness_ttl_seconds,
        ),
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


def test_dag_times_each_started_check_and_quiesces_mutations_before_reporting() -> None:
    finished = {
        "slow.first": Event(),
        "slow.second": Event(),
        "slow.invalid": Event(),
    }
    mutation_ticks: list[float] = []

    def timed_check(
        check_id: str,
        probe: Callable[[CheckContext], CheckProbe],
    ) -> RegisteredCheck:
        return RegisteredCheck(
            spec=_spec(
                check_id,
                tier=3,
                stage=StageCapability.ISOLATED_REHEARSAL,
                mutation_class=MutationClass.ISOLATED,
                timeout_seconds=1,
            ),
            implementation_version="v1",
            operations={CheckOperation.PROBE: probe},
        )

    def finishes_late(_context: CheckContext) -> CheckProbe:
        time.sleep(1.2)
        finished["slow.first"].set()
        return CheckProbe(passed=True, evidence={"status.value": "ready"})

    def stops_only_after_cancellation(context: CheckContext) -> CheckProbe:
        while not context.cancellation_requested:
            mutation_ticks.append(time.monotonic())
            time.sleep(0.01)
        finished["slow.second"].set()
        return CheckProbe(passed=False, evidence={"status.value": "cancelled"})

    def returns_invalid_evidence_after_deadline(_context: CheckContext) -> CheckProbe:
        time.sleep(1.05)
        finished["slow.invalid"].set()
        return CheckProbe(passed=True, evidence={"status.value": 123})

    reported: list[str] = []

    def on_execution(execution: CheckExecution) -> None:
        assert finished[execution.check_id].is_set()
        reported.append(execution.check_id)

    results = PreflightDag(
        (
            timed_check("slow.first", finishes_late),
            timed_check("slow.second", stops_only_after_cancellation),
            timed_check("slow.invalid", returns_invalid_evidence_after_deadline),
        ),
        max_concurrency=3,
        cancellation_grace_seconds=0.5,
    ).run(_context(), through_tier=3, on_execution=on_execution)

    assert {result.check_id: result.outcome for result in results} == {
        "slow.first": CheckOutcome.TIMEOUT,
        "slow.second": CheckOutcome.TIMEOUT,
        "slow.invalid": CheckOutcome.TIMEOUT,
    }
    assert reported == ["slow.first", "slow.second", "slow.invalid"]
    ticks_after_return = len(mutation_ticks)
    time.sleep(0.05)
    assert len(mutation_ticks) == ticks_after_return


def test_dag_keeps_on_time_completion_when_observed_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = Event()
    release = Event()
    completion_stamped = Event()
    worker_monotonic_calls = 0

    def controlled_monotonic() -> float:
        nonlocal worker_monotonic_calls
        if current_thread().name.startswith("loom-preflight"):
            worker_monotonic_calls += 1
            if worker_monotonic_calls == 1:
                return 0.0
            completion_stamped.set()
            return 0.9
        assert started.wait(timeout=1)
        release.set()
        assert completion_stamped.wait(timeout=1)
        return 1.1

    def completes_before_deadline(_context: CheckContext) -> CheckProbe:
        started.set()
        assert release.wait(timeout=1)
        return CheckProbe(passed=True, evidence={"status.value": "ready"})

    monkeypatch.setattr(preflight_contract, "monotonic", controlled_monotonic)
    check = RegisteredCheck(
        spec=_spec("on-time.check", timeout_seconds=1),
        implementation_version="v1",
        operations={CheckOperation.PROBE: completes_before_deadline},
    )

    result = PreflightDag((check,), cancellation_grace_seconds=0.5).run(_context())[0]

    assert result.outcome is CheckOutcome.PASS


def test_dag_prewarms_workers_before_any_mutation_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mutation_started = Event()
    original_start = Thread.start
    preflight_starts = 0

    def fail_second_preflight_thread_start(thread: Thread) -> None:
        nonlocal preflight_starts
        if thread.name.startswith("loom-preflight"):
            preflight_starts += 1
            if preflight_starts == 2:
                raise RuntimeError("synthetic thread start failure after enqueue")
        original_start(thread)

    def mutation(_context: CheckContext) -> CheckProbe:
        mutation_started.set()
        return CheckProbe(passed=True, evidence={"status.value": "mutated"})

    monkeypatch.setattr(Thread, "start", fail_second_preflight_thread_start)
    checks = (
        RegisteredCheck(
            spec=_spec(
                "first.mutation",
                tier=3,
                stage=StageCapability.ISOLATED_REHEARSAL,
                mutation_class=MutationClass.ISOLATED,
            ),
            implementation_version="v1",
            operations={CheckOperation.PROBE: mutation},
        ),
        _check("second.check"),
    )

    with pytest.raises(RuntimeError, match="synthetic thread start failure after enqueue"):
        PreflightDag(checks, cancellation_grace_seconds=0.5).run(
            _context(),
            through_tier=3,
        )

    assert not mutation_started.is_set()


@pytest.mark.parametrize(
    ("close_stderr", "expected_stderr"),
    (
        (
            False,
            "fatal: preflight.runner.cancel-timeout check_id=hung.mutation "
            "failure_code=hung.mutation.failed discovered_stage=isolated-rehearsal\n",
        ),
        (True, ""),
    ),
    ids=("diagnostic-written", "stderr-unavailable"),
)
def test_dag_fatally_terminates_runner_when_cancellation_cannot_quiesce(
    close_stderr: bool,
    expected_stderr: str,
) -> None:
    script = """
import os
import time

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

def never_returns(_context):
    while True:
        time.sleep(0.05)

check = RegisteredCheck(
    spec=CheckSpec(
        check_id="hung.mutation",
        failure_code="hung.mutation.failed",
        tier=3,
        stage=StageCapability.ISOLATED_REHEARSAL,
        dependencies=(),
        mutation_class=MutationClass.ISOLATED,
        input_keys=("candidate.sha",),
        evidence_schema=(EvidenceField("status.value", "string"),),
        timeout_seconds=1,
        freshness_ttl_seconds=60,
        remediation="repair the hung isolated mutation",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
    ),
    implementation_version="v1",
    operations={CheckOperation.PROBE: never_returns},
)
CLOSE_STDERR
PreflightDag((check,), cancellation_grace_seconds=0.1).run(
    CheckContext({"candidate.sha": "a" * 40}),
    through_tier=3,
)
""".replace("CLOSE_STDERR", "os.close(2)" if close_stderr else "")

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=3,
    )

    assert result.returncode == 70
    assert result.stdout == ""
    assert result.stderr == expected_stderr


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


def test_tier_zero_allows_only_isolated_or_nonmutating_checks() -> None:
    isolated = CheckSpec(
        check_id="lifecycle.launch-cancel",
        failure_code="lifecycle.launch-cancel.failed",
        tier=0,
        stage=StageCapability.STATIC,
        dependencies=(),
        mutation_class=MutationClass.ISOLATED,
        input_keys=("candidate.sha",),
        evidence_schema=(EvidenceField("ready", "boolean"),),
        timeout_seconds=75,
        freshness_ttl_seconds=120,
        remediation="repair the isolated transient lifecycle probe",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
    )
    assert isolated.mutation_class is MutationClass.ISOLATED
    with pytest.raises(ValueError, match="mutation class"):
        CheckSpec(
            check_id="lifecycle.protected",
            failure_code="lifecycle.protected.failed",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.PROTECTED_STAGING,
            input_keys=("candidate.sha",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=75,
            freshness_ttl_seconds=120,
            remediation="never mutate protected staging in Tier 0",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        )


def test_tier_zero_live_readonly_stage_round_trips_but_tier_one_is_rejected() -> None:
    check = _check(
        "external-supervisor.predecessor",
        tier=0,
        stage=StageCapability.BASELINE_LIVE_READONLY,
    )

    execution = PreflightDag((check,)).run(_context(), now=lambda: NOW)[0]

    assert execution.stage is StageCapability.BASELINE_LIVE_READONLY
    assert CheckExecution.from_dict(execution.to_dict()) == execution
    with pytest.raises(ValueError, match="tier does not match stage"):
        replace(check.spec, tier=1)


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
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "d" * 64,
        "loom-autoscaler-gb10-staging.timer": "e" * 64,
    }
    oldlab_predecessor_units = {
        "loom-autoscaler-oldlab-staging.service": "4" * 64,
        "loom-autoscaler-oldlab-staging.timer": "5" * 64,
    }
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
        "supervisor_predecessor_kind": "legacy-manifest",
        "supervisor_predecessor_digest": "f" * 64,
        "supervisor_predecessor_pointer_digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        "supervisor_predecessor_unit_sha256": predecessor_units,
        "supervisor_predecessor_unit_set_digest": external_supervisor_unit_set_digest(
            predecessor_units
        ),
        "supervisor_predecessor_live_evidence_digest": "1" * 64,
        "supervisor_predecessor_pending_transition_digest": "2" * 64,
        "supervisor_transition_digest": "3" * 64,
        "supervisor_controller_bindings": {
            "gx10-01c7/authority-kind": "legacy-manifest",
            "gx10-01c7/authority-digest": "f" * 64,
            "gx10-01c7/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            "gx10-01c7/unit-set-digest": external_supervisor_unit_set_digest(predecessor_units),
            "gx10-01c7/live-evidence-digest": "1" * 64,
            "gx10-01c7/pending-transition-digest": "2" * 64,
            "gx10-01c7/runtime-state": "ready",
            "gx10-01c7/unit-directory": GB10_CANONICAL_UNIT_DIR,
            "gx10-01c7/transition-digest": "3" * 64,
            **{f"gx10-01c7/unit/{name}": digest for name, digest in predecessor_units.items()},
            "TRT-EAI-OLDLAB-1/authority-kind": "legacy-manifest",
            "TRT-EAI-OLDLAB-1/authority-digest": "6" * 64,
            "TRT-EAI-OLDLAB-1/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            "TRT-EAI-OLDLAB-1/unit-set-digest": external_supervisor_unit_set_digest(
                oldlab_predecessor_units
            ),
            "TRT-EAI-OLDLAB-1/live-evidence-digest": "7" * 64,
            "TRT-EAI-OLDLAB-1/pending-transition-digest": "8" * 64,
            "TRT-EAI-OLDLAB-1/runtime-state": "ready",
            "TRT-EAI-OLDLAB-1/unit-directory": PROTECTED_CANONICAL_UNIT_DIR,
            "TRT-EAI-OLDLAB-1/transition-digest": "9" * 64,
            **{
                f"TRT-EAI-OLDLAB-1/unit/{name}": digest
                for name, digest in oldlab_predecessor_units.items()
            },
        },
    }
    values.update(overrides)
    return AttestationBindings(**values)  # type: ignore[arg-type]


def test_attestation_rejects_incomplete_external_supervisor_controller_coverage() -> None:
    controller_bindings = {
        key: value
        for key, value in _bindings().supervisor_controller_bindings.items()
        if key.startswith("gx10-01c7/")
    }

    with pytest.raises(ValueError, match="controller binding set"):
        _bindings(supervisor_controller_bindings=controller_bindings)


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
    with pytest.raises(TypeError):
        decoded.bindings.supervisor_predecessor_unit_sha256[  # type: ignore[index]
            "loom-autoscaler-gb10-staging.service"
        ] = "0" * 64


def test_attestation_v2_rejects_absent_predecessor_or_v1_schema() -> None:
    with pytest.raises(ValueError, match="supervisor predecessor binding"):
        _bindings(
            supervisor_predecessor_kind="absent",
            supervisor_predecessor_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            supervisor_predecessor_pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            supervisor_predecessor_unit_sha256={},
            supervisor_predecessor_unit_set_digest="0" * 64,
        )

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
    with pytest.raises(ValueError, match="schema is unsupported"):
        replace(attestation, schema_version=1)


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


def test_attestation_uses_non_rechecked_evidence_as_freshness_authority() -> None:
    executions = PreflightDag(
        [
            _check("candidate.identity", freshness_ttl_seconds=60),
            _check(
                "rehearsal.cleanup",
                dependencies=("candidate.identity",),
                tier=3,
                stage=StageCapability.ISOLATED_REHEARSAL,
                freshness_ttl_seconds=3600,
            ),
        ]
    ).run(_context(), through_tier=3, now=lambda: NOW)

    attestation = PreflightAttestation.issue(
        bindings=_bindings(),
        executions=executions,
        issued_at=NOW + timedelta(minutes=2),
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )

    assert attestation.expires_at == NOW + timedelta(hours=1)
    with pytest.raises(ValueError, match="expired evidence"):
        PreflightAttestation.issue(
            bindings=_bindings(),
            executions=executions,
            issued_at=NOW + timedelta(hours=1, seconds=1),
            registry_digest="9" * 64,
            coverage_digest="a" * 64,
        )
