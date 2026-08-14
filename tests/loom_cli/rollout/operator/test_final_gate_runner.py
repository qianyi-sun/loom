from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType

import pytest

from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    PROTECTED_MUTATION_CHECK_IDS,
    FinalGateResult,
)
from loom_cli.rollout.operator.final_gate_runner import FinalGateRunner
from loom_cli.rollout.operator.final_gate_store import FinalGateExecutionStore
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    PreflightAttestation,
    StageCapability,
    _hash_json,
    _preflight_attestation_payload,
)
from tests.loom_cli.rollout.operator.test_protected_apply_baseline import (
    _baseline_executions,
)
from tests.loom_cli.rollout.operator.test_worker import valid_envelope
from tests.loom_cli.rollout.test_attested_final_gate import (
    NOW,
    _attestation,
)


class _SimulatedWorkerCrash(BaseException):
    pass


def _valid_attestation(*, expires_at=None) -> PreflightAttestation:
    provisional = _attestation(expires_at=expires_at)
    return replace(
        provisional,
        attestation_digest=_hash_json(_preflight_attestation_payload(provisional)),
    )


def _admission(attestation: PreflightAttestation) -> FinalAttestationAdmission:
    evidence = MappingProxyType({"ready": True})
    tier0 = CheckExecution(
        check_id="candidate.identity",
        failure_code="candidate.identity.failed",
        tier=0,
        stage=StageCapability.STATIC,
        operation=CheckOperation.PROBE,
        outcome=CheckOutcome.PASS,
        input_fingerprint="1" * 64,
        implementation_digest="2" * 64,
        evidence=evidence,
        evidence_hash=hashlib.sha256(
            json.dumps(dict(evidence), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        started_at=NOW,
        finished_at=NOW,
        expires_at=attestation.expires_at,
        remediation=None,
    )
    return FinalAttestationAdmission(attestation, (tier0,), _baseline_executions())


def _actions(calls, attestation_digest: str):
    def action(check_id: str):
        def execute(operation: CheckOperation) -> FinalGateResult:
            calls.append((check_id, operation))
            return FinalGateResult(
                check_id=check_id,
                operation=operation,
                candidate_sha="a" * 40,
                attestation_digest=attestation_digest,
                observed_epoch=7,
                evidence_digest="9" * 64,
                protected_mutation=(
                    check_id in PROTECTED_MUTATION_CHECK_IDS and operation is CheckOperation.APPLY
                ),
                blockers={},
            )

        return execute

    return {check_id: action(check_id) for check_id in FINAL_CHECK_IDS}


def _runner(tmp_path: Path, calls, *, epoch: int = 7) -> FinalGateRunner:
    state = tmp_path / "state"
    store = PreflightAttestationStore(state)
    attestation = _valid_attestation()
    store.publish(attestation)
    attempt = state / "requests" / "req-alpha" / "attempts" / "1"
    attempt.mkdir(parents=True, mode=0o700)
    os.chmod(attempt, 0o700)
    return FinalGateRunner(
        attestation_store=store,
        actions_factory=lambda _envelope, _attestation, _epoch, _admission: _actions(
            calls, attestation.attestation_digest
        ),
        read_mutation_epoch=lambda: epoch,
        now=lambda: NOW,
        state_root=state,
        service_uid=os.geteuid(),
    )


def _envelope(attestation: PreflightAttestation | None = None):
    attestation = attestation or _valid_attestation()
    return replace(
        valid_envelope(),
        source_mode="sealed-cumulative",
        resolved_tree=attestation.bindings.candidate_tree,
        approved_base_sha="c" * 40,
        preflight_attestation_sha256=attestation.attestation_digest,
        preflight_registry_sha256=attestation.registry_digest,
        preflight_coverage_sha256=attestation.coverage_digest,
    )


def test_final_gate_runner_journals_once_and_resumes_without_reapply(tmp_path: Path) -> None:
    runner, attestation, _state_root, _state, calls = _resume_runner(
        tmp_path,
        blocked=set(),
    )
    envelope = _envelope(attestation)

    assert runner(envelope, _admission(attestation)) == 0
    first_calls = list(calls)
    assert len(first_calls) == 6

    assert (
        runner(
            envelope,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert calls == first_calls


def test_final_gate_runner_refuses_epoch_drift_before_actions(tmp_path: Path) -> None:
    calls = []
    runner = _runner(tmp_path, calls, epoch=8)

    with pytest.raises(ValueError, match="epoch drifted"):
        runner(_envelope(), _admission(_valid_attestation()))

    assert calls == []


def test_final_gate_runner_resumes_after_apply_without_repeating_mutation(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    attestation = _valid_attestation()
    store = PreflightAttestationStore(state)
    store.publish(attestation)
    for attempt_number in (1, 2):
        attempt = state / "requests" / "req-alpha" / "attempts" / str(attempt_number)
        attempt.mkdir(parents=True, mode=0o700)
        os.chmod(attempt, 0o700)

    epoch = 7
    convergence_ready = False
    calls: list[tuple[int, str, CheckOperation]] = []

    def actions_factory(envelope, _attestation, _epoch, _admission):
        if envelope.attempt_number == 2:
            before_plan = FinalGateExecutionStore(
                state,
                request_id="req-alpha",
                attempt_number=2,
            ).read_all()
            assert "final.protected-apply" not in before_plan

        def action(check_id: str):
            def execute(operation: CheckOperation) -> FinalGateResult:
                nonlocal epoch
                calls.append((envelope.attempt_number, check_id, operation))
                if check_id == "final.protected-apply":
                    if epoch != 7:
                        raise AssertionError("protected apply was repeated after epoch advance")
                    epoch = 8
                blockers = (
                    {"runtime": "stale-owned-job"}
                    if check_id == "final.convergence" and not convergence_ready
                    else {}
                )
                return FinalGateResult(
                    check_id=check_id,
                    operation=operation,
                    candidate_sha="a" * 40,
                    attestation_digest=attestation.attestation_digest,
                    observed_epoch=epoch,
                    evidence_digest="9" * 64,
                    protected_mutation=(
                        check_id in PROTECTED_MUTATION_CHECK_IDS
                        and operation is CheckOperation.APPLY
                    ),
                    blockers=blockers,
                )

            return execute

        return {check_id: action(check_id) for check_id in FINAL_CHECK_IDS}

    runner = FinalGateRunner(
        attestation_store=store,
        actions_factory=actions_factory,
        read_mutation_epoch=lambda: epoch,
        now=lambda: NOW,
        state_root=state,
        service_uid=os.geteuid(),
    )
    first = _envelope()

    assert runner(first, _admission(attestation)) == 1
    assert epoch == 8
    assert [check_id for _, check_id, _ in calls].count("final.protected-apply") == 1

    convergence_ready = True
    resumed = replace(
        first,
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    assert (
        runner(
            resumed,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert [check_id for _, check_id, _ in calls].count("final.protected-apply") == 1
    carried = FinalGateExecutionStore(
        state,
        request_id="req-alpha",
        attempt_number=2,
    ).read_all()
    assert set(carried) == set(FINAL_CHECK_IDS)
    assert carried["final.protected-apply"].passed


def _resume_runner(
    tmp_path: Path,
    *,
    blocked: set[str],
    attestation_expires_at=NOW + timedelta(hours=4),
    crash_once: set[str] | None = None,
):
    state_root = tmp_path / "state"
    attestation = _valid_attestation(expires_at=attestation_expires_at)
    store = PreflightAttestationStore(state_root)
    store.publish(attestation)
    for attempt_number in (1, 2, 3):
        attempt = state_root / "requests" / "req-alpha" / "attempts" / str(attempt_number)
        attempt.mkdir(parents=True, mode=0o700)
        os.chmod(attempt, 0o700)
    state = {"epoch": 7, "now": NOW}
    calls: list[tuple[int, str, CheckOperation]] = []
    pending_crashes = set(crash_once or ())

    def actions_factory(envelope, _attestation, _epoch, _admission):
        def action(check_id: str):
            def execute(operation: CheckOperation) -> FinalGateResult:
                calls.append((envelope.attempt_number, check_id, operation))
                if check_id == "final.protected-apply":
                    if state["epoch"] != 7:
                        raise AssertionError("protected apply was repeated after epoch advance")
                    state["epoch"] = 8
                if check_id in pending_crashes:
                    pending_crashes.remove(check_id)
                    raise _SimulatedWorkerCrash("simulated worker crash")
                return FinalGateResult(
                    check_id=check_id,
                    operation=operation,
                    candidate_sha="a" * 40,
                    attestation_digest=attestation.attestation_digest,
                    observed_epoch=state["epoch"],
                    evidence_digest="9" * 64,
                    protected_mutation=(
                        check_id in PROTECTED_MUTATION_CHECK_IDS
                        and operation is CheckOperation.APPLY
                    ),
                    blockers={"test": "blocked"} if check_id in blocked else {},
                )

            return execute

        return {check_id: action(check_id) for check_id in FINAL_CHECK_IDS}

    runner = FinalGateRunner(
        attestation_store=store,
        actions_factory=actions_factory,
        read_mutation_epoch=lambda: state["epoch"],
        now=lambda: state["now"],
        state_root=state_root,
        service_uid=os.geteuid(),
    )
    return runner, attestation, state_root, state, calls


def test_final_gate_runner_keeps_expired_protected_apply_as_durable_resume_proof(
    tmp_path: Path,
) -> None:
    blocked = {"final.convergence"}
    runner, attestation, _state_root, state, calls = _resume_runner(
        tmp_path,
        blocked=blocked,
        attestation_expires_at=NOW + timedelta(minutes=30),
    )

    assert runner(_envelope(attestation), _admission(attestation)) == 1
    blocked.clear()
    state["now"] = NOW + timedelta(hours=2)
    resumed = replace(
        _envelope(attestation),
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    assert (
        runner(
            resumed,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert [check_id for _, check_id, _ in calls].count("final.protected-apply") == 1


def test_final_gate_runner_reruns_expired_repeatable_checks(tmp_path: Path) -> None:
    blocked = {"final.drift"}
    runner, attestation, _state_root, state, calls = _resume_runner(
        tmp_path,
        blocked=blocked,
    )

    assert runner(_envelope(attestation), _admission(attestation)) == 1
    blocked.clear()
    state["now"] = NOW + timedelta(hours=2)
    resumed = replace(
        _envelope(attestation),
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    assert (
        runner(
            resumed,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert [check_id for _, check_id, _ in calls].count("final.protected-apply") == 1
    assert [check_id for _, check_id, _ in calls].count("final.convergence") == 2


def test_final_gate_runner_carries_newest_successful_historical_evidence(
    tmp_path: Path,
) -> None:
    runner, attestation, state_root, state, calls = _resume_runner(tmp_path, blocked=set())
    first = _envelope(attestation)

    assert runner(first, _admission(attestation)) == 0
    first_executions = FinalGateExecutionStore(
        state_root,
        request_id="req-alpha",
        attempt_number=1,
    ).read_all()
    second_store = FinalGateExecutionStore(
        state_root,
        request_id="req-alpha",
        attempt_number=2,
    )
    newer: dict[str, CheckExecution] = {}
    for check_id in ("final.smoke", "final.browser"):
        evidence = dict(first_executions[check_id].evidence)
        evidence["evidence-digest"] = "8" * 64
        newer[check_id] = replace(
            first_executions[check_id],
            evidence=MappingProxyType(evidence),
            evidence_hash=_hash_json(evidence),
            started_at=NOW + timedelta(minutes=1),
            finished_at=NOW + timedelta(minutes=1),
            expires_at=NOW + timedelta(minutes=61),
        )
        second_store.publish(newer[check_id])
    state["now"] = NOW + timedelta(minutes=2)
    resumed = replace(
        first,
        attempt_number=3,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    assert (
        runner(
            resumed,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert len(calls) == len(FINAL_CHECK_IDS)
    carried = FinalGateExecutionStore(
        state_root,
        request_id="req-alpha",
        attempt_number=3,
    ).read_all()
    assert carried["final.smoke"] == newer["final.smoke"]
    assert carried["final.browser"] == newer["final.browser"]


def test_final_gate_runner_restarts_same_attempt_after_protected_apply(
    tmp_path: Path,
) -> None:
    runner, attestation, _state_root, _state, calls = _resume_runner(
        tmp_path,
        blocked=set(),
        crash_once={"final.convergence"},
    )
    envelope = _envelope(attestation)

    with pytest.raises(_SimulatedWorkerCrash, match="simulated worker crash"):
        runner(envelope, _admission(attestation))

    assert (
        runner(
            envelope,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert [check_id for _, check_id, _ in calls].count("final.protected-apply") == 1


def test_final_gate_runner_reruns_expired_checks_in_same_attempt(tmp_path: Path) -> None:
    blocked = {"final.drift"}
    runner, attestation, state_root, state, calls = _resume_runner(
        tmp_path,
        blocked=blocked,
    )
    envelope = _envelope(attestation)

    assert runner(envelope, _admission(attestation)) == 1
    blocked.clear()
    state["now"] = NOW + timedelta(hours=2)

    assert (
        runner(
            envelope,
            replace(_admission(attestation), post_apply_resume=True),
        )
        == 0
    )
    assert [check_id for _, check_id, _ in calls].count("final.protected-apply") == 1
    assert [check_id for _, check_id, _ in calls].count("final.convergence") == 2
    assert all(
        execution.passed
        for execution in FinalGateExecutionStore(
            state_root,
            request_id="req-alpha",
            attempt_number=1,
        )
        .read_all()
        .values()
    )
