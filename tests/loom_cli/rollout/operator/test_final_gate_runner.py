from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

import pytest

from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.final_gate_readiness import FINAL_CHECK_IDS, FinalGateResult
from loom_cli.rollout.operator.final_gate_runner import FinalGateRunner
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


def _valid_attestation() -> PreflightAttestation:
    provisional = _attestation()
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
                    check_id == "final.protected-apply" and operation is CheckOperation.APPLY
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


def _envelope():
    attestation = _valid_attestation()
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
    calls = []
    runner = _runner(tmp_path, calls)

    assert runner(_envelope(), _admission(_valid_attestation())) == 0
    first_calls = list(calls)
    assert len(first_calls) == 6

    assert runner(_envelope(), _admission(_valid_attestation())) == 0
    assert calls == first_calls


def test_final_gate_runner_refuses_epoch_drift_before_actions(tmp_path: Path) -> None:
    calls = []
    runner = _runner(tmp_path, calls, epoch=8)

    with pytest.raises(ValueError, match="epoch drifted"):
        runner(_envelope(), _admission(_valid_attestation()))

    assert calls == []
