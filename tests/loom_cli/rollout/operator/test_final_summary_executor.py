from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType

from loom_cli.rollout.operator.final_gate_store import FinalGateExecutionStore
from loom_cli.rollout.operator.final_summary_executor import FinalSummaryExecutor
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan

_NOW = datetime(2026, 7, 19, 22, tzinfo=UTC)
_CHECKS = (
    "final.protected-apply",
    "final.convergence",
    "final.drift",
    "final.smoke",
    "final.browser",
)


def _execution(check_id: str, plan, *, candidate_sha: str | None = None) -> CheckExecution:
    operation = (
        CheckOperation.APPLY
        if check_id in {"final.protected-apply", "final.smoke", "final.browser"}
        else CheckOperation.VERIFY
    )
    evidence = {
        "attestation-digest": plan.attestation_digest,
        "blockers": {},
        "candidate-sha": candidate_sha or plan.candidate_sha,
        "evidence-digest": "3" * 64,
        "observed-epoch": plan.starting_mutation_epoch + 1,
        "protected-mutation": operation is CheckOperation.APPLY,
        "ready": True,
    }
    return CheckExecution(
        check_id=check_id,
        failure_code=f"{check_id}.failed",
        tier=4,
        stage=StageCapability.FINAL_ONLY,
        operation=operation,
        outcome=CheckOutcome.PASS,
        input_fingerprint="1" * 64,
        implementation_digest="2" * 64,
        evidence=MappingProxyType(evidence),
        evidence_hash=hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        started_at=_NOW,
        finished_at=_NOW,
        expires_at=_NOW + timedelta(hours=1),
        remediation=None,
    )


def _store(tmp_path: Path, plan) -> FinalGateExecutionStore:
    attempt = (
        tmp_path / "state" / "requests" / plan.request_id / "attempts" / str(plan.attempt_number)
    )
    attempt.mkdir(parents=True, mode=0o700)
    return FinalGateExecutionStore(
        tmp_path / "state",
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
    )


def test_final_summary_seals_exact_complete_predecessor_evidence(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    store = _store(tmp_path, plan)
    for check_id in _CHECKS:
        store.publish(_execution(check_id, plan))

    result = FinalSummaryExecutor(tmp_path / "state", store.service_uid)(
        "final.summary",
        CheckOperation.VERIFY,
        plan,
    )

    assert result.ready
    assert not result.protected_mutation
    assert result.observed_epoch == plan.starting_mutation_epoch + 1


def test_final_summary_fails_closed_on_missing_or_drifted_evidence(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    store = _store(tmp_path, plan)
    store.publish(_execution("final.protected-apply", plan, candidate_sha="f" * 40))

    result = FinalSummaryExecutor(tmp_path / "state", store.service_uid)(
        "final.summary",
        CheckOperation.VERIFY,
        plan,
    )

    assert result.blockers == {
        "coverage": "final-gate-evidence-incomplete",
        "final.protected-apply": "final-gate-evidence-drift",
    }
