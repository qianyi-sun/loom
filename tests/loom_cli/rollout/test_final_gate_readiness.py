from __future__ import annotations

from collections.abc import Callable

from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    FinalGateResult,
    FinalGateSession,
)
from loom_cli.rollout.preflight_contract import CheckOperation


def _result(check_id: str, operation: CheckOperation) -> FinalGateResult:
    return FinalGateResult(
        check_id=check_id,
        operation=operation,
        candidate_sha="a" * 40,
        attestation_digest="b" * 64,
        observed_epoch=8 if operation is CheckOperation.APPLY else 7,
        evidence_digest="c" * 64,
        protected_mutation=bool(
            check_id == "final.protected-apply" and operation is CheckOperation.APPLY
        ),
        blockers={},
    )


def test_final_gate_session_caches_each_exact_operation() -> None:
    calls: list[tuple[str, CheckOperation]] = []

    def action(check_id: str) -> Callable[[CheckOperation], FinalGateResult]:
        def execute(operation: CheckOperation) -> FinalGateResult:
            calls.append((check_id, operation))
            return _result(check_id, operation)

        return execute

    actions = {
        check_id: action(check_id)
        for check_id in FINAL_CHECK_IDS
    }
    session = FinalGateSession(
        actions,
        candidate_sha="a" * 40,
        attestation_digest="b" * 64,
        mutation_epoch=7,
    )
    first = session.execute("final.protected-apply", CheckOperation.APPLY)
    second = session.execute("final.protected-apply", CheckOperation.APPLY)
    assert first is second
    assert calls == [("final.protected-apply", CheckOperation.APPLY)]


def test_only_protected_apply_may_report_protected_mutation() -> None:
    result = _result("final.protected-apply", CheckOperation.APPLY)
    assert result.protected_mutation

    try:
        FinalGateResult(
            check_id="final.browser",
            operation=CheckOperation.VERIFY,
            candidate_sha="a" * 40,
            attestation_digest="b" * 64,
            observed_epoch=8,
            evidence_digest="c" * 64,
            protected_mutation=True,
            blockers={},
        )
    except ValueError:
        pass
    else:
        raise AssertionError("non-apply final gate accepted protected mutation")
