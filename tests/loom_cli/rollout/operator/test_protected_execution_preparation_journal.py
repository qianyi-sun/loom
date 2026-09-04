from __future__ import annotations

import os
from dataclasses import replace

import pytest

from loom_cli.rollout.operator.protected_execution_preparation_journal import (
    ExecutionPreparationOperationIntent,
    ExecutionPreparationOperationJournal,
    ExecutionPreparationOperationTerminal,
    ExecutionPreparationRecoveryState,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _execution_plan


def _journal(tmp_path):  # type: ignore[no-untyped-def]
    plan = _execution_plan(tmp_path)
    journal = ExecutionPreparationOperationJournal(
        (tmp_path / "state").resolve(),
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    return journal, plan


def _intent(plan, operation: str, *, request_sha256: str):  # type: ignore[no-untyped-def]
    return ExecutionPreparationOperationIntent.build(
        plan=plan,
        artifact_sha256="a" * 64,
        operation=operation,
        request_sha256=request_sha256,
        prepared_execution_epoch=(None if operation == "manager-preparation" else 1),
        prepared_execution_manifest_sha256=(
            None if operation == "manager-preparation" else "b" * 64
        ),
    )


def _terminal(intent, *, result_state: str = "prepared"):  # type: ignore[no-untyped-def]
    return ExecutionPreparationOperationTerminal.build(
        intent=intent,
        evidence_sha256="c" * 64,
        prepared_execution_epoch=1,
        prepared_execution_manifest_sha256="b" * 64,
        result_state=result_state,
    )


def test_operation_journal_classifies_every_recovery_state_from_write_once_records(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    journal, plan = _journal(tmp_path)

    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.NO_MUTATION
    )

    preparation = _intent(plan, "manager-preparation", request_sha256="1" * 64)
    journal.record_intent(preparation)
    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.UNRESOLVED
    )

    journal.record_terminal(_terminal(preparation))
    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.PREPARED
    )

    for index, operation in enumerate(
        (
            "controller-files-gb10",
            "controller-files-oldlab",
            "prepared-timer-gb10",
            "prepared-timer-oldlab",
            "prepared-tick-gb10",
            "prepared-tick-oldlab",
        ),
        start=2,
    ):
        intent = _intent(plan, operation, request_sha256=f"{index:064x}")
        journal.record_intent(intent)
        journal.record_terminal(_terminal(intent))

    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.FORWARD_COMPLETE
    )

    abort = _intent(plan, "manager-abort", request_sha256="f" * 64)
    journal.record_intent(abort)
    journal.record_terminal(_terminal(abort, result_state="shadow"))
    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.COMPENSATED
    )


def test_operation_journal_recovers_linked_temporary_publication_residue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    """Break caught: a crash after link publication makes a valid record unreadable."""

    journal, plan = _journal(tmp_path)
    preparation = _intent(plan, "manager-preparation", request_sha256="1" * 64)
    journal.record_intent(preparation)
    final = journal.root / "manager-preparation.intent.json"
    temporary = journal.root / (f"..{final.name}.loom-{'0' * 32}.tmp")
    os.link(final, temporary)
    assert final.stat().st_nlink == 2

    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.UNRESOLVED
    )
    assert final.stat().st_nlink == 1
    assert not temporary.exists()


def test_operation_journal_ignores_unpublished_temporary_residue(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    """Break caught: an unlinked temporary write is mistaken for durable intent."""

    journal, plan = _journal(tmp_path)
    preparation = _intent(plan, "manager-preparation", request_sha256="1" * 64)
    journal.record_intent(preparation)
    final = journal.root / "manager-preparation.intent.json"
    temporary = journal.root / (f"..{final.name}.loom-{'0' * 32}.tmp")
    final.rename(temporary)

    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.NO_MUTATION
    )

    journal.record_intent(preparation)
    assert journal.recovery_state(plan, artifact_sha256="a" * 64) is (
        ExecutionPreparationRecoveryState.UNRESOLVED
    )


def test_operation_journal_refuses_unrecognized_second_link(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    """Break caught: an arbitrary hard link is accepted as publication residue."""

    journal, plan = _journal(tmp_path)
    preparation = _intent(plan, "manager-preparation", request_sha256="1" * 64)
    journal.record_intent(preparation)
    os.link(
        journal.root / "manager-preparation.intent.json",
        tmp_path / "unrecognized-hard-link",
    )

    with pytest.raises(RuntimeError, match="temporary link residue is ambiguous"):
        journal.recovery_state(plan, artifact_sha256="a" * 64)


def test_operation_journal_rejects_replacement_and_terminal_without_exact_intent(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    journal, plan = _journal(tmp_path)
    intent = _intent(plan, "manager-preparation", request_sha256="1" * 64)
    journal.record_intent(intent)

    with pytest.raises(RuntimeError, match="already drifted"):
        journal.record_intent(_intent(plan, "manager-preparation", request_sha256="2" * 64))

    mismatched = _intent(plan, "manager-preparation", request_sha256="2" * 64)
    with pytest.raises(RuntimeError, match="intent drifted"):
        journal.record_terminal(_terminal(mismatched))


def test_operation_journal_rejects_malformed_inventory_and_plan_binding_drift(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    journal, plan = _journal(tmp_path)
    journal.record_intent(_intent(plan, "manager-preparation", request_sha256="1" * 64))
    unexpected = journal.root / "manager-preparation.unknown.json"
    unexpected.write_text("{}\n", encoding="ascii")
    unexpected.chmod(0o600)

    with pytest.raises(RuntimeError, match="inventory"):
        journal.recovery_state(plan, artifact_sha256="a" * 64)

    unexpected.unlink()
    with pytest.raises(RuntimeError, match="binding"):
        journal.recovery_state(
            replace(plan, plan_digest="d" * 64),
            artifact_sha256="a" * 64,
        )
