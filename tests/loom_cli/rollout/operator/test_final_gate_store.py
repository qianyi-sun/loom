from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType

import pytest

from loom_cli.rollout.operator.final_gate_store import (
    FinalGateExecutionStore,
    FinalGateStoreError,
)
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
)

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)


def _execution() -> CheckExecution:
    evidence = {
        "ready": True,
        "evidence-digest": "3" * 64,
    }
    return CheckExecution(
        check_id="final.protected-apply",
        failure_code="final.protected-apply.failed",
        tier=4,
        stage=StageCapability.FINAL_ONLY,
        operation=CheckOperation.APPLY,
        outcome=CheckOutcome.PASS,
        input_fingerprint="1" * 64,
        implementation_digest="2" * 64,
        evidence=MappingProxyType(evidence),
        evidence_hash=hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        started_at=NOW,
        finished_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        remediation=None,
    )


def _store(tmp_path: Path) -> FinalGateExecutionStore:
    attempt = tmp_path / "requests" / "req-alpha" / "attempts" / "1"
    attempt.mkdir(parents=True, mode=0o700)
    return FinalGateExecutionStore(
        tmp_path,
        request_id="req-alpha",
        attempt_number=1,
    )


def test_final_gate_store_publishes_once_and_round_trips(tmp_path: Path) -> None:
    store = _store(tmp_path)
    execution = _execution()

    path = store.publish(execution)

    assert store.publish(execution) == path
    assert store.read_all() == {execution.check_id: execution}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.root.stat().st_mode) == 0o700


def test_final_gate_store_refuses_replacement_or_linked_evidence(tmp_path: Path) -> None:
    store = _store(tmp_path)
    execution = _execution()
    path = store.publish(execution)

    with pytest.raises(FinalGateStoreError, match="cannot be replaced"):
        store.publish(replace(execution, expires_at=execution.expires_at + timedelta(hours=1)))

    target = tmp_path / "outside.json"
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises((FinalGateStoreError, OSError)):
        store.read_all()


def test_final_gate_store_appends_repeatable_check_revisions(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = replace(
        _execution(),
        check_id="final.convergence",
        failure_code="final.convergence.failed",
        operation=CheckOperation.VERIFY,
        outcome=CheckOutcome.FAIL,
    )
    second = replace(
        first,
        outcome=CheckOutcome.PASS,
        started_at=NOW + timedelta(minutes=2),
        finished_at=NOW + timedelta(minutes=2),
        expires_at=NOW + timedelta(hours=1, minutes=2),
    )

    first_path = store.publish(first)
    second_path = store.publish(second)

    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()
    assert store.read_all() == {"final.convergence": second}
