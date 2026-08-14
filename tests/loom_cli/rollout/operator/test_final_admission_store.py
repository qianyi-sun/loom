from __future__ import annotations

import os
import stat
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.final_admission_store import (
    FinalAdmissionStore,
    FinalAdmissionStoreError,
)
from tests.loom_cli.rollout.operator.test_final_gate_runner import (
    _admission,
    _valid_attestation,
)


def _store(tmp_path: Path) -> FinalAdmissionStore:
    attempt = tmp_path / "requests" / "req-alpha" / "attempts" / "1"
    attempt.mkdir(parents=True, mode=0o700)
    os.chmod(attempt, 0o700)
    return FinalAdmissionStore(
        tmp_path,
        request_id="req-alpha",
        attempt_number=1,
    )


def _persistable_admission():
    attestation = _valid_attestation()
    admission = _admission(attestation)
    return replace(
        admission,
        tier0_executions=tuple(
            replace(
                execution,
                implementation_digest=attestation.check_implementation_digests[execution.check_id],
            )
            for execution in admission.tier0_executions
        ),
        tier2_executions=tuple(
            replace(
                execution,
                implementation_digest=attestation.check_implementation_digests[execution.check_id],
            )
            for execution in admission.tier2_executions
        ),
    )


def test_final_admission_store_round_trips_exact_executions(tmp_path: Path) -> None:
    store = _store(tmp_path)
    admission = _persistable_admission()
    attestation = admission.attestation

    path = store.publish(admission)

    assert store.publish(admission) == path
    assert store.read(attestation) == admission
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.stat().st_nlink == 1


def test_final_admission_store_refuses_replacement_or_linked_evidence(tmp_path: Path) -> None:
    store = _store(tmp_path)
    admission = _persistable_admission()
    attestation = admission.attestation
    path = store.publish(admission)

    changed = replace(
        admission,
        tier0_executions=(
            replace(
                admission.tier0_executions[0],
                remediation="different admission",
            ),
        ),
    )
    with pytest.raises(FinalAdmissionStoreError, match="cannot be replaced"):
        store.publish(changed)

    outside = tmp_path / "outside.json"
    path.rename(outside)
    path.symlink_to(outside)
    with pytest.raises((FinalAdmissionStoreError, OSError)):
        store.read(attestation)


def test_final_admission_store_ignores_nonserializable_runtime_plan_on_republish(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    admission = replace(
        _persistable_admission(),
        preflight_plan=SimpleNamespace(candidate="runtime-only"),  # type: ignore[arg-type]
    )

    path = store.publish(admission)

    assert store.publish(admission) == path
    assert store.read(admission.attestation) == replace(admission, preflight_plan=None)
