from __future__ import annotations

from pathlib import Path

from loom_cli.rollout import final_gate_helper as helper
from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlanStore
from loom_cli.rollout.preflight_contract import CheckOperation
from tests.loom_cli.rollout.operator.test_final_gate_action_source import _authority
from tests.loom_cli.rollout.operator.test_final_gate_plan import _envelope
from tests.loom_cli.rollout.operator.test_final_gate_runner import _admission


def _prepared(tmp_path: Path, monkeypatch):
    source, attestation, _calls = _authority(tmp_path)
    envelope = _envelope(attestation)
    source(envelope, attestation, 7, _admission(attestation))
    state = tmp_path / "state"
    path = state / "requests/req-alpha/attempts/1/final-gate-plan.json"
    digest = FinalGatePlanStore(state, request_id="req-alpha", attempt_number=1).read().plan_digest
    monkeypatch.setattr(helper, "_STATE_ROOT", state)
    monkeypatch.setattr(helper, "_verify_checkpoint", lambda _plan: None)
    return attestation, path, digest


def test_final_gate_helper_loads_exact_plan_and_emits_strict_result(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    attestation, path, digest = _prepared(tmp_path, monkeypatch)

    def execute(check_id, operation, _plan):
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="a" * 40,
            attestation_digest=attestation.attestation_digest,
            observed_epoch=7,
            evidence_digest="e" * 64,
            protected_mutation=False,
            blockers={},
        )

    rc = helper.main(
        [
            "execute",
            "--check-id",
            "final.browser",
            "--operation",
            "verify",
            "--plan",
            str(path),
            "--plan-sha256",
            digest,
        ],
        execute=execute,
    )

    assert rc == 0
    assert '"check_id":"final.browser"' in capsys.readouterr().out


def test_final_gate_helper_fails_closed_without_executor_or_on_wrong_operation(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _attestation, path, digest = _prepared(tmp_path, monkeypatch)
    base = [
        "execute",
        "--check-id",
        "final.protected-apply",
        "--plan",
        str(path),
        "--plan-sha256",
        digest,
    ]

    assert helper.main([*base, "--operation", "apply"]) == 2
    assert helper.main([*base, "--operation", "verify"]) == 2
    assert capsys.readouterr().out == ""


def test_final_gate_helper_rejects_plan_digest_drift(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _attestation, path, _digest = _prepared(tmp_path, monkeypatch)

    assert (
        helper.main(
            [
                "execute",
                "--check-id",
                "final.summary",
                "--operation",
                CheckOperation.VERIFY.value,
                "--plan",
                str(path),
                "--plan-sha256",
                "f" * 64,
            ]
        )
        == 2
    )
    assert capsys.readouterr().out == ""


def test_final_gate_helper_requires_one_epoch_advance_for_successful_apply(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    attestation, path, digest = _prepared(tmp_path, monkeypatch)

    def execute(check_id, operation, _plan):
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha="a" * 40,
            attestation_digest=attestation.attestation_digest,
            observed_epoch=7,
            evidence_digest="e" * 64,
            protected_mutation=True,
            blockers={},
        )

    assert (
        helper.main(
            [
                "execute",
                "--check-id",
                "final.protected-apply",
                "--operation",
                "apply",
                "--plan",
                str(path),
                "--plan-sha256",
                digest,
            ],
            execute=execute,
        )
        == 2
    )
    assert capsys.readouterr().out == ""
