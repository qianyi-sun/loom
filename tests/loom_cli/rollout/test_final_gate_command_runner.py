from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.final_gate_command_runner import InstalledFinalGateStepRunner
from loom_cli.rollout.preflight_contract import CheckOperation


def _files(tmp_path: Path) -> tuple[Path, Path]:
    executable = tmp_path / "helper"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    plan = tmp_path / "plan.json"
    plan.write_text("{}\n")
    plan.chmod(0o600)
    return executable, plan


def test_installed_final_gate_runner_uses_fixed_argv_and_strict_result(tmp_path: Path) -> None:
    executable, plan = _files(tmp_path)
    captured: list[tuple[tuple[str, ...], dict[str, str], int]] = []

    def run(argv, environment, timeout):
        captured.append((tuple(argv), dict(environment), timeout))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "attestation_digest": "b" * 64,
                    "blockers": {},
                    "candidate_sha": "a" * 40,
                    "check_id": "final.protected-apply",
                    "evidence_digest": "c" * 64,
                    "observed_epoch": 8,
                    "operation": "apply",
                    "protected_mutation": True,
                    "schema_version": 1,
                },
                sort_keys=True,
            ),
            "",
        )

    runner = InstalledFinalGateStepRunner(
        service_uid=os.geteuid(),
        plan_path=plan,
        plan_digest="d" * 64,
        run=run,
        executable=executable,
        executable_owner_uid=os.geteuid(),
    )
    result = runner(
        "final.protected-apply",
        CheckOperation.APPLY,
        candidate_sha="a" * 40,
        attestation_digest="b" * 64,
        mutation_epoch=7,
    )

    assert result.ready and result.protected_mutation
    assert captured[0][0] == (
        str(executable),
        "execute",
        "--check-id",
        "final.protected-apply",
        "--operation",
        "apply",
        "--plan",
        str(plan),
        "--plan-sha256",
        "d" * 64,
    )
    assert set(captured[0][1]) == {"HOME", "LANG", "LC_ALL", "PATH", "XDG_RUNTIME_DIR"}
    assert captured[0][2] == 3600


def test_installed_final_gate_runner_rejects_wrong_operation_or_output(tmp_path: Path) -> None:
    executable, plan = _files(tmp_path)
    runner = InstalledFinalGateStepRunner(
        service_uid=os.geteuid(),
        plan_path=plan,
        plan_digest="d" * 64,
        run=lambda argv, _environment, _timeout: subprocess.CompletedProcess(argv, 0, "{}", ""),
        executable=executable,
        executable_owner_uid=os.geteuid(),
    )

    with pytest.raises(ValueError, match="operation"):
        runner(
            "final.browser",
            CheckOperation.APPLY,
            candidate_sha="a" * 40,
            attestation_digest="b" * 64,
            mutation_epoch=7,
        )
    with pytest.raises(ValueError, match="operation"):
        runner(
            "final.smoke",
            CheckOperation.VERIFY,
            candidate_sha="a" * 40,
            attestation_digest="b" * 64,
            mutation_epoch=7,
        )
    with pytest.raises(ValueError, match="evidence drifted"):
        runner(
            "final.browser",
            CheckOperation.VERIFY,
            candidate_sha="a" * 40,
            attestation_digest="b" * 64,
            mutation_epoch=7,
        )
