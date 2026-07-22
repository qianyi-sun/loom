"""Contract tests for the native TB2.1 reward-file verifier shim."""

from __future__ import annotations

import json
import shutil
import subprocess
from importlib.resources import files
from pathlib import Path

import pytest

from loom.models.verifier import VerifierResult


@pytest.fixture
def shim_path(tmp_path: Path) -> Path:
    dst = tmp_path / "run.sh"
    dst.write_bytes(
        files("loom_benchmark_terminal_bench_2").joinpath("verifier_shim.sh").read_bytes(),
    )
    dst.chmod(0o755)
    return dst


def _make_native_task(tmp_path: Path, test_body: str) -> Path:
    task_dir = tmp_path / "native-task"
    tests = task_dir / "tests"
    tests.mkdir(parents=True)
    test_sh = tests / "test.sh"
    test_sh.write_text("#!/bin/sh\nset -eu\n" + test_body)
    test_sh.chmod(0o755)
    return task_dir


def _run_shim(
    shim: Path,
    *,
    task_dir: Path,
    output: Path,
) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None, "bash required for verifier_shim contract tests"
    scratch = output.parent / "verifier-scratch"
    return subprocess.run(
        [bash, str(shim)],
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LOOM_TASK_DIR": str(task_dir),
            "LOOM_VERIFIER_OUTPUT": str(output),
            "TB21_REWARD_PATH": str(scratch / "reward.txt"),
            "TB21_VERIFIER_LOG_DIR": str(scratch),
            "TB21_TEST_MOUNT_DIR": str(scratch / "tests"),
        },
        capture_output=True,
        text=True,
        check=False,
    )


def test_shim_emits_numeric_reward_and_retains_ctrf_metadata(
    shim_path: Path,
    tmp_path: Path,
) -> None:
    task_dir = _make_native_task(
        tmp_path,
        'printf "1\\n" > "$TB21_REWARD_PATH"\nprintf "{}\\n" > "$TB21_VERIFIER_LOG_DIR/ctrf.json"\n',
    )
    output = tmp_path / "verifier.json"

    proc = _run_shim(shim_path, task_dir=task_dir, output=output)

    assert proc.returncode == 0, proc.stderr
    result = VerifierResult.model_validate(json.loads(output.read_text()))
    assert result.rewards == {"resolved": 1.0}
    assert result.checks[0].name == "tb21_native_tests"
    assert result.checks[0].passed is True
    assert result.structured is not None
    assert result.structured["artifacts"]["ctrf_path"].endswith("ctrf.json")


def test_shim_keeps_zero_reward_as_a_successful_verifier_result(
    shim_path: Path,
    tmp_path: Path,
) -> None:
    task_dir = _make_native_task(
        tmp_path,
        'printf "0\\n" > "$TB21_REWARD_PATH"\n',
    )
    output = tmp_path / "verifier.json"

    proc = _run_shim(shim_path, task_dir=task_dir, output=output)

    assert proc.returncode == 0, proc.stderr
    result = VerifierResult.model_validate(json.loads(output.read_text()))
    assert result.rewards == {"resolved": 0.0}
    assert result.checks[0].passed is False
    assert result.error is None


@pytest.mark.parametrize(
    ("test_body", "failure_kind"),
    [
        ("true\n", "missing_reward"),
        ('printf "\\n" > "$TB21_REWARD_PATH"\n', "empty_reward"),
        ('printf "bad\\n" > "$TB21_REWARD_PATH"\n', "malformed_reward"),
        ("exit 124\n", "timeout"),
        ('printf "0\\n" > "$TB21_REWARD_PATH"\nexit 124\n', "timeout"),
    ],
)
def test_shim_turns_invalid_reward_evidence_into_verifier_failure(
    shim_path: Path,
    tmp_path: Path,
    test_body: str,
    failure_kind: str,
) -> None:
    task_dir = _make_native_task(tmp_path, test_body)
    output = tmp_path / "verifier.json"

    proc = _run_shim(shim_path, task_dir=task_dir, output=output)

    assert proc.returncode != 0
    assert not output.exists()
    assert f"tb21_reward_error={failure_kind}" in proc.stderr
