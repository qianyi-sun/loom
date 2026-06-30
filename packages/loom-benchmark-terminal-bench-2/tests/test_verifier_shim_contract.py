"""Drift guard for `verifier_shim.sh`.

The shim is the bridge between upstream TB-2's `run-tests.sh` exit code
and Loom's `VerifierResult` JSON contract consumed by `ScriptVerifier`.
It is short and dependency-free, which makes it easy to refactor and
just as easy to silently break the JSON contract — pinned by these
tests so a future shim or pin bump surfaces the drift.

Tests subprocess `bash` to exercise the real script (not a Python
re-implementation), redirect `LOOM_VERIFIER_OUTPUT` to a tmp file, then
validate the captured JSON via the same pydantic `VerifierResult`
model the production worker uses.
"""

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
    bytes_ = (
        files("loom_benchmark_terminal_bench_2")
        .joinpath("verifier_shim.sh")
        .read_bytes()
    )
    dst = tmp_path / "run.sh"
    dst.write_bytes(bytes_)
    dst.chmod(0o755)
    return dst


def _make_test_dir(tmp_path: Path, *, exit_code: int) -> Path:
    test_dir = tmp_path / "tb2-tests"
    test_dir.mkdir()
    run_tests = test_dir / "run-tests.sh"
    run_tests.write_text(f"#!/bin/sh\nexit {exit_code}\n")
    run_tests.chmod(0o755)
    return test_dir


def _run_shim(
    shim: Path, *, test_dir: Path, verifier_output: Path,
) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None, "bash required for verifier_shim contract tests"
    return subprocess.run(
        [bash, str(shim)],
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "TEST_DIR": str(test_dir),
            "LOOM_VERIFIER_OUTPUT": str(verifier_output),
        },
        capture_output=True,
        text=True,
        check=False,
    )


def test_shim_emits_valid_verifier_result_on_pass(
    shim_path: Path, tmp_path: Path,
) -> None:
    test_dir = _make_test_dir(tmp_path, exit_code=0)
    output = tmp_path / "verifier.json"
    proc = _run_shim(shim_path, test_dir=test_dir, verifier_output=output)
    assert proc.returncode == 0, (
        f"shim exited non-zero: stderr={proc.stderr!r}"
    )
    parsed = json.loads(output.read_text())
    result = VerifierResult.model_validate(parsed)
    assert result.rewards["resolved"] == 1.0
    assert len(result.checks) == 1
    check = result.checks[0]
    assert check.name == "tb2_run_tests"
    assert check.passed is True
    assert check.score == 1.0
    assert check.message == "exit=0"


def test_shim_emits_valid_verifier_result_on_fail(
    shim_path: Path, tmp_path: Path,
) -> None:
    test_dir = _make_test_dir(tmp_path, exit_code=1)
    output = tmp_path / "verifier.json"
    proc = _run_shim(shim_path, test_dir=test_dir, verifier_output=output)
    assert proc.returncode == 0, (
        f"shim exited non-zero on test-failure: stderr={proc.stderr!r}"
    )
    parsed = json.loads(output.read_text())
    result = VerifierResult.model_validate(parsed)
    assert result.rewards["resolved"] == 0.0
    assert len(result.checks) == 1
    check = result.checks[0]
    assert check.name == "tb2_run_tests"
    assert check.passed is False
    assert check.score == 0.0
    assert check.message == "exit=1"


def test_shim_preserves_nonzero_test_exit_in_check_message(
    shim_path: Path, tmp_path: Path,
) -> None:
    """Operators rely on the `exit=<N>` message to distinguish ordinary
    test failure (1) from runtime classes (timeout 124, missing-bash 127,
    etc.). Pin the format so log-greppers and dashboards keep working."""
    test_dir = _make_test_dir(tmp_path, exit_code=42)
    output = tmp_path / "verifier.json"
    proc = _run_shim(shim_path, test_dir=test_dir, verifier_output=output)
    assert proc.returncode == 0
    parsed = json.loads(output.read_text())
    result = VerifierResult.model_validate(parsed)
    assert result.checks[0].message == "exit=42"
    assert result.checks[0].passed is False


def test_shim_creates_output_directory_when_missing(
    shim_path: Path, tmp_path: Path,
) -> None:
    """`ScriptVerifier` materializes `LOOM_VERIFIER_OUTPUT` under a
    per-step `steps/<name>/` path that may not exist yet; the shim is
    responsible for creating the parent. Without this, a fresh task
    bundle would fail with `No such file or directory` before any test
    even runs."""
    test_dir = _make_test_dir(tmp_path, exit_code=0)
    nested = tmp_path / "nested" / "step-main" / "verifier.json"
    proc = _run_shim(shim_path, test_dir=test_dir, verifier_output=nested)
    assert proc.returncode == 0, (
        f"shim failed to create parent dir: stderr={proc.stderr!r}"
    )
    assert nested.exists()
    VerifierResult.model_validate(json.loads(nested.read_text()))
