"""MBPP adapter contract (Plan 15 Phase 6)."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.mbpp import MBPPAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "mbpp" / "sample.json"


def test_mbpp_emits_pytest_per_case(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]),
        split="test",
        raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "pytest"
    solve_sh = tmp_path / "solution" / "solve.sh"
    assert solve_sh.exists()
    assert solve_sh.stat().st_mode & 0o111
    tests = list((tmp_path / "tests").glob("test_mbpp_*.py"))
    assert len(tests) == 3
    # Upstream reference lives at `_reference.py`; the agent-editable
    # `solution.py` is a stub (raises NotImplementedError on import)
    # so non-oracle agents can't silently pass on the pre-shipped
    # answer. Oracle's solve.sh copies _reference over solution.
    assert (
        (tmp_path / "solution" / "_reference.py")
        .read_text()
        .startswith(
            "\ndef remove_Occ",
        )
    )
    stub = (tmp_path / "solution" / "solution.py").read_text()
    assert "NotImplementedError" in stub


def test_mbpp_task_id_namespaced(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]),
        split="test",
        raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id == "mbpp/11"


def test_mbpp_solution_runs_against_tests_after_oracle_copy(
    tmp_path: Path,
) -> None:
    """End-to-end: simulating oracle's solve.sh (copy _reference →
    solution.py), pytest must pass. Proves both the reference is
    correct AND the oracle copy mechanism wires up properly."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]),
        split="test",
        raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    # Run the bundled solve.sh (what oracle does at trial start).
    copy_result = subprocess.run(
        ["bash", "solution/solve.sh"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert copy_result.returncode == 0, copy_result.stderr
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"


def test_mbpp_solution_fails_pytest_when_stub_not_replaced(
    tmp_path: Path,
) -> None:
    """Without running solve.sh (i.e. agent didn't write solution.py),
    pytest must FAIL — the stub raises NotImplementedError on import.
    Guards against the #388 false-positive class of bug."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]),
        split="test",
        raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, (
        "pytest passed against the stub — #388 regression "
        f"(stdout={result.stdout})"
    )
