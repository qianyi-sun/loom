"""HumanEvalAdapter contract: emits a valid TaskConfig and a runnable
solution+tests bundle (Plan 14 Task 5)."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.humaneval import HumanEvalAdapter
from loom_benchmarks.base import BenchmarkInstance
from loom_benchmarks.registry import REGISTRY

FIXTURE = Path(__file__).parent / "fixtures" / "humaneval" / "sample.json"


def test_registry_lists_humaneval() -> None:
    adapter = REGISTRY["humaneval"]
    assert adapter.name == "humaneval"
    assert adapter.license_spdx == "MIT"


def test_convert_instance_writes_complete_fixture(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"],
        split="test",
        raw=rec,
    )
    adapter = HumanEvalAdapter()
    out = tmp_path / "HumanEval__0"
    out.mkdir()
    converted = adapter.convert_instance(inst, out_dir=out)

    assert converted.task_id == "humaneval/HumanEval/0"
    assert converted.license_spdx == "MIT"
    assert converted.warnings == ()
    assert len(converted.checksum) == 64
    assert (out / "task.toml").exists()
    assert (out / "instruction.md").exists()
    # Upstream reference at `_reference.py`; `solution.py` is a stub
    # (raises NotImplementedError). Oracle's solve.sh copies the
    # reference; other agents must overwrite the stub.
    assert (out / "solution" / "_reference.py").exists()
    stub = (out / "solution" / "solution.py").read_text()
    assert "NotImplementedError" in stub
    solve_sh = out / "solution" / "solve.sh"
    assert solve_sh.exists()
    assert solve_sh.stat().st_mode & 0o111
    assert (out / "solution" / "__init__.py").exists()
    assert (out / "tests" / "test_humaneval.py").exists()

    # The emitted task.toml validates against TaskConfig.
    from loom.models.task import TaskConfig

    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.task.id == "humaneval/HumanEval/0"
    assert cfg.verifier.name == "pytest"
    assert cfg.environment.docker_image == "python:3.11-slim"
    assert cfg.agent.name == "oracle"


def test_convert_instance_solution_runs_after_oracle_copy(tmp_path: Path) -> None:
    """End-to-end: after oracle's solve.sh runs (copy _reference →
    solution.py), pytest must pass. Proves the reference is correct
    and the oracle copy mechanism wires up properly."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"],
        split="test",
        raw=rec,
    )
    adapter = HumanEvalAdapter()
    out = tmp_path / "HumanEval__0"
    out.mkdir()
    adapter.convert_instance(inst, out_dir=out)

    copy_result = subprocess.run(
        ["bash", "solution/solve.sh"],
        cwd=out, capture_output=True, text=True,
    )
    assert copy_result.returncode == 0, copy_result.stderr
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=out,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"


def test_convert_instance_pytest_fails_without_oracle_copy(tmp_path: Path) -> None:
    """Stub solution.py must cause pytest to FAIL — guards #388
    (false positives when bundle ships the reference solution)."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"],
        split="test",
        raw=rec,
    )
    adapter = HumanEvalAdapter()
    out = tmp_path / "HumanEval__0_stub"
    out.mkdir()
    adapter.convert_instance(inst, out_dir=out)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=out, capture_output=True, text=True,
    )
    assert result.returncode != 0, (
        f"pytest passed against the stub — #388 regression "
        f"(stdout={result.stdout})"
    )


def test_checksum_deterministic(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"],
        split="test",
        raw=rec,
    )
    adapter = HumanEvalAdapter()
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    out1.mkdir()
    out2.mkdir()
    c1 = adapter.convert_instance(inst, out_dir=out1)
    c2 = adapter.convert_instance(inst, out_dir=out2)
    assert c1.checksum == c2.checksum
