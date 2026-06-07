"""MBPP adapter contract (Plan 15 Phase 6)."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

from loom.models.task import TaskConfig
from loom_benchmarks.adapters.mbpp import MBPPAdapter
from loom_benchmarks.base import BenchmarkInstance

FIXTURE = Path(__file__).parent / "fixtures" / "mbpp" / "sample.json"


def test_mbpp_emits_pytest_per_case(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]), split="test", raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "pytest"
    tests = list((tmp_path / "tests").glob("test_mbpp_*.py"))
    assert len(tests) == 3
    assert (tmp_path / "solution" / "solution.py").read_text().startswith(
        "\ndef remove_Occ",
    )


def test_mbpp_task_id_namespaced(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]), split="test", raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id == "mbpp/11"


def test_mbpp_solution_runs_against_tests(tmp_path: Path) -> None:
    """Subprocess pytest run proves the converted dir is end-to-end
    executable — the canonical solution passes its own test_list."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]), split="test", raw=rec,
    )
    MBPPAdapter().convert_instance(inst, out_dir=tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
