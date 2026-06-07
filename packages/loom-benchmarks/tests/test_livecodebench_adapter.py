"""LiveCodeBench adapter contract (Plan 15 Phase 9)."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.livecodebench import LiveCodeBenchAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "livecodebench" / "sample.json"


def test_livecodebench_emits_io_pytest(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "pytest"
    assert cfg.task.id == "livecodebench/lcb-9001"
    tests = list((tmp_path / "tests").glob("test_lcb_*.py"))
    assert len(tests) == 3  # 2 public + 1 private
    assert "55" in (tmp_path / "tests" / "test_lcb_2.py").read_text()


def test_livecodebench_license_is_cc_by_nc(tmp_path: Path) -> None:
    """Spec §7: LiveCodeBench tasks must carry `CC-BY-NC-4.0` so the
    Plan 13 license-allowlist keeps them out of the default allowlist
    until an operator opts in for non-commercial use (audit license-bypass)."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    converted = LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    assert converted.license_spdx == "CC-BY-NC-4.0"
    assert LiveCodeBenchAdapter.license_spdx == "CC-BY-NC-4.0"


def test_livecodebench_solution_passes_subprocess_run(tmp_path: Path) -> None:
    """End-to-end: the canonical Fibonacci solution passes its own
    stdin tests when run via pytest in a subprocess."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["question_id"], split="test", raw=rec,
    )
    LiveCodeBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
