"""SkillFlow + SkillLearnBench adapter contract (Plan 15 Phase 11).

Both adapters share the same passthrough behavior; one parametrized
test exercises both."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from loom_benchmarks.adapters.skillflow import SkillFlowAdapter
from loom_benchmarks.adapters.skilllearnbench import SkillLearnBenchAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_name"),
    [
        (SkillFlowAdapter, "skillflow"),
        (SkillLearnBenchAdapter, "skilllearnbench"),
    ],
)
def test_skill_adapter_passthrough(
    adapter_cls: type, fixture_name: str, tmp_path: Path,
) -> None:
    rec = json.loads(
        (
            Path(__file__).parent / "fixtures" / fixture_name / "sample.json"
        ).read_text(),
    )[0]
    adapter = adapter_cls()
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    adapter.convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "pytest"
    assert cfg.task.id.startswith(f"{adapter.name}/")
    assert (tmp_path / "instruction.md").exists()
    assert (tmp_path / "tests" / "test_main.py").exists()


@pytest.mark.parametrize(
    ("adapter_cls", "fixture_name"),
    [
        (SkillFlowAdapter, "skillflow"),
        (SkillLearnBenchAdapter, "skilllearnbench"),
    ],
)
def test_skill_solution_runs(
    adapter_cls: type, fixture_name: str, tmp_path: Path,
) -> None:
    """The pre-baked solution + tests in the upstream bundle are
    expected to pass under pytest end-to-end."""
    rec = json.loads(
        (
            Path(__file__).parent / "fixtures" / fixture_name / "sample.json"
        ).read_text(),
    )[0]
    adapter = adapter_cls()
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    adapter.convert_instance(inst, out_dir=tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
