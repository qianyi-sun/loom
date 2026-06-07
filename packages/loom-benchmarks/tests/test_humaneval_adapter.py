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
        instance_id=rec["task_id"], split="test", raw=rec,
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
    assert (out / "solution" / "solution.py").exists()
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


def test_convert_instance_solution_runs(tmp_path: Path) -> None:
    """The canonical solution + upstream tests must pass under pytest."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="test", raw=rec,
    )
    adapter = HumanEvalAdapter()
    out = tmp_path / "HumanEval__0"
    out.mkdir()
    adapter.convert_instance(inst, out_dir=out)

    # Run pytest in a subprocess so sys.modules state doesn't leak into
    # the outer test runner. Working dir is the converted task dir so
    # `from solution import ...` resolves from the local solution/.
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q"],
        cwd=out,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )


def test_checksum_deterministic(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="test", raw=rec,
    )
    adapter = HumanEvalAdapter()
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    out1.mkdir()
    out2.mkdir()
    c1 = adapter.convert_instance(inst, out_dir=out1)
    c2 = adapter.convert_instance(inst, out_dir=out2)
    assert c1.checksum == c2.checksum
