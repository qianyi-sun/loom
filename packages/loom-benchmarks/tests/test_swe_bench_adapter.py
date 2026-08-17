"""SWE-Bench (full) adapter contract (Plan 15 Phase 2)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.swe_bench import SWEBenchAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "swe_bench" / "sample.json"


def test_swe_bench_full_uses_swe_bench_prefix(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id == "swe-bench/astropy__astropy-12907"
    assert cfg.verifier.name == "pytest"
    assert cfg.required_agent_capabilities == frozenset({"workspace_exec"})


def test_swe_bench_full_inherits_image_rule(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    # Inherited from the verified adapter — same image slug rule.
    assert "swebench/sweb.eval.x86_64" in (cfg.environment.docker_image or "")
    assert "_1776_" in (cfg.environment.docker_image or "")
