"""WebArena adapter contract (Plan 15 Phase 5)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom.models.task import TaskConfig
from loom_benchmarks.adapters.webarena import WebArenaAdapter
from loom_benchmarks.base import BenchmarkInstance

FIXTURE = Path(__file__).parent / "fixtures" / "webarena" / "sample.json"


def test_webarena_writes_eval_descriptor(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]), split="test", raw=rec,
    )
    WebArenaAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.task.id == "webarena/42"
    assert "running shoes" in (tmp_path / "instruction.md").read_text()
    descriptor = json.loads(
        (tmp_path / "eval_descriptor.json").read_text(),
    )
    assert descriptor["reference_url"].endswith("max_price=50")
    assert descriptor["eval_types"] == ["url_match", "program_html"]


def test_webarena_verifier_script_calls_evaluator(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=str(rec["task_id"]), split="test", raw=rec,
    )
    WebArenaAdapter().convert_instance(inst, out_dir=tmp_path)
    run_sh = (tmp_path / "verifier" / "run.sh").read_text()
    assert "/opt/webarena/evaluator.py" in run_sh
    assert "$LOOM_TASK_DIR/eval_descriptor.json" in run_sh
