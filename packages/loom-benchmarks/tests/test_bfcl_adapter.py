"""BFCL adapter contract (Plan 15 Phase 7)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom.models.task import TaskConfig
from loom_benchmarks.adapters.bfcl import BFCLAdapter
from loom_benchmarks.base import BenchmarkInstance

FIXTURE = Path(__file__).parent / "fixtures" / "bfcl" / "sample.json"


def test_bfcl_writes_ground_truth(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    BFCLAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.task.id == "bfcl/simple_0"

    gt = json.loads((tmp_path / "ground_truth.json").read_text())
    assert gt[0]["get_current_weather"]["unit"] == ["celsius"]
    assert "weather in Paris" in (tmp_path / "instruction.md").read_text()
    assert "get_current_weather" in (tmp_path / "instruction.md").read_text()


def test_bfcl_verifier_shells_to_evaluator(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    BFCLAdapter().convert_instance(inst, out_dir=tmp_path)
    run_sh = (tmp_path / "verifier" / "run.sh").read_text()
    assert "/opt/bfcl/evaluator.py" in run_sh
    assert "$LOOM_TASK_DIR/ground_truth.json" in run_sh
    assert "$LOOM_AGENT_OUTPUT" in run_sh
