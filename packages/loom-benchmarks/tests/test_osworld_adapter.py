"""OSWorld adapter contract (Plan 15 Phase 4)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom.models.task import TaskConfig
from loom_benchmarks.adapters.osworld import OSWorldAdapter
from loom_benchmarks.base import BenchmarkInstance

FIXTURE = Path(__file__).parent / "fixtures" / "osworld" / "sample.json"


def test_osworld_emits_structured_verifier(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    OSWorldAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert (tmp_path / "verifier" / "run.sh").stat().st_mode & 0o111
    run_sh = (tmp_path / "verifier" / "run.sh").read_text()
    assert "/opt/osworld/eval/run.py" in run_sh
    desc = json.loads((tmp_path / "verifier_descriptor.json").read_text())
    assert desc["func"] == "check_bookmark_exists"
    assert "Open Firefox" in (tmp_path / "instruction.md").read_text()


def test_osworld_task_id_namespaced(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    OSWorldAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id.startswith("osworld/")
