"""GAIA adapter contract (Plan 15 Phase 8)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.gaia import GAIAAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "gaia" / "sample.json"


def test_gaia_emits_llm_judge_verifier(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="validation", raw=rec,
    )
    GAIAAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "llm-judge"
    rubric = (tmp_path / "verifier" / "rubric.md").read_text()
    assert "Honolulu, Quincy" in rubric
    # candidate_answer left as a placeholder for verify-time substitution.
    assert "{candidate_answer}" in rubric
    assert "presidents were born" in (tmp_path / "instruction.md").read_text()


def test_gaia_warns_when_attachment_missing(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    rec["file_name"] = "missing.pdf"
    rec["file_path"] = "/no/such/path"
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="validation", raw=rec,
    )
    converted = GAIAAdapter().convert_instance(inst, out_dir=tmp_path)
    assert any("attachment missing" in w for w in converted.warnings)


def test_gaia_task_id_namespaced(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="validation", raw=rec,
    )
    GAIAAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id.startswith("gaia/")
