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
    # candidate_answer left as a marker for verify-time substitution.
    assert "<<CANDIDATE_ANSWER>>" in rubric
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


def test_gaia_rubric_handles_braces_in_reference_answer(tmp_path: Path) -> None:
    """Reference answer in set notation like `{Honolulu, Quincy}` and
    the rubric's literal `{"score": ...}` JSON both survive the
    convert→verify two-pass substitution because we use `<<MARKERS>>`
    instead of `str.format` (audit H1)."""
    rec = json.loads(FIXTURE.read_text())[0]
    rec["Final answer"] = "{Honolulu, Quincy}"
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="validation", raw=rec,
    )
    GAIAAdapter().convert_instance(inst, out_dir=tmp_path)
    rubric = (tmp_path / "verifier" / "rubric.md").read_text()
    assert "{Honolulu, Quincy}" in rubric
    assert '{"score": float, "reasoning": str}' in rubric  # JSON intact
    # Verify-time substitution recovers the literal answer.
    rendered = rubric.replace("<<CANDIDATE_ANSWER>>", "Honolulu, Quincy")
    assert "CANDIDATE ANSWER: Honolulu, Quincy" in rendered
    assert "{Honolulu, Quincy}" in rendered


def test_gaia_attachment_path_traversal_sanitized(tmp_path: Path) -> None:
    """A malicious upstream row with `file_name="../poison"` must NOT
    write outside the attachments/ dir (audit H2)."""
    rec = json.loads(FIXTURE.read_text())[0]
    rec["file_name"] = "../poison"
    fake_src = tmp_path / "_src"
    fake_src.write_bytes(b"x")
    rec["file_path"] = str(fake_src)
    inst = BenchmarkInstance(
        instance_id=rec["task_id"], split="validation", raw=rec,
    )
    out = tmp_path / "out"
    out.mkdir()
    GAIAAdapter().convert_instance(inst, out_dir=out)
    # Sanitized to basename `poison` — landed inside attachments/.
    assert (out / "attachments" / "poison").exists()
    # And nothing escaped out_dir's parent.
    assert not (tmp_path / "poison").exists()


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
