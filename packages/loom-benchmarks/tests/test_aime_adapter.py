"""AIME adapter contract (Plan 15 Phase 10)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.aime import AIMEAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "aime" / "sample.json"


def test_aime_emits_structured_integer_verifier(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIMEAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.task.id == "aime/2022-I-1"
    assert (tmp_path / "expected_answer.txt").read_text() == "45"
    assert "ordered pairs" in (tmp_path / "instruction.md").read_text()
    assert "verifier/check.py" in (tmp_path / "verifier" / "run.sh").read_text()


def test_aime_checker_extracts_last_integer(tmp_path: Path) -> None:
    """check.py is the actual verification logic — run it directly
    against a fake agent output and assert the JSON it produces."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIMEAdapter().convert_instance(inst, out_dir=tmp_path)

    agent_out = tmp_path / "agent_output.txt"
    agent_out.write_text(
        "Some reasoning here...\nThe answer is 45.\n",
    )
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env["LOOM_AGENT_OUTPUT"] = str(agent_out)
    env["LOOM_TASK_DIR"] = str(tmp_path)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    result = subprocess.run(
        [sys.executable, str(tmp_path / "verifier" / "check.py")],
        env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr

    payload = json.loads(verifier_out.read_text())
    assert payload["pass"] is True
    assert payload["got"] == "45"
    assert payload["expected"] == "45"


def test_aime_license_spdx_is_proprietary_maa(tmp_path: Path) -> None:
    """Spec §7: AIME tasks must carry `proprietary-MAA` so the Plan 13
    license-allowlist enforcement keeps them out of the default
    `[MIT, Apache-2.0, BSD-3-Clause, CC-BY-4.0]` allowlist (audit
    license-bypass finding)."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    converted = AIMEAdapter().convert_instance(inst, out_dir=tmp_path)
    assert converted.license_spdx == "proprietary-MAA"
    assert AIMEAdapter.license_spdx == "proprietary-MAA"


def test_aime_checker_picks_last_integer(tmp_path: Path) -> None:
    """The checker should extract the LAST integer on the final line —
    "45 is the answer, out of 100 candidates" must extract 100 (the
    final number), not 45. AIME asks for a single integer on the last
    line, so a malformed output is correctly graded as wrong (audit L2)."""
    rec = json.loads(FIXTURE.read_text())[0]
    rec["answer"] = "100"  # rig so 'last integer' is the correct one
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIMEAdapter().convert_instance(inst, out_dir=tmp_path)

    agent_out = tmp_path / "agent_output.txt"
    agent_out.write_text("Some reasoning... 45 is partial... final: 100\n")
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env["LOOM_AGENT_OUTPUT"] = str(agent_out)
    env["LOOM_TASK_DIR"] = str(tmp_path)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    subprocess.run(
        [sys.executable, str(tmp_path / "verifier" / "check.py")],
        env=env, capture_output=True, text=True, check=True,
    )
    payload = json.loads(verifier_out.read_text())
    assert payload["pass"] is True
    assert payload["got"] == "100"


def test_aime_checker_rejects_wrong_answer(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIMEAdapter().convert_instance(inst, out_dir=tmp_path)

    agent_out = tmp_path / "agent_output.txt"
    agent_out.write_text("Final answer: 42\n")
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env["LOOM_AGENT_OUTPUT"] = str(agent_out)
    env["LOOM_TASK_DIR"] = str(tmp_path)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    subprocess.run(
        [sys.executable, str(tmp_path / "verifier" / "check.py")],
        env=env, capture_output=True, text=True, check=True,
    )
    payload = json.loads(verifier_out.read_text())
    assert payload["pass"] is False
    assert payload["got"] == "42"
