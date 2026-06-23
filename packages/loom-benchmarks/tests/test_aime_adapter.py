"""AIME adapter contract (Plan 15 Phase 10)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.aime import AIME22Adapter
from loom_benchmarks.adapters.aime_2025 import AIME25Adapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult

FIXTURE = Path(__file__).parent / "fixtures" / "aime" / "sample.json"


def test_aime_url_parser_extracts_year_exam_problem() -> None:
    """PR-1: instance_ids come from the upstream `url` field, not the
    opaque AI-MO row id. Tags carry year/exam/problem so the SPA's
    tag filter can slice the benchmark."""
    from loom_benchmarks.adapters.aime import _parse_aime_url

    assert _parse_aime_url(
        "https://artofproblemsolving.com/wiki/index.php/2024_AIME_II_Problems/Problem_7",
    ) == ("2024", "II", "7")
    assert _parse_aime_url("") is None
    assert _parse_aime_url("https://example.com/random") is None


def test_aime_adapter_declares_series() -> None:
    """SPA's dropdown groups benchmarks by series. PR-1 puts AIME
    variants under series='aime' so AIME 2025 + AIME (AIMO validation)
    appear together."""
    assert AIME22Adapter.series == "aime"
    assert AIME22Adapter.name == "aime-22"


def test_aime_emits_structured_integer_verifier(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    # PR-1 (series/tags): the adapter now derives structured instance_ids
    # from the upstream `url` ("2022-I/1"); the test constructs the
    # BenchmarkInstance directly so the helper isn't exercised here, but
    # the resulting `task.id` reflects the renamed adapter slug
    # `aime-22` (year-22 adapter).
    inst = BenchmarkInstance(
        instance_id="2022-I/1",
        split="train",
        raw=rec,
    )
    AIME22Adapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.verifier.args["script_path"] == "/workspace/verifier/run.sh"
    assert cfg.task.id == "aime-22/2022-I/1"
    assert (tmp_path / "expected_answer.txt").read_text() == "45"
    assert "ordered pairs" in (tmp_path / "instruction.md").read_text()
    assert "verifier/check.py" in (tmp_path / "verifier" / "run.sh").read_text()


def test_aime_2025_emits_script_path(tmp_path: Path) -> None:
    inst = BenchmarkInstance(
        instance_id="2025-I/1",
        split="train",
        raw={"problem": "What is 40 + 2?", "answer": "42"},
    )
    AIME25Adapter().convert_instance(inst, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.verifier.args["script_path"] == "/workspace/verifier/run.sh"


def test_aime_checker_extracts_last_integer(tmp_path: Path) -> None:
    """check.py is the actual verification logic — run it directly
    against a fake agent output and assert the JSON it produces."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIME22Adapter().convert_instance(inst, out_dir=tmp_path)

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
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert parsed.checks[0].name == "answer"
    assert parsed.checks[0].passed is True
    assert parsed.structured == {"got": "45", "expected": "45"}


def test_aime_run_sh_is_self_contained_and_writes_verifier_result(
    tmp_path: Path,
) -> None:
    """ScriptVerifier only injects LOOM_VERIFIER_OUTPUT. The generated
    AIME run.sh must derive its task paths from its own location so the
    task bundle is self-contained after publication/registration."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIME22Adapter().convert_instance(inst, out_dir=tmp_path)

    (tmp_path / "final_answer.txt").write_text("Reasoning...\n45\n")
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env.pop("LOOM_AGENT_OUTPUT", None)
    env.pop("LOOM_TASK_DIR", None)
    env["PYTHON"] = sys.executable
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    result = subprocess.run(
        ["sh", str(tmp_path / "verifier" / "run.sh")],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert len(parsed.checks) == 1
    assert parsed.checks[0].name == "answer"
    assert parsed.checks[0].passed is True
    assert parsed.structured == {"got": "45", "expected": "45"}


def test_aime_license_spdx_is_proprietary_maa(tmp_path: Path) -> None:
    """AIME tasks keep source/license metadata even though their catalog
    execution policy is notice-only for internal research launches."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    converted = AIME22Adapter().convert_instance(inst, out_dir=tmp_path)
    assert converted.license_spdx == "proprietary-MAA"
    assert AIME22Adapter.license_spdx == "proprietary-MAA"
    assert AIME22Adapter.license_execution_policy == "notice"


def test_aime_checker_picks_last_integer(tmp_path: Path) -> None:
    """The checker should extract the LAST integer on the final line —
    "45 is the answer, out of 100 candidates" must extract 100 (the
    final number), not 45. AIME asks for a single integer on the last
    line, so a malformed output is correctly graded as wrong (audit L2)."""
    rec = json.loads(FIXTURE.read_text())[0]
    rec["answer"] = "100"  # rig so 'last integer' is the correct one
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIME22Adapter().convert_instance(inst, out_dir=tmp_path)

    agent_out = tmp_path / "agent_output.txt"
    agent_out.write_text("Some reasoning... 45 is partial... final: 100\n")
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env["LOOM_AGENT_OUTPUT"] = str(agent_out)
    env["LOOM_TASK_DIR"] = str(tmp_path)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    subprocess.run(
        [sys.executable, str(tmp_path / "verifier" / "check.py")],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert parsed.checks[0].passed is True
    assert parsed.structured == {"got": "100", "expected": "100"}


def test_aime_checker_extracts_boxed_answer_when_final_line_has_no_integer(
    tmp_path: Path,
) -> None:
    """LiteLLM may preserve math answers as display math:
    ``\\[\n\\boxed{45}\n\\]``. The verifier should extract the boxed
    answer instead of reporting ``got <none>`` just because the last
    line is the closing display delimiter."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIME22Adapter().convert_instance(inst, out_dir=tmp_path)

    agent_out = tmp_path / "agent_output.txt"
    agent_out.write_text(
        "Reasoning with helper code already stripped.\n\n"
        "Thus, the final answer is:\n\n"
        "\\[\n"
        "\\boxed{45}\n"
        "\\]\n",
    )
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env["LOOM_AGENT_OUTPUT"] = str(agent_out)
    env["LOOM_TASK_DIR"] = str(tmp_path)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    subprocess.run(
        [sys.executable, str(tmp_path / "verifier" / "check.py")],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert parsed.checks[0].passed is True
    assert parsed.structured == {"got": "45", "expected": "45"}


def test_aime_checker_rejects_wrong_answer(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="train", raw=rec)
    AIME22Adapter().convert_instance(inst, out_dir=tmp_path)

    agent_out = tmp_path / "agent_output.txt"
    agent_out.write_text("Final answer: 42\n")
    verifier_out = tmp_path / "verifier_output.json"

    env = dict(os.environ)
    env["LOOM_AGENT_OUTPUT"] = str(agent_out)
    env["LOOM_TASK_DIR"] = str(tmp_path)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    subprocess.run(
        [sys.executable, str(tmp_path / "verifier" / "check.py")],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 0.0}
    assert parsed.checks[0].passed is False
    assert parsed.structured == {"got": "42", "expected": "45"}
