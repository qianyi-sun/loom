"""BFCL adapter contract (Plan 15 Phase 7)."""

from __future__ import annotations

import json
import os
import subprocess
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.bfcl import BFCLAdapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = Path(__file__).parent / "fixtures" / "bfcl" / "sample.json"


def test_bfcl_writes_ground_truth(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    BFCLAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.verifier.name == "script"
    assert cfg.verifier.args["script_path"] == "/workspace/verifier/run.sh"
    assert cfg.task.id == "bfcl/simple_0"

    gt = json.loads((tmp_path / "ground_truth.json").read_text())
    assert gt[0]["get_current_weather"]["unit"] == ["celsius"]
    instruction = (tmp_path / "instruction.md").read_text()
    assert "weather in Paris" in instruction
    assert "get_current_weather" in instruction
    assert "agent_output.json" in instruction
    assert '"calls"' in instruction


def test_bfcl_emits_self_contained_script_verifier(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    BFCLAdapter().convert_instance(inst, out_dir=tmp_path)
    run_sh = (tmp_path / "verifier" / "run.sh").read_text()
    assert "/opt/bfcl/evaluator.py" not in run_sh
    assert "LOOM_TASK_DIR" in run_sh
    assert "LOOM_AGENT_OUTPUT" in run_sh
    assert "LOOM_VERIFIER_OUTPUT" in run_sh
    assert (tmp_path / "verifier" / "check.py").is_file()


def test_bfcl_verifier_scores_function_call_output(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(instance_id=rec["id"], split="test", raw=rec)
    BFCLAdapter().convert_instance(inst, out_dir=tmp_path)
    (tmp_path / "agent_output.json").write_text(json.dumps({
        "calls": [
            {
                "name": "get_current_weather",
                "arguments": {"location": "Paris", "unit": "celsius"},
            }
        ]
    }))
    verifier_out = tmp_path / "verifier-output.json"
    env = os.environ.copy()
    env.pop("LOOM_TASK_DIR", None)
    env.pop("LOOM_AGENT_OUTPUT", None)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    proc = subprocess.run(
        ["sh", str(tmp_path / "verifier" / "run.sh")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(verifier_out.read_text())
    assert result["rewards"] == {"score": 1.0}
    assert result["checks"][0]["passed"] is True
    assert result["structured"]["mode"] == "function_call_match"


def test_bfcl_v4_list_instances_merges_possible_answers(
    tmp_path: Path,
) -> None:
    data_dir = (
        tmp_path
        / "repo"
        / "berkeley-function-call-leaderboard"
        / "bfcl_eval"
        / "data"
    )
    answers_dir = data_dir / "possible_answer"
    answers_dir.mkdir(parents=True)
    (data_dir / "BFCL_v4_simple_python.json").write_text(
        json.dumps(
            {
                "id": "simple_python_0",
                "question": [
                    [
                        {
                            "role": "user",
                            "content": "Find the area of a triangle.",
                        }
                    ]
                ],
                "function": [
                    {
                        "name": "calculate_triangle_area",
                        "description": "Calculate triangle area.",
                        "parameters": {
                            "type": "dict",
                            "properties": {
                                "base": {"type": "integer"},
                                "height": {"type": "integer"},
                            },
                            "required": ["base", "height"],
                        },
                    }
                ],
            },
            separators=(",", ":"),
        )
        + "\n",
    )
    (answers_dir / "BFCL_v4_simple_python.json").write_text(
        json.dumps(
            {
                "id": "simple_python_0",
                "ground_truth": [
                    {
                        "calculate_triangle_area": {
                            "base": [10],
                            "height": [5],
                        }
                    }
                ],
            },
            separators=(",", ":"),
        )
        + "\n",
    )

    [instance] = list(
        BFCLAdapter().list_instances(source_dir=tmp_path, split="test"),
    )
    assert instance.raw["ground_truth"] == [
        {"calculate_triangle_area": {"base": [10], "height": [5]}},
    ]

    out_dir = tmp_path / "converted"
    BFCLAdapter().convert_instance(instance, out_dir=out_dir)
    assert json.loads((out_dir / "ground_truth.json").read_text()) == [
        {"calculate_triangle_area": {"base": [10], "height": [5]}},
    ]


def test_bfcl_v4_relevance_files_get_official_call_presence_ground_truth(
    tmp_path: Path,
) -> None:
    data_dir = (
        tmp_path
        / "repo"
        / "berkeley-function-call-leaderboard"
        / "bfcl_eval"
        / "data"
    )
    data_dir.mkdir(parents=True)
    base_record = {
        "question": [[{"role": "user", "content": "Use the right tool."}]],
        "function": [
            {
                "name": "search_web",
                "description": "Search the web.",
                "parameters": {"type": "object", "properties": {}},
            }
        ],
    }
    (data_dir / "BFCL_v4_irrelevance.json").write_text(
        json.dumps({"id": "irrelevance_0", **base_record}) + "\n",
    )
    (data_dir / "BFCL_v4_live_relevance.json").write_text(
        json.dumps({"id": "live_relevance_0-0-0", **base_record}) + "\n",
    )

    instances = {
        instance.instance_id: instance
        for instance in BFCLAdapter().list_instances(
            source_dir=tmp_path,
            split="test",
        )
    }

    assert instances["irrelevance_0"].tags["bfcl_category"] == "irrelevance"
    assert instances["live_relevance_0-0-0"].tags["bfcl_category"] == (
        "live_relevance"
    )

    irrelevance_dir = tmp_path / "irrelevance"
    BFCLAdapter().convert_instance(
        instances["irrelevance_0"],
        out_dir=irrelevance_dir,
    )
    assert json.loads((irrelevance_dir / "ground_truth.json").read_text()) == {
        "mode": "no_function_call",
        "category": "irrelevance",
    }

    relevance_dir = tmp_path / "relevance"
    BFCLAdapter().convert_instance(
        instances["live_relevance_0-0-0"],
        out_dir=relevance_dir,
    )
    assert json.loads((relevance_dir / "ground_truth.json").read_text()) == {
        "mode": "requires_function_call",
        "category": "live_relevance",
    }


def test_bfcl_verifier_scores_relevance_and_irrelevance_modes(
    tmp_path: Path,
) -> None:
    base_record = {
        "question": [[{"role": "user", "content": "Use the right tool."}]],
        "function": [
            {
                "name": "search_web",
                "description": "Search the web.",
                "parameters": {"type": "object", "properties": {}},
            }
        ],
    }
    cases = [
        (
            BenchmarkInstance(
                instance_id="irrelevance_0",
                split="test",
                raw={"id": "irrelevance_0", **base_record},
                tags={"bfcl_category": "irrelevance"},
            ),
            {"calls": []},
            "no_function_call",
        ),
        (
            BenchmarkInstance(
                instance_id="live_relevance_0-0-0",
                split="test",
                raw={"id": "live_relevance_0-0-0", **base_record},
                tags={"bfcl_category": "live_relevance"},
            ),
            {"calls": [{"name": "search_web", "arguments": {}}]},
            "requires_function_call",
        ),
    ]

    for inst, agent_output, mode in cases:
        out_dir = tmp_path / inst.instance_id
        BFCLAdapter().convert_instance(inst, out_dir=out_dir)
        (out_dir / "agent_output.json").write_text(json.dumps(agent_output))
        verifier_out = out_dir / "verifier-output.json"
        env = os.environ.copy()
        env.pop("LOOM_TASK_DIR", None)
        env.pop("LOOM_AGENT_OUTPUT", None)
        env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

        proc = subprocess.run(
            ["sh", str(out_dir / "verifier" / "run.sh")],
            cwd=out_dir,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        assert proc.returncode == 0, proc.stderr
        result = json.loads(verifier_out.read_text())
        assert result["rewards"] == {"score": 1.0}
        assert result["checks"][0]["passed"] is True
        assert result["structured"]["mode"] == mode


def test_bfcl_verifier_scores_multi_turn_output(tmp_path: Path) -> None:
    inst = BenchmarkInstance(
        instance_id="multi_turn_base_0",
        split="test",
        raw={
            "id": "multi_turn_base_0",
            "question": [
                [{"role": "user", "content": "Move the report."}],
                [{"role": "user", "content": "Search inside it."}],
            ],
            "path": ["GorillaFileSystem.cd", "GorillaFileSystem.grep"],
            "ground_truth": [
                ["cd(folder='document')"],
                ["grep(file_name='final_report.pdf',pattern='budget analysis')"],
            ],
        },
        tags={"bfcl_category": "multi_turn_base"},
    )
    BFCLAdapter().convert_instance(inst, out_dir=tmp_path)
    (tmp_path / "agent_output.json").write_text(json.dumps({
        "turns": [
            [{"name": "cd", "arguments": {"folder": "document"}}],
            [
                {
                    "name": "grep",
                    "arguments": {
                        "file_name": "final_report.pdf",
                        "pattern": "budget analysis",
                    },
                }
            ],
        ]
    }))
    verifier_out = tmp_path / "verifier-output.json"
    env = os.environ.copy()
    env.pop("LOOM_TASK_DIR", None)
    env.pop("LOOM_AGENT_OUTPUT", None)
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)

    proc = subprocess.run(
        ["sh", str(tmp_path / "verifier" / "run.sh")],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(verifier_out.read_text())
    assert result["rewards"] == {"score": 1.0}
    assert result["checks"][0]["passed"] is True
    assert result["structured"]["mode"] == "multi_turn_match"
