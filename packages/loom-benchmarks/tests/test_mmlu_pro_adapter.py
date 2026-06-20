from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.mmlu_pro import MMLUProAdapter

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult


class _FakeSplit:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


def test_mmlu_pro_lists_official_test_split(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_load_dataset(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {
            "test": _FakeSplit(
                [
                    {
                        "question_id": 70,
                        "question": "Typical advertising bodies say adverts must not...",
                        "options": ["A text", "B text", "C text"],
                        "answer": "C",
                        "answer_index": 2,
                        "category": "business",
                        "src": "ori_mmlu-business_ethics",
                    },
                ],
            ),
        }

    monkeypatch.setattr(
        "loom_benchmarks.adapters.mmlu_pro.datasets.load_dataset",
        fake_load_dataset,
    )

    instances = list(MMLUProAdapter().list_instances(source_dir=Path("/cache"), split="test"))

    assert calls == [
        {
            "args": ("TIGER-Lab/MMLU-Pro",),
            "kwargs": {
                "revision": "b189ec765aa7ed75c8acfea42df31fdae71f97be",
                "cache_dir": "/cache",
            },
        },
    ]
    assert instances[0].instance_id == "test/70"
    assert instances[0].tags == {
        "category": "business",
        "source": "ori_mmlu-business_ethics",
    }


def test_mmlu_pro_convert_writes_valid_multiple_choice_task(tmp_path: Path) -> None:
    instance = next(
        MMLUProAdapter()._instances_from_rows(
            [
                {
                    "question_id": 70,
                    "question": "Which answer is correct?",
                    "options": ["first", "second", "third"],
                    "answer": "C",
                    "answer_index": 2,
                    "category": "math",
                    "src": "cot_lib-abstract_algebra",
                },
            ],
            split="test",
        ),
    )

    converted = MMLUProAdapter().convert_instance(instance, out_dir=tmp_path)

    cfg = TaskConfig.model_validate(tomllib.loads((tmp_path / "task.toml").read_text()))
    assert converted.task_id == "mmlu-pro/test/70"
    assert cfg.task.id == "mmlu-pro/test/70"
    assert cfg.environment.docker_image == "python:3.11-slim"
    assert cfg.steps[0].artifacts == ["final_answer.txt"]
    instruction = (tmp_path / "instruction.md").read_text()
    assert "Which answer is correct?" in instruction
    assert "A. first" in instruction
    assert "C. third" in instruction
    assert json.loads((tmp_path / "answer_key.json").read_text()) == {
        "answer": "C",
        "answer_index": 2,
        "instance_id": "test/70",
        "letters": "ABC",
    }


def test_mmlu_pro_verifier_scores_last_standalone_letter(tmp_path: Path) -> None:
    instance = next(
        MMLUProAdapter()._instances_from_rows(
            [
                {
                    "question_id": 71,
                    "question": "Pick the answer.",
                    "options": ["first", "second", "third", "fourth"],
                    "answer": "D",
                    "answer_index": 3,
                    "category": "law",
                    "src": "ori_mmlu-professional_law",
                },
            ],
            split="test",
        ),
    )
    MMLUProAdapter().convert_instance(instance, out_dir=tmp_path)
    (tmp_path / "final_answer.txt").write_text("Reasoning mentions A.\nFinal answer: D\n")
    verifier_out = tmp_path / "verifier-output.json"

    env = dict(os.environ)
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
    assert parsed.checks[0].passed is True
