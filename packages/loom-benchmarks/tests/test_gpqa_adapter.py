from __future__ import annotations

import csv
import io
import json
import subprocess
import zipfile
from pathlib import Path

from loom_benchmarks.adapters.gpqa import GPQAAdapter

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult


def _write_gpqa_zip(source_dir: Path) -> None:
    rows = [
        {
            "Record ID": "recAlpha123",
            "Question": "Which particle mediates electromagnetism?",
            "Correct Answer": "Photon",
            "Incorrect Answer 1": "Gluon",
            "Incorrect Answer 2": "W boson",
            "Incorrect Answer 3": "Graviton",
            "Explanation": "Photons mediate electromagnetic interactions.",
            "High-level domain": "Physics",
            "Subdomain": "Quantum mechanics",
        },
        {
            "Record ID": "recBeta456",
            "Question": "What carries genetic information in cells?",
            "Correct Answer": "DNA",
            "Incorrect Answer 1": "ATP",
            "Incorrect Answer 2": "Collagen",
            "Incorrect Answer 3": "Insulin",
            "Explanation": "DNA stores genetic information.",
            "High-level domain": "Biology",
            "Subdomain": "Molecular biology",
        },
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(source_dir / "dataset.zip", "w") as zf:
        zf.writestr("dataset/gpqa_extended.csv", buf.getvalue())


def test_gpqa_lists_full_extended_instances(tmp_path: Path) -> None:
    _write_gpqa_zip(tmp_path)
    instances = list(GPQAAdapter().list_instances(source_dir=tmp_path, split="test"))

    assert [i.instance_id for i in instances] == ["recAlpha123", "recBeta456"]
    assert instances[0].tags == {
        "gpqa_subset": "extended",
        "domain": "Physics",
        "subdomain": "Quantum mechanics",
    }


def test_gpqa_lists_instances_from_fetch_upstream_repo_wrapper(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _write_gpqa_zip(repo)

    instances = list(GPQAAdapter().list_instances(source_dir=tmp_path, split="test"))

    assert [i.instance_id for i in instances] == ["recAlpha123", "recBeta456"]


def test_gpqa_convert_writes_valid_multiple_choice_task(tmp_path: Path) -> None:
    _write_gpqa_zip(tmp_path)
    (instance,) = list(
        GPQAAdapter().list_instances(source_dir=tmp_path, split="test"),
    )[:1]
    out = tmp_path / "out"

    converted = GPQAAdapter().convert_instance(instance, out_dir=out)

    cfg = TaskConfig.model_validate_json(
        json.dumps(__import__("tomllib").loads((out / "task.toml").read_text())),
    )
    assert converted.task_id == "gpqa/recAlpha123"
    assert cfg.task.id == "gpqa/recAlpha123"
    assert cfg.environment.docker_image == "python:3.11-slim"
    assert cfg.verifier.name == "script"
    assert cfg.steps[0].artifacts == ["final_answer.txt"]
    instruction = (out / "instruction.md").read_text()
    assert "Which particle mediates electromagnetism?" in instruction
    assert "A." in instruction and "D." in instruction
    key = json.loads((out / "answer_key.json").read_text())
    assert key["correct_answer"] == "Photon"
    assert key["correct_letter"] in {"A", "B", "C", "D"}


def test_gpqa_verifier_scores_correct_letter(tmp_path: Path) -> None:
    _write_gpqa_zip(tmp_path)
    (instance,) = list(
        GPQAAdapter().list_instances(source_dir=tmp_path, split="test"),
    )[:1]
    out = tmp_path / "out"
    GPQAAdapter().convert_instance(instance, out_dir=out)
    key = json.loads((out / "answer_key.json").read_text())
    (out / "final_answer.txt").write_text(
        f"Reasoning...\nFinal answer: {key['correct_letter']}\n",
    )
    output = tmp_path / "verifier-output.json"

    subprocess.run(
        ["sh", str(out / "verifier" / "run.sh")],
        env={"LOOM_VERIFIER_OUTPUT": str(output), "LOOM_TASK_DIR": str(out)},
        check=True,
        text=True,
        capture_output=True,
    )

    result = VerifierResult.model_validate_json(output.read_text())
    assert result.rewards == {"score": 1.0}
    assert result.checks[0].passed is True
