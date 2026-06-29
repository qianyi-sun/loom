from __future__ import annotations

import csv
import io
import json
import subprocess
import zipfile
from pathlib import Path

from loom_benchmarks.adapters.gpqa import GPQAAdapter, GPQADiamondAdapter

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult

_EXTENDED_ROWS = [
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

_DIAMOND_ROWS = [
    {
        "Record ID": "recDiamond001",
        "Question": "Which gauge boson mediates the weak interaction?",
        "Correct Answer": "W boson",
        "Incorrect Answer 1": "Photon",
        "Incorrect Answer 2": "Gluon",
        "Incorrect Answer 3": "Graviton",
        "Explanation": "W and Z bosons mediate the weak interaction.",
        "High-level domain": "Physics",
        "Subdomain": "Particle physics",
    },
    {
        "Record ID": "recDiamond002",
        "Question": "Which organelle synthesizes proteins?",
        "Correct Answer": "Ribosome",
        "Incorrect Answer 1": "Mitochondrion",
        "Incorrect Answer 2": "Lysosome",
        "Incorrect Answer 3": "Golgi apparatus",
        "Explanation": "Ribosomes assemble polypeptides.",
        "High-level domain": "Biology",
        "Subdomain": "Cell biology",
    },
    {
        "Record ID": "recDiamond003",
        "Question": "Which element has atomic number 6?",
        "Correct Answer": "Carbon",
        "Incorrect Answer 1": "Oxygen",
        "Incorrect Answer 2": "Nitrogen",
        "Incorrect Answer 3": "Boron",
        "Explanation": "Carbon is element 6.",
        "High-level domain": "Chemistry",
        "Subdomain": "Inorganic chemistry",
    },
]


def _csv_bytes(rows: list[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _write_gpqa_zip(source_dir: Path) -> None:
    with zipfile.ZipFile(source_dir / "dataset.zip", "w") as zf:
        zf.writestr("dataset/gpqa_extended.csv", _csv_bytes(_EXTENDED_ROWS))
        zf.writestr("dataset/gpqa_diamond.csv", _csv_bytes(_DIAMOND_ROWS))


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


def test_gpqa_diamond_adapter_lists_only_diamond_instances(tmp_path: Path) -> None:
    _write_gpqa_zip(tmp_path)
    instances = list(
        GPQADiamondAdapter().list_instances(source_dir=tmp_path, split="test"),
    )

    assert [i.instance_id for i in instances] == [
        "recDiamond001",
        "recDiamond002",
        "recDiamond003",
    ]
    for inst in instances:
        assert inst.tags["gpqa_subset"] == "diamond"
    assert instances[0].tags == {
        "gpqa_subset": "diamond",
        "domain": "Physics",
        "subdomain": "Particle physics",
    }


def test_gpqa_adapter_subset_param_defaults_to_extended(tmp_path: Path) -> None:
    """The default `gpqa` slug must keep reading gpqa_extended.csv even
    after the adapter is parameterized on `_params['subset']`."""
    _write_gpqa_zip(tmp_path)
    instances = list(GPQAAdapter().list_instances(source_dir=tmp_path, split="test"))

    assert [i.instance_id for i in instances] == ["recAlpha123", "recBeta456"]
    assert all(inst.tags["gpqa_subset"] == "extended" for inst in instances)
