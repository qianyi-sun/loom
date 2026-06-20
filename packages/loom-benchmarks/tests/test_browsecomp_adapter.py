from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.browsecomp import BrowseCompAdapter

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult


def _encrypt(text: str, password: str) -> str:
    h = hashlib.sha256()
    h.update(password.encode())
    key = h.digest()
    full_key = key * (len(text.encode()) // len(key)) + key[: len(text.encode()) % len(key)]
    encrypted = bytes(a ^ b for a, b in zip(text.encode(), full_key, strict=True))
    return base64.b64encode(encrypted).decode()


def _write_browsecomp_csv(path: Path) -> None:
    rows = [
        {
            "problem": _encrypt("Find the obscure answer.", "bird"),
            "answer": _encrypt("Exact Thing", "bird"),
            "problem_topic": "Art",
            "canary": "bird",
        },
        {
            "problem": _encrypt("Which teams played?", "fish"),
            "answer": _encrypt("Ireland v Romania", "fish"),
            "problem_topic": "Sports",
            "canary": "fish",
        },
    ]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_browsecomp_lists_decrypted_instances_from_csv(tmp_path: Path) -> None:
    _write_browsecomp_csv(tmp_path / "browse_comp_test_set.csv")

    instances = list(BrowseCompAdapter().list_instances(source_dir=tmp_path, split="test"))

    assert [i.instance_id for i in instances] == ["test/0000", "test/0001"]
    assert instances[0].raw["problem"] == "Find the obscure answer."
    assert instances[0].raw["answer"] == "Exact Thing"
    assert instances[0].tags == {"topic": "Art", "requires_network": "true"}


def test_browsecomp_convert_writes_valid_browsing_task(tmp_path: Path) -> None:
    _write_browsecomp_csv(tmp_path / "browse_comp_test_set.csv")
    instance = next(BrowseCompAdapter().list_instances(source_dir=tmp_path, split="test"))
    out = tmp_path / "out"

    converted = BrowseCompAdapter().convert_instance(instance, out_dir=out)

    cfg = TaskConfig.model_validate(tomllib.loads((out / "task.toml").read_text()))
    assert converted.task_id == "browsecomp/test/0000"
    assert cfg.task.id == "browsecomp/test/0000"
    assert cfg.environment.docker_image == "python:3.11-slim"
    assert cfg.steps[0].artifacts == ["final_answer.txt"]
    assert "Find the obscure answer." in (out / "instruction.md").read_text()
    assert json.loads((out / "answer_key.json").read_text()) == {
        "answer": "Exact Thing",
        "instance_id": "test/0000",
    }


def test_browsecomp_verifier_scores_normalized_exact_answer(tmp_path: Path) -> None:
    _write_browsecomp_csv(tmp_path / "browse_comp_test_set.csv")
    instance = next(BrowseCompAdapter().list_instances(source_dir=tmp_path, split="test"))
    out = tmp_path / "out"
    BrowseCompAdapter().convert_instance(instance, out_dir=out)
    (out / "final_answer.txt").write_text("Explanation: ...\nExact Answer: exact thing\n")
    verifier_out = tmp_path / "verifier-output.json"

    env = dict(os.environ)
    env["PYTHON"] = sys.executable
    env["LOOM_VERIFIER_OUTPUT"] = str(verifier_out)
    result = subprocess.run(
        ["sh", str(out / "verifier" / "run.sh")],
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    parsed = VerifierResult.model_validate_json(verifier_out.read_text())
    assert parsed.rewards == {"score": 1.0}
    assert parsed.checks[0].passed is True
