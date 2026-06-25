"""GPQA — graduate-level Google-proof Q&A benchmark.

The official repository ships password-protected CSVs inside dataset.zip:
``dataset/gpqa_extended.csv`` (546 rows), ``dataset/gpqa_main.csv``
(~448 rows), and ``dataset/gpqa_diamond.csv`` (198 rows). Loom exposes
two catalog slugs against the same upstream zip:

* ``gpqa`` — full Extended set (default).
* ``gpqa-diamond`` — the 198-task Diamond reporting subset, used for
  paired runs against the Harbor adapter of the same name.

Both share a single adapter class parameterized on
``self._params["subset"]`` (see the AIME per-year precedent in
``adapters/aime.py``)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import textwrap
import zipfile
from collections.abc import Iterator
from pathlib import Path

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.util import (
    sha256_of_dir,
    structured_verifier_script,
    toml_string,
)

_DATASET_PASSWORD = b"deserted-untie-orchid"
_DEFAULT_SUBSET = "extended"


def _dataset_member(subset: str) -> str:
    return f"dataset/gpqa_{subset}.csv"

_CHECK_PY = (
    textwrap.dedent(r"""
    import json
    import os
    import pathlib
    import re

    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    output_path = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "final_answer.txt"))
    )
    answer_key = json.loads((task_dir / "answer_key.json").read_text())
    text = output_path.read_text() if output_path.is_file() else ""
    matches = re.findall(r"\b([ABCD])\b", text.upper())
    got = matches[-1] if matches else ""
    expected = answer_key["correct_letter"]
    passed = got == expected
    score = 1.0 if passed else 0.0
    result = {
        "rewards": {"score": score},
        "checks": [
            {
                "name": "multiple_choice_letter",
                "passed": passed,
                "score": score,
                "message": f"expected {expected}, got {got or '<none>'}",
            }
        ],
        "structured": {
            "got": got,
            "expected": expected,
            "correct_answer": answer_key["correct_answer"],
        },
        "confidence": 1.0,
    }
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(
        json.dumps(result),
    )
""").strip()
    + "\n"
)

_RUN_SH = textwrap.dedent("""
    script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
    export LOOM_TASK_DIR="$task_dir"
    export LOOM_AGENT_OUTPUT="${LOOM_AGENT_OUTPUT:-$task_dir/final_answer.txt}"
    "${PYTHON:-python3}" "$task_dir/verifier/check.py"
""").strip()


class GPQAAdapter(CatalogBackedAdapter):
    name = "gpqa"

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        subset = self._subset()
        zip_path = self._resolve_dataset_zip(source_dir)
        with zipfile.ZipFile(zip_path) as zf:
            raw = zf.read(
                _dataset_member(subset),
                pwd=_DATASET_PASSWORD,
            ).decode("utf-8-sig")

        for record in csv.DictReader(io.StringIO(raw)):
            record_id = str(record["Record ID"]).strip()
            tags = {
                "gpqa_subset": subset,
                "domain": str(record.get("High-level domain", "")).strip(),
                "subdomain": str(record.get("Subdomain", "")).strip(),
            }
            yield BenchmarkInstance(
                instance_id=record_id,
                split=split,
                raw=dict(record),
                tags={k: v for k, v in tags.items() if v},
            )

    def _subset(self) -> str:
        return self._params.get("subset", _DEFAULT_SUBSET)

    @staticmethod
    def _resolve_dataset_zip(source_dir: Path) -> Path:
        for candidate in (
            source_dir / "dataset.zip",
            source_dir / "repo" / "dataset.zip",
        ):
            if candidate.is_file():
                return candidate
        return source_dir / "dataset.zip"

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        subset = self._subset()
        out_dir.mkdir(parents=True, exist_ok=True)

        choices, correct_letter = _shuffled_choices(
            record_id=instance.instance_id,
            correct=str(r["Correct Answer"]).strip(),
            incorrects=[
                str(r["Incorrect Answer 1"]).strip(),
                str(r["Incorrect Answer 2"]).strip(),
                str(r["Incorrect Answer 3"]).strip(),
            ],
        )
        (out_dir / "instruction.md").write_text(
            _render_instruction(
                question=str(r["Question"]).strip(),
                choices=choices,
                domain=str(r.get("High-level domain", "")).strip(),
                subdomain=str(r.get("Subdomain", "")).strip(),
                subset=subset,
            ),
        )
        (out_dir / "answer_key.json").write_text(
            json.dumps(
                {
                    "correct_letter": correct_letter,
                    "correct_answer": str(r["Correct Answer"]).strip(),
                    "record_id": instance.instance_id,
                    "gpqa_subset": subset,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        (verifier_dir / "check.py").write_text(_CHECK_PY)
        structured_verifier_script(_RUN_SH, out_dir=out_dir)

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        (out_dir / "task.toml").write_text(
            textwrap.dedent(f"""
            schema_version = "1"

            [task]
            id = {toml_id}
            name = {toml_name}

            [environment]
            os = "linux"
            docker_image = "python:3.11-slim"

            [agent]
            name = "oracle"

            [verifier]
            name = "script"

            [verifier.args]
            script_path = "/workspace/verifier/run.sh"

            [[steps]]
            name = "main"
            artifacts = ["final_answer.txt"]
        """).strip()
            + "\n",
        )

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )


def _shuffled_choices(
    *,
    record_id: str,
    correct: str,
    incorrects: list[str],
) -> tuple[list[tuple[str, str]], str]:
    entries = [(True, correct), *((False, value) for value in incorrects)]
    seed = int.from_bytes(
        hashlib.sha256(record_id.encode("utf-8")).digest()[:8],
        "big",
    )
    rng = random.Random(seed)
    rng.shuffle(entries)
    letters = ["A", "B", "C", "D"]
    choices = [
        (letter, answer)
        for letter, (_is_correct, answer) in zip(letters, entries, strict=True)
    ]
    correct_letter = next(
        letter
        for letter, (is_correct, _answer) in zip(letters, entries, strict=True)
        if is_correct
    )
    return choices, correct_letter


def _render_instruction(
    *,
    question: str,
    choices: list[tuple[str, str]],
    domain: str,
    subdomain: str,
    subset: str,
) -> str:
    context = ""
    if domain or subdomain:
        context = f"Domain: {domain or 'unknown'}"
        if subdomain:
            context += f" / {subdomain}"
        context += "\n\n"
    rendered_choices = "\n".join(f"{letter}. {answer}" for letter, answer in choices)
    return (
        f"# GPQA ({subset})\n\n"
        f"{context}"
        f"{question}\n\n"
        f"{rendered_choices}\n\n"
        "Write your final answer as a single letter A, B, C, or D in "
        "final_answer.txt. The verifier uses the last standalone letter in "
        "that file.\n"
    )


class GPQADiamondAdapter(GPQAAdapter):
    """198-task Diamond reporting subset of GPQA.

    Same upstream zip + verifier as :class:`GPQAAdapter`; the catalog
    entry's ``params.subset == "diamond"`` flips :meth:`_subset` to
    read ``dataset/gpqa_diamond.csv`` instead of the Extended CSV.
    Sibling slug used for paired runs against
    ``harbor-framework/harbor`` ``adapters/gpqa-diamond``."""

    name = "gpqa-diamond"
