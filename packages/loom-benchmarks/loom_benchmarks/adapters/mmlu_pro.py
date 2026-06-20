"""MMLU-Pro full official test split adapter."""

from __future__ import annotations

import json
import textwrap
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, cast

import datasets  # type: ignore[import-untyped]

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

_LETTERS = "ABCDEFGHIJ"

_CHECK_PY = (
    textwrap.dedent(r'''
    import json
    import os
    import pathlib
    import re

    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    output_path = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "final_answer.txt"))
    )
    answer_key = json.loads((task_dir / "answer_key.json").read_text())
    valid_letters = answer_key["letters"]
    pattern = r"\b([" + re.escape(valid_letters) + r"])\b"
    text = output_path.read_text() if output_path.is_file() else ""
    matches = re.findall(pattern, text.upper())
    got = matches[-1] if matches else ""
    expected = answer_key["answer"]
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
        "structured": {"got": got, "expected": expected},
        "confidence": 1.0,
    }
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(json.dumps(result))
''').strip()
    + "\n"
)

_RUN_SH = textwrap.dedent("""
    script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
    export LOOM_TASK_DIR="$task_dir"
    export LOOM_AGENT_OUTPUT="${LOOM_AGENT_OUTPUT:-$task_dir/final_answer.txt}"
    "${PYTHON:-python3}" "$task_dir/verifier/check.py"
""").strip()


class MMLUProAdapter(CatalogBackedAdapter):
    name = "mmlu-pro"

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = datasets.load_dataset(
            self.upstream_source.locator,
            revision=self.upstream_source.revision,
            cache_dir=str(source_dir),
        )
        rows = cast(Iterable[dict[str, Any]], ds[split])
        yield from self._instances_from_rows(rows, split=split)

    def _instances_from_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        for idx, record in enumerate(rows):
            rec = dict(record)
            question_id = str(rec.get("question_id", idx)).strip()
            category = str(rec.get("category", "")).strip()
            source = str(rec.get("src", "")).strip()
            yield BenchmarkInstance(
                instance_id=f"{split}/{question_id}",
                split=split,
                raw=rec,
                tags={
                    **({"category": category} if category else {}),
                    **({"source": source} if source else {}),
                },
            )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        options = [str(option).strip() for option in cast(list[Any], r["options"])]
        if not options or len(options) > len(_LETTERS):
            raise ValueError(
                f"MMLU-Pro task {instance.instance_id} has {len(options)} options",
            )
        answer_index = int(r.get("answer_index", -1))
        answer = str(r.get("answer") or "").strip().upper()
        if not answer and 0 <= answer_index < len(options):
            answer = _LETTERS[answer_index]
        if answer not in _LETTERS[: len(options)]:
            raise ValueError(f"MMLU-Pro task {instance.instance_id} has invalid answer")

        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            _render_instruction(
                question=str(r["question"]).strip(),
                options=options,
                category=str(r.get("category", "")).strip(),
            ),
        )
        (out_dir / "answer_key.json").write_text(
            json.dumps(
                {
                    "answer": answer,
                    "answer_index": answer_index,
                    "instance_id": instance.instance_id,
                    "letters": _LETTERS[: len(options)],
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


def _render_instruction(*, question: str, options: list[str], category: str) -> str:
    option_lines = "\n".join(
        f"{letter}. {option}"
        for letter, option in zip(_LETTERS, options, strict=False)
    )
    metadata = f"Category: {category}\n\n" if category else ""
    return (
        "# MMLU-Pro\n\n"
        f"{metadata}"
        f"{question}\n\n"
        f"{option_lines}\n\n"
        "Write your reasoning, then write the final option letter in "
        "final_answer.txt.\n"
    )
