"""BrowseComp adapter for the OpenAI simple-evals release."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import textwrap
from collections.abc import Iterator
from pathlib import Path

import httpx

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

_DEFAULT_CSV_URL = "https://openaipublic.blob.core.windows.net/simple-evals/browse_comp_test_set.csv"

_CHECK_PY = (
    textwrap.dedent(r'''
    import json
    import os
    import pathlib
    import re


    def extract_answer(text):
        matches = re.findall(r"(?im)^\s*exact answer\s*:\s*(.+?)\s*$", text)
        if matches:
            return matches[-1]
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return lines[-1] if lines else ""


    def normalize(text):
        text = str(text).strip().strip("`'\"")
        text = re.sub(r"\s+", " ", text)
        text = text.strip(" .。")
        return text.casefold()


    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    output_path = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "final_answer.txt"))
    )
    answer_key = json.loads((task_dir / "answer_key.json").read_text())
    text = output_path.read_text() if output_path.is_file() else ""
    got = extract_answer(text)
    expected = answer_key["answer"]
    passed = normalize(got) == normalize(expected)
    score = 1.0 if passed else 0.0
    result = {
        "rewards": {"score": score},
        "checks": [
            {
                "name": "exact_answer",
                "passed": passed,
                "score": score,
                "message": f"expected {expected!r}, got {got!r}",
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


class BrowseCompAdapter(CatalogBackedAdapter):
    name = "browsecomp"

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        rows = _load_rows(source_dir, params=self._params)
        for idx, row in enumerate(rows):
            canary = row.get("canary", "")
            problem = decrypt(row.get("problem", ""), canary)
            answer = decrypt(row.get("answer", ""), canary)
            topic = str(row.get("problem_topic", "")).strip()
            yield BenchmarkInstance(
                instance_id=f"{split}/{idx:04d}",
                split=split,
                raw={
                    "problem": problem,
                    "answer": answer,
                    "topic": topic,
                },
                tags={
                    **({"topic": topic} if topic else {}),
                    "requires_network": "true",
                },
            )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            _render_instruction(
                question=str(r["problem"]).strip(),
                topic=str(r.get("topic", "")).strip(),
            ),
        )
        (out_dir / "answer_key.json").write_text(
            json.dumps(
                {
                    "answer": str(r["answer"]).strip(),
                    "instance_id": instance.instance_id,
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


def derive_repeating_xor_key(canary: str, length: int) -> bytes:
    hasher = hashlib.sha256()
    hasher.update(canary.encode())
    key = hasher.digest()
    return key * (length // len(key)) + key[: length % len(key)]


def decrypt(ciphertext_b64: str, canary: str) -> str:
    encrypted = base64.b64decode(ciphertext_b64)
    key = derive_repeating_xor_key(canary, len(encrypted))
    decrypted = bytes(a ^ b for a, b in zip(encrypted, key, strict=True))
    return decrypted.decode()


def _load_rows(source_dir: Path, *, params: dict[str, str]) -> list[dict[str, str]]:
    for candidate in (
        source_dir / "browse_comp_test_set.csv",
        source_dir / "repo" / "browse_comp_test_set.csv",
    ):
        if candidate.is_file():
            return list(csv.DictReader(io.StringIO(candidate.read_text())))

    url = params.get("csv_url", _DEFAULT_CSV_URL)
    response = httpx.get(url, timeout=120.0, follow_redirects=True)
    response.raise_for_status()
    expected_etag = params.get("csv_etag")
    observed_etag = response.headers.get("etag")
    if expected_etag and observed_etag and observed_etag != expected_etag:
        raise ValueError(
            f"BrowseComp CSV ETag changed: expected {expected_etag}, got {observed_etag}",
        )
    return list(csv.DictReader(io.StringIO(response.text)))


def _render_instruction(*, question: str, topic: str) -> str:
    topic_block = f"Topic: {topic}\n\n" if topic else ""
    return (
        "# BrowseComp\n\n"
        f"{topic_block}"
        f"{question}\n\n"
        "Use web browsing or network retrieval when available. Your response "
        "must be written to `final_answer.txt` in this format:\n\n"
        "Explanation: <brief evidence and reasoning>\n"
        "Exact Answer: <succinct final answer>\n"
        "Confidence: <0-100%>\n"
    )
