"""AIME — American Invitational Mathematics Exam. Spec §5.2 row 11.

The agent emits its work; the verifier extracts the last integer from
the agent output's final line and compares to `expected_answer.txt`.
Implemented as a standalone python verifier (`verifier/check.py`)
invoked by `verifier/run.sh` so we don't have to thread quotes through
a shell here-doc.

PR-2 (per-year split): the AI-MO `aimo-validation-aime` dataset covers
2022/2023/2024. We ship one adapter per year so users can pick AIME-22
vs AIME-24 vs both with a single click on the NewBatch picker, rather
than going through the tag-filter card every time. AIME-25 has its own
adapter (`aime_25.py`) because it lives in a different upstream
(MathArena, released after AI-MO froze the validation set).
"""

from __future__ import annotations

import re
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import datasets

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

_AIME_CHECK_PY = (
    textwrap.dedent("""
    import json
    import os
    import pathlib
    import re

    task_dir = pathlib.Path(os.environ.get("LOOM_TASK_DIR", "/workspace"))
    agent_output = pathlib.Path(
        os.environ.get("LOOM_AGENT_OUTPUT", str(task_dir / "final_answer.txt"))
    )
    ans = agent_output.read_text().strip() if agent_output.is_file() else ""
    exp = (task_dir / "expected_answer.txt").read_text().strip()
    last_line = ans.splitlines()[-1] if ans else ""
    # Use the LAST integer on the line, not the first. Phrasings like
    # "answer: 45 (out of 1000)" should extract 45 only if it's the
    # final integer mentioned, so this matches AIME's "return final
    # integer on last line" convention.
    matches = re.findall(r"-?\\d+", last_line)
    got = matches[-1] if matches else ""
    passed = got == exp
    score = 1.0 if passed else 0.0
    got_display = got if got else "<none>"
    result = {
        "rewards": {"score": score},
        "checks": [
            {
                "name": "answer",
                "passed": passed,
                "score": score,
                "message": f"expected {exp}, got {got_display}",
            }
        ],
        "structured": {"got": got, "expected": exp},
        "confidence": 1.0,
    }
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(
        json.dumps(result),
    )
""").strip()
    + "\n"
)

_AIME_RUN_SH = textwrap.dedent("""
    script_dir="$(CDPATH= cd "$(dirname "$0")" && pwd)"
    task_dir="${LOOM_TASK_DIR:-$(dirname "$script_dir")}"
    export LOOM_TASK_DIR="$task_dir"
    export LOOM_AGENT_OUTPUT="${LOOM_AGENT_OUTPUT:-$task_dir/final_answer.txt}"
    "${PYTHON:-python3}" "$task_dir/verifier/check.py"
""").strip()


_URL_RE = re.compile(r"(?P<year>\d{4})_AIME_(?P<exam>I+)_Problems/Problem_(?P<num>\d+)")


def _parse_aime_url(url: str) -> tuple[str, str, str] | None:
    """`https://.../wiki/.../2022_AIME_I_Problems/Problem_7` →
    `("2022", "I", "7")`. Returns None when the upstream URL doesn't
    follow the canonical pattern."""
    m = _URL_RE.search(url or "")
    if m is None:
        return None
    return m.group("year"), m.group("exam"), m.group("num")


class _AIMEYearBase(CatalogBackedAdapter):
    """Shared list/convert logic for the per-year AIME adapters.

    Subclasses set `name`; everything else (display_name, series,
    upstream, license, splits) loads from benchmarks.json. The year to
    filter on comes from `cls._params["year"]` — also set by the
    catalog. So adding AIME-21 = one JSON entry + one 2-line subclass.
    """

    name: ClassVar[str]

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = datasets.load_dataset(
            self.upstream_source.locator,
            cache_dir=str(source_dir),
        )[split]
        year_filter = self._params.get("year", "")
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            parsed = _parse_aime_url(str(rec.get("url", "")))
            if parsed is None:
                continue
            year, exam, num = parsed
            if year != year_filter:
                continue
            yield BenchmarkInstance(
                instance_id=f"{year}-{exam}/{num}",
                split=split,
                raw=rec,
                tags={
                    "year": year,
                    "exam": exam,
                    "problem": num,
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
            f"# AIME {instance.instance_id}\n\n{r['problem']}\n\n"
            "Return your final integer answer as the LAST LINE of your "
            "output.\n",
        )
        (out_dir / "expected_answer.txt").write_text(str(r["answer"]).strip())

        # check.py lives at verifier/check.py so run.sh can `python check.py`
        # without escape gymnastics.
        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        (verifier_dir / "check.py").write_text(_AIME_CHECK_PY)
        structured_verifier_script(
            _AIME_RUN_SH,
            out_dir=out_dir,
        )

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
            + "\n"
        )

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )


class AIME22Adapter(_AIMEYearBase):
    name = "aime-22"


class AIME23Adapter(_AIMEYearBase):
    name = "aime-23"


class AIME24Adapter(_AIMEYearBase):
    name = "aime-24"
