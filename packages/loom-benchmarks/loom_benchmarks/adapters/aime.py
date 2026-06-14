"""AIME — American Invitational Mathematics Exam. Spec §5.2 row 11.

The agent emits its work; the verifier extracts the last integer from
the agent output's final line and compares to `expected_answer.txt`.
Implemented as a standalone python verifier (`verifier/check.py`)
invoked by `verifier/run.sh` so we don't have to thread quotes through
a shell here-doc.
"""

from __future__ import annotations

import re
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import datasets  # type: ignore[import-untyped]

from loom_benchmarks.base import (
    BenchmarkInstance,
    ConvertedTask,
    UpstreamSource,
)
from loom_benchmarks.util import (
    sha256_of_dir,
    structured_verifier_script,
    toml_string,
)

_AIME_CHECK_PY = textwrap.dedent("""
    import json
    import os
    import pathlib
    import re

    ans = pathlib.Path(os.environ["LOOM_AGENT_OUTPUT"]).read_text().strip()
    exp = pathlib.Path(
        os.environ["LOOM_TASK_DIR"] + "/expected_answer.txt"
    ).read_text().strip()
    last_line = ans.splitlines()[-1] if ans else ""
    # Use the LAST integer on the line, not the first. Phrasings like
    # "answer: 45 (out of 1000)" should extract 45 only if it's the
    # final integer mentioned, so this matches AIME's "return final
    # integer on last line" convention.
    matches = re.findall(r"-?\\d+", last_line)
    got = matches[-1] if matches else ""
    result = {"pass": got == exp, "got": got, "expected": exp}
    pathlib.Path(os.environ["LOOM_VERIFIER_OUTPUT"]).write_text(
        json.dumps(result),
    )
""").strip() + "\n"


_URL_RE = re.compile(r"(?P<year>\d{4})_AIME_(?P<exam>I+)_Problems/Problem_(?P<num>\d+)")


def _parse_aime_url(url: str) -> tuple[str, str, str] | None:
    """`https://.../wiki/.../2022_AIME_I_Problems/Problem_7` →
    `("2022", "I", "7")`. Returns None when the upstream URL doesn't
    follow the canonical pattern — those rows fall back to the
    legacy integer-id format so the row isn't silently dropped."""
    m = _URL_RE.search(url or "")
    if m is None:
        return None
    return m.group("year"), m.group("exam"), m.group("num")


class AIMEAdapter:
    """AIME AIMO-validation subset.

    Covers 2022/I, 2022/II, 2023/I, 2023/II, 2024/I, 2024/II — 6 exams
    × 15 problems = 90 instances. This is the AI-MO team's curated
    validation set, NOT the full AIME archive (which spans 1983–
    present). For peer benchmarks in the `aime` series see the AIME-
    2025 adapter (and any future wider-archive adapter).

    PR-1 series/tags rework:
    - `name = "aime-aimo-validation"` (was `"aime"`) so we can ship
      siblings like `aime-2025` without slug collision
    - `series = "aime"` groups this with siblings in the SPA dropdown
    - `instance_id = "2024-I/7"` (was the opaque AI-MO row id)
    - `task_id = "aime/2024-I/7"` — globally unique, self-describing
    - `tags = {year, exam, problem}` so the SPA's tag filter can slice
    """

    name = "aime-aimo-validation"
    display_name = "AIME (AIMO validation 2022–2024)"
    series = "aime"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="AI-MO/aimo-validation-aime",
        revision=None,
    )
    # Spec §7: AIME problem text is owned by the Mathematical
    # Association of America. License is `proprietary-MAA`, NOT in
    # any default allowlist. Plan 16 ships an `--accept-maa-terms`
    # gate on `loom_benchmark_tool import`; this adapter just stamps
    # the license tag so submit-time enforcement does the rest.
    license_spdx = "proprietary-MAA"
    license_url = "https://maa.org/maa-disclaimer-of-warranties-and-limitation-of-liability"
    splits = ("train",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = datasets.load_dataset(
            self.upstream_source.locator, cache_dir=str(source_dir),
        )[split]
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            parsed = _parse_aime_url(str(rec.get("url", "")))
            if parsed is None:
                # Fallback: keep the upstream row id verbatim so we
                # don't silently lose rows whose URL pattern changed.
                yield BenchmarkInstance(
                    instance_id=str(rec["id"]),
                    split=split, raw=rec, tags={},
                )
                continue
            year, exam, num = parsed
            yield BenchmarkInstance(
                instance_id=f"{year}-{exam}/{num}",
                split=split, raw=rec,
                tags={
                    "year": year,
                    "exam": exam,
                    "problem": num,
                },
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
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
            'python "$LOOM_TASK_DIR/verifier/check.py"',
            out_dir=out_dir,
        )

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        (out_dir / "task.toml").write_text(textwrap.dedent(f"""
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

            [[steps]]
            name = "main"
            artifacts = ["final_answer.txt"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
