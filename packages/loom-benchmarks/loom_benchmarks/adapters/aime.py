"""AIME — American Invitational Mathematics Exam. Spec §5.2 row 11.

The agent emits its work; the verifier extracts the last integer from
the agent output's final line and compares to `expected_answer.txt`.
Implemented as a standalone python verifier (`verifier/check.py`)
invoked by `verifier/run.sh` so we don't have to thread quotes through
a shell here-doc.
"""

from __future__ import annotations

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


class AIMEAdapter:
    name = "aime"
    display_name = "AIME"
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
            yield BenchmarkInstance(
                instance_id=str(rec["id"]), split=split, raw=rec,
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
