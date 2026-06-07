"""LiveCodeBench — competitive-programming bench. Spec §5.2 row 10.

Each test case is an (input, output) pair against a stdin-driven
script. We emit one pytest file per case that runs `solution.py` in a
subprocess and compares stdout to the expected output.
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
from loom_benchmarks.util import sha256_of_dir, toml_string


def _stdin_pytest_case(idx: int, inp: str, expected: str) -> str:
    return textwrap.dedent(f"""
        import subprocess
        import sys
        from pathlib import Path

        SOLUTION = Path(__file__).parent.parent / "solution" / "solution.py"


        def test_lcb_{idx}() -> None:
            result = subprocess.run(
                [sys.executable, str(SOLUTION)],
                input={inp!r}, capture_output=True, text=True, timeout=10,
            )
            assert result.returncode == 0, result.stderr
            assert result.stdout == {expected!r}
    """).strip() + "\n"


class LiveCodeBenchAdapter:
    name = "livecodebench"
    display_name = "LiveCodeBench"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="livecodebench/code_generation_lite",
        revision=None,
    )
    license_spdx = "MIT"
    license_url = "https://github.com/LiveCodeBench/LiveCodeBench/blob/main/LICENSE"
    splits = ("test",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = datasets.load_dataset(
            self.upstream_source.locator,
            cache_dir=str(source_dir),
            revision=self.upstream_source.revision,
        )[split]
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            yield BenchmarkInstance(
                instance_id=str(rec["question_id"]), split=split, raw=rec,
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            f"# LiveCodeBench {instance.instance_id} "
            f"({r.get('difficulty', '?')})\n\n"
            f"{r['question_content']}\n\n"
            f"## Starter\n\n```python\n{r.get('starter_code', '')}```\n",
        )

        sol_dir = out_dir / "solution"
        sol_dir.mkdir(parents=True, exist_ok=True)
        (sol_dir / "solution.py").write_text(
            str(r.get("code") or r.get("starter_code", "")),
        )

        tests_dir = out_dir / "tests"
        tests_dir.mkdir(parents=True, exist_ok=True)
        cases = (
            (r.get("public_test_cases") or [])
            + (r.get("private_test_cases") or [])
        )
        for idx, c in enumerate(cases):
            (tests_dir / f"test_lcb_{idx}.py").write_text(
                _stdin_pytest_case(idx, str(c["input"]), str(c["output"])),
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
            name = "pytest"

            [[steps]]
            name = "main"
            artifacts = ["solution/solution.py"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
