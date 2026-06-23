"""HumanEval adapter — OpenAI's canonical code-completion benchmark.

Spec §5.2 row 6. The upstream record has `prompt` (signature +
docstring), `canonical_solution` (the body), `test` (a `check(candidate)`
function), and `entry_point` (the symbol the agent must define).

Conversion:
- `instruction.md` ← the prompt rendered in a code fence; the agent
  reads it as the user-facing problem.
- `solution/solution.py` ← `prompt + canonical_solution`. The pytest
  verifier imports this reference solution for oracle smoke runs.
- `solution/solve.sh` ← no-op OracleAgent contract script; the verifier
  performs the correctness check against `solution/solution.py`.
- `solution/__init__.py` re-exports `entry_point` so tests can do
  `from solution import {entry_point}`.
- `tests/test_humaneval.py` ← the upstream `check(...)` function plus a
  `test_check()` wrapper pytest collects.
- `task.toml` ← schema-version 1, agent=oracle, verifier=pytest,
  image=python:3.11-slim.
"""

from __future__ import annotations

import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import datasets  # type: ignore[import-untyped]

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.util import (
    oracle_copy_reference_solve_script,
    sha256_of_dir,
    toml_string,
    write_stub_solution_module,
)


class HumanEvalAdapter(CatalogBackedAdapter):
    # All metadata (display_name, series, upstream_source, license_*,
    # splits) loads from benchmarks.json keyed on `name`.
    name = "humaneval"

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
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            yield BenchmarkInstance(
                instance_id=str(rec["task_id"]),
                split=split,
                raw=rec,
            )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        prompt = str(r["prompt"])
        canonical = str(r["canonical_solution"])
        test_src = str(r["test"])
        entry_point = str(r["entry_point"])
        task_id = f"{self.name}/{instance.instance_id}"

        out_dir.mkdir(parents=True, exist_ok=True)

        # instruction.md — the upstream prompt is the user-facing problem.
        (out_dir / "instruction.md").write_text(
            f"# HumanEval — {instance.instance_id}\n\n"
            "Complete the following Python function so that it satisfies "
            "its docstring.\n\n"
            f"```python\n{prompt}```\n",
        )

        # Upstream reference goes to `_reference.py`; agent-editable
        # working file `solution.py` is a stub that raises NotImplementedError
        # on import. OracleAgent's solve.sh copies _reference → solution
        # at trial start; other agents must overwrite solution.py
        # themselves (the file tests import from).
        sol_dir = out_dir / "solution"
        sol_dir.mkdir(parents=True, exist_ok=True)
        (sol_dir / "_reference.py").write_text(prompt + canonical)
        write_stub_solution_module(solution_dir=sol_dir)
        oracle_copy_reference_solve_script(solution_dir=sol_dir)
        (sol_dir / "__init__.py").write_text(
            f"from solution.solution import {entry_point}\n",
        )

        # tests/ — upstream check + pytest wrapper.
        tests_dir = out_dir / "tests"
        tests_dir.mkdir(parents=True, exist_ok=True)
        (tests_dir / "__init__.py").write_text("")
        # Explicit conftest so `from solution import ...` resolves
        # regardless of pytest invocation cwd / --rootdir.
        (tests_dir / "conftest.py").write_text(
            "import sys\n"
            "from pathlib import Path\n"
            "sys.path.insert(0, str(Path(__file__).parent.parent))\n",
        )
        (tests_dir / "test_humaneval.py").write_text(
            f"from solution import {entry_point} as candidate\n\n"
            + test_src
            + "\n\ndef test_check() -> None:\n    check(candidate)\n",
        )

        # task.toml — agent=oracle, verifier=pytest. Quote with
        # `toml_string` so any quote / backslash / newline in the
        # upstream instance_id can't break the TOML literal.
        toml_id = toml_string(task_id)
        toml_name = toml_string(f"HumanEval — {instance.instance_id}")
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
            name = "pytest"

            [[steps]]
            name = "main"
            artifacts = ["solution/solution.py"]
        """).strip()
            + "\n"
        )

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
