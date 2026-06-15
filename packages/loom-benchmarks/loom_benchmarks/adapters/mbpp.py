"""MBPP — Mostly Basic Python Problems. Spec §5.2 row 7.

Upstream `test_list` is a list of literal `assert ...` strings;
`pytest_from_test_strings` turns each into a one-test file. Solution is
the upstream `code` field; we wrap it via `solution/__init__.py` so the
generated `from solution import *` lifts the function into scope.
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
    pytest_from_test_strings,
    sha256_of_dir,
    toml_string,
)


class MBPPAdapter:
    name = "mbpp"
    display_name = "MBPP"
    series = "code"
    upstream_source = UpstreamSource(
        kind="huggingface",
        # Namespaced form required by HuggingFace Hub >=1.x.
        locator="google-research-datasets/mbpp",
        revision=None,
        subset="sanitized",
    )
    license_spdx = "CC-BY-4.0"
    license_url = (
        "https://github.com/google-research/google-research/blob/master/mbpp/LICENSE"
    )
    splits = ("test",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = datasets.load_dataset(
            self.upstream_source.locator,
            self.upstream_source.subset,
            cache_dir=str(source_dir),
        )[split]
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            yield BenchmarkInstance(
                instance_id=str(rec["task_id"]), split=split, raw=rec,
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            f"# MBPP {instance.instance_id}\n\n{r['text']}\n",
        )

        sol_dir = out_dir / "solution"
        sol_dir.mkdir(parents=True, exist_ok=True)
        body = (r.get("test_setup_code") or "") + "\n" + str(r["code"])
        (sol_dir / "solution.py").write_text(body)
        (sol_dir / "__init__.py").write_text(
            "from solution.solution import *  # noqa: F401,F403\n",
        )

        tests_dir = out_dir / "tests"
        tests_dir.mkdir(parents=True, exist_ok=True)
        (tests_dir / "__init__.py").write_text("")
        # Explicit conftest so `from solution import ...` resolves
        # regardless of pytest invocation cwd / --rootdir. Without
        # this, discovery is implicit on the runtime's auto-rootdir
        # behavior — fragile when the verifier shells out from
        # elsewhere.
        (tests_dir / "conftest.py").write_text(
            "import sys\n"
            "from pathlib import Path\n"
            "sys.path.insert(0, str(Path(__file__).parent.parent))\n",
        )
        pytest_from_test_strings(
            list(r["test_list"]), out_dir=tests_dir, prefix="mbpp",
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
