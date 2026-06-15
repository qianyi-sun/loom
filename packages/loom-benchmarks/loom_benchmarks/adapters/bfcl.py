"""BFCL — Berkeley Function-Calling Leaderboard. Spec §5.2 row 8.

Each upstream row carries a `question` (list of conversation turns), a
`function` schema list, and a `ground_truth` describing the expected
function-call arguments. The agent emits its function call as
`agent_output.json`; the structured verifier shells out to the BFCL
evaluator with both the ground truth and the agent output.
"""

from __future__ import annotations

import json
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

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


class BFCLAdapter:
    name = "bfcl"
    display_name = "BFCL"
    series = "code"
    upstream_source = UpstreamSource(
        kind="git",
        locator="https://github.com/ShishirPatil/gorilla.git",
        revision="main",
    )
    license_spdx = "Apache-2.0"
    license_url = "https://github.com/ShishirPatil/gorilla/blob/main/LICENSE"
    splits = ("test",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        data_dir = (
            source_dir / "repo" / "berkeley-function-call-leaderboard" / "data"
        )
        for path in sorted(data_dir.glob("BFCL_*.json")):
            for line in path.read_text().splitlines():
                if not line.strip():
                    continue
                rec = cast(dict[str, Any], json.loads(line))
                yield BenchmarkInstance(
                    instance_id=str(rec["id"]), split=split, raw=rec,
                )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        question_text = " ".join(
            turn["content"] for turns in r["question"] for turn in turns
        )
        (out_dir / "instruction.md").write_text(
            f"# BFCL {instance.instance_id}\n\n"
            f"{question_text}\n\n"
            f"## Available functions\n\n"
            f"```json\n{json.dumps(r['function'], indent=2)}\n```\n",
        )
        (out_dir / "ground_truth.json").write_text(
            json.dumps(r["ground_truth"], indent=2),
        )

        structured_verifier_script(
            'python /opt/bfcl/evaluator.py '
            '--ground-truth "$LOOM_TASK_DIR/ground_truth.json" '
            '--agent-output "$LOOM_AGENT_OUTPUT" '
            '--output "$LOOM_VERIFIER_OUTPUT"',
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
            artifacts = ["agent_output.json"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
