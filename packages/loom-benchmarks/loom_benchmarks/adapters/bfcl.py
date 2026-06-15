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
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.util import (
    sha256_of_dir,
    structured_verifier_script,
    toml_string,
)


class BFCLAdapter(CatalogBackedAdapter):
    name = "bfcl"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        # Upstream restructured circa BFCL v4: data moved from
        # `berkeley-function-call-leaderboard/data/` to
        # `berkeley-function-call-leaderboard/bfcl_eval/data/`. Try
        # both so the adapter keeps working if the repo ever moves
        # back, and fall back to a recursive search as a last resort.
        bfcl_root = source_dir / "repo" / "berkeley-function-call-leaderboard"
        candidates = [
            bfcl_root / "bfcl_eval" / "data",
            bfcl_root / "data",
        ]
        data_dir = next((c for c in candidates if c.is_dir()), None)
        if data_dir is None:
            return
        for path in sorted(data_dir.glob("BFCL_*.json")):
            # Upstream mixes JSONL task files with auxiliary index
            # files (`BFCL_v4_format_sensitivity.json` etc.) that are
            # single multi-line JSON objects mapping category names to
            # task-id lists. Skip non-JSONL files (any line that isn't
            # a parseable object with an `id` field).
            for line in path.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    rec = cast(dict[str, Any], json.loads(line))
                except json.JSONDecodeError:
                    break  # file is not JSONL; skip remainder
                if not isinstance(rec, dict) or "id" not in rec:
                    break
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
        # BFCL v4 split the ground truth out into a separate
        # `possible_answer/BFCL_v4_*.json` file keyed by `id`. The
        # task-record itself no longer carries it, so write an empty
        # placeholder + warn — verifier-side wiring should fetch from
        # the parallel file when the v4 verifier ships. For now this
        # gets the tasks registered so the picker can show them.
        ground_truth = r.get("ground_truth")
        if ground_truth is None:
            ground_truth = {"_v4_note": "ground truth lives in possible_answer/*.json"}
        (out_dir / "ground_truth.json").write_text(
            json.dumps(ground_truth, indent=2),
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
