"""OSWorld — desktop automation benchmark. Spec §5.2 row 4.

The evaluator descriptor (`evaluator` field on the upstream record) is
the source of truth for whether a trajectory succeeded — it names a
function (`check_bookmark_exists`, etc.) and the arguments to pass.
We stash it next to the converted task as `verifier_descriptor.json`
and let the structured verifier shell out to the OSWorld harness with
the descriptor path on the command line.
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


class OSWorldAdapter(CatalogBackedAdapter):
    name = "osworld"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        examples_dir = source_dir / "repo" / "evaluation_examples" / "examples"
        for path in sorted(examples_dir.rglob("*.json")):
            rec = cast(dict[str, Any], json.loads(path.read_text()))
            yield BenchmarkInstance(
                instance_id=str(rec["id"]), split=split, raw=rec,
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(str(r["instruction"]))

        descriptor = json.dumps(r["evaluator"], indent=2)
        (out_dir / "verifier_descriptor.json").write_text(descriptor)
        structured_verifier_script(
            'python /opt/osworld/eval/run.py '
            '--descriptor "$LOOM_TASK_DIR/verifier_descriptor.json" '
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
            docker_image = "osworld/ubuntu-24.04:1.0"

            [agent]
            name = "oracle"

            [verifier]
            name = "script"

            [verifier.args]
            script_path = "/workspace/verifier/run.sh"

            [[steps]]
            name = "main"
            artifacts = ["screenshot.png"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
