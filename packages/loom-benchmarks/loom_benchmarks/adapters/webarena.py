"""WebArena. Spec §5.2 row 5.

Each upstream JSON config_file is an instance. The `eval` block is the
verifier descriptor — we stash it as `eval_descriptor.json` next to the
task and let the structured verifier shell out to the WebArena
evaluator with the descriptor path.
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


class WebArenaAdapter:
    name = "webarena"
    display_name = "WebArena"
    series = "agents"
    upstream_source = UpstreamSource(
        kind="git",
        locator="https://github.com/web-arena-x/webarena.git",
        revision="main",
    )
    license_spdx = "Apache-2.0"
    license_url = "https://github.com/web-arena-x/webarena/blob/main/LICENSE"
    splits = ("test",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        cfg_dir = source_dir / "repo" / "config_files"
        for path in sorted(cfg_dir.glob("*.json")):
            rec = cast(dict[str, Any], json.loads(path.read_text()))
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
            f"# WebArena task {instance.instance_id}\n\n"
            f"{r['intent']}\n\n"
            f"Start URL: {r.get('start_url', '')}\n",
        )

        (out_dir / "eval_descriptor.json").write_text(
            json.dumps(r["eval"], indent=2),
        )
        structured_verifier_script(
            'python /opt/webarena/evaluator.py '
            '--config "$LOOM_TASK_DIR/eval_descriptor.json" '
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
            docker_image = "webarena/playwright-base:1.0"

            [agent]
            name = "oracle"

            [verifier]
            name = "script"

            [[steps]]
            name = "main"
            artifacts = ["trace.zip"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
