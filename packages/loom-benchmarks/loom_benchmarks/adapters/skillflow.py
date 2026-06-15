"""SkillFlow benchmark adapter. Spec §5.2 row 12.

The current converter expects a fetched task bundle where each instance
is represented by a per-instance `manifest.json` whose files are already
in Loom layout (task.toml + instruction.md + solution/ + tests/). Direct
conversion from the original public upstream repository may require an
additional source-shape parser before this passthrough conversion step.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

from loom_benchmarks.base import (
    BenchmarkInstance,
    CatalogBackedAdapter,
    ConvertedTask,
)
from loom_benchmarks.util import sha256_of_dir, toml_string


class SkillFlowAdapter(CatalogBackedAdapter):
    name = "skillflow"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        base = source_dir / "repo" / "tasks"
        for path in sorted(base.rglob("manifest.json")):
            manifest = cast(dict[str, Any], json.loads(path.read_text()))
            yield BenchmarkInstance(
                instance_id=str(manifest["instance_id"]),
                split=split, raw=manifest,
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        files: dict[str, str] = instance.raw["files"]
        for rel, body in files.items():
            target = out_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body)

        # Re-stamp task.toml's `id` field to the namespaced form.
        # Upstream bundles use the bare instance_id (which already
        # matches the namespaced form for our v1 bundles), but force
        # consistency so future upstream changes can't drift.
        toml_path = out_dir / "task.toml"
        if toml_path.exists():
            old = toml_path.read_text()
            # Replace any `id = "..."` value under [task]; cheap line-by-line
            # walk avoids needing tomli-w.
            new_lines: list[str] = []
            in_task = False
            replaced = False
            for line in old.splitlines():
                stripped = line.strip()
                if stripped.startswith("[task]"):
                    in_task = True
                elif stripped.startswith("[") and stripped != "[task]":
                    in_task = False
                if in_task and stripped.startswith("id =") and not replaced:
                    line = f"id = {toml_string(task_id)}"
                    replaced = True
                new_lines.append(line)
            toml_path.write_text("\n".join(new_lines) + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
