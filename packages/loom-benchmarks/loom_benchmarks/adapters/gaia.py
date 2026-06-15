"""GAIA — generalist AI assistant benchmark. Spec §5.2 row 9.

Two-track verifier: the rubric template (with the per-instance
reference answer baked in) lives at `verifier/rubric.md`; the
llm-judge verifier substitutes the candidate answer at verify time.
Optional file attachments are copied into `attachments/` from the
upstream `file_path`; if missing, the adapter records a warning.
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
from loom_benchmarks.judges.gaia import GAIA_RUBRIC_TEMPLATE
from loom_benchmarks.util import sha256_of_dir, toml_string


class GAIAAdapter:
    name = "gaia"
    display_name = "GAIA"
    series = "agents"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="gaia-benchmark/GAIA",
        revision=None,
        subset="2023_all",
    )
    license_spdx = "CC-BY-4.0"
    license_url = "https://huggingface.co/datasets/gaia-benchmark/GAIA"
    splits = ("validation", "test")

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        ds = datasets.load_dataset(
            self.upstream_source.locator, self.upstream_source.subset,
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

        body = (
            f"# GAIA {instance.instance_id} (Level {r.get('Level', '?')})\n\n"
            f"{r['Question']}\n"
        )
        if r.get("file_name"):
            body += (
                f"\nAn attached file is available at "
                f"`attachments/{r['file_name']}`.\n"
            )
        (out_dir / "instruction.md").write_text(body)

        warnings: list[str] = []
        if r.get("file_name"):
            attach = out_dir / "attachments"
            attach.mkdir(parents=True, exist_ok=True)
            # Sanitize file_name: only keep the basename so an upstream
            # row with `file_name="../poison"` or `/etc/passwd` can't
            # escape `attachments/` once we join (`Path.name` strips
            # every directory component).
            safe_name = Path(str(r["file_name"])).name
            if not safe_name or safe_name in (".", ".."):
                warnings.append(
                    f"attachment file_name {r['file_name']!r} sanitized "
                    f"to empty; skipping",
                )
            else:
                src_str = r.get("file_path") or ""
                src = Path(src_str) if src_str else Path("")
                try:
                    if src and src.exists():
                        (attach / safe_name).write_bytes(src.read_bytes())
                    else:
                        warnings.append(f"attachment missing: {safe_name}")
                except OSError as exc:
                    warnings.append(f"attachment copy failed: {exc}")

        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        # Direct marker substitution — no `.format()` so the rubric's
        # literal `{"score": ...}` JSON and any `{...}` set notation
        # in the reference answer pass through unchanged.
        rubric = GAIA_RUBRIC_TEMPLATE.replace(
            "<<REFERENCE_ANSWER>>", str(r["Final answer"]),
        )
        (verifier_dir / "rubric.md").write_text(rubric)

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
            name = "llm-judge"

            [[steps]]
            name = "main"
            artifacts = ["final_answer.txt"]
        """).strip() + "\n")

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=tuple(warnings),
        )
