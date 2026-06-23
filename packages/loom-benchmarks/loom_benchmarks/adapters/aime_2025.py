"""AIME 2025 — AIME I + AIME II.

Sibling to `aime-aimo-validation` under the `aime` series. Pulls from
MathArena's two HF datasets (one per exam half) since the AI-MO team
hasn't published a unified 2025 set yet.

Same verifier wiring as AIMEAdapter — the agent emits a final
integer; `verifier/check.py` prefers the last integer from the final
line, falls back to common boxed math-answer markers, and compares to
expected_answer.txt.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import datasets

from loom_benchmarks.adapters.aime import _AIME_CHECK_PY, _AIME_RUN_SH
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


class AIME25Adapter(CatalogBackedAdapter):
    """AIME 2025 I + II.

    Series: `aime`. Peer of `aime-22`, `aime-23`, `aime-24`. Combines
    `MathArena/aime_2025_I` and `MathArena/aime_2025_II` into a single
    benchmark of 30 problems (15 per exam) tagged by `exam` ("I"/"II")
    and `problem` (1-15). The `year` tag is always "2025"; included
    explicitly so the SPA's tag UI is uniform across the AIME series.
    """

    # Metadata loads from benchmarks.json. `upstream.locator` points at
    # AIME-I; AIME-II is loaded separately inside `list_instances`
    # (HF rejects locators with `+`, so the composite name can't go
    # in benchmarks.json directly).
    name = "aime-25"

    _SOURCES: tuple[tuple[str, str], ...] = (
        ("MathArena/aime_2025_I", "I"),
        ("MathArena/aime_2025_II", "II"),
    )

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        for locator, exam in self._SOURCES:
            ds = datasets.load_dataset(locator, cache_dir=str(source_dir))[split]
            for record in ds:
                rec = cast(dict[str, Any], dict(record))
                num = str(rec["problem_idx"])
                yield BenchmarkInstance(
                    instance_id=f"2025-{exam}/{num}",
                    split=split,
                    raw={
                        "problem": rec["problem"],
                        "answer": rec["answer"],
                        "exam": exam,
                        "problem_idx": num,
                    },
                    tags={
                        "year": "2025",
                        "exam": exam,
                        "problem": num,
                    },
                )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "instruction.md").write_text(
            f"# AIME 2025 {instance.instance_id}\n\n{r['problem']}\n\n"
            "Return your final integer answer as the LAST LINE of your "
            "output.\n",
        )
        (out_dir / "expected_answer.txt").write_text(
            str(r["answer"]).strip(),
        )

        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        (verifier_dir / "check.py").write_text(_AIME_CHECK_PY)
        (out_dir / "task.toml").write_text(_aime_2025_toml(task_id))

        # Same self-contained wrapper script as the per-year AIMEAdapter.
        # The helper writes verifier/run.sh + chmods +x; no further wiring.
        structured_verifier_script(
            _AIME_RUN_SH,
            out_dir=out_dir,
        )

        checksum = sha256_of_dir(out_dir)
        return ConvertedTask(
            task_id=task_id,
            checksum=checksum,
            license_spdx=self.license_spdx,
            warnings=(),
        )


def _aime_2025_toml(task_id: str) -> str:
    """Mirrors the AIMEAdapter task.toml shape so the worker treats
    2025 problems identically to AIMO-validation ones. `toml_string`
    is a *string-escape* helper (not a serializer) — hand-render the
    document with the same shape the per-year AIME adapters use."""
    import textwrap as _tw

    toml_id = toml_string(task_id)
    toml_name = toml_string(f"AIME 2025 — {task_id}")
    return (
        _tw.dedent(f"""
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

        [verifier.args]
        script_path = "/workspace/verifier/run.sh"

        [[steps]]
        name = "main"
        artifacts = ["final_answer.txt"]
    """).strip()
        + "\n"
    )
