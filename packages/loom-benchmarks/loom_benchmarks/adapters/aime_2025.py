"""AIME 2025 — AIME I + AIME II.

Sibling to `aime-aimo-validation` under the `aime` series. Pulls from
MathArena's two HF datasets (one per exam half) since the AI-MO team
hasn't published a unified 2025 set yet.

Same verifier wiring as AIMEAdapter — the agent emits a final
integer; `verifier/check.py` extracts the last integer from the
final line and compares to expected_answer.txt.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import datasets  # type: ignore[import-untyped]

from loom_benchmarks.adapters.aime import _AIME_CHECK_PY
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


class AIME2025Adapter:
    """AIME 2025 I + II.

    Series: `aime`. Sibling of `aime-aimo-validation`. Combines
    `MathArena/aime_2025_I` and `MathArena/aime_2025_II` into a single
    benchmark of 30 problems (15 per exam) tagged by `exam` ("I"/"II")
    and `problem` (1-15). The `year` tag is always "2025"; included
    explicitly so the SPA's tag UI is uniform across the AIME series.
    """

    name = "aime-2025"
    display_name = "AIME 2025"
    series = "aime"
    # `locator` is informational only — we actually load two HF
    # datasets in list_instances. The display string here is what
    # surfaces in benchmark metadata + the manifest.
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="MathArena/aime_2025_I+II",
        revision=None,
    )
    license_spdx = "proprietary-MAA"
    license_url = "https://maa.org/maa-disclaimer-of-warranties-and-limitation-of-liability"
    splits = ("train",)

    _SOURCES: tuple[tuple[str, str], ...] = (
        ("MathArena/aime_2025_I", "I"),
        ("MathArena/aime_2025_II", "II"),
    )

    def list_instances(
        self, *, source_dir: Path, split: str,
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
        self, instance: BenchmarkInstance, *, out_dir: Path,
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

        # Re-use the structured verifier helper (same wrapper script
        # as AIMEAdapter — see loom_benchmarks.util).
        _script = structured_verifier_script(
            command=["python", "verifier/check.py"],
            agent_output_var="LOOM_AGENT_OUTPUT",
            verifier_output_var="LOOM_VERIFIER_OUTPUT",
            task_dir_var="LOOM_TASK_DIR",
        )
        (verifier_dir / "run.sh").write_text(_script)
        (verifier_dir / "run.sh").chmod(0o755)

        checksum = sha256_of_dir(out_dir)
        return ConvertedTask(
            task_id=task_id,
            checksum=checksum,
            license_spdx=self.license_spdx,
            warnings=(),
        )


def _aime_2025_toml(task_id: str) -> str:
    """Mirrors the AIMEAdapter task.toml shape so the worker treats
    2025 problems identically to AIMO-validation ones."""
    return toml_string({
        "schema_version": "1",
        "task": {"id": task_id, "name": f"AIME 2025 — {task_id}"},
        "environment": {
            "os": "linux", "docker_image": "python:3.11-slim",
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "script"},
        "steps": [
            {"name": "main", "artifacts": ["final_answer.txt"]},
        ],
    })
