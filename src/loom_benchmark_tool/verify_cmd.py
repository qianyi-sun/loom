"""`python -m loom_benchmark_tool verify <benchmark>` (spec §8).

Pulls a random sample of imported tasks from MinIO under
`<bucket>/<benchmark>/`, validates each `task.toml` against
`TaskConfig`, and runs the Oracle baseline in a throwaway container
(see `oracle_runner.run_oracle_for_task`). Returns an aggregate report
of pass/fail per task — `__main__` prints + exits non-zero if any
sampled task failed.
"""

from __future__ import annotations

import random
import re
import tempfile
import tomllib
from dataclasses import asdict
from pathlib import Path
from typing import Any

from loom.models.task import TaskConfig
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.oracle_runner import (
    OracleResult,
    run_oracle_for_task,
)

# Restricts the per-task tempdir name to characters safe for the
# `docker exec sh -c` interpolation in oracle_runner. Mirrors the
# allowlist Plan 14 applied at import time but stricter — no `/`
# because we take only the prefix's last segment here.
_SAFE_SLUG_RE = re.compile(r"^[A-Za-z0-9._+\-]+$")


async def run_verify(
    *,
    benchmark: str,
    object_store: ObjectStore,
    bucket: str = "loom-benchmarks",
    limit: int = 10,
    seed: int = 0,
) -> dict[str, Any]:
    """Sample `limit` task prefixes under `<bucket>/<benchmark>/`, pull
    each to a tempdir, validate `task.toml` against `TaskConfig`, run
    the Oracle baseline. Returns an aggregate report."""
    all_prefixes = [
        prefix
        for prefix in await object_store.list_task_prefixes(
            bucket=bucket, benchmark=benchmark,
        )
        # Immutable publication history is not a set of live tasks. The
        # registered task rows are the authority for the current revision.
        if ".loom-revisions" not in prefix.rstrip("/").split("/")
    ]
    if not all_prefixes:
        return {"total": 0, "passed": 0, "failed": 0, "results": []}

    rng = random.Random(seed)
    sample = (
        all_prefixes
        if limit >= len(all_prefixes)
        else rng.sample(all_prefixes, limit)
    )

    results: list[OracleResult] = []
    with tempfile.TemporaryDirectory(prefix="loom-verify-") as root_str:
        root = Path(root_str)
        for prefix in sample:
            # task_dir.name is interpolated into docker exec commands
            # downstream; restrict it to the last segment of the prefix
            # and treat any errors per-task so one bad bundle doesn't
            # abort the whole sample.
            slug = prefix.rstrip("/").split("/")[-1]
            if not _SAFE_SLUG_RE.match(slug):
                # Refuse to interpolate into docker-exec commands
                # downstream — a poisoned prefix could otherwise smuggle
                # shell metacharacters even though our list-form
                # subprocess calls block direct injection.
                results.append(OracleResult(
                    task_id=prefix.rstrip("/"),
                    passed=False, return_code=-1,
                    stdout_tail="",
                    stderr_tail=(
                        f"unsafe prefix slug {slug!r}; refusing to verify"
                    ),
                ))
                continue
            task_dir = root / slug
            task_dir.mkdir(parents=True, exist_ok=True)
            try:
                await object_store.download_prefix(
                    bucket=bucket, prefix=prefix, out_dir=task_dir,
                )
                cfg = TaskConfig.model_validate(
                    tomllib.loads((task_dir / "task.toml").read_text()),
                )
                image = cfg.environment.docker_image or "python:3.11-slim"
                result = await run_oracle_for_task(
                    task_id=cfg.task.id, task_dir=task_dir, image=image,
                )
            except Exception as exc:
                # Per-task fail-soft: capture and keep going so the
                # report shows every failure, not just the first.
                results.append(OracleResult(
                    task_id=prefix.rstrip("/"),
                    passed=False, return_code=-1,
                    stdout_tail="",
                    stderr_tail=f"verify pipeline error: {exc!r}"[-500:],
                ))
            else:
                results.append(result)

    passed = sum(1 for r in results if r.passed)
    return {
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "results": [asdict(r) for r in results],
    }
