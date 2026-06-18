"""Workspace materialization — upload the task bundle into the sandbox.

The worker materializes `bundle["source"]` into a host-side `task_dir`
that contains everything the bundle ships: `instruction.md`,
`solution/`, `tests/`, `task.toml`, etc. Before #186 the trial loop
left that directory on the host — only the OracleAgent uploaded its
own `solve.sh`, and the verifier path was effectively dead for any
benchmark whose grading tests live in the bundle (HumanEval,
SWE-Bench).

This module ports the `docker cp task_dir → /workspace` step that
`loom_benchmark_tool.oracle_runner` has used for the verify CLI into
the production trial path, using the Driver protocol's single-file
`upload()` (no Protocol change needed).
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from loom.driver.base import Driver

# Files the worker considers metadata, not workspace content. `task.toml`
# is read on the host before the trial starts; uploading it would just
# clutter the sandbox. Anything else in the bundle (instructions,
# solution scaffolding, tests, fixtures) goes in.
_SKIP_NAMES = frozenset({"task.toml"})


async def materialize_workspace(
    *, driver: Driver, task_dir: Path, dst: PurePosixPath,
) -> int:
    """Recursively upload every regular file under `task_dir` to `dst`
    inside the sandbox, preserving the relative path layout. Returns
    the number of files uploaded.

    Empty directories are not created — the driver's `upload()` calls
    `mkdir -p` on each file's parent before writing, which is the only
    way a directory becomes observable inside the container anyway.
    """
    if not task_dir.is_dir():
        return 0
    count = 0
    for src in sorted(task_dir.rglob("*")):
        if not src.is_file():
            continue
        rel = src.relative_to(task_dir)
        if rel.parts and rel.parts[0] in _SKIP_NAMES:
            continue
        target = dst / PurePosixPath(*rel.parts)
        await driver.upload(src, target)
        count += 1
    return count
