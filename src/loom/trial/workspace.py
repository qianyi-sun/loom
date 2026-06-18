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

import asyncio
from pathlib import Path, PurePosixPath

from loom.driver.base import Driver

# Path components the worker excludes from the upload set.
#
# `task.toml` is host-only metadata (read before the trial starts).
# The rest are common dev-cruft that hand-authored or git-cloned
# bundles can carry — uploading them is wasteful and, in a few cases
# (.git/), can leak the bundle's full history into the sandbox where
# the agent could exfiltrate it via the LLM. Match is on the FIRST
# component of the relative path; nested matches require the same
# name (e.g., src/__pycache__ is skipped because rglob walks into it,
# but its files have `.parts == ("src", "__pycache__", "x.pyc")` so
# the per-file check below specifically also looks at any segment).
_SKIP_NAMES = frozenset({
    "task.toml",
    ".git",
    "__pycache__",
    "node_modules",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".DS_Store",
})

# Suffix-match (case-sensitive) for individual files to skip. Cheaper
# than a full glob and covers the common compiled-Python case.
_SKIP_SUFFIXES = frozenset({".pyc", ".pyo"})

# Cap concurrent uploads so a multi-hundred-file bundle (SWE-Bench)
# doesn't open hundreds of simultaneous docker HTTP connections. 16
# is comfortably under docker's default 100-req cap with headroom for
# other in-flight calls (exec, status, etc.).
_MAX_PARALLEL_UPLOADS = 16


async def materialize_workspace(
    *, driver: Driver, task_dir: Path, dst: PurePosixPath,
) -> int:
    """Recursively upload every regular file under `task_dir` to `dst`
    inside the sandbox, preserving the relative path layout. Returns
    the number of files uploaded.

    Empty directories are not created — the driver's `upload()` calls
    `mkdir -p` on each file's parent before writing, which is the only
    way a directory becomes observable inside the container anyway.

    Uploads run in parallel up to `_MAX_PARALLEL_UPLOADS` to keep
    SWE-Bench-sized bundles (~100s of files) under a few seconds
    instead of one-per-file serial round-trips.
    """
    if not task_dir.is_dir():
        return 0
    targets: list[tuple[Path, PurePosixPath]] = []
    for src in sorted(task_dir.rglob("*")):
        if not src.is_file():
            continue
        rel = src.relative_to(task_dir)
        # Skip if ANY path segment matches a skip name (catches both
        # top-level `.git/` AND nested `src/__pycache__/`).
        if any(part in _SKIP_NAMES for part in rel.parts):
            continue
        if src.suffix in _SKIP_SUFFIXES:
            continue
        targets.append((src, dst / PurePosixPath(*rel.parts)))
    if not targets:
        return 0
    semaphore = asyncio.Semaphore(_MAX_PARALLEL_UPLOADS)

    async def _upload_one(src: Path, target: PurePosixPath) -> None:
        async with semaphore:
            await driver.upload(src, target)

    await asyncio.gather(*(_upload_one(s, t) for s, t in targets))
    return len(targets)
