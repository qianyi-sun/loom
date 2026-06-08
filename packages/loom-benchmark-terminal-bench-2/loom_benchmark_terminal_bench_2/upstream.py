"""Pinned upstream constants for terminal-bench-core v0.1.1.

The SHA was probed against
https://github.com/laude-institute/terminal-bench/blob/main/registry.json
on 2026-06-08; see docs/notes/2026-06-08-tb2-upstream-probe.md.

Upgrades to a newer TB-2 dataset version are out of scope for Plan 25 and
require a follow-up plan that updates this constant AND the probe notes.
The pin-guard test in `tests/test_upstream_pin.py` enforces lockstep.
"""

from __future__ import annotations

from loom_benchmarks.base import UpstreamSource

UPSTREAM_REVISION = "91e10457b5410f16c44364da1a34cb6de8c488a5"
"""terminal-bench-core v0.1.1 commit on the
`dataset/terminal-bench-core/v0.1.x` branch."""

UPSTREAM_SOURCE = UpstreamSource(
    kind="git",
    locator="https://github.com/laude-institute/terminal-bench.git",
    revision=UPSTREAM_REVISION,
)
"""Passed to `loom_benchmarks.fetch.fetch_upstream`. The SHA pin flows
through `_looks_like_sha` and uses the `git init && git fetch <sha>` path
instead of `--branch` (raw SHAs are not valid branch refs)."""

DATASET_VERSION = "0.1.1"
"""Surfaced into TB-2 report JSON as a top-level field if Harbor's
reference shape adds one in a future schema bump."""

TASK_SUBDIR = "tasks"
"""Path relative to the repo root that holds per-task directories."""
