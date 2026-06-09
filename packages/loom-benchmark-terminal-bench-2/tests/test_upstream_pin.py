"""Pin guard — UPSTREAM_REVISION must match the SHA pinned literally
in this test. Silent drift in upstream.py fails the assertion below.
"""

from __future__ import annotations

from loom_benchmark_terminal_bench_2 import UPSTREAM_REVISION

EXPECTED_SHA = "91e10457b5410f16c44364da1a34cb6de8c488a5"


def test_upstream_revision_is_pinned() -> None:
    assert UPSTREAM_REVISION == EXPECTED_SHA, (
        "UPSTREAM_REVISION drifted; if intentional, bump the probe notes too."
    )


def test_upstream_revision_is_full_sha() -> None:
    assert len(UPSTREAM_REVISION) == 40
    assert all(c in "0123456789abcdef" for c in UPSTREAM_REVISION)
