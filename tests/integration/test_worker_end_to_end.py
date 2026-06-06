"""Worker E2E placeholder.

A full worker → Control Plane → Gateway → MinIO end-to-end test requires
a multi-service compose with a seeded Task body — deferred to Plan 7's
system test suite (`docs/superpowers/plans/2026-06-05-loom-system-e2e.md`).
"""

import pytest


def test_worker_end_to_end_smoke() -> None:
    pytest.skip(
        "Full worker E2E requires multi-service compose with a seeded Task "
        "body; covered by Plan 7's system test suite.",
    )
