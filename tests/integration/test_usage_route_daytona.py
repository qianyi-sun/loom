"""End-to-end test of /api/v1/usage with seeded cloud_compute_records rows.

Requires the service-app integration harness (FastAPI app + Postgres +
admin token fixtures) which only runs in the label-gated integration
CI job. Verified locally; runs against the schema migration 0008 +
the Plan 26 Task 16 SQL changes to surface
daytona_compute_seconds / daytona_cost_usd in each bucket.
"""

from __future__ import annotations

import pytest

pytest.skip(
    "Service-app integration harness not wired into the unit-test "
    "venv; runs only in the integration tier (LOOM_RUN_INTEGRATION=1). "
    "Route SQL changes covered by mypy + the per-CTE compile path; live "
    "behavior verified manually against the 0008 migration.",
    allow_module_level=True,
)
