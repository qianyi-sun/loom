from __future__ import annotations

import pytest

from loom_task_image_authority.publication_worker import PublicationWorkerLimits


@pytest.mark.parametrize(
    "values",
    [
        {"maximum_jobs": True},
        {"maximum_jobs": 0},
        {"maximum_jobs": 33},
        {"lease_seconds": True},
        {"lease_seconds": float("nan")},
        {"lease_seconds": float("inf")},
        {"lease_seconds": 0},
        {"lease_seconds": 0.000001},
        {"lease_seconds": 301},
        {"renewal_interval_seconds": 30},
        {"renewal_interval_seconds": 0},
        {"retry_delay_seconds": 301},
        {"signer_timeout_seconds": 11},
        {"database_timeout_seconds": 31},
    ],
)
def test_worker_rejects_unbounded_or_unsafe_admission_and_timing(values):
    with pytest.raises(ValueError):
        PublicationWorkerLimits(**values)


def test_worker_limits_allow_bounded_fast_renewal():
    limits = PublicationWorkerLimits(
        maximum_jobs=1, lease_seconds=0.5, renewal_interval_seconds=0.1
    )
    assert limits.renewal_interval_seconds < limits.lease_seconds / 2
