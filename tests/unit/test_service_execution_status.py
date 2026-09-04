from __future__ import annotations

import pytest

from loom_service.service_execution_status import service_execution_lifecycle_stage


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, "queued"),
        ({"observed_state": "creating"}, "provisioning"),
        ({"observed_state": "running"}, "running"),
        ({"output_commit_state": "uploading"}, "verifying"),
        ({"materialization_state": "pending"}, "materializing"),
        ({"trial_state": "succeeded"}, "succeeded"),
        ({"trial_state": "failed"}, "failed"),
        ({"trial_state": "cancelled"}, "cancelled"),
        ({"error_code": "unschedulable"}, "admission_blocked"),
        ({"materialization_state": "unavailable"}, "output_unavailable"),
    ],
)
def test_service_execution_lifecycle_stage(
    overrides: dict[str, str],
    expected: str,
) -> None:
    values: dict[str, str | None] = {
        "trial_state": "running",
        "observed_state": "reserved",
        "output_commit_state": "not_started",
        "materialization_state": "not_started",
        "error_code": None,
        "error_class": None,
    }
    values.update(overrides)
    assert service_execution_lifecycle_stage(**values) == expected  # type: ignore[arg-type]
