from __future__ import annotations

import pytest

from loom_cli.rollout.failure_authority import (
    STEP_CHECK_IDS,
    classify_rollout_failure,
)
from loom_cli.rollout.preflight_contract import StageCapability
from loom_cli.rollout.preflight_coverage import load_coverage_manifest
from loom_cli.rollout.steps import default_step_sequence


def test_every_rollout_step_has_checked_in_failure_coverage() -> None:
    steps = default_step_sequence()
    assert {step.name for step in steps} == set(STEP_CHECK_IDS)
    coverage_ids = {entry.check_id for entry in load_coverage_manifest().checks}
    assert set(STEP_CHECK_IDS.values()) <= coverage_ids


def test_late_early_predicate_is_normalized_as_coverage_defect() -> None:
    failure = classify_rollout_failure(
        step_number=2,
        step_name="build-images",
        reason="immutable image unexpectedly missing",
    )

    assert failure.check_id == "images.build"
    assert failure.failure_code == "images.build.failed"
    assert failure.declared_stage is StageCapability.STATIC
    assert failure.discovered_stage is StageCapability.FINAL_ONLY
    assert failure.declared_tier == 1
    assert failure.discovered_tier == 4
    assert failure.coverage_defect


def test_justified_final_only_failure_is_not_coverage_defect() -> None:
    failure = classify_rollout_failure(
        step_number=16,
        step_name="staging-admin-browser-acceptance",
        reason="canonical protected route did not converge",
    )

    assert failure.check_id == "final.browser"
    assert failure.failure_code == "final.browser.failed"
    assert not failure.coverage_defect


def test_unclassified_rollout_step_fails_closed() -> None:
    with pytest.raises(ValueError, match="absent from failure coverage"):
        classify_rollout_failure(
            step_number=18,
            step_name="mystery-step",
            reason="unknown predicate",
        )
