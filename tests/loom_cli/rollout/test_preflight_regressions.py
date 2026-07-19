from __future__ import annotations

import pytest

from loom_cli.rollout.preflight_regressions import (
    DEFAULT_REGRESSION_MANIFEST,
    is_preflight_coverage_defect,
    load_regression_manifest,
)


def test_historical_blockers_are_checked_in_and_earliest_stage_classified() -> None:
    manifest = load_regression_manifest()
    assert DEFAULT_REGRESSION_MANIFEST.is_file()
    assert {fixture.fixture_id for fixture in manifest.fixtures} == {
        "browser-token-authority-mismatch",
        "gb10-timer-transient-state",
        "gb10-candidate-source-drift",
        "systemd-user-manager-latency",
        "backup-object-inode-growth",
        "release-baseline-drift",
        "candidate-api-smoke-binding",
        "candidate-browser-binding",
    }


def test_late_discovery_is_a_coverage_defect_and_unknown_is_rejected() -> None:
    assert is_preflight_coverage_defect(check_id="browser.runtime", discovered_tier=4)
    assert not is_preflight_coverage_defect(check_id="browser.runtime", discovered_tier=1)
    with pytest.raises(ValueError, match="absent from preflight coverage"):
        is_preflight_coverage_defect(check_id="unknown.failure", discovered_tier=4)
