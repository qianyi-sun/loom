"""Checked-in historical blocker fixtures and late-discovery policy."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.preflight_coverage import CoverageManifest, load_coverage_manifest

DEFAULT_REGRESSION_MANIFEST = (
    Path(__file__).resolve().parents[3] / "config/staging-rollout-preflight-regressions.json"
)
_ID_RE = re.compile(r"^[a-z][a-z0-9.-]{2,95}$")


@dataclass(frozen=True, slots=True)
class HistoricalBlockerFixture:
    fixture_id: str
    check_id: str
    expected_failure_code: str
    declared_tier: int
    historical_discovered_tier: int
    fault: str

    @classmethod
    def from_dict(cls, value: object) -> HistoricalBlockerFixture:
        expected = {
            "fixture_id",
            "check_id",
            "expected_failure_code",
            "declared_tier",
            "historical_discovered_tier",
            "fault",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("preflight regression fixture fields are invalid")
        strings = {
            key: value[key]
            for key in ("fixture_id", "check_id", "expected_failure_code", "fault")
        }
        if not all(isinstance(item, str) for item in strings.values()):
            raise ValueError("preflight regression fixture strings are invalid")
        fixture_id = str(strings["fixture_id"])
        check_id = str(strings["check_id"])
        failure_code = str(strings["expected_failure_code"])
        fault = str(strings["fault"])
        declared = value["declared_tier"]
        historical = value["historical_discovered_tier"]
        if (
            _ID_RE.fullmatch(fixture_id) is None
            or _ID_RE.fullmatch(check_id) is None
            or _ID_RE.fullmatch(failure_code) is None
            or type(declared) is not int
            or declared not in range(5)
            or type(historical) is not int
            or historical not in range(5)
            or historical < declared
            or len(fault.strip()) < 30
        ):
            raise ValueError("preflight regression fixture contract is invalid")
        return cls(
            fixture_id=fixture_id,
            check_id=check_id,
            expected_failure_code=failure_code,
            declared_tier=declared,
            historical_discovered_tier=historical,
            fault=fault,
        )


@dataclass(frozen=True, slots=True)
class RegressionManifest:
    schema_version: int
    fixtures: tuple[HistoricalBlockerFixture, ...]

    def __post_init__(self) -> None:
        if self.schema_version != 1 or not self.fixtures:
            raise ValueError("preflight regression manifest schema is invalid")
        if len({fixture.fixture_id for fixture in self.fixtures}) != len(self.fixtures):
            raise ValueError("preflight regression fixtures contain duplicate identities")

    def validate_coverage(self, coverage: CoverageManifest) -> None:
        by_id = {entry.check_id: entry for entry in coverage.checks}
        for fixture in self.fixtures:
            entry = by_id.get(fixture.check_id)
            if entry is None:
                raise ValueError(f"regression fixture {fixture.fixture_id} is unclassified")
            if (
                entry.failure_code != fixture.expected_failure_code
                or entry.tier != fixture.declared_tier
            ):
                raise ValueError(f"regression fixture {fixture.fixture_id} coverage drifted")


def load_regression_manifest(
    path: Path = DEFAULT_REGRESSION_MANIFEST,
) -> RegressionManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("preflight regression manifest is unreadable") from exc
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "fixtures"}:
        raise ValueError("preflight regression manifest root is invalid")
    fixtures = payload["fixtures"]
    if not isinstance(fixtures, list):
        raise ValueError("preflight regression fixtures must be a list")
    manifest = RegressionManifest(
        schema_version=payload["schema_version"],
        fixtures=tuple(HistoricalBlockerFixture.from_dict(value) for value in fixtures),
    )
    manifest.validate_coverage(load_coverage_manifest())
    return manifest


def is_preflight_coverage_defect(*, check_id: str, discovered_tier: int) -> bool:
    """Return true when a normalized failure escaped its earliest declared tier."""
    if discovered_tier not in range(5):
        raise ValueError("failure discovered tier is invalid")
    by_id = {entry.check_id: entry for entry in load_coverage_manifest().checks}
    try:
        declared_tier = by_id[check_id].tier
    except KeyError as exc:
        raise ValueError("failure check is absent from preflight coverage") from exc
    return discovered_tier > declared_tier


__all__ = [
    "DEFAULT_REGRESSION_MANIFEST",
    "HistoricalBlockerFixture",
    "RegressionManifest",
    "is_preflight_coverage_defect",
    "load_regression_manifest",
]
