"""Checked-in historical blocker fixtures and late-discovery policy."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    PreflightDag,
    RegisteredCheck,
)
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


@dataclass(frozen=True, slots=True)
class RegressionReplayCase:
    """One injected historical fault against its production check implementation."""

    fixture_id: str
    check: RegisteredCheck
    context: CheckContext


@dataclass(frozen=True, slots=True)
class RegressionReplayEvidence:
    """Normalized proof that every historical fault fails at its declared tier."""

    implementation_digests: Mapping[str, str]
    evidence_hashes: Mapping[str, str]

    def __post_init__(self) -> None:
        implementations = dict(self.implementation_digests)
        evidence = dict(self.evidence_hashes)
        if (
            not implementations
            or set(implementations) != set(evidence)
            or any(_SHA256_RE.fullmatch(value) is None for value in (*implementations.values(), *evidence.values()))
        ):
            raise ValueError("preflight regression replay evidence is invalid")
        object.__setattr__(self, "implementation_digests", MappingProxyType(implementations))
        object.__setattr__(self, "evidence_hashes", MappingProxyType(evidence))


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def replay_regression_manifest(
    cases: Sequence[RegressionReplayCase],
    *,
    manifest: RegressionManifest | None = None,
) -> RegressionReplayEvidence:
    """Execute every injected fault through its real registered probe, fail closed."""
    expected = manifest or load_regression_manifest()
    by_fixture = {case.fixture_id: case for case in cases}
    fixtures = {fixture.fixture_id: fixture for fixture in expected.fixtures}
    if len(by_fixture) != len(cases) or set(by_fixture) != set(fixtures):
        raise ValueError("preflight regression replay coverage is incomplete")
    implementations: dict[str, str] = {}
    evidence_hashes: dict[str, str] = {}
    for fixture_id in sorted(fixtures):
        fixture = fixtures[fixture_id]
        case = by_fixture[fixture_id]
        check = case.check
        if (
            check.spec.check_id != fixture.check_id
            or check.spec.failure_code != fixture.expected_failure_code
            or check.spec.tier != fixture.declared_tier
        ):
            raise ValueError(f"preflight regression replay contract drifted for {fixture_id}")
        isolated = replace(
            check,
            spec=replace(
                check.spec,
                dependencies=(),
                run_after_failed_dependencies=False,
            ),
        )
        execution = PreflightDag((isolated,), max_concurrency=1).run(
            case.context,
            operation=CheckOperation.PROBE,
            through_tier=fixture.declared_tier,
            now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
        )[0]
        if execution.passed or execution.failure_code != fixture.expected_failure_code:
            raise ValueError(f"historical blocker {fixture_id} no longer fails at preflight")
        implementations[fixture_id] = check.implementation_digest
        evidence_hashes[fixture_id] = execution.evidence_hash
    return RegressionReplayEvidence(implementations, evidence_hashes)


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
    "RegressionReplayCase",
    "RegressionReplayEvidence",
    "is_preflight_coverage_defect",
    "load_regression_manifest",
    "replay_regression_manifest",
]
