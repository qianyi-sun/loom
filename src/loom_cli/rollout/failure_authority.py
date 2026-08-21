"""Normalized rollout failure classification and coverage-defect evidence.

The protected driver runs only after a deep-preflight attestation exists.  A
predicate that fails there must therefore identify the checked-in invariant it
belongs to and whether it escaped the invariant's earliest declared stage.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from loom_cli.rollout.preflight_contract import StageCapability
from loom_cli.rollout.preflight_coverage import load_coverage_manifest

_ID_RE = re.compile(r"^[a-z][a-z0-9.-]{2,95}$")

# Every legacy driver step is classified while the brokered final-only sequence
# is migrated to direct Check consumers.  Early-stage entries deliberately
# become coverage defects if the protected driver discovers them.
STEP_CHECK_IDS = MappingProxyType(
    {
        "resolve-target": "candidate.identity",
        "worktree": "candidate.identity",
        "build-images": "images.build",
        "cluster-target": "kubernetes.client",
        "publish-images": "images.contract",
        "backup": "backup.lease-eligibility",
        "audit": "runner.install",
        "render": "manifests.render",
        "preflight": "staging.release-baseline",
        "migrate": "final.protected-apply",
        "cluster-up": "final.protected-apply",
        "env-state": "final.convergence",
        "gb10-prep": "final.protected-apply",
        "production-defaults": "final.protected-apply",
        "release-gate": "final.drift",
        "smoke": "final.smoke",
        "staging-admin-browser-acceptance": "final.browser",
        "summary": "final.summary",
    }
)


@dataclass(frozen=True, slots=True)
class RolloutFailureEvidence:
    """Secret-free terminal classification for one protected driver failure."""

    schema_version: int
    check_id: str
    failure_code: str
    declared_stage: StageCapability
    discovered_stage: StageCapability
    declared_tier: int
    discovered_tier: int
    step_number: int
    step_name: str
    coverage_defect: bool
    reason: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or _ID_RE.fullmatch(self.check_id) is None
            or _ID_RE.fullmatch(self.failure_code) is None
            or self.declared_tier not in range(5)
            or self.discovered_tier not in range(5)
            or self.step_number < 0
            or not self.step_name
            or self.step_name != self.step_name.strip()
            or len(self.step_name) > 128
            or not self.reason
            or self.reason != self.reason.strip()
            or len(self.reason) > 512
            or self.coverage_defect != (self.discovered_tier > self.declared_tier)
        ):
            raise ValueError("rollout failure evidence is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "check_id": self.check_id,
            "coverage_defect": self.coverage_defect,
            "declared_stage": self.declared_stage.value,
            "declared_tier": self.declared_tier,
            "discovered_stage": self.discovered_stage.value,
            "discovered_tier": self.discovered_tier,
            "failure_code": self.failure_code,
            "reason": self.reason,
            "schema_version": self.schema_version,
            "step_name": self.step_name,
            "step_number": self.step_number,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> RolloutFailureEvidence:
        expected = {
            "check_id",
            "coverage_defect",
            "declared_stage",
            "declared_tier",
            "discovered_stage",
            "discovered_tier",
            "failure_code",
            "reason",
            "schema_version",
            "step_name",
            "step_number",
        }
        if set(data) != expected:
            raise ValueError("rollout failure evidence fields are invalid")
        strings = (
            data["check_id"],
            data["failure_code"],
            data["step_name"],
            data["reason"],
        )
        if (
            not all(isinstance(value, str) for value in strings)
            or type(data["schema_version"]) is not int
            or type(data["declared_tier"]) is not int
            or type(data["discovered_tier"]) is not int
            or type(data["step_number"]) is not int
            or type(data["coverage_defect"]) is not bool
        ):
            raise ValueError("rollout failure evidence types are invalid")
        try:
            declared_stage = StageCapability(str(data["declared_stage"]))
            discovered_stage = StageCapability(str(data["discovered_stage"]))
        except ValueError as exc:
            raise ValueError("rollout failure evidence stage is invalid") from exc
        return cls(
            schema_version=data["schema_version"],
            check_id=str(data["check_id"]),
            failure_code=str(data["failure_code"]),
            declared_stage=declared_stage,
            discovered_stage=discovered_stage,
            declared_tier=data["declared_tier"],
            discovered_tier=data["discovered_tier"],
            step_number=data["step_number"],
            step_name=str(data["step_name"]),
            coverage_defect=data["coverage_defect"],
            reason=str(data["reason"]),
        )


def classify_rollout_failure(
    *,
    step_number: int,
    step_name: str,
    reason: str,
    discovered_stage: StageCapability = StageCapability.FINAL_ONLY,
) -> RolloutFailureEvidence:
    """Bind a driver failure to its single checked-in coverage invariant."""
    try:
        check_id = STEP_CHECK_IDS[step_name]
    except KeyError as exc:
        raise ValueError("rollout step is absent from failure coverage") from exc
    by_id = {entry.check_id: entry for entry in load_coverage_manifest().checks}
    try:
        entry = by_id[check_id]
    except KeyError as exc:  # pragma: no cover - validated by repository contract tests
        raise ValueError("rollout failure check is absent from coverage manifest") from exc
    discovered_tier = {
        StageCapability.STATIC: 1,
        StageCapability.BASELINE_LIVE_READONLY: 2,
        StageCapability.ISOLATED_REHEARSAL: 3,
        StageCapability.FINAL_ONLY: 4,
    }[discovered_stage]
    return RolloutFailureEvidence(
        schema_version=1,
        check_id=entry.check_id,
        failure_code=entry.failure_code,
        declared_stage=entry.stage,
        discovered_stage=discovered_stage,
        declared_tier=entry.tier,
        discovered_tier=discovered_tier,
        step_number=step_number,
        step_name=step_name,
        coverage_defect=discovered_tier > entry.tier,
        reason=reason.strip()[:512],
    )


__all__ = [
    "STEP_CHECK_IDS",
    "RolloutFailureEvidence",
    "classify_rollout_failure",
]
