"""Checked-in earliest-stage coverage for every rollout predicate."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from loom_cli.rollout.preflight_contract import (
    MutationClass,
    RegisteredCheck,
    StageCapability,
)

DEFAULT_COVERAGE_MANIFEST = (
    Path(__file__).resolve().parents[3] / "config/staging-rollout-preflight-coverage.json"
)
_ID_RE = re.compile(r"^[a-z][a-z0-9.-]{2,95}$")


@dataclass(frozen=True, slots=True)
class FinalPredicateCoverageEntry:
    """One concrete late predicate and its earliest executable authority."""

    predicate_id: str
    final_check_id: str
    earliest_check_id: str
    preflight_capable: bool
    predicate: str
    final_only_justification: str | None

    @classmethod
    def from_dict(cls, value: object) -> FinalPredicateCoverageEntry:
        if not isinstance(value, dict):
            raise ValueError("final predicate coverage entry must be an object")
        expected = {
            "earliest_check_id",
            "final_check_id",
            "predicate",
            "predicate_id",
            "preflight_capable",
        }
        allowed = expected | {"final_only_justification"}
        if not expected <= value.keys() or not value.keys() <= allowed:
            raise ValueError("final predicate coverage fields are incomplete or unknown")
        strings = {
            name: value[name]
            for name in ("earliest_check_id", "final_check_id", "predicate", "predicate_id")
        }
        if not all(isinstance(item, str) for item in strings.values()):
            raise ValueError("final predicate coverage identities must be strings")
        if (
            _ID_RE.fullmatch(str(strings["predicate_id"])) is None
            or _ID_RE.fullmatch(str(strings["final_check_id"])) is None
            or _ID_RE.fullmatch(str(strings["earliest_check_id"])) is None
            or len(str(strings["predicate"]).strip()) < 20
            or type(value["preflight_capable"]) is not bool
        ):
            raise ValueError("final predicate coverage entry is invalid")
        justification = value.get("final_only_justification")
        if justification is not None and not isinstance(justification, str):
            raise ValueError("final predicate justification must be a string")
        if value["preflight_capable"] is True:
            if justification is not None:
                raise ValueError("preflight-capable final predicate cannot be justified late")
        elif justification is None or len(justification.strip()) < 20:
            raise ValueError("final-only subpredicate requires a technical justification")
        return cls(
            predicate_id=str(strings["predicate_id"]),
            final_check_id=str(strings["final_check_id"]),
            earliest_check_id=str(strings["earliest_check_id"]),
            preflight_capable=value["preflight_capable"],
            predicate=str(strings["predicate"]),
            final_only_justification=justification,
        )


@dataclass(frozen=True, slots=True)
class CoverageEntry:
    check_id: str
    failure_code: str
    tier: int
    stage: StageCapability
    dependencies: tuple[str, ...]
    mutation_class: MutationClass
    consumers: tuple[str, ...]
    legacy_checks: tuple[str, ...]
    predicate: str
    final_only_justification: str | None

    @classmethod
    def from_dict(cls, value: object) -> CoverageEntry:
        if not isinstance(value, dict):
            raise ValueError("coverage entry must be an object")
        expected = {
            "check_id",
            "failure_code",
            "tier",
            "stage",
            "dependencies",
            "mutation_class",
            "consumers",
            "legacy_checks",
            "predicate",
        }
        allowed = expected | {"final_only_justification"}
        if not expected <= value.keys() or not value.keys() <= allowed:
            raise ValueError("coverage entry fields are incomplete or unknown")
        sequences: dict[str, tuple[str, ...]] = {}
        for name in ("dependencies", "consumers", "legacy_checks"):
            raw = value[name]
            if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
                raise ValueError(f"coverage {name} must be a string list")
            sequences[name] = tuple(raw)
        if not sequences["consumers"]:
            raise ValueError("coverage entry must name at least one consumer")
        if len(set(sequences["consumers"])) != len(sequences["consumers"]):
            raise ValueError("coverage consumers contain duplicates")
        predicate = value["predicate"]
        if not isinstance(predicate, str) or len(predicate.strip()) < 20:
            raise ValueError("coverage predicate must be explicit")
        justification = value.get("final_only_justification")
        if justification is not None and not isinstance(justification, str):
            raise ValueError("final-only justification must be a string")
        try:
            stage = StageCapability(value["stage"])
            mutation_class = MutationClass(value["mutation_class"])
        except (TypeError, ValueError) as exc:
            raise ValueError("coverage stage or mutation class is invalid") from exc
        tier = value["tier"]
        if type(tier) is not int:
            raise ValueError("coverage tier must be an integer")
        if stage is StageCapability.FINAL_ONLY:
            if tier != 4 or justification is None or len(justification.strip()) < 20:
                raise ValueError("final-only coverage requires a technical justification")
        elif justification is not None:
            raise ValueError("non-final coverage cannot carry final-only justification")
        check_id = value["check_id"]
        failure_code = value["failure_code"]
        if not isinstance(check_id, str) or not isinstance(failure_code, str):
            raise ValueError("coverage identities must be strings")
        return cls(
            check_id=check_id,
            failure_code=failure_code,
            tier=tier,
            stage=stage,
            dependencies=sequences["dependencies"],
            mutation_class=mutation_class,
            consumers=sequences["consumers"],
            legacy_checks=sequences["legacy_checks"],
            predicate=predicate,
            final_only_justification=justification,
        )


@dataclass(frozen=True, slots=True)
class CoverageManifest:
    schema_version: int
    checks: tuple[CoverageEntry, ...]
    final_predicates: tuple[FinalPredicateCoverageEntry, ...]

    def __post_init__(self) -> None:
        if self.schema_version != 1 or not self.checks or not self.final_predicates:
            raise ValueError("coverage manifest schema is invalid")
        ids = {entry.check_id for entry in self.checks}
        if len(ids) != len(self.checks):
            raise ValueError("coverage manifest contains duplicate check ids")
        if len({entry.failure_code for entry in self.checks}) != len(self.checks):
            raise ValueError("coverage manifest contains duplicate failure codes")
        predicate_ids = {entry.predicate_id for entry in self.final_predicates}
        if len(predicate_ids) != len(self.final_predicates):
            raise ValueError("coverage manifest contains duplicate final predicate ids")
        for entry in self.checks:
            missing = set(entry.dependencies) - ids
            if missing:
                raise ValueError(
                    f"coverage entry {entry.check_id} has unknown dependencies: {sorted(missing)}"
                )
            for dependency in entry.dependencies:
                dependency_tier = next(
                    item.tier for item in self.checks if item.check_id == dependency
                )
                if dependency_tier > entry.tier:
                    raise ValueError("coverage dependency points to a later tier")
        pending = {entry.check_id: set(entry.dependencies) for entry in self.checks}
        completed: set[str] = set()
        while pending:
            ready = {name for name, dependencies in pending.items() if dependencies <= completed}
            if not ready:
                raise ValueError("coverage manifest contains a dependency cycle")
            completed.update(ready)
            for name in ready:
                pending.pop(name)
        final_ids = {
            entry.check_id for entry in self.checks if entry.stage is StageCapability.FINAL_ONLY
        }
        covered_final_ids = {entry.final_check_id for entry in self.final_predicates}
        if covered_final_ids != final_ids:
            raise ValueError("coverage manifest final predicate groups are incomplete")
        by_id = {entry.check_id: entry for entry in self.checks}
        for predicate in self.final_predicates:
            earliest = by_id.get(predicate.earliest_check_id)
            if predicate.final_check_id not in final_ids or earliest is None:
                raise ValueError("final predicate coverage references an unknown check")
            if predicate.preflight_capable:
                if earliest.tier >= 4:
                    raise ValueError("preflight-capable final predicate is classified too late")
            elif predicate.earliest_check_id != predicate.final_check_id:
                raise ValueError("final-only subpredicate must bind its consuming final check")

    @property
    def consumers(self) -> frozenset[str]:
        return frozenset(consumer for entry in self.checks for consumer in entry.consumers)

    @property
    def legacy_checks(self) -> frozenset[str]:
        return frozenset(name for entry in self.checks for name in entry.legacy_checks)

    def require_exact_registry(
        self,
        checks: Sequence[RegisteredCheck],
        *,
        through_tier: int,
    ) -> None:
        """Fail closed unless every declared check through the tier is implemented once."""
        if through_tier not in {0, 1, 2, 3, 4}:
            raise ValueError("coverage registry tier must be in [0, 4]")
        expected = {entry.check_id: entry for entry in self.checks if entry.tier <= through_tier}
        registered = {check.spec.check_id: check for check in checks}
        if len(registered) != len(checks):
            raise ValueError("preflight registry contains duplicate check ids")
        if set(registered) != set(expected):
            missing = sorted(set(expected) - set(registered))
            unexpected = sorted(set(registered) - set(expected))
            raise ValueError(
                "preflight registry does not match coverage manifest: "
                f"missing={missing} unexpected={unexpected}"
            )
        for check_id, entry in expected.items():
            spec = registered[check_id].spec
            if (
                spec.failure_code != entry.failure_code
                or spec.tier != entry.tier
                or spec.stage is not entry.stage
                or spec.dependencies != entry.dependencies
                or spec.mutation_class is not entry.mutation_class
                or spec.final_only_justification != entry.final_only_justification
            ):
                raise ValueError(f"preflight registry contract drifts from coverage for {check_id}")

    def require_exact_tier(
        self,
        checks: Sequence[RegisteredCheck],
        *,
        tier: int,
    ) -> None:
        """Validate one separately executed tier against the same manifest."""
        if tier not in range(5):
            raise ValueError("coverage registry tier must be in [0, 4]")
        expected = {entry.check_id: entry for entry in self.checks if entry.tier == tier}
        registered = {check.spec.check_id: check for check in checks}
        if len(registered) != len(checks) or set(registered) != set(expected):
            raise ValueError("separate preflight tier does not match coverage manifest")
        for check_id, entry in expected.items():
            spec = registered[check_id].spec
            if (
                spec.failure_code != entry.failure_code
                or spec.tier != entry.tier
                or spec.stage is not entry.stage
                or spec.dependencies != entry.dependencies
                or spec.mutation_class is not entry.mutation_class
                or spec.final_only_justification != entry.final_only_justification
            ):
                raise ValueError(f"preflight tier contract drifts from coverage for {check_id}")


def load_coverage_manifest(path: Path = DEFAULT_COVERAGE_MANIFEST) -> CoverageManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("preflight coverage manifest is unreadable") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "checks",
        "final_predicates",
    }:
        raise ValueError("preflight coverage manifest root is invalid")
    checks = payload["checks"]
    final_predicates = payload["final_predicates"]
    if not isinstance(checks, list) or not isinstance(final_predicates, list):
        raise ValueError("preflight coverage checks must be a list")
    return CoverageManifest(
        schema_version=payload["schema_version"],
        checks=tuple(CoverageEntry.from_dict(value) for value in checks),
        final_predicates=tuple(
            FinalPredicateCoverageEntry.from_dict(value) for value in final_predicates
        ),
    )


__all__ = [
    "DEFAULT_COVERAGE_MANIFEST",
    "CoverageEntry",
    "CoverageManifest",
    "FinalPredicateCoverageEntry",
    "load_coverage_manifest",
]
